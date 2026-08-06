# Fund Dividend History v1

`GET /v1/funds/{symbol}/dividends` returns individual cash distributions for a fund or ETF.
It is the preferred contract for investment analysis that needs an exact date window and
auditable source documents. The older `GET /v1/dividends/{symbol}?fiscal_year=...` remains
unchanged for fiscal/payment-year assessment consumers.

## Request

```bash
curl --get 'http://127.0.0.1:8790/v1/funds/563020.XSHG/dividends' \
  --data-urlencode 'from=2025-08-06' \
  --data-urlencode 'to=2026-08-06' \
  --data-urlencode 'refresh=true'
```

- `symbol` must be a provider-neutral canonical identifier such as `563020.XSHG`.
  Legacy `563020.SH` is intentionally rejected rather than silently normalized.
- `from` and `to` are inclusive ISO dates. The filter and aggregate use `payment_date`.
- `refresh=true` refreshes missing or stale yearly evidence through MarketCow's cache policy.
  `refresh=false` is a cache-only read.
- The maximum range is ten years.

The matching MCP tool is `get_fund_dividend_history` with `symbol`, `from`, `to`, and an
optional `refresh` boolean.

## Stable response fields

The response schema is `marketcow.fund_dividend_history.v1`. Each `events[]` item contains:

- `symbol` and `instrument_id`: canonical identity;
- `fund_name`;
- `announcement_date`, `record_date`, `ex_date`, and `payment_date`;
- `amount_per_unit`, `currency`, and `dividend_type` (`cash`);
- `source_url`, `source_category`, `source_name`, and `source_document_id`;
- `observed_at`, `ingested_at`, and `provenance`, including whether the evidence is official,
  its confirmation status, raw Artifact ID, and original declared unit.

Source omissions remain JSON `null`; MarketCow does not infer dates. Decimal amounts are
serialized as strings. For a declaration such as CNY 0.120 per 10 fund units,
`amount_per_unit` is `0.012`, while provenance retains `declared_amount=0.120` and
`declared_unit_count=10`.

`aggregate.amount_per_unit_total` is populated only when all returned events share one
currency. `totals_by_currency` is always safe for deterministic calculation. The aggregate
does not expose an inferred yield: `yield` is `null`. A trailing cash-distribution yield needs
an explicit price and price observation timestamp supplied by the analysis layer. An index or
constituent dividend yield is never treated as a cash distribution paid by the fund.

## Sources, completeness, and freshness

For Shanghai-listed funds, MarketCow queries the Shanghai Stock Exchange fund disclosure
catalog (`COMMON_PL_JJXX_JJGG_NEW_L`), downloads the announcement PDF, and parses the
declared cash amount and four dates. Confirmed rows are `source_category=exchange_announcement`.
The original PDF is hash-addressed in MarketCow's local raw Artifact store.

`coverage.status` has three values:

- `complete`: all events in the requested payment window are confirmed by official sources
  and all stable fields are present;
- `incomplete`: an event uses third-party evidence or a source field is missing;
- `no_dividends`: the source/cache query completed but no event has a payment date in range.

Machine-readable `coverage.warnings[]` distinguishes `no_dividend_events`,
`third_party_evidence_present`, and `incomplete_event_fields`. `freshness.years[]` records
each payment year's cache state, refresh state, last successful refresh, and query source.
Third-party evidence is always `unverified`; it is never labeled official.

## Error contract

- HTTP 400, `invalid_request`: non-canonical/unrecognized symbol, malformed dates, reversed
  range, or a range over ten years.
- HTTP 422, `unsupported_asset_type`: the instrument is not recognized as a fund/ETF.
- HTTP 502, `provider_unavailable`: official source/network/parser failure when no usable
  cache can be returned.
- HTTP 200 with `status=no_dividends`: valid completed query with no cash distribution in
  the payment-date window.

## 563020.XSHG reference window

For `2025-08-06` through `2026-08-06`, the official SSE documents produce four events:

| Announcement | Record | Ex-date | Payment | CNY per unit |
|---|---|---|---|---:|
| 2025-09-05 | 2025-09-09 | 2025-09-10 | 2025-09-15 | 0.012 |
| 2025-12-05 | 2025-12-09 | 2025-12-10 | 2025-12-15 | 0.012 |
| 2026-03-06 | 2026-03-10 | 2026-03-11 | 2026-03-16 | 0.012 |
| 2026-06-05 | 2026-06-09 | 2026-06-10 | 2026-06-15 | 0.012 |

The deterministic date-window total is CNY `0.048` per fund unit.
