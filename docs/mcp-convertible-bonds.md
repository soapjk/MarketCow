# MarketCow Convertible-Bond MCP v1

Contract version: `marketcow.convertible-bond.v1`.

This contract is read-only from an MCP consumer's perspective. The MCP process
loads a runtime snapshot through the local MarketCow API, which records the raw
Tushare responses in MarketCow's local evidence store. No bond is compiled into
the production package. Tests inject small fixtures explicitly.

## Tools

### `search_convertible_bonds`

Input:

```json
{"query": "113052", "limit": 12}
```

`query` accepts a short name, full name, bare six-digit code, Tushare exchange
code (`113052.SH`) or MarketCow canonical ID (`113052.XSHG`). `limit` is 1–30.

Output identity fields are `bond_id`, `provider_code`, `code`, `name`,
`full_name`, `issuer`, `underlying_instrument_id`, `source`, `source_url`,
`observed_at`, `published_at`, `ingested_at`, `quality_status` and
`cache_status`. `catalog_size` reports the loaded data surface. An empty result
means only that the current provider snapshot did not match; it is not evidence
that a security does not exist.

### `get_convertible_bond`

Input:

```json
{"bond_id": "118070.XSHG", "as_of": "2026-06-18T23:59:59+08:00"}
```

`bond_id` accepts the same normalized forms as search. `as_of` is optional. The
response contains identity, `facts`, and a complete `scorer_input_map`.

Each fact has:

- `value`, `raw_value`, `status`, and `missing_reason`;
- `source`, `source_url`, `observed_at`, `published_at`, and `ingested_at`;
- `quality_status`, `cache_status`, and `point_in_time`.

Facts from `cb_issue` use their issuance or result announcement date. Facts from
`cb_basic`, including listing date, rating and conversion price, have no
field-level disclosure timestamp in that source: `published_at=null` and
`point_in_time=false`. With `as_of`, those fields are hidden as
`data_missing/point_in_time_unavailable`; they are never backdated to the
issuance announcement. Facts whose known publication date is after the cutoff
are hidden as `data_missing/not_published_as_of_cutoff`.

The schema covers:

- issue price, par, initial/latest conversion price, term and maturity;
- redemption, put and downward-revision clauses;
- normalized issuer/bond rating with the provider value retained in
  `raw_value`;
- audit opinion, overdue/default, major violation/fraud and going-concern risk;
- issue/remaining size and shareholder placement (allocated bond volume ×
  placement price ÷ issue amount);
- record, subscription, result/winning, payment and listing dates.

Unsupported risk or audit facts are `value=null,status=data_missing`; the
interface never substitutes zero, an empty string, or an inferred negative.

### `get_convertible_bond_market`

Input:

```json
{"bond_id": "113052.XSHG"}
```

The tool reads the latest common Tushare `cb_daily` date plus A-share `daily`
closes. It derives stock price and conversion value from the same-date underlying
close and latest conversion price. When the target bond is already listed, it
also reports the target's own close and conversion premium. A not-yet-listed
target can still obtain comparable statistics without inventing a target-bond
price.

Comparable selection progressively matches:

1. conversion-value band;
2. normalized rating distance;
3. issue-size ratio.

The response states the selected `comparable_tier`, exact tier rules,
`sample_bond_ids`, `comparable_sample` and `sample_size`.
`comparable_premium_median_pct` is the median conversion premium of that selected
sample and is available only with at least three complete comparables. This is
the MCP input for scorer field `expected_market_premium_pct`; the target bond's
own `conversion_premium_pct` is never used as the expectation.

All single-date ranking fields are explicitly named
`target_cross_sectional_*_percentile`. They describe where the target bond ranks
against comparable bonds or the current market on one trade date. They are not
a market valuation history.

`historical_market_valuation_percentile` is currently null with status
`data_missing` and reason
`historical_market_valuation_percentile_not_available`, because v1 does not yet
maintain a time series of market-wide aggregate convertible-bond valuation.
Accordingly, the scorer's `cb_valuation_percentile` remains missing rather than
being populated from a cross-sectional rank. YTM likewise remains explicit
`data_missing/cash_flow_engine_not_implemented`.

Example for an unlisted target:

```json
{
  "bond_quote": null,
  "conversion_premium_pct": null,
  "comparable_tier": "balanced",
  "sample_size": 4,
  "sample_bond_ids": ["A.XSHG", "B.XSHE", "C.XSHG", "D.XSHE"],
  "comparable_premium_median_pct": 18.35,
  "comparable_premium_statistic_status": "available",
  "historical_market_valuation_percentile": null,
  "historical_market_valuation_percentile_status": "data_missing",
  "historical_market_valuation_percentile_missing_reason":
    "historical_market_valuation_percentile_not_available",
  "scorer_input_map": {
    "expected_market_premium_pct": {
      "status": "available",
      "value": 18.35,
      "source_path": "comparable_premium_median_pct"
    },
    "cb_valuation_percentile": {
      "status": "data_missing",
      "value": null,
      "source_path": null,
      "missing_reason":
        "historical_market_valuation_percentile_not_available"
    }
  }
}
```

## Provider datasets and time semantics

| Dataset | Use | Provider documentation | Time semantics |
|---|---|---|---|
| `cb_basic` | identities, underlying mapping, terms, rating, sizes, listing date | <https://tushare.pro/document/2?doc_id=185> | current reference snapshot; no field publication timestamp |
| `cb_issue` | issuance announcement, result, online subscription and shareholder placement | <https://tushare.pro/document/2?doc_id=186> | `ann_date` or `res_ann_date` per fact |
| `stock_basic` | issuer legal/display name | <https://tushare.pro/document/1?doc_id=25> | current reference snapshot |
| `cb_daily` | bond daily close | <https://tushare.pro/document/2?doc_id=187> | `trade_date`; official documentation says daily data updates after market close |
| `daily` | underlying A-share close | <https://tushare.pro/document/1?doc_id=27> | `trade_date` |

`observed_at` is when the MCP snapshot was assembled. `ingested_at` is the local
ingestion observation for that snapshot. `published_at` is a disclosure/trade
date only when the underlying dataset supplies one; null does not mean “known
since issuance.” Tushare is an aggregation provider rather than a second
independent primary source, so primary-document verification remains required
for a final subscription decision.

The provider documents a 2,000-row single-call limit for each convertible-bond
dataset. MarketCow's current full snapshots are below that limit. Provider
permission, rate limits and upstream availability still apply. A load failure is
returned as `catalog_status=data_missing` plus `catalog_error`; an empty catalog
or search result is never converted into “security does not exist.” A later tool
call retries a failed load after checking `service_health`; do not retry
validation errors. The three tools advertise `openWorldHint=true` because their
local API path can read Tushare, while remaining non-destructive and read-only to
the MCP consumer.

## `cb-subscribe-v1.0.0` mapping

`get_convertible_bond.scorer_input_map` has exactly one status entry for every
required scorer field. Available facts include `value`, `raw_value` and
`source_path`; unavailable inputs include a machine-readable reason:

- `query_required`: call the named MarketCow detail, market, fundamental or
  financial-statement path;
- `scorer_judgment_required`: the deterministic scorer expects an analyst
  judgment and the MCP does not invent one;
- source-level missing reasons from the fact envelope.

Direct mappings:

| Scorer field | Source path |
|---|---|
| `bond_name`, `issuer` | `name`, `issuer` |
| `issue_price`, `conversion_price`, `credit_rating` | detail facts |
| `stock_price` | market `stock_quote.price` |
| `expected_market_premium_pct` | market `comparable_premium_median_pct`, using at least three selected comparables |
| `issue_size_billion`, `shareholder_placement_pct` | detail facts |
| `cb_valuation_percentile` | explicit `data_missing/historical_market_valuation_percentile_not_available` until a market-wide valuation history exists |
| `days_to_listing_estimate` | caller computes from assessment date and verified listing date |
| audit and four veto-risk fields | detail facts where supported, otherwise explicit missing |
| cash flow, core profit and cash/debt coverage | `get_financial_statements` |
| quality, outlook, valuation, relevance, confidence, attractiveness, scarcity and market regime | scorer judgment |

Ratings are normalized to the scorer enum (`AA+sti` becomes `AA+`) while
preserving `raw_value="AA+sti"`. An unrecognized rating is
`data_missing/rating_not_in_scorer_enum`.

## Migration and rollback

There is no database migration. The MCP adds three tool registrations and the
normalization/derivation module. The local MarketCow evidence store receives the
same raw-provider records already supported by the generic Tushare adapter.
Rollback removes the three registrations and `convertible_bonds.py`; existing
PostgreSQL and ClickHouse data are unchanged.

## Canonical scheduler repair

Canonical task timestamps now normalize equivalent UTC representations before
task-ID hashing. When the builder returns `truncated`, the scheduler creates
durable child ranges with `parent_task_id` and
`split_reason=canonical_limit_truncated` instead of exhausting retries.
Non-truncation failures retain structured builder diagnostics.

The audited failed range was `AAPL.XNAS`, one-minute qfq,
2025-01-02–2025-05-02: 64,836 raw rows exceeded the 50,000-row task limit. The
two prior IDs differed only by timestamp precision. After replay through the
fixed scheduler, the canonical range contains 64,836 rows and the failed and
pending queues are empty.
