# FX API and provider protocol migration

## Public FX contract

MarketCow 0.2 exposes `GET /v1/fx`. The path is intentionally the same as the
Investrace provider protocol path so OpenAPI path discovery and older consumers do not
silently lose the capability.

Example:

```http
GET /v1/fx?base=USD&symbols=CNY,HKD
```

```json
{
  "base": "USD",
  "rates": {"USD": 1.0, "CNY": 7.2, "HKD": 7.8},
  "source": "yahoo_chart",
  "sourceUrls": {
    "CNY": "https://query1.finance.yahoo.com/v8/finance/chart/CNY=X?...",
    "HKD": "https://query1.finance.yahoo.com/v8/finance/chart/HKD=X?..."
  },
  "asOf": "2026-07-27T12:00:00+00:00",
  "fetchedAt": "2026-07-27T12:01:00+00:00",
  "ingestedAt": "2026-07-27T12:01:00+00:00",
  "cached": false,
  "stale": false,
  "cacheStatus": "refreshed",
  "cacheTtlSeconds": 900,
  "staleMaxSeconds": 86400,
  "errors": []
}
```

`rates[X]` is the amount of `X` per one unit of `base`. `base` and every requested
symbol may be `USD`, `CNY`, or `HKD`; cross rates are calculated from the two
upstream USD pairs. The identity rate of the base is always `1`. Market values are
never hardcoded.

The source is MarketCow's existing Yahoo Chart provider. `CNY=X` and `HKD=X` are
queried with their source URLs retained for audit. `asOf` is the oldest market event
needed by the response; `fetchedAt` records the fetch that populated the selected
cache entries; `ingestedAt` records this MarketCow request.

`refresh=true` bypasses a fresh cache entry. Configuration:

- `MARKETCOW_FX_CACHE_TTL_SECONDS`, default 900 seconds.
- `MARKETCOW_FX_STALE_MAX_SECONDS`, default 86400 seconds.

Within TTL, the response has `cached=true`, `stale=false`, and `cacheStatus=hit`.
If refresh fails but a cache entry is no older than the stale maximum, MarketCow
returns it with `stale=true`, `cacheStatus=stale_if_error`, and a per-currency error.
It never labels a stale rate as fresh.

Invalid currencies return HTTP 422 with `detail.code=invalid_currency`. An upstream
failure without usable cache returns HTTP 503 with one of:

- `provider_unavailable`: the upstream request could not be completed.
- `no_data`: the upstream response had no positive rate or market timestamp.
- `stale_data`: the only cache entry exceeded the configured stale maximum.

## Legacy provider protocol audit

Audit baseline:

- Legacy consumer contract:
  `llmay-suite/investrace/openapi/market-data-provider.yaml`, version 1.0.
- MarketCow public OpenAPI: `/openapi.json`, API version 0.2.0.
- Canonical security identity: provider-neutral `SYMBOL.MIC`.

The classifications below are exhaustive for the legacy protocol's five capabilities
and its required discovery endpoint.

| Area | Legacy protocol | MarketCow 0.2 contract | Classification and migration |
| --- | --- | --- | --- |
| Capability discovery | `GET /v1/capabilities` with five booleans | Inspect `/openapi.json` paths; `/v1/admin/capabilities` is an operator read model, not a consumer capability contract | **Existing replacement, documented.** OpenAPI is authoritative because it cannot advertise a route absent from the running application. Do not treat a 404 from the retired `/v1/capabilities` as proof of legacy support. |
| Instrument search | `GET /v1/instruments/search?q=&limit=`; provider symbols | Same path, `{count, items}`; items use canonical CN/HK `symbol` | **Existing replacement, documented.** Search does not guess a US MIC when upstream search metadata is insufficient. Resolve US provider symbols instead. Current upstream-bound limit is 30 rather than the old generic maximum 100. |
| Instrument resolution | Implicitly delegated to provider | `GET /v1/instruments:resolve` for registered mappings; `POST /v1/instruments:resolve/query` dynamically resolves 1–20 provider symbols and persists them | **New replacement, documented.** Use `namespace=provider:longport`; send LongPort forms such as `MU.US`, `700.HK`, and `600519.SH`. Batch items preserve request order and contain either `instrument_id` or a structured error. |
| Current quote, single | `GET /v1/quotes/{symbol}` with old `.SH/.SZ/.HK` or bare US symbol | Same path with canonical `SYMBOL.MIC`; `provider` requires `refresh=true` | **Existing replacement, documented.** Resolve first, then use the returned identity. Stable identity fields are `instrument_id` and `symbol`, both canonical. |
| Current quote, batch | No standard batch path | `POST /v1/quotes/query`, 1–20 canonical symbols; legacy-compatible `GET /v1/quotes?symbols=` also exists | **Existing replacement, documented.** Associate successes by `items[].symbol` and failures by `errors[].symbol`; do not rely on completion order. |
| Quote response | camel-case `changePct`, `asOf`, `ingestedAt`, `cached` | richer snake-case provenance: `change_pct`, `quote_at`/`observed_at`, `ingested_at`, `cached`/`is_cached`, plus source and routing fields | **Existing replacement, documented.** Consumers may accept both spellings during rolling upgrades. `instrument_id`/`symbol`, not array position, is the response key. |
| Historical bars, single | `GET /v1/quotes/{symbol}/history` | Same path; canonical symbol; explicit source/routing and cache metadata | **Existing replacement, documented.** Legacy `raw` maps directly to `raw`. Ambiguous legacy `split`/`all` names are intentionally retired; select `qfq` or `hfq` explicitly. Bar identity is `bar_at` (legacy adapters may read `barAt`). |
| Historical bars, batch | No standard batch path | `POST /v1/market-bars/query`, 1–20 canonical symbols | **Existing replacement, documented.** Results and errors are correlated by canonical symbol. |
| FX | `GET /v1/fx?base=USD&symbols=CNY,HKD` | Same public path with auditable source, timestamps, and freshness/error fields | **Compatibility omission fixed.** The path is now declared in OpenAPI and retains `base`, `rates`, `asOf`, `source`, and `cached` required by the legacy consumer. |
| Fundamentals | `GET /v1/fundamentals` was described as an optional instrument catalog, not full company fundamentals | `/v1/fundamentals` is actual point-in-time A-share fundamental data; instrument discovery uses search/resolve | **Intentional semantic retirement.** Do not use fundamentals as an instrument catalog. Existing Investrace clients may continue parsing `items`, but new discovery must use search/resolve. The path remains because real fundamentals are a supported MarketCow domain, not as a compatibility alias. |

## Identifiers, batching, keys, and errors

All quote and history operations require provider-neutral identities such as
`600519.XSHG`, `700.XHKG`, and `MU.XNAS`. Deterministic legacy migrations are:

- `.SH`/`.SS` → `.XSHG`
- `.SZ` → `.XSHE`
- `.BJ` → `.XBSE`
- `.HK` → `.XHKG` after numeric normalization

Bare US symbols are not deterministic. Call
`POST /v1/instruments:resolve/query` with `namespace=provider:longport` and the
provider form (`MU.US`, `BRK.B.US`, and so on). MarketCow determines XNAS, XNYS,
ARCX, or XASE from upstream metadata.

Batch quote/history successes are keyed by canonical `symbol`; resolution results are
keyed by `external_symbol`. Per-item errors carry the same key. HTTP-level invalid
requests use 4xx with a machine-readable `detail` object where a public compatibility
contract exists. Upstream-wide unavailability uses 502/503; batch data failures remain
per-item so one symbol does not erase successful peers.

## Compatibility conclusion

The audit found one silent capability loss requiring implementation: FX. It is fixed
at the original public path. All other legacy areas either already have a public 0.2
replacement or were intentionally retired because their old semantics were ambiguous
or misleading. No additional compatibility alias is introduced for
`/v1/capabilities`: duplicating capability truth outside the generated OpenAPI would
allow the two declarations to drift again.
