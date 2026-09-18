# MarketCow Python SDK public surface v1

This document freezes the HTTP scope wrapped by `marketcow-client` 0.1. It is intentionally
smaller than the server's complete OpenAPI document. Administration, import, deployment,
operator control, and experimental source-specific routes are excluded.

| SDK method | HTTP operation | Retry when safe | Notes |
| --- | --- | --- | --- |
| `health.check` | `GET /v1/health` | yes | service and storage readiness |
| `instruments.search` | `GET /v1/instruments/search` | yes | discovery, not identity authority |
| `instruments.get` | `GET /v1/instruments/{id}` | yes | canonical instrument ID |
| `instruments.resolve` | `GET /v1/instruments:resolve` | yes | registered mapping only |
| `instruments.resolve_many` | `POST /v1/instruments:resolve/query` | no | may query provider and register mappings |
| `quotes.get` | `POST /v1/quotes/query` | no | may acquire missing or stale data |
| `bars.get` | `POST /v1/market-bars/query` | no | raw/provider history facade |
| `bars.canonical_page` | `GET /v1/canonical-bars/{id}` | yes | signed snapshot-bound cursor |
| `fx.get` | `GET /v1/fx` | no | may fill a missing or stale cache |
| `dividends.get` | `GET /v1/dividends/{symbol}` | no | auditable document shape |
| `dividends.get_fund_history` | `GET /v1/funds/{symbol}/dividends` | no | explicit date window |
| `exposure.get` | `GET /v1/exposure-facts/{symbol}` | no | no inferred themes or weights |

## Compatibility rules

- Financial values are parsed as `Decimal`; timestamps are timezone-aware `datetime` values.
- Unknown additive response fields are retained by Pydantic in `model_extra`.
- A new or unsupported event/schema version will be rejected when realtime support is added.
- Canonical pagination never combines different `snapshot_id` values.
- Explicit provider selection never silently enables fallback.
- `refresh=False` is the SDK default even when a legacy server endpoint defaults to refresh.

## Authentication surfaces

The SDK supports no-auth trusted endpoints, a single bearer token, MarketCow's current
dual-JWT public boundary, and custom header callbacks. It does not issue tokens or retain
OAuth client secrets. A public deployment can set the SDK base URL to an origin ending in
`/public`; all SDK paths remain `/v1/...` relative to that base.

The current server public middleware accepts only GET/HEAD. Therefore POST-backed SDK
operations require a trusted/internal endpoint until the public read-only POST allowlist is
formally implemented and documented.

## Realtime boundary

`/v1/market-data/stream` is not part of SDK 0.1. The server contract already defines
sequence, watermark, resume, gap, and slow-consumer semantics, but an externally
authenticated WebSocket handshake is not yet frozen. The SDK must not present an
unauthenticated internal stream as a public third-party capability.
