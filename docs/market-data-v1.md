# MarketCow Market Data Contract v1

Status: Phase 0/Phase 1 integration candidate. Schema version: `1`.

## Responsibility boundary

MarketCow owns instrument identity, provider mappings, canonical history and market-data
quality. Consumers use HTTP and, after v1 acceptance, WebSocket. MarketCow does not expose
PostgreSQL/ClickHouse as an integration API and does not handle orders, accounts or positions.

## Instrument master

The authority is PostgreSQL `instrument_master`. IDs use `SYMBOL.MIC`, for example
`AAPL.XNAS`, `700.XHKG`, `600519.XSHG`. A market is not a venue: callers must supply MIC,
currency, price/size precision, tick size and lot size. MarketCow never infers XNAS versus
XNYS from `US`.

Mappings use explicit namespaces such as `provider:longport` and `broker:longport`.
`(namespace, external_symbol)` is unique and a conflict returns `instrument_conflict`.

## Machine-readable schemas

`GET /v1/schemas/{name}` serves JSON Schema for:

`instrument`, `event`, `quote`, `trade`, `order_book_snapshot`, `order_book_delta`,
`bar`, `stream_status`, `heartbeat`, and `error`.

Unknown fields are rejected. Financial values are decimal strings; JSON numbers are invalid.
Schema v1 only accepts `schema_version: 1`. Additive optional fields require a documented
minor contract update; removing, renaming or changing meaning requires a new schema version.

## Event time and sequence

- `ts_event`: UTC time assigned by the originating market/provider to the event.
- `ts_ingest`: UTC time MarketCow first accepted the event.
- `ts_publish`: UTC time MarketCow emitted the normalized envelope.
- `sequence`: monotonically increasing within one `stream_id`; it is not global.

All timestamps are timezone-aware UTC. A consumer must reject unsupported schema versions
and detect any non-consecutive sequence. Realtime resume/gap behavior will be frozen with
the Phase 2 WebSocket implementation after this contract is accepted.

## Deterministic canonical history

`GET /v1/canonical-bars/{instrument_id}` requires explicit `start`, `end`, `interval`,
`adjustment`, and `page_size`. The response includes a manifest with `dataset_id`,
`snapshot_id`, `canonical_version`, total `row_count`, and SHA-256 content identity.

The signed cursor binds every query parameter and the snapshot ID. Each subsequent page
recomputes the canonical identity. If canonical content changes, the old cursor fails
instead of silently mixing revisions. Pagination is ascending keyset pagination; no offset
is used. `truncated=true` means another page exists.

OHLCV values in this v1 endpoint are decimal strings. Provenance identifies the canonical
ClickHouse layer; individual bars retain selected source and quality fields.

## Bar semantics frozen for historical v1

- `bar_at` is the window start.
- Windows are half-open `[start, end)`.
- `raw` means unadjusted provider observations; `adjusted` is a distinct stored series.
- Historical canonical bars are never synthesized for empty periods.
- One canonical row has one selected source; a row cannot mix providers.
- Revisions change canonical content identity and invalidate prior snapshot cursors.

Realtime 1-minute session, extended-hours, late-event and provider-switch rules remain
Phase 2 work and must not be guessed by consumers before the WebSocket contract is accepted.

## Examples

```http
GET /v1/instruments/AAPL.XNAS
GET /v1/instruments:resolve?namespace=provider:longport&external_symbol=AAPL.US
GET /v1/canonical-bars/AAPL.XNAS?start=2026-07-01T00:00:00Z&end=2026-07-22T23:59:59Z&interval=1d&adjustment=raw&page_size=1000
```

Base URL for local production is configured by the consumer; MarketCow does not prescribe
an implicit endpoint. The current local convention is HTTP `http://127.0.0.1:8790`.
