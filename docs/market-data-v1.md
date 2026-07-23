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

Contract v1 supports `instrument_type=equity` and `asset_class=equity` only. This is
deliberately explicit: `size_precision` is fixed at `0` and `size_increment` at `"1"` to
match Nautilus `Equity`. `ts_event` is the authoritative effective time of the instrument
definition; `ts_init` is when MarketCow initialized that definition, and
`ts_event <= ts_init`.

Mappings use explicit namespaces such as `provider:longport` and `broker:longport`.
`(namespace, external_symbol)` is unique and a conflict returns `instrument_conflict`.

## Machine-readable schemas

`GET /v1/schemas/{name}` serves JSON Schema for:

`instrument`, `event`, `quote`, `trade`, `order_book_snapshot`, `order_book_delta`,
`bar`, `stream_status`, `heartbeat`, `error`, `historical_manifest`,
`historical_bar`, and `canonical_bar_page`.

Unknown fields are rejected. Financial values are decimal strings; JSON numbers are invalid.
The `event` schema is a discriminated `oneOf` keyed by `event_type`, so an event cannot
validate with another event type's payload. Quote, trade, book and bar events require
`instrument_id`; heartbeat forbids it; status and error allow it when scoped to an instrument.
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

Trade IDs are required provider IDs. If a provider has none, the MarketCow adapter must
generate a stable ID from provider namespace plus immutable raw-event identity before
validation. `aggressor_side` maps directly to Nautilus buyer/seller aggressor; unavailable
or non-applicable values use `NO_AGGRESSOR`. Trade prices and sizes are positive.

Book v1 is explicitly top-of-book market-by-price (`L1_MBP`, depth 1), not MBO.
Every synthetic MBP level uses `order_id="0"`. A snapshot establishes
`baseline_sequence`; deltas refer to that same baseline. A sequence gap invalidates the
baseline and requires a new snapshot before deltas may be applied.

## Deterministic canonical history

`GET /v1/canonical-bars/{instrument_id}` requires explicit `start`, `end`, `interval`,
`adjustment`, and `page_size`. The response includes a manifest with `dataset_id`,
`snapshot_id`, `canonical_version`, total `row_count`, and SHA-256 content identity.

The signed cursor binds every query parameter and the snapshot ID. MarketCow computes
identity before and after every page read. A revision during the current read returns
`409 canonical_snapshot_changed`; a later request with an old cursor fails validation.
Pagination is ascending keyset pagination; no offset is used. `truncated=true` if and only
if `next_cursor` is present.

OHLCV values in this v1 endpoint are decimal strings. Provenance identifies the canonical
ClickHouse layer; individual bars retain selected source and quality fields.
`content_hash` is SHA-256 over the complete, stably ordered canonical row content, not a
sample or aggregate fingerprint.

## Bar semantics frozen for historical v1

- Query bounds include both bar-window start timestamps: `start <= bar_at <= end`.
- `window_start` is the stored `bar_at`; `window_end` is derived from the frozen interval.
- Windows are half-open `[start, end)`.
- `ts_event` equals `window_end`; `ts_init` is the canonical row ingestion timestamp.
- `price_type` is `LAST` and `aggregation_source` is `EXTERNAL` for MarketCow-published
  historical and realtime bars.
- Supported intervals are `1-MINUTE`, `5-MINUTE`, `15-MINUTE`, `30-MINUTE`, `1-HOUR`,
  and `1-DAY`; arbitrary interval strings are rejected.
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
GET /v1/canonical-bars/AAPL.XNAS?start=2026-07-01T00:00:00Z&end=2026-07-22T23:59:59Z&interval=1-DAY&adjustment=raw&page_size=1000
```

Base URL for local production is configured by the consumer; MarketCow does not prescribe
an implicit endpoint. The current local convention is HTTP `http://127.0.0.1:8790`.
