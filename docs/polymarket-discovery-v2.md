# Polymarket catalog and bounded discovery v2

This private protocol separates the complete Gamma metadata catalog, a bounded
realtime discovery universe, and the bounded Rust hot Scope. MarketCow publishes only
source-bound market, book, fee, lifecycle, and relation facts. It does not publish
edge, expected profit, APY, projected capital release, strategy scores, or final Scope
rankings.

## Runtime topology

The catalog and two realtime boundaries have distinct responsibilities:

- `polymarket-discovery-collector` runs `run_polymarket_live.py` without any
  `--market-id`. It retains the complete `closed=false` Gamma catalog as metadata,
  then applies `MARKETCOW_POLYMARKET_DISCOVERY_REALTIME_MARKET_LIMIT` (1000 in
  production) to explicitly order-book-enabled markets ranked by recent CLOB volume
  and liquidity. Only that checksum-bound universe is REST-bootstrapped, subscribed,
  and exposed through discovery pagination. Eligible markets from the incumbent Rust
  Scope are retained before filling the remaining capacity.
- the authoritative Rust data plane owns the final Tradude-selected Scope of at most
  100 markets and writes `prediction-markets/polymarket-rust`.

The unified API does not redefine either boundary. A failed discovery refresh cannot
partially replace the active Rust Scope. Gamma `active=true`/`closed=false` is treated
as directory lifecycle metadata, not as proof that every outcome token has a current
CLOB book.

Depth tiers must be explicit decimal base-outcome sizes:

```dotenv
MARKETCOW_POLYMARKET_DISCOVERY_DEPTH_NOTIONALS=10,50,100,500
MARKETCOW_POLYMARKET_DISCOVERY_REALTIME_MARKET_LIMIT=1000
```

They are never supplied by a code default. The standalone read API refuses to start
without `--discovery-depth-notional`; the shared API returns an explicit 503 when its
setting is absent.

## Routes and schema

All new payloads use schema/protocol v2. There is no v1 adapter or field probing.

```text
GET /v1/prediction-markets/polymarket/live/discovery/snapshot
GET /v1/prediction-markets/polymarket/live/discovery/events
WS  /v1/prediction-markets/polymarket/live/discovery/stream
GET /v1/prediction-markets/polymarket/live/discovery/metadata
GET /v1/prediction-markets/polymarket/live/discovery/relations/{relation_id}
GET /v1/prediction-markets/polymarket/history/lifecycle-events
```

OpenAPI describes every HTTP operation and model. Because OpenAPI has no standard
WebSocket operation, `x-websocket-paths` describes its message schema, resume
parameter, resync frame, and close code.

## Atomic materialization and pagination

A dedicated single-flight background worker materializes discovery independently of
HTTP request workers. The complete catalog remains the source-bound metadata record,
but only the checksum-bound realtime universe is copied into the serving SQLite
boundary. Raw hashes from the complete catalog still determine expected relation
membership, so excluding a relation member makes that relation incomplete rather than
silently redefining it. Later book events append quote versions only for affected
markets. Catalog, realtime-universe, or relation membership changes build a replacement
database and publish it atomically while the preceding boundary remains readable.

The first snapshot request omits `snapshot_id` and reads the latest published boundary.
It never calls the materializer. Before the first boundary is ready it returns promptly
with HTTP 503 `discovery_snapshot_materializing`; concurrent requests do not start more
builders. A successful response contains `snapshot_id`, `catalog_revision`,
`boundary_cursor`, `observed_at`, explicit depth tiers, counts, items, and an opaque
`next_page_cursor`. Health does not wait for discovery materialization.

Later pages supply both `snapshot_id` and `page_cursor`. The cursor is validated
against that snapshot. Book/catalog changes can only appear under a new snapshot ID.
Quote versions are selected at that snapshot's boundary cursor, so later incremental
updates cannot alter an earlier page.
An expired snapshot returns HTTP 410 `discovery_snapshot_expired`; an unbound page
cursor returns 422.

Markets with source-backed YES/NO identities expose both token IDs. Some upstream
binary markets instead publish named outcomes such as team names or `Up`/`Down` and
provide no auditable YES/NO mapping. They remain in the complete catalog with their
source outcome/token pairs, but `yes_token_id` and `no_token_id` are null,
`book_status=missing_outcome_identity`, and both fields appear in `missing_fields`.
MarketCow never guesses that mapping from outcome order. Each depth tier walks asks
for `buy_cost_at_notional` and bids for `sell_proceeds_at_notional`.
`complete`, `insufficient_depth`, and `book_unavailable` distinguish real lack of
liquidity from missing data. Missing values are never serialized as zero.

`book_status=ready` requires, at the same boundary:

- both YES and NO books with two-sided depth;
- freshness inside the configured maximum age;
- one consistent source-backed tick revision;
- source-backed minimum order size and fee schedule;
- no unresolved source gap; and
- a complete Standard Negative Risk relation for negative-risk markets.

Otherwise `book_status` is fail closed and `missing_fields` is non-empty. Prices,
quantities, fees, ticks, and depth values remain decimal strings.

## Events, gaps, and resume

`after_cursor=N` means strictly after N. Pages advance over the authoritative global
cursor even when an event does not affect a lightweight quote. Event types are
`quote_changed`, `book_fail_closed`, `relation_changed`, and
`market_lifecycle_changed`.

- a cursor ahead of the boundary returns 422;
- an expired cursor returns HTTP 410 `discovery_cursor_expired`;
- a source gap emits `book_fail_closed` and cannot retain ready status;
- catalog, relation-member, and lifecycle changes require resync;
- WebSocket emits `resync_required` and closes with code 1012.

Consumers fetch a new snapshot after any resync. Relation changes identify added,
removed, and changed relation IDs.

## Rules, settlement, and history

Metadata is bound to a retained discovery snapshot. It distinguishes `event_end_at`,
`market_close_at`, `resolved_at`, `redeemable_at`, and `terminal_at`. Elapsed `end_at`
never manufactures a closed, resolved, or redeemable fact.

Rules and lifecycle fields carry source identity, revision, observation time, and
evidence SHA-256. Unsupported upstream fields stay null and appear in
`missing_fields`. MarketCow does not predict future resolution or redemption.

Lifecycle history pages source-observed `market_closed`, `resolution_proposed`,
`resolution_disputed`, `market_resolved`, and `redemption_available` events. Each row
preserves upstream event identity when available, source and receipt times, resolution,
raw-payload hash, and cursor. Identical facts are content-deduplicated.

## Relations

Standard Negative Risk expected counts come from the raw catalog group, not the number
of normalized copies. A relation is complete only when expected and actual members
match, every member is present, revisions agree, and there are at least two members.
The relation endpoint returns the full YES/NO mapping and active member quotes from one
snapshot. Title-based implication is not inferred.

## Tradude-owned Scope selection

`manage_polymarket_scopes.py generate` accepts only an explicit
`tradude.prediction_market.scope_selection.v2` document with discovery snapshot and
catalog hashes, ordered `market_ids`, complete relation members, and a selection
evidence hash.

MarketCow preserves market order exactly. It does not retain incumbents, fill
vacancies, sort on liquidity/end time, or choose a Top 100. It validates identity, the
1–100 hot-Scope transport bound, and relation completeness, then uses the existing
candidate warmup, acceptance, atomic activation, `universe_changed`, and full-sync
path. Neither service needs a restart.

```bash
PYTHONPATH=src python scripts/manage_polymarket_scopes.py \
  --registry-root /absolute/polymarket-scope-registry generate \
  --selection /absolute/tradude-scope-selection-v2.json \
  --output /absolute/candidate-manifest.json
```

## Verification

`tests/test_polymarket_discovery.py` covers more than 100 active markets, immutable
pagination, per-market missing outcome identity, affected-market-only quote
materialization, initial single-flight behavior,
non-blocking health, cursor expiry, WebSocket resync, source-only lifecycle history,
metadata non-inference, complete relations, and OpenAPI. Existing Python and Rust Scope
tests cover candidate warmup, atomic activation, cursor continuity, `universe_changed`,
and scoped full-sync.
