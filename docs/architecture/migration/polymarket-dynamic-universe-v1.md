# Polymarket dynamic universe v2 ownership model

Status: implemented locally; production activation requires a separately approved deployment.

## Decision

MarketCow owns read-only market discovery, source-fact validation, Scope preheating and atomic
market-data publication. Tradude owns candidate selection, opportunity ranking, projected capital
release, positions, accounts, risk and orders. MarketCow exposes no
order, cancellation, signing, wallet or execution operation, and
MarketCow remains a market-data service and has no order-submission interface.

The stable identity is a 64-hex `universe_id`. Membership changes do not change this identity.
Every accepted refresh increments a durable `generation` by exactly one. A generation is built in
an isolated runtime from one immutable projection boundary, receives a complete catalog and a
two-sided full book for every active token, passes catalog/facts/tick/minimum-count validation, and
is checkpointed before publication. The active writer is never modified in place.

## Artifact and discovery schemas

- generation artifact: `marketcow.polymarket.rust-live-scope.v4`
- universe object: `marketcow.polymarket.universe.v2`
- scope discovery: `marketcow.polymarket.scope-discovery.v3`
- WebSocket: `marketcow.market-stream.v2`

The universe object contains:

- `universe_id`, `generation`, `target_market_count`, `minimum_market_count`, `validated_at`;
- fail-closed filters `require_two_sided_books` and `require_complete_instrument_facts`;
- complete `active_markets`, each retaining stable `market_id`, `condition_id`, two `token_ids`,
  and `end_at` identities;
- exact `added_markets` and `removed_markets` IDs relative to the preceding generation, plus
  `added_market_identities` and `removed_market_identities` carrying condition/token/end-time facts;
- any explicitly selected market that cannot be activated in `excluded_markets`, with
  `reason_code`, `retryable`, `retry_after`, and `observed_at`.

Machine exclusion reasons are `market_expired`, `market_not_found`, `one_sided_book`,
`token_missing`, `book_missing`, `instrument_facts_missing`, `instrument_facts_invalid`,
and `fee_facts_unavailable`. Retryable exclusions must have a future `retry_after`;
non-retryable exclusions must not. MarketCow does not replace an invalid selection with another
market and does not apply a capital-lock horizon.

`/v1/prediction-markets/polymarket/live/full-sync` retains its existing live schema and adds the
independently versioned `universe_schema_version`, `universe_id`, `universe_generation`, and full
`universe` object. These fields and `snapshot` are returned from one validated generation and one
`boundary_cursor`. A transition-visible mixed read returns HTTP 503
`polymarket_generation_transition_in_progress`.

## Refresh and failure semantics

`build_polymarket_dynamic_universe.py` consumes exactly one
`tradude.prediction_market.scope_selection.v2` manifest. It preserves Tradude's market order and
validates the complete selected set. An expired/404-equivalent, one-sided, missing-token/book/facts,
unavailable-fee market, or incomplete selected relation fails the candidate closed. MarketCow does
not rank, replenish, truncate, or fill the selection. No artifact is emitted unless the exact
selection is atomically ready.

`auto_refresh_polymarket_dynamic_universe.py` is the MarketCow validation and publication one-shot
refresh
transaction intended for a five-minute service-manager schedule. It takes an advisory lock,
requires a coherent ready scope/full-sync boundary, fetches an auditable exact-decimal CLOB book
snapshot, builds and validates `generation + 1`, and compares stable market/condition/token/end
identities. An unchanged membership is an explicit idempotent `no_change`; a changed membership is
hash-pinned into the local registry and activated through the authenticated loopback-only admin
route. An overlap, unavailable input, incomplete boundary, below-minimum candidate, activation
error, or post-activation mismatch never mutates the active generation and writes a
`failed_closed` audit result. The admin credential is supplied by MarketCow lifecycle configuration
and is never returned in audit evidence.

The daemon accepts only the exact set delta and `previous_generation + 1`. It forks the current
projection into a generation-specific storage root, appends the replacement catalog and full-book
barrier to a new WAL, verifies exact per-token tick consistency and checkpoints it. Only then does
it persist the active v4 artifact and atomically replace the in-memory projection/config pair.
Restart resolves the persisted artifact and reopens the matching generation root.

Existing WebSocket clients receive:

```json
{
  "protocol_version": "marketcow.market-stream.v2",
  "scope_id": "<universe_id>",
  "cursor": 123,
  "type": "universe_changed",
  "universe_id": "<universe_id>",
  "old_generation": 7,
  "new_generation": 8,
  "added_markets": ["..."],
  "removed_markets": ["..."],
  "switch_boundary_cursor": 123,
  "full_sync_required": true
}
```

The server then closes with `full_sync_required`. Consumers must validate the new full-sync and
replace their projection atomically. Cursor expiry/gap, generation mixing, invalid catalog/facts,
incomplete books or a count below the minimum remain global fail-closed conditions. Removal from
the scanning universe never changes the stable market/condition/token identities needed by
Tradude to manage an already-held position.

## Reproducible local checks

```sh
python3 -m pytest -q tests/test_build_polymarket_rust_scope.py
cargo test --workspace --no-fail-fast
cargo clippy --workspace --all-targets -- -D warnings
```

Production activation must use a registered hash-pinned v4 artifact and the authenticated
`/v1/admin/polymarket/scope:activate` route. Tradude must not start or restart MarketCow.
