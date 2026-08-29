# Polymarket dynamic universe v1

Status: implemented locally; production activation requires a separately approved deployment.

## Decision

MarketCow owns read-only market discovery, eligibility checks, subscription membership and atomic
market-data publication. Tradude owns positions, accounts, risk and orders. MarketCow exposes no
order, cancellation, signing, wallet or execution operation, and
`real_order_submission_enabled` remains `false`.

The stable identity is a 64-hex `universe_id`. Membership changes do not change this identity.
Every accepted refresh increments a durable `generation` by exactly one. A generation is built in
an isolated runtime from one immutable projection boundary, receives a complete catalog and a
two-sided full book for every active token, passes catalog/facts/tick/minimum-count validation, and
is checkpointed before publication. The active writer is never modified in place.

## Artifact and discovery schemas

- generation artifact: `marketcow.polymarket.rust-live-scope.v4`
- universe object: `marketcow.polymarket.universe.v1`
- scope discovery: `marketcow.polymarket.scope-discovery.v3`
- WebSocket: `marketcow.market-stream.v2`

The universe object contains:

- `universe_id`, `generation`, `target_market_count`, `minimum_market_count`, `validated_at`;
- filters `require_two_sided_books`, `require_complete_instrument_facts`, and
  `maximum_capital_lock_seconds`;
- complete `active_markets`, each retaining stable `market_id`, `condition_id`, two `token_ids`,
  and `end_at` identities;
- exact `added_markets` and `removed_markets` IDs relative to the preceding generation, plus
  `added_market_identities` and `removed_market_identities` carrying condition/token/end-time facts;
- every evaluated but inactive candidate in `excluded_markets`, with `reason_code`, `retryable`,
  `retry_after`, and `observed_at`.

Machine exclusion reasons are `market_expired`, `market_not_found`, `one_sided_book`,
`token_missing`, `book_missing`, `instrument_facts_missing`, `instrument_facts_invalid`,
`capital_lock_exceeded`, `fee_facts_unavailable`, and `target_capacity`. Retryable exclusions must
have a future `retry_after`; non-retryable exclusions must not.

`/v1/prediction-markets/polymarket/live/full-sync` retains its existing live schema and adds the
independently versioned `universe_schema_version`, `universe_id`, `universe_generation`, and full
`universe` object. These fields and `snapshot` are returned from one validated generation and one
`boundary_cursor`. A transition-visible mixed read returns HTTP 503
`polymarket_generation_transition_in_progress`.

## Refresh and failure semantics

`build_polymarket_dynamic_universe.py` scans a ranked MarketCow candidate manifest. A single
expired/404-equivalent, one-sided, missing-token/book/facts, excessive-lock, or unavailable-fee
candidate is excluded and the next eligible candidate fills the target. No artifact is emitted
when qualified markets fall below `minimum_market_count`.

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
