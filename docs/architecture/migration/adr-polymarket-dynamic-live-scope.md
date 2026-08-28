# ADR: Polymarket live scope is dynamically activated inside MarketCow

- Status: accepted
- Date: 2026-08-28
- Safety boundary: market-data reads only; real order submission remains outside MarketCow

## Decision

Polymarket strategies may change their monitored market universe without restarting `marketcowd`.
Every distinct universe is an immutable, hash-pinned scope artifact and receives a distinct stable
`scope_id`. The active list is never edited in place.

An authenticated admin request activates a registered scope by `scope_id` and exact artifact
SHA-256. MarketCow resolves only `<registry-root>/<scope_id>.json`, rejects symlinks and path
escape, validates the complete catalog/outcome/token/relation/instrument facts, opens and replays
the scope-specific WAL/checkpoint, and seeds the catalog before interrupting the current upstream
subscription.

The Rust transport supervisor then checkpoints the previous scope, atomically swaps the active
runtime/projection, and reconnects with the new token list. The new scope is `unready` and public
snapshot/full-sync/stream endpoints return HTTP 503 until every required book is present, all
gaps are closed, the catalog revision matches, and freshness gates pass.

Existing WebSocket clients are bound to the scope present at subscription time. A scope change
emits `resync_required` with reason `scope_changed` and closes the connection. Clients must call
scope discovery and full-sync again; cursors are never compared across scopes.

## Operational contract

- Endpoint: `POST /v1/admin/polymarket/scope:activate`
- Request schema: `marketcow.polymarket.scope-activation.v1`
- Registry root: `MARKETCOW_POLYMARKET_SCOPE_REGISTRY_ROOT`
- Required fields: `scope_id`, `scope_file_sha256`
- Receipt schema: `marketcow.polymarket.scope-activation-receipt.v1`
- Successful activation status is initially `activated_unready`; it is not a readiness claim.
- The endpoint is protected by the existing admin Bearer authentication and append-only audit.
- The bounded switch queue fails closed when saturated.
- Tradude may request a data-scope activation but never starts, stops, or restarts MarketCow.
- No order, cancel, signing, wallet, or trading-control API is introduced.

## Consequences

Scope artifacts must be generated and registered before activation. MarketCow maintains isolated
WAL/checkpoint state per dynamic scope, so switching back can replay that scope without mixing its
cursor or books with another universe. Strategy clients must treat `scope_id` as part of every
cache key and discard an old projection after `scope_changed`.
