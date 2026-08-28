# Phase 3 read API and recovery milestone

This Artifact records a local shadow milestone only. It does not claim Phase 3 or the overall
migration is complete.

- Implementation commit: `dc87cf931a34349352dba6a4ccbf95dc95826903`
- Authoritative plan SHA-256: `62d1a0418eedf6dbf00c37dabce827e0cf1b4d3c8fc43536ffe3e0421d3742d4`
- Environment: dedicated WorkItem worktree; no production service, external write, or real order
  path was used.

## Implemented evidence

- `marketcow-core` validates exact raw-payload SHA-256 evidence before durable application.
- A full bounded ingress queue returns ownership of rejected events; queue pressure cannot silently
  discard an authoritative event.
- WAL reopen verifies the complete segment chain and preserves the previous-segment hash across
  process restart.
- Checkpoint recovery replays only records after the checkpoint cursor and rejects any difference
  between recorded and newly-derived apply/reject decisions.
- `marketcow-api` builds bounded snapshot, events, and checkpoint read models without database or
  network dependencies. Monetary values remain decimal strings and responses expose both
  published and persisted cursors.
- `marketcowd` returns HTTP 503 for readiness, snapshot, events, and checkpoint until a trusted
  projection is ready. Events are limited to 1–1000 records and expired cursors are explicit.

## Reproduction

```text
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
python3 -m pytest -q tests/test_rust_python_worker.py tests/test_rust_migration_architecture.py
python3 scripts/migration/verify_phase0.py
```

Result: all 24 Rust unit tests passed, Clippy passed with warnings denied, all 8 selected Python
architecture/worker tests passed, and Phase 0 verification passed while continuing to report raw
r2 as unavailable. Machine-readable command timestamps, output hashes, and output tails are in
`local-verification.json`.

## Open gates

- Raw r2 cursor 24906390–25040506 is not present, so zero-divergence r2 replay cannot be claimed.
- Polymarket provider adapter, bootstrap/full-sync/stream, typed database implementations, and
  real-instance differential tests remain incomplete.
- The required long shadow/soak gates have not run. They must be executed by Corptie scheduled
  Automations when that capability is available; this interactive Session will not poll them.
- No cutover, production replacement, or deployment is authorized or claimed.
