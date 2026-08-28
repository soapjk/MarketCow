# Phase 5 Rust-owned worker Artifact registration milestone

Status: implementation and local socket integration verified at commit
`70e2cb092a32780961ccff0bfcab214f78c82c9d`. This is a partial Phase 4/5 milestone, not a Phase 5
or overall migration completion claim.

## Implemented boundary

- The Rust job engine authorizes the active running lease before any worker result is promoted.
- `marketcow-storage` verifies the staging file type, direct-child containment, exact byte size and
  SHA-256 before an atomic same-filesystem rename.
- The durable path is content addressed as `dataset/revision/hash-prefix/hash` and an already
  verified target makes retry after a database failure idempotent.
- The Rust coordinator replaces the worker-provided staging name with the durable Artifact-relative
  path.
- PostgreSQL inserts `raw_artifact_manifest` and advances the provider job revision to `succeeded`
  in one transaction. Manifest collision, job revision conflict or persistence failure rolls the
  in-memory job state back; a promoted but unregistered body remains non-authoritative and can be
  retried.
- The safe-forward migration is checksum-pinned as `rust-artifact-manifest-v1` under the existing
  PostgreSQL advisory-lock migration owner.
- Python contains no manifest registration or database client. It remains an untrusted staging
  producer over the versioned UDS protocol.

Real order submission remains disabled. Tradude does not start, restart or supervise MarketCow.

## Reproducible verification

Passed:

```sh
cargo test --workspace
# 65 passed, 0 failed, 2 ignored external-instance tests
# includes the Rust UDS worker completion path and real TCP WebSocket test

cargo clippy --workspace --all-targets -- -D warnings
# passed

PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python3 -m pytest -q \
  tests/test_rust_python_worker.py tests/test_rust_migration_architecture.py
# 10 passed, including the Python AF_UNIX handshake

PYTHONPATH=src python3 -m ruff check tests/test_rust_migration_architecture.py
# passed
```

Both external-instance tests were subsequently run through
`scripts/migration/verify_storage_integrations.sh` at commit
`cb8b5b16c69143fb3fed8503d00ab35a9f0c87b2`. PostgreSQL 17.10 verified the safe-forward migration
and atomic `compare_and_swap_with_artifact` transition. ClickHouse 25.8 verified its migration,
insert idempotency and typed latest read. Machine-readable result:
`phase4-storage-real-instances.json`.

## HTTP soak result handled in the same continuation

The prior HTTP/network 45-minute soak remains failed negative evidence:

- result SHA-256: `968e085caa9e9c3a82781c574e0e3d06d9d602b48fa9d495107f904faeadf0e4`
- maximum persistence latency: `767399 us` against a `50000 us` gate
- `passed=false`
- result storage was deleted by the v2 runner, so artifact-specific restart recovery is impossible
- formal negative Artifact: `artifact:ccb30525-c669-4d46-8f70-53bb546e219b`

It is not replaced by, or conflated with, the independently scheduled headless soak.

## Remaining gates

- Implement actual Python Provider handlers and worker supervision; remove Python production DB
  credentials and public API dependency.
- Continue the remaining Phase 0–7 gates. No acceptance criterion is reported passed by this file.
