# Phase 4 PostgreSQL control-plane milestone

Date: 2026-08-28

Status: partial Phase 4 evidence. Rust owns the runtime config version and migration checkpoint
repositories and persists the active daemon config revision; domain cutover workflows still need
to consume the checkpoint port before the Python writer can be disabled.

Implementation commits: `b2c40a27f4ef53220c225cc4053fb17e4a4a716d` and
`b42d6862a1452cf9b5ff23e5ea7a390ba3b24512`.

## Contract and safety

- Runtime configurations are canonical JSON, SHA-256 verified, append-only by `(config_id,
  version)`, content-idempotent by `(config_id, config_sha256)`, and readable point-in-time.
- Migration checkpoints use create-once plus revision compare-and-swap. A stale or concurrent
  loser receives `RevisionConflict`; no last-write-wins fallback exists.
- The safe-forward schema is checksum-pinned and advisory-lock fenced, matching the Python final
  tables, constraints and indexes.
- MarketCow derives a secret-free SHA-256 revision from serializable Rust configuration, persists
  it before opening the Polymarket runtime, and reuses the same version on an identical restart.
  Provider secret reference paths are excluded from serialization.
- Production still fails startup when PostgreSQL or the attributable binary commit is absent.
  Real orders remain disabled and Tradude does not manage MarketCow lifecycle.

## Reproducible verification

```sh
scripts/migration/verify_control_plane_postgres.sh \
  "$PWD/artifacts/rust-python-migration/results/phase4-control-plane-postgres-real.json"
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
uv run --isolated --frozen python -m unittest -v \
  tests.test_postgres_repositories tests.test_mcp_server
uv run --isolated --frozen python -m ruff check \
  scripts/migration/verify_native_instrument_api.py tests/test_postgres_repositories.py
cargo fmt --all -- --check
```

The isolated PostgreSQL 17.10 run passed all seven repository gates: safe-forward migration
idempotence, verified runtime-config hash, append-only/content idempotence, point-in-time read,
checkpoint create/CAS, stale revision rejection and concurrent single winner. The real daemon
differential additionally proved healthy control-plane persistence, SHA-256 revision propagation,
four recorded Rust migrations and no duplicate config row after restart.

Workspace regression reported 84 passed, 0 failed and 4 explicit environment-gated tests ignored.
Python inventory/MCP reported 27 passed and 18 explicitly PostgreSQL-gated tests skipped; Clippy,
Ruff and format checks passed.

The repository result SHA-256 is
`cb6bf83cf78347c59b855ab873f4e4630a3db9530ff5697e9e24b235fa3778a4`. The expanded daemon
result SHA-256 is `d8ae0c1840a3af7f2d57c2f80311a9931cb166d0fa67d51ef02e9a24ed710ee0`.
Neither result is longevity/soak evidence.
