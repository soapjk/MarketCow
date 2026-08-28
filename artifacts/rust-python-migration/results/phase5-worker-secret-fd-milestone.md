# Phase 5 worker secret FD milestone

Date: 2026-08-28

Status: partial Phase 5 evidence. This does not claim Phase 5 or the migration is complete.

## Security boundary

- Rust accepts an optional capability-to-absolute-file reference map, never inline secret
  values.
- A reference is opened with `O_NOFOLLOW` and must resolve to a 1–65536-byte regular file
  owned by MarketCow's effective user with no group or other permission bits.
- Validation occurs during daemon preflight and again immediately before each worker spawn.
- Rust supplies only the assigned capability's open descriptor as FD 3. The child environment
  contains the descriptor number, but not the secret value or path.
- Workers for other capabilities receive no secret descriptor. PostgreSQL, ClickHouse and
  admin credentials remain removed by `env_clear`.
- Any missing, replaced-with-symlink, weakly permissioned, wrong-owner or invalid-size
  reference prevents that worker command from spawning and enters the bounded fail-closed
  restart path.

## Reproducible verification

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python3 -m pytest -q \
  tests/test_rust_python_worker.py tests/test_rust_migration_architecture.py \
  tests/test_sec_dividends.py
python3 -m ruff check python/marketcow_workers/worker.py \
  tests/test_rust_python_worker.py tests/test_rust_migration_architecture.py
```

The Rust integration test starts a real child shell, proves it can read the expected secret
only from FD 3, proves database/admin environment variables are absent, and verifies the CPU,
file-descriptor and core-dump limits. A separate test proves weak permissions and symlink
references are rejected.

Observed result: 73 Rust tests passed, 0 failed and 2 explicitly environment-gated storage
tests were ignored (their real PostgreSQL 17.10/ClickHouse 25.8 result is recorded separately);
Clippy passed with warnings denied; 19 selected Python tests passed; Ruff passed.

Real-order submission remains disabled. Tradude does not manage MarketCow lifecycle.
