# Phase 5 Rust-owned Python worker supervision milestone

Date: 2026-08-28

Status: partial Phase 5 evidence. This is not a production migration completion claim.

## Safety and ownership

- `marketcowd` is the sole worker-pool supervisor; Tradude has no MarketCow lifecycle role.
- The pool is disabled by default and requires paired absolute executable/script paths.
- Every worker command starts from `env_clear()` and receives only Python isolation flags,
  its UDS path and immutable worker revision. PostgreSQL, ClickHouse and MarketCow admin
  credentials are not inherited.
- Python workers expose no network listener and connect only to the daemon-owned mode-0600
  Unix socket.
- Real-order submission remains hard-disabled by the existing configuration safety gate.
- Worker exits degrade only the worker component. They do not stop or restart the HTTP/WAL
  platform.

## Lifecycle and observability

- Pool size is bounded to 1–16 workers when enabled.
- Each slot uses a bounded sliding-window restart budget (default: 3 restarts per 60 seconds)
  and bounded backoff (default: 1 second).
- Child processes use kill-on-drop and are reaped during daemon shutdown before the UDS is
  removed.
- `/v1/health` reports configured/live worker counts, restarts and budget exhaustion.
- Prometheus output includes `marketcow_python_workers_configured`,
  `marketcow_python_workers_live`, `marketcow_python_worker_restarts_total`, and
  `marketcow_python_worker_restart_budget_exhaustions_total`.

## Reproducible verification

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python3 -m pytest -q \
  tests/test_rust_python_worker.py tests/test_rust_migration_architecture.py \
  tests/test_sec_dividends.py
python3 -m ruff check python/marketcow_workers/worker.py \
  tests/test_rust_python_worker.py tests/test_rust_migration_architecture.py
python3 -m py_compile python/marketcow_workers/worker.py \
  tests/test_rust_python_worker.py
```

Observed results:

- Rust workspace: 67 passed, 0 failed, 2 environment-gated real-database tests ignored.
- Rust doc tests: passed.
- Clippy with warnings denied: passed.
- Selected Python tests: 14 passed.
- Ruff and Python bytecode compilation: passed.
- Credential isolation and sliding-window restart-budget tests are included in the 14
  `marketcowd` tests.

The real PostgreSQL 17.10 and ClickHouse 25.8 checks for the implemented repositories are
separately preserved in `phase4-storage-real-instances.json`.

## Independent soak status

The HTTP/network 45-minute soak in `http-shadow-soak-45m.json` remains a failed gate
(`passed=false`) because persistence latency peaked at 767,399 microseconds. This milestone
does not reinterpret or replace that negative evidence. A headless soak also cannot replace
the HTTP/network gate.

## Remaining Phase 5 work

- Migrate remaining provider handlers.
- Add explicit per-worker memory/CPU resource enforcement.
- Remove the corresponding Python public API and database-writer dependencies only after
  differential and rollback gates pass.
