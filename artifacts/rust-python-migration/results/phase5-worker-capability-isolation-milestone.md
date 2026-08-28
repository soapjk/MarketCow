# Phase 5 worker capability isolation milestone

Date: 2026-08-28

Status: partial Phase 5 evidence. This does not claim Phase 5 or the migration is complete.

## Isolation behavior

- Each supervised Python process receives exactly one Rust-registered capability.
- Its nonce-bound UDS hello advertises only that capability.
- Python constructs a handler subset and rejects any unknown or empty capability selection
  before connecting to the daemon.
- The daemon rejects policy entries without a compiled Rust/Python result contract.
- Configured pool size must provide at least one isolated process per capability.
- Slot restarts retain the same capability and its independent dispatch/restart/resource
  policy.
- The process environment remains cleared; no database or admin credential is inherited.

This creates the process boundary required for future provider-specific secret FD/reference
delivery without exposing that secret to unrelated SEC, CSV or other workers.

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

Tests verify a CSV-only process advertises and executes only CSV, while an unimplemented
capability fails before any socket connection. Rust tests verify capability-bound UDS claims,
credential clearing, restart budgets and result registration.

Real-order submission remains disabled. Tradude does not manage MarketCow lifecycle.
