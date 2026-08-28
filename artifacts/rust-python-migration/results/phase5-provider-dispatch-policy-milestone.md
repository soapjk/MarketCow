# Phase 5 provider dispatch policy milestone

Date: 2026-08-28

Status: partial Phase 5 evidence. This does not claim Phase 5 or the migration is complete.

## Implemented policy

- Rust owns a complete capability-to-policy map; Python cannot choose its concurrency or
  rate limit.
- Each policy bounds `max_in_flight` to 1–1024 and `minimum_interval_millis` to at most one
  day.
- A worker capability with no configured policy receives no task lease.
- Claimed and running jobs count against the capability's independent concurrency limit.
- Every claim records `claimed_at` in the authoritative versioned job payload.
- The minimum interval considers the latest persisted claim, including terminal jobs and
  retries, so daemon/PostgreSQL recovery cannot reset the rate gate.
- Invalid policy JSON or values fail configuration before service side effects.

Default shadow policies:

| Capability | Maximum in flight | Minimum interval |
|---|---:|---:|
| `transform.sec_dividend_filing` | 1 | 1000 ms |
| `transform.csv_inference` | 2 | 0 ms |

Operators can replace the whole map with
`MARKETCOW_PYTHON_DISPATCH_POLICIES_JSON`. `/v1/health` exposes the effective map without
credentials.

## Reproducible verification

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python3 -m pytest -q \
  tests/test_rust_python_worker.py tests/test_rust_migration_architecture.py \
  tests/test_sec_dividends.py
```

Tests prove concurrent claims stop at the configured bound, dispatch remains blocked until
the exact interval boundary, policy state survives serialize/recover, and absent or invalid
policies fail closed. The actual UDS poll path uses the policy-aware claim operation.

Observed results: Rust workspace 72 passed, 0 failed, with two environment-gated database
tests ignored; selected Python tests 18 passed; Clippy and Ruff passed with warnings denied.

Real-order submission remains disabled. Tradude does not manage MarketCow lifecycle.
