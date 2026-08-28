# Phase 4/5 UDS-backed Instrument batch resolution milestone

Date: 2026-08-28

Status: verified partial Phase 4/5 evidence. Rust now owns the public ordered batch-resolution
boundary and can delegate missing LongPort symbols to an explicitly enabled, non-public Python
UDS worker. This milestone does not claim Phase 4, Phase 5, or the overall migration complete.

Implementation commit: `ac04d58e34f7bbc827baca8f25b187f38fabfb0d`.

## Implemented boundary

- `POST /v1/instruments:resolve/query` validates the versioned request, preserves input order,
  resolves registry hits first, and returns bounded per-item errors for unsupported or unavailable
  providers.
- LongPort misses use the durable leased-job and content-addressed Artifact path. Rust validates the
  worker result schema and remains the only component allowed to persist Instrument Master records
  and mappings.
- The Python worker is opt-in, communicates only over owner-mode UDS, exposes no public port, and
  receives only LongPort credentials through inherited file descriptor 3. It receives no database
  or management credentials.
- Price ticks and lot sizes remain decimal strings through the worker contract and Rust repository;
  no binary floating point is introduced for financial values.
- An unconfigured or unavailable worker fails closed with `provider_unavailable`; it never falls
  through to the legacy Python public API.

## Reproducible evidence

- `phase4-instrument-batch-worker-differential.json`, SHA-256
  `329a06a897048cc353a6647aea9741674c9476973a21aeeed3f8ddb9cf8ab554`.
- Exact source commit `ac04d58e34f7bbc827baca8f25b187f38fabfb0d` and binary SHA-256
  `62625e9db4e24cebacc462adb8ce1b9ddf60125333e442becb475219ef8a4aee`.
- All 33 real-process PostgreSQL/HTTP/MCP/audit/checkpoint/restart gates passed, including the
  ordered batch registry-hit plus worker-unavailable contract. Both daemon processes exited 0.
- Rust workspace: 89 passed, 0 failed, 5 explicitly external-instance tests ignored. Workspace
  Clippy passed with warnings denied.
- Python focused regression: 31 passed, 0 failed. Worker-only tests: 14 passed, 0 failed.
- The real PostgreSQL durable-job repository round-trip test passed independently.

Commands:

```sh
cargo build -p marketcowd
shasum -a 256 target/debug/marketcow
PYTHONPATH=src python3 scripts/migration/verify_native_instrument_api.py \
  --binary "$PWD/target/debug/marketcow" \
  --expected-binary-sha256 62625e9db4e24cebacc462adb8ce1b9ddf60125333e442becb475219ef8a4aee \
  --source-commit ac04d58e34f7bbc827baca8f25b187f38fabfb0d \
  --output "$PWD/artifacts/rust-python-migration/results/phase4-instrument-batch-worker-differential.json"

cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
PYTHONPATH=src:. uv run --isolated --frozen --with pytest python -m pytest -q \
  tests/test_rust_python_worker.py tests/test_instrument_resolution.py \
  tests/test_market_data_api.py
PYTHONPATH=src:. uv run --isolated --frozen --with pytest python -m pytest -q \
  tests/test_rust_python_worker.py
cargo test -p marketcow-storage \
  postgres_job_repository_round_trip_when_test_dsn_is_configured -- --exact
```

## Safety and remaining gates

- `real_order_submission_enabled` is false; Tradude did not start, stop, restart, or otherwise
  manage MarketCow.
- `headless_substitutes_http_network_soak` is false. This short differential is not a headless or
  HTTP/network duration soak and cannot replace either longevity gate.
- No real LongPort credential or external-network call was used, so real upstream exchange-to-MIC
  mapping and provider failure behavior still require credentialed differential and fault-injection
  evidence.
- The legacy Python public endpoint and writer remain present. Production dispatch is disabled by
  default, cutover is prohibited, and writer freeze/single-writer handoff remain incomplete.
