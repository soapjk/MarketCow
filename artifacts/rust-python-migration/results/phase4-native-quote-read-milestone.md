# Phase 4 native cached quote read milestone

Date: 2026-08-28

Status: verified partial Phase 2/4 evidence. Rust now owns cache-only
`POST /v1/quotes/query` and MCP `get_quotes` reads from the typed ClickHouse repository. This does
not enable a Rust quote writer, disable the Python quote writer, migrate quote history, or claim
Phase 4 or the overall migration complete.

Implementation commits:

- `ac0bfe29276d7082af1e01551213cded7c1bfd0f` — native public/MCP boundary, typed batch payload
  read, production ClickHouse startup requirement, golden contract, ordered duplicate/missing-item
  behavior, and cache-only fail-closed policy.
- `d55c7b1550bebd685187f9251fdb4c5800f844b4` — corrected the real-instance assertion to compare
  the returned currency with the exact `USDC` fixture written by the test.

## Contract and safety

- Requests contain 1–20 symbols. Response items preserve request order and duplicates; missing
  cached symbols produce bounded per-item `unavailable` errors.
- Stored payload JSON is returned without converting decimal strings to binary floats. The real
  test preserved bid `0.100000000000000001` and currency `USDC`.
- `refresh=true`, explicit provider selection, and `allow_fallback=true` are rejected. ClickHouse
  unavailability returns 503; the route never calls Python, an upstream provider, or SQLite.
- Production requires `MARKETCOW_CLICKHOUSE_DATABASE`; the client remains loopback-bounded by the
  existing typed configuration guard.
- Real orders remained disabled. Tradude did not manage MarketCow lifecycle. This short storage
  integration is neither a headless nor HTTP/network duration soak and cannot replace either.

## Reproducible verification

- `phase4-native-quote-storage-real.json`, SHA-256
  `7d55b929406f90094d1177989304d8b3765e35b3041d053e85a9a39ef032eda4`.
- PostgreSQL 17.10 durable-job regression passed.
- ClickHouse `clickhouse/clickhouse-server:25.8-alpine`, image ID
  `sha256:87e0a5b72f5465b18eacca7c76850e7ff551c9795c50e451f5646299e5e24146`, passed
  quote migration, write, identical retry, typed latest read and batch payload read.
- Rust workspace: 91 passed, 0 failed, 5 explicitly external-instance tests ignored. Workspace
  Clippy passed with warnings denied.
- Python quote/API/MCP differential regression: 47 passed plus 3 subtests. Ruff passed.
- Exact locally built binary SHA-256 at source commit `d55c7b1550bebd685187f9251fdb4c5800f844b4`:
  `0f5716a8e802a9ca5df238f180864f6b11bd12dd9b24ec91d99ad75473e5d568`.

Commands:

```sh
scripts/migration/verify_storage_integrations.sh \
  "$PWD/artifacts/rust-python-migration/results/phase4-native-quote-storage-real.json"
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
PYTHONPATH=src:. uv run --isolated --frozen --with pytest python -m pytest -q \
  tests/test_mcp_server.py tests/test_market_data_api.py \
  tests/test_clickhouse_direct_repository.py
PYTHONPATH=src:. uv run --isolated --frozen python -m ruff check tests/test_mcp_server.py
cargo build -p marketcowd
shasum -a 256 target/debug/marketcow
```

## Preserved failed attempt

The first real ClickHouse run failed one assertion after successfully reading the new batch result:
the fixture wrote currency `USDC`, while the new assertion incorrectly expected `USD`. No passing
JSON was created by that run. Commit `d55c7b1` changed only the assertion to compare with the exact
fixture; the complete storage script was then rerun from scratch and passed. This correction does
not hide a repository failure or relax a gate.

Remaining boundary: quote writes and quote history remain Python-owned, and bar, adjustment factor
and canonical repositories/public APIs are not migrated. Writer freeze, dual-run, cutover and
rollback gates remain disabled pending separate evidence.
