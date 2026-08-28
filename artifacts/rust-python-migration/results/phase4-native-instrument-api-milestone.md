# Phase 4 native Instrument API milestone

Date: 2026-08-28

Status: partial Phase 2/4 evidence. Rust now owns `get_instrument` over public HTTP and MCP,
but Instrument resolve/admin writes and the other 12 legacy MCP tools are not migrated.

Implementation commits: `d0f066ed3edf0504b624275f5d6888fd94138d1b` and
`3edf374ab86b48bb3a00d10cc4f1eacd26854a3c`.

## Boundary and compatibility

- `GET /v1/instruments/{instrument_id}` and MCP `get_instrument` read the same typed
  PostgreSQL repository. PostgreSQL absence/failure returns a bounded 503 and never falls back to
  the legacy Python proxy.
- Missing IDs preserve the public `instrument_not_found` 404 detail. Invalid IDs and repository
  failures are explicit machine-readable errors.
- MCP tools/list replaces the staged Python definition with a Rust definition that is golden-equal
  to Python. Calls validate the same required/extra argument rules and preserve both structured
  JSON and byte-compatible `content[].text` field order.
- Decimal values remain strings from PostgreSQL `NUMERIC` through HTTP and MCP. The checked record
  preserves `0.0100` and `0.00000001` exactly.
- `search_instruments` remains staged/provider-backed; it was deliberately not replaced with a
  database search of different semantics.
- Real-order submission remains disabled. Tradude does not manage MarketCow lifecycle.

## Reproducible verification

```sh
cargo build -p marketcowd
shasum -a 256 target/debug/marketcow
uv run --isolated --frozen python scripts/migration/verify_native_instrument_api.py \
  --binary target/debug/marketcow \
  --expected-binary-sha256 \
    46637d46531bdabe8d86558ac3ccfa7d15b043682f9a3f47ccaa193d90134559 \
  --source-commit 3edf374ab86b48bb3a00d10cc4f1eacd26854a3c \
  --output artifacts/rust-python-migration/results/phase4-native-instrument-api-differential.json
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
uv run --isolated --frozen python -m unittest -v tests.test_mcp_server
uv run --isolated --frozen python -m ruff check \
  scripts/migration/verify_native_instrument_api.py tests/test_mcp_server.py
cargo fmt --all -- --check
```

The differential started isolated PostgreSQL and two consecutive real MarketCow processes on
loopback TCP. All 13 recorded gates passed: exact HTTP record, machine-readable 404, Python-equal
MCP definition and result, persistence health, three unique Rust migrations, and same-record read
after restart. Both processes exited cleanly with code 0.

Workspace regression reported 80 passed, 0 failed and 3 explicit environment-gated storage tests
ignored. All 17 Python MCP tests, Clippy with warnings denied, Ruff and format checks passed.

The checked binary SHA-256 is
`46637d46531bdabe8d86558ac3ccfa7d15b043682f9a3f47ccaa193d90134559`. The result JSON SHA-256
is `729e1d54260e1b1eac0d974c797bedb77cb9a1d46ade86ea2745d2ae20c6a659`.
This short integration result is not a headless or HTTP/network soak and does not satisfy the
remaining longevity gates.
