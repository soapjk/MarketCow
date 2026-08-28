# Phase 4 native Instrument API milestone

Date: 2026-08-28

Status: partial Phase 2/4 evidence. Rust now owns `get_instrument` over public HTTP and MCP plus
public provider/broker mapping resolve and authenticated single-Instrument administration, but
batch resolution/provider fallback and the other 12 legacy MCP tools are not migrated.

Implementation commits: `d0f066ed3edf0504b624275f5d6888fd94138d1b`,
`3edf374ab86b48bb3a00d10cc4f1eacd26854a3c`, and
`7a3206aadf2f8c2c7a35939bfde53202f03a5d6d`, and
`a14b98916176896cd39a40bbb03ccea70b75ef61`, plus control-plane integration commits
`b2c40a27f4ef53220c225cc4053fb17e4a4a716d` and
`b42d6862a1452cf9b5ff23e5ea7a390ba3b24512`.

## Boundary and compatibility

- `GET /v1/instruments/{instrument_id}` and MCP `get_instrument` read the same typed
  PostgreSQL repository. PostgreSQL absence/failure returns a bounded 503 and never falls back to
  the legacy Python proxy.
- Missing IDs preserve the public `instrument_not_found` 404 detail. Invalid IDs and repository
  failures are explicit machine-readable errors.
- `GET /v1/instruments:resolve` uses the atomic provider/broker mapping table, preserves the
  `instrument_mapping_not_found` contract, and survives a process restart without rebuilding from
  SQLite or querying it on the hot path.
- `PUT /v1/admin/instruments/{instrument_id}` uses the existing constant-time Bearer gate,
  rejects path/payload conflicts, validates the typed financial contract, generates the same
  sorted ASCII canonical hash as Python (including Unicode surrogate escaping), and commits the
  Instrument plus mappings in one PostgreSQL transaction.
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
    d6ed3a9a52a868da08bf0f51ae2a54559cf72a590aacf2afe6ffbb2ce697af3d \
  --source-commit b42d6862a1452cf9b5ff23e5ea7a390ba3b24512 \
  --output artifacts/rust-python-migration/results/phase4-native-instrument-api-differential.json
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
uv run --isolated --frozen python -m unittest -v tests.test_mcp_server
uv run --isolated --frozen python -m ruff check \
  scripts/migration/verify_native_instrument_api.py tests/test_mcp_server.py
cargo fmt --all -- --check
```

The differential started isolated PostgreSQL and two consecutive real MarketCow processes on
loopback TCP. All 22 recorded gates passed: Bearer rejection and authenticated admin write with
Python-equal content hash, exact HTTP record, exact mapping resolution,
machine-readable missing-ID and missing-mapping 404s, Python-equal MCP definition and result,
persistence health, four unique Rust migrations, hashed config revision, idempotent runtime config
registration, and same-record/mapping reads after restart. Both processes exited cleanly with code
0.

Workspace regression reported 84 passed, 0 failed and 4 explicit environment-gated storage tests
ignored. All 17 Python MCP tests, Clippy with warnings denied, Ruff and format checks passed.

The checked binary SHA-256 is
`d6ed3a9a52a868da08bf0f51ae2a54559cf72a590aacf2afe6ffbb2ce697af3d`. The result JSON SHA-256
is `d8ae0c1840a3af7f2d57c2f80311a9931cb166d0fa67d51ef02e9a24ed710ee0`.
This short integration result is not a headless or HTTP/network soak and does not satisfy the
remaining longevity gates.
