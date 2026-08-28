# Phase 5 Rust worker-result validation milestone

Date: 2026-08-28

Status: partial Phase 5 evidence. This does not claim Phase 5 or the migration is complete.

## Trust boundary

Python staging files are untrusted. After content-addressed hash/path verification and before
manifest registration or the `succeeded` job transition, Rust now requires:

- a registered `(job_type, request_schema)` result contract;
- `application/json` and a nonzero result no larger than 16 MiB;
- the Artifact dataset/revision to equal the authoritative job type/request schema;
- an exact versioned result schema and bounded shape.

Unknown contracts and any mismatch leave the job nonterminal and unregistered.

## SEC financial validation

- `amount_per_share` must be a positive decimal string, never a JSON number or exponent.
- Precision is bounded to 18 decimal places and parsed with `rust_decimal`.
- Currency must be USD for this SEC handler.
- Fiscal year, ISO dates, confirmation state, SEC source identity, source URL and document ID
  are validated.
- Rows are bounded to 10,000 and may not carry undeclared fields.

## CSV validation

- Source, RFC 3339 observation time and 64-character hexadecimal content hash are required.
- Delimiter is limited to comma, tab, semicolon or pipe.
- Headers are unique strings; columns are bounded to 256 and rows to 10,000.
- Every row has the declared width, `row_count` matches, and every field remains a string.

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

- Rust workspace: 70 passed, 0 failed, 2 environment-gated real-database tests ignored.
- Clippy with warnings denied: passed.
- Selected Python tests: 18 passed.
- Ruff and Python bytecode compilation: passed.

The UDS integration test proves valid CSV bytes reach content-addressed storage and a
transactional job/manifest success. Negative tests prove an unknown contract, mismatched
row count, JSON-number SEC amount and mismatched Artifact dataset remain rejected.

Real-order submission remains disabled. Tradude does not manage MarketCow lifecycle.
