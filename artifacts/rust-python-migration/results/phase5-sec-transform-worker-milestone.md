# Phase 5 SEC filing transform worker milestone

Status: locally implemented and verified at commit
`74135bc3e630b3c7841f69f5d024bcc0365cb6a3`. Phase 5 remains partial.

## Implemented contract

- Capability: `transform.sec_dividend_filing`.
- Request schema: `marketcow.worker.transform.sec-dividend-filing.v1`.
- Result schema: `marketcow.worker.transform.sec-dividend-filing-result.v1`.
- Transport: length-prefixed `marketcow.worker.v1` over a daemon-owned UDS.
- Lifecycle: nonce-bound hello, capability advertisement, poll, leased task, start, handler execution,
  content-addressed staging response, complete or classified fail.
- The worker validates message correlation, request SHA-256, deadline, exact request keys and staging
  containment. It writes canonical JSON with `allow_nan=false`, fsyncs the temporary file and uses an
  atomic rename before reporting SHA-256 and size.
- SEC dividend amounts remain decimal strings with explicit USD currency and regulatory-filing
  provenance.
- Input validation errors are terminal; timeouts and provider execution errors are reported as
  retryable. The worker never mutates job state directly: Rust accepts or rejects every transition.

The worker has no public listener, PostgreSQL/ClickHouse client, migration authority, WAL access or
API authorization role. Real orders remain disabled and Tradude does not manage MarketCow.

## Reproducible verification

```sh
cargo test --workspace
# 65 passed, 0 failed, 2 ignored; includes Rust UDS and TCP WebSocket tests

cargo clippy --workspace --all-targets -- -D warnings
# passed

PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python3 -m pytest -q \
  tests/test_rust_python_worker.py tests/test_rust_migration_architecture.py \
  tests/test_sec_dividends.py
# 14 passed

PYTHONPATH=src python3 -m ruff check \
  python/marketcow_workers/worker.py tests/test_rust_python_worker.py \
  tests/test_rust_migration_architecture.py
# passed

python3 -m py_compile python/marketcow_workers/worker.py
# passed
```

The protocol integration test runs an actual AF_UNIX server and verifies the generated staging body,
reported SHA-256 and exact decimal result before returning a Rust-shaped `succeeded` response.

## Remaining Phase 5 gates

- Replace inline raw filing input with Rust-registered input Artifact references for large documents.
- Implement remaining structured, filings, history and transform handlers.
- Add Rust worker pool supervision, concurrency/memory limits and bounded restart budgets.
- Remove Python production database credentials and public API dependencies after corresponding
  domains cut over.
- Run fault injection against worker crash, deadline expiry and daemon restart with PostgreSQL-backed
  leases.

No phase or acceptance criterion is claimed complete by this milestone.
