# Phase 5 CSV inference worker milestone

Date: 2026-08-28

Status: partial Phase 5 evidence. This does not claim Phase 5 or the migration is complete.

## Contract

- Job type: `transform.csv_inference`
- Request schema: `marketcow.worker.transform.csv-inference.v1`
- Result schema: `marketcow.worker.transform.csv-inference-result.v1`
- Input carries CSV content, source locator and timezone-aware observation time.
- Output carries immutable input SHA-256, source, normalized UTC observation time, detected
  delimiter, ordered columns and string-valued rows.

## Financial-data safety

- Values are never coerced to binary floating point; exact decimal text is preserved.
- Detection is restricted to comma, tab, semicolon and pipe delimiters.
- Empty input, NUL bytes, duplicate/empty headers, inconsistent row width, unsupported
  delimiters, malformed quoting, more than 256 columns or more than 10,000 rows fail closed.
- The handler does not open a network socket, access a database, advance job state or publish
  canonical data.
- Execution occurs through the existing leased, nonce-bound mode-0600 UDS worker protocol;
  Rust verifies and registers the resulting staging Artifact.

## Reproducible verification

```sh
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python3 -m pytest -q \
  tests/test_rust_python_worker.py tests/test_rust_migration_architecture.py \
  tests/test_sec_dividends.py
python3 -m ruff check python/marketcow_workers/worker.py \
  tests/test_rust_python_worker.py tests/test_rust_migration_architecture.py
python3 -m py_compile python/marketcow_workers/worker.py \
  tests/test_rust_python_worker.py
```

Tests cover exact 18-decimal-place text, quoted commas, UTC normalization, source/hash
provenance, malformed shapes and an end-to-end leased UDS task/staging completion.

Real-order submission remains disabled. Tradude does not start, stop or restart MarketCow.
