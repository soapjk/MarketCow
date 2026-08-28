# Phase 4 PostgreSQL Instrument repository milestone

Date: 2026-08-28

Status: partial Phase 4 evidence. This proves the Rust-owned persistence boundary only; native
public API/MCP integration and retirement of the corresponding Python writer remain open.

Implementation commit: `33fff78c9e33b125ddfe247047e418dc26ff6719`.

## Contract and safety

- The safe-forward migration is checksum-pinned, transactionally fenced by a PostgreSQL
  advisory lock, idempotent, and compatible with the final Python Instrument schema.
- Price tick, size increment and lot size use `rust_decimal::Decimal`; PostgreSQL receives exact
  decimal strings and parses them as `NUMERIC`, with no binary floating-point conversion.
- Each upsert validates the frozen Instrument domain, ISO-like identifiers, precision bounds,
  positive increments, timestamps, mapping namespaces and content hash before opening a write
  transaction.
- Provider and broker symbol mappings are conflict-checked and replaced atomically with the
  Instrument row. A mapping already owned by another Instrument fails closed.
- Real-order submission remains disabled. Tradude does not manage MarketCow lifecycle.

## Reproducible verification

```sh
cargo fmt --all -- --check
cargo test -p marketcow-storage
cargo clippy -p marketcow-storage --all-targets -- -D warnings
scripts/migration/verify_instrument_postgres.sh \
  "$PWD/artifacts/rust-python-migration/results/phase4-instrument-postgres-real.json"
shasum -a 256 \
  artifacts/rust-python-migration/results/phase4-instrument-postgres-real.json
```

The isolated real-instance test used PostgreSQL 17.10 and passed all five recorded gates:
safe-forward migration idempotence, exact decimal round trip, provider mapping resolution,
stale mapping removal, and cross-Instrument mapping conflict rejection. The storage unit suite
reported 8 passed, 0 failed, with 3 explicit environment-gated tests ignored; Clippy and format
checks passed.

The checked JSON result has SHA-256
`d127c6bf33e227e2537beed60811a6249ebadfea38c32a6c24554778f7b52d34` and records the exact
implementation commit. Its `passed`, PostgreSQL test, real-order and Tradude lifecycle gates must
all be inspected independently; this milestone does not claim Phase 4 or overall migration
completion.
