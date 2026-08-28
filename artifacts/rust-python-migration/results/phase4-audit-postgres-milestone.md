# Phase 4 append-only admin audit milestone

Status: verified partial Phase 4 evidence. This milestone does not claim Phase 4 or the
Rust + Python migration is complete, and it does not replace either headless or HTTP/network
long-duration soak evidence.

Implementation commits:

- `19ef1c23d01759ad2bf201eb1e79b939f0400221` — typed PostgreSQL audit repository,
  safe-forward checksum migration, bounded queries, exact duplicate idempotency, conflict
  rejection, and database triggers that reject update/delete/truncate.
- `222b729625738a560afa81062ddb0b65fa62ff75` — `marketcowd` dual persistence and
  fail-closed management request boundary. A management request must durably append its
  `accepted` event to the local JSONL copy and PostgreSQL before the handler runs; rejected
  authentication and final handler outcomes are also appended. Health reports audit
  persistence independently.
- `6f98b230cc437d32483313ce48fdf024667f626d` — authenticated, bounded native
  `GET /v1/admin/audit` with Python-compatible schema/filter/page semantics and a local-only
  development fallback that explicitly reports `durable: false`.

Reproducible results:

- `phase4-audit-postgres-real.json`, SHA-256
  `ed1decdd095341bab1b7b7cc217f14f7f386b1a9686503fba9fbed4cd95102de`.
  PostgreSQL 17.10 proved migration idempotency, schema-v1 Unicode/decimal-string round trip,
  exact retry idempotency, conflicting identity rejection, bounded filtering/pagination, and
  database-enforced append-only update/delete rejection.
- `phase4-native-instrument-api-differential.json`, SHA-256
  `204a9753e4509dc60622e3f37b8f635e6d818877edf8d1c5636865f88ddefd95`.
  Exact binary SHA-256
  `c4e51c9b3050fa3d7bb9ed52b81ac196b1b6b80093d2664430af86d71ec53885`
  from source commit `6f98b230cc437d32483313ce48fdf024667f626d` passed all 26 real-process
  PostgreSQL/HTTP/MCP/restart gates. The audit-specific gates prove healthy PostgreSQL audit,
  rejected/accepted/succeeded events, exact equality of local and PostgreSQL admin audit IDs,
  and the native read model's compatibility with the Python contract.

Commands:

```sh
scripts/migration/verify_audit_postgres.sh \
  "$PWD/artifacts/rust-python-migration/results/phase4-audit-postgres-real.json"

cargo build -p marketcowd
PYTHONPATH=src python3 scripts/migration/verify_native_instrument_api.py \
  --binary "$PWD/target/debug/marketcow" \
  --expected-binary-sha256 c4e51c9b3050fa3d7bb9ed52b81ac196b1b6b80093d2664430af86d71ec53885 \
  --source-commit 6f98b230cc437d32483313ce48fdf024667f626d \
  --output "$PWD/artifacts/rust-python-migration/results/phase4-native-instrument-api-differential.json"

cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
uv run --isolated --frozen python -m unittest \
  tests.test_postgres_repositories tests.test_mcp_server
```

Regression verdicts: Rust workspace 86 passed, 0 failed, with 5 explicitly external-instance
tests ignored; Clippy passed with warnings denied; Python 27 passed, 0 failed, with 18
environment-gated skips. The repository `.venv` symlink is currently self-referential, so the
first system-Python attempt failed before test execution due to missing `psycopg` and `pypdf`;
the recorded passing command uses the frozen lock in an isolated environment. `shellcheck` was
not installed; `sh -n scripts/migration/verify_audit_postgres.sh` passed.

Safety and remaining boundaries:

- `real_order_submission_enabled` remained `false`; Tradude did not start, stop, restart, or
  otherwise manage MarketCow.
- Local append-only JSONL remains a durable admin audit copy. PostgreSQL is required in the
  production profile and a PostgreSQL audit failure prevents management handlers from running.
- The generic non-admin request access log remains local-only. Transaction coupling between
  individual domain mutations and their final outcome event, the remaining storage domains,
  Python writer disablement, and wider differential tests remain incomplete.
- This evidence is a short real-network integration run, not the required 45-minute HTTP shadow
  soak and not a substitute for it.
