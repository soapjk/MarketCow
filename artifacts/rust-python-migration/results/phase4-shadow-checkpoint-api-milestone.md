# Phase 4 shadow migration checkpoint API milestone

Status: verified partial Phase 4 evidence. This wires the PostgreSQL CAS checkpoint repository
to the authenticated Rust management boundary for shadow workflows only. It does not freeze a
Python writer, enable a Rust production writer, permit cutover, or claim Phase 4 completion.

Implementation commit: `5b4dd798ef28acd0470c69beceda1dff31e0cd7f`.

The native control surface provides:

- versioned `marketcow.migration-control.v1` status with the SHA-256 of the checked-in
  `marketcow.domain-ownership.v1` registry;
- authenticated GET/PUT checkpoint operations for an allow-list of migration domains;
- create-at-revision-zero and compare-and-swap updates, with stale revisions rejected as 409;
- typed source/target watermarks, cursor evidence, status, error and server timestamp;
- explicit `cutover_allowed:false`, `real_order_submission_enabled:false`, and
  `tradude_may_manage_marketcow:false` safety fields.

Real-process evidence:

- `phase4-native-instrument-api-differential.json`, SHA-256
  `06f258cd20c6c992949b940156b7f7873548ccf492fa0f1d38749300c614c20d`;
- source commit `5b4dd798ef28acd0470c69beceda1dff31e0cd7f`;
- binary SHA-256 `ffb4fbc2bd1f86e401a2637292ad67678332669d9ba1ef4841ea678f90eb2484`;
- all 32 PostgreSQL/HTTP/MCP/audit/checkpoint/restart gates passed, including revision 1 create,
  revision 2 completion, stale revision conflict, exact GET, ownership hash, and exact checkpoint
  recovery after daemon restart.

Reproduction:

```sh
cargo build -p marketcowd
PYTHONPATH=src python3 scripts/migration/verify_native_instrument_api.py \
  --binary "$PWD/target/debug/marketcow" \
  --expected-binary-sha256 ffb4fbc2bd1f86e401a2637292ad67678332669d9ba1ef4841ea678f90eb2484 \
  --source-commit 5b4dd798ef28acd0470c69beceda1dff31e0cd7f \
  --output "$PWD/artifacts/rust-python-migration/results/phase4-native-instrument-api-differential.json"

cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Regression verdict: Rust workspace 87 passed, 0 failed; 5 explicitly external-instance tests
were skipped. Clippy passed with warnings denied.

Remaining boundary: the checkpoint API records and fences shadow progress, but the technical
plan's writer-freeze, boundary record, single-writer handoff, observation, rollback and Python
credential removal steps remain deliberately disabled until their independent evidence and
operator gates exist. This short real-network verification is not a 45-minute HTTP soak.
