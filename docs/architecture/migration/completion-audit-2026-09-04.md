# Performance-sensitive Rust migration scope audit

Audit date: 2026-09-04 (Asia/Shanghai)
Base revision: `21b38f6`

## Scope decision

Rust migration is required only for performance-sensitive runtime paths. Python remains an
intentional supported implementation for historical data, MCP tools, low-frequency discovery,
administrative workflows and provider-specific transformations unless profiling demonstrates a
material throughput, latency or resource bottleneck.

This scope replaces the earlier assumption that every public route, storage repository and MCP
tool had to become native Rust. A Python module is therefore not migration debt merely because it
is Python. It is debt only when it remains on a declared realtime hot path, duplicates an active
authoritative realtime implementation, or violates a frozen boundary.

## Compatibility requirements

The scoped migration must preserve:

- exact decimal values, provider/source timestamps, instrument identity and raw evidence hashes;
- WebSocket cursor continuity, bounded replay, explicit resync and fail-closed gap semantics;
- sync-before-publish WAL durability and exactly one authoritative writer per realtime domain;
- bounded queues, backpressure, reconnect/retry budgets and deterministic shutdown;
- the existing Python HTTP and MCP contracts at the Rust/Python boundary;
- no PostgreSQL, ClickHouse, SQLite, MCP or historical-data dependency in a realtime hot path;
- real order submission remaining disabled.

## Ownership boundary and remaining work

| Domain | Final boundary | Completion evidence | Status |
|---|---|---|---|
| Polymarket realtime normalization, WAL and projection | Rust owns the entire hot path | Rust adapter, grouped sync-before-publish WAL, checkpoint/replay, local recovery, bounded stream and fail-closed tests | Complete |
| Hyperliquid realtime processing | Rust transport, normalizer, durable hub and public fan-out | Allow-listed WSS transport, provider-neutral hub, bounded replay, reconnect and unified-stream tests | Complete |
| LongPort realtime processing | Python may own the SDK callback lifecycle and emit bounded typed frames; Rust owns validation, normalization and durable-hub primitives | Shared Python/Rust golden, exact raw hashes/decimals, session/sequence validation and durable-hub batch/replay tests | Complete for the agreed boundary |
| Unified realtime WebSocket | Rust for migrated high-throughput streams | Full-sync/resume/resync, filtering, watermarks, backpressure and slow-consumer tests | Complete |
| WAL startup/recovery | Rust hash-chained WAL, checkpoint v2 and sparse index | Corruption, restart, rollback, sparse lookup/rebuild and fallback tests | Complete; a closed-segment manifest is a future optimization, not migration scope |
| Historical bars, fundamentals, dividends and calendars | Python | Existing APIs, repositories and regression tests | Retained by design |
| MCP | Python, with the existing optional Rust-native subset | Frozen 14-tool read-only contract and staged proxy tests | Retained by design |
| Discovery v3 and low-frequency catalog refresh | Python | Existing lifecycle/materialization and typed Rust catalog boundary | Retained by design |
| EastMoney/Sina polling | Python | Existing bounded polling providers | Retained by design |
| PostgreSQL/ClickHouse business-data writers | Python outside realtime hot paths | Ownership registry and hot-path dependency tests | Retained by design |

## Reproducible checks

```bash
# Machine-readable ownership and phase evidence
sed -n '1,220p' docs/architecture/migration/domain-ownership.yaml
sed -n '1,180p' artifacts/rust-python-migration/phase-status.md

# Ensure realtime code does not call historical/MCP/storage request paths
rg -n 'MCP|get_market_bars|fundamental|dividend|ClickHouse|Postgres|SQLite' \
  crates/marketcow-realtime crates/marketcow-polymarket crates/marketcow-runtime

# Rust quality and compatibility
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --no-fail-fast

# Python boundary regression in a clean environment
task_venv=$(mktemp -d /tmp/marketcow-rust-migration-venv.XXXXXX)
UV_PROJECT_ENVIRONMENT="$task_venv" uv sync --frozen
MARKETCOW_HOME=$(mktemp -d /tmp/marketcow-test-home.XXXXXX) \
  "$task_venv/bin/python" -m unittest discover -s tests -q
"$task_venv/bin/ruff" check src tests scripts
```

Local completion verification on 2026-09-04: Rust 198 passed and 5 external
storage tests skipped; Python 766 passed and 21 externally gated tests skipped. Formatting,
Clippy with warnings denied, Ruff, bytecode compilation and frozen-lock installation passed.

Performance evidence is `artifacts/rust-python-migration/results/shadow-soak-smoke-v10-release.json`:
100 markets/200 books, four readers, two consumers and 200 token updates/second; all declared gates
passed, apply p99 was 11.867 ms, WAL p99 10.140 ms, publication p99 9 us, readiness p99 11.831 ms,
cursor lag/disconnects/gaps/queue depth were zero, and maximum RSS was 136432 KiB. This bounded
deterministic run proves the migration increment has no obvious local hot-path regression; longer
production observation remains a release/operations gate rather than part of this scoped refactor.

## Acceptance consequence

Under the user-confirmed performance-sensitive boundary, every required migration row above is
complete. Historical/MCP Python code, a native LongPort SDK client, a closed-segment WAL manifest,
raw-r2 expansion and multi-day production observation are explicitly outside this work item; they
must not be presented as defects in this scoped completion.

## Acceptance mapping

1. **Scope recorded:** this document and `domain-ownership.yaml` define the Rust realtime hot path
   and intentionally retained Python modules.
2. **Agreed modules complete:** Polymarket, Hyperliquid and the provider-neutral/LongPort realtime
   processing primitives contain no placeholder or unhandled TODO; optional follow-ups are outside
   the confirmed boundary.
3. **Compatibility:** shared golden fixtures plus snapshot, event, cursor, WebSocket, decimal,
   sequence, raw-hash and error-path tests cover the upstream/downstream contracts.
4. **Operations:** startup validation, lifecycle audit, bounded queues, timeouts, reconnect/retry
   budgets, checkpoints, shutdown and resource-limit tests passed.
5. **Automated tests:** final Python result is 766 passed/21 external skips; final Rust result is
   198 passed/5 non-realtime business-storage integration skips.
6. **Rust quality:** `cargo fmt`, strict Clippy and the complete workspace test command passed.
7. **Cleanup:** no Python fallback is allowed for the Rust-owned Polymarket realtime writer; code
   retained for historical/MCP/SDK boundaries is intentional rather than duplicate hot-path code.
8. **Performance:** the pinned v10 deterministic soak passed every gate with the metrics above and
   the Python visualization bounded-publish/replay performance test also passed.
9. **Documentation:** ownership, phase status, configuration/run commands, verification results and
   the migration boundary are updated in the repository.
