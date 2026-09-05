# Rust + Python migration phase status

Updated 2026-09-04. Rust migration is scoped to performance-sensitive realtime paths; historical
data, MCP and low-frequency control-plane modules may remain Python. The scoped gap matrix is in
`docs/architecture/migration/completion-audit-2026-09-04.md`.

| Phase | Status | Reproducible evidence | Remaining gate |
|---|---|---|---|
| 0 | Scoped complete | `phase0/inventory.json`, ADR, ownership, baseline, corpus manifest | additional raw-r2 coverage is a future evidence expansion |
| 1 | Shadow skeleton complete | Rust CLI/config/lifecycle/health/metrics; PostgreSQL and ClickHouse typed connection/migration foundations and real-instance tests; UDS handshake | production cutover is intentionally deferred to Phase 7 |
| 2 | Scoped complete | health/readiness/scope/admin/metrics, auth/audit tests, fail-closed snapshot/events/checkpoint plus network WebSocket full-sync/resume/gap tests; bounded Rust MCP transport with the existing native subset and exact 14-tool loopback staged proxy | historical HTTP and remaining MCP implementations intentionally stay Python and are not a realtime migration gate |
| 3 | Scoped complete | adapter/normalizer, exact decimal, catalog/lifecycle, single writer, immutable ArcSwap projection, grouped append-only WAL, checkpoint/replay, full-sync/events/WS stream, backpressure and fail-closed tests | multi-day production observation remains a release gate |
| 4 | Scoped complete | workload ownership boundaries and hashed registry; append-only hashed runtime config versions; fenced PostgreSQL job state; typed repositories and compatibility tests; disposable SQLite index; Rust content-addressed Artifact promotion | historical/business-data repositories and writers intentionally remain Python; realtime hot paths must not depend on them |
| 5 | Scoped complete | UDS 0600 protocol; leased persistent job state/retry/cancel; per-capability isolation; owner-only secret references via FD 3; Rust-only hash/path/result-schema verification; bounded worker restart/CPU/memory/FD/core limits and health metrics | additional low-frequency Python provider handlers are optional and are not a Rust migration gate |
| 6 | Scoped complete | provider-neutral Rust realtime crate; Python/Rust Hyperliquid and LongPort golden normalization; exact decimals/source time/raw SHA; Rust Hyperliquid transport; grouped sync-before-publish WAL; single-writer lock; checkpoint fallback; ArcSwap hub; bounded replay/backpressure; sparse WAL index; unified stream and recovery tests; passing deterministic performance gates | native LongPort SDK and closed-segment manifest are optional follow-up optimizations |
| 7 | Release gate, outside refactor | guarded non-mutating scripts and runbook | production cutover, 24-hour/7-day observation and operator approval are operational actions |

Real orders remain disabled and Tradude lifecycle management remains prohibited. The guarded
scripts always refuse state changes at this milestone.
