# Rust + Python migration phase status

Updated 2026-08-28. This is a shadow milestone, not a production migration completion claim.

| Phase | Status | Reproducible evidence | Remaining gate |
|---|---|---|---|
| 0 | Partial | `phase0/inventory.json`, ADR, ownership, baseline, corpus manifest | raw r2 cursor 24906390–25040506 is absent |
| 1 | Shadow skeleton complete | Rust CLI/config/lifecycle/health/metrics; PostgreSQL and ClickHouse typed connection/migration foundations; UDS handshake | real PostgreSQL/ClickHouse integration tests remain environment-gated |
| 2 | Partial | health/readiness/scope/admin/metrics, auth/audit tests, fail-closed snapshot/events/checkpoint plus network WebSocket full-sync/resume/gap tests; bounded Rust MCP transport with native `service_health` and `get_instrument`, exact 14-tool loopback staged proxy, Python/Rust golden definitions and cross-process real-TCP differential calls | remaining legacy HTTP route compatibility, native/worker-backed replacement of 12 MCP tools and removal of the Python public API proxy |
| 3 | Polymarket sample partial | adapter/normalizer, exact decimal, catalog/lifecycle, single writer, immutable ArcSwap projection, grouped append-only WAL, checkpoint/replay, full-sync/events/WS stream, backpressure and fail-closed tests | raw r2 differential corpus; passing scheduled 45/60-minute and 24-hour shadow/network gates |
| 4 | Partial | workload ownership boundaries; fenced PostgreSQL job state; typed PostgreSQL Instrument repository with exact decimal semantics and atomic symbol mapping replacement; native HTTP/MCP `get_instrument` and public mapping resolve with real-process PostgreSQL/TCP differential and restart recovery; typed ClickHouse quote repository; disposable SQLite index; Rust content-addressed Artifact promotion and transactional job/manifest registration; PostgreSQL 17.10 and ClickHouse 25.8 real-instance tests for implemented repositories | native Instrument admin write and batch resolve paths; config/audit and remaining storage domains; broader Python/Rust differential tests; disable corresponding Python writers |
| 5 | Partial | UDS 0600 protocol; leased persistent job state/retry/cancel; persistent per-capability concurrency/minimum-interval dispatch policy and one-capability-per-process isolation; capability-specific owner-only secret references delivered only as inherited FD 3; pre-promotion lease authorization; Rust-only hash/path/result-schema verification, promotion and transactional registration; full Python poll/start/complete/fail loop; deterministic SEC filing and bounded CSV inference handlers; Rust-owned optional worker pool with cleared credentials, bounded restart budgets, CPU/memory/FD/core limits and health/metrics | remaining provider handlers and deletion of Python public API dependency |
| 6 | Not started | none | all other realtime providers |
| 7 | Intentionally blocked | guarded non-mutating scripts and runbook | cutover evidence, 24-hour/7-day gates, operator approval |

Real orders remain disabled and Tradude lifecycle management remains prohibited. The guarded
scripts always refuse state changes at this milestone.
