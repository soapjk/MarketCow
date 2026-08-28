# Rust + Python migration phase status

Updated 2026-08-28. This is a shadow milestone, not a production migration completion claim.

| Phase | Status | Reproducible evidence | Remaining gate |
|---|---|---|---|
| 0 | Partial | `phase0/inventory.json`, ADR, ownership, baseline, corpus manifest | raw r2 cursor 24906390–25040506 is absent |
| 1 | Shadow skeleton complete | Rust CLI/config/lifecycle/health/metrics; PostgreSQL and ClickHouse typed connection/migration foundations; UDS handshake | real PostgreSQL/ClickHouse integration tests remain environment-gated |
| 2 | Partial | health/readiness/scope/admin/metrics, auth/audit tests, fail-closed snapshot/events/checkpoint plus network WebSocket full-sync/resume/gap tests | remaining legacy gateway proxy/golden route compatibility and MCP transport |
| 3 | Polymarket sample partial | adapter/normalizer, exact decimal, catalog/lifecycle, single writer, immutable ArcSwap projection, grouped append-only WAL, checkpoint/replay, full-sync/events/WS stream, backpressure and fail-closed tests | raw r2 differential corpus; passing scheduled 45/60-minute and 24-hour shadow/network gates |
| 4 | Partial | workload ownership boundaries; fenced PostgreSQL job state; typed ClickHouse quote repository; disposable SQLite index; Rust content-addressed Artifact promotion and transactional job/manifest registration | Instrument/config/audit and remaining storage domains; real-instance PostgreSQL/ClickHouse differential tests; disable corresponding Python writers |
| 5 | Partial | UDS 0600 protocol; leased persistent job state/retry/cancel; pre-promotion lease authorization; Rust-only Artifact verification/promotion/registration | provider handlers, worker pool supervision, timeouts, credential removal and deletion of Python public API dependency |
| 6 | Not started | none | all other realtime providers |
| 7 | Intentionally blocked | guarded non-mutating scripts and runbook | cutover evidence, 24-hour/7-day gates, operator approval |

Real orders remain disabled and Tradude lifecycle management remains prohibited. The guarded
scripts always refuse state changes at this milestone.
