# Rust + Python migration phase status

Updated 2026-08-28. This is a shadow milestone, not a production migration completion claim.

| Phase | Status | Reproducible evidence | Remaining gate |
|---|---|---|---|
| 0 | Partial | `phase0/inventory.json`, ADR, ownership, baseline, corpus manifest | raw r2 cursor 24906390–25040506 is absent |
| 1 | Shadow skeleton complete | `cargo test --workspace`; `marketcow doctor`; loopback lifecycle and UDS handshake | real PostgreSQL/ClickHouse connectivity is not implemented |
| 2 | Partial | health/readiness/scope/admin/metrics, auth/audit tests, fail-closed Rust snapshot/events/checkpoint routes | remaining 114-route gateway proxy/golden compatibility and WS/MCP transport |
| 3 | Offline core partial | exact decimal, single writer, immutable ArcSwap projection, bounded ingress/client queues, raw evidence, segmented WAL restart chain, checkpoint boundary replay and read-model tests | provider adapter/normalizer, bootstrap/full-sync/stream, raw r2 diff, scheduled 45-minute/60-minute/24-hour shadow |
| 4 | Boundary only | canonical storage crate routes hot reads to memory and workload owners to PostgreSQL/ClickHouse/SQLite; architecture tests | typed DB implementations and real-instance differential tests |
| 5 | Handshake only | UDS 0600, framed protocol, containment tests | provider handlers, leases, retry/cancel, credential removal |
| 6 | Not started | none | all other realtime providers |
| 7 | Intentionally blocked | guarded non-mutating scripts and runbook | cutover evidence, 24-hour/7-day gates, operator approval |

Real orders remain disabled and Tradude lifecycle management remains prohibited. The guarded
scripts always refuse state changes at this milestone.
