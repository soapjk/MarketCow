# Rust + Python migration phase status

Updated 2026-08-27. This is a shadow milestone, not a production migration completion claim.

| Phase | Status | Reproducible evidence | Remaining gate |
|---|---|---|---|
| 0 | Partial | `phase0/inventory.json`, ADR, ownership, baseline, corpus manifest | raw r2 cursor 24906390–25040506 is absent |
| 1 | Shadow skeleton complete | `cargo test --workspace`; `marketcow doctor`; loopback lifecycle and UDS handshake | real PostgreSQL/ClickHouse connectivity is not implemented |
| 2 | Partial | health/readiness/scope/admin/metrics, auth and audit tests | 117-route gateway proxy/golden compatibility and WS/MCP transport |
| 3 | Offline core partial | deterministic decimal, single writer, immutable ArcSwap projection, WAL/checkpoint/replay/fault tests | full Polymarket adapter/API, raw r2 diff, 60-minute/24-hour production shadow |
| 4 | Boundary only | repository traits; architecture test proves realtime crate has no DB dependency | typed DB implementations and real-instance differential tests |
| 5 | Handshake only | UDS 0600, framed protocol, containment tests | provider handlers, leases, retry/cancel, credential removal |
| 6 | Not started | none | all other realtime providers |
| 7 | Intentionally blocked | guarded non-mutating scripts and runbook | cutover evidence, 24-hour/7-day gates, operator approval |

Real orders remain disabled and Tradude lifecycle management remains prohibited. The guarded
scripts always refuse state changes at this milestone.
