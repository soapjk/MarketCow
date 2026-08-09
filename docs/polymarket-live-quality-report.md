# Polymarket live local-candidate quality report

Date: 2026-08-09. Baseline: MarketCow `48a8055`.

## Delivered scope

- Gamma keyset full-market catalog with bounded rate-limit backoff and cursor-loop guard.
- Canonical identities, metadata revisions, explicit binary and cross-market standard
  negative-risk relations, lifecycle/rules/fee completeness.
- Public WebSocket sharding, dynamic subscriptions, custom lifecycle events, PING/PONG,
  reconnect loop, and `/books` recovery into a new epoch.
- Decimal-safe book state, append-only raw/canonical evidence, hashes, checkpoint,
  replay cursor, health, coverage, gap ledger, and consistent fail-closed frames.
- Public Data API trade/activity/position/holder normalization with profile and privacy
  semantics preserved.
- Seven versioned, OpenAPI-discoverable Tradude read paths.
- A single-writer/durable-tail reader boundary, so a running FastAPI process sees
  collector writes without restart.
- Breaking `marketcow.polymarket.live.v2` Nautilus facts, complete typed fee schedules,
  and YES-only standard negative-risk relations with reversible YES/NO pairs.
- Shared typed-fact missing-field derivation for invalid instrument and fee intervals.
- Hash-verified terminal Gamma spool reuse after normalization/publication failure.
- Partial `/books` coverage keeps affected frames closed without suppressing unrelated
  complete markets or preventing the catalog/API from starting.
- Immutable catalog offset indexing and a mutable derived latest-state/event-offset WAL
  index make all main-API live reads explicitly scoped and bounded.
- Explicit offline catalog/state migrations validate all source hashes and publish only
  after deterministic recovery; request paths never trigger full recovery.

## Explicit limits

The public market channel has no globally continuous official sequence. The candidate
does not claim one. `deterministic_normalized` means application order inside a local
epoch, while disconnect windows are gaps closed only by full REST snapshots.

The candidate does not run an always-on production recorder, publish data, place orders,
use the authenticated user channel, or identify private/unfilled-order owners. Public
profile text is provenance, not proof of legal identity. Rules, fees, relations, or
books that are absent or ambiguous stop frame readiness.

## Verification matrix

| Boundary | Regression evidence |
|---|---|
| Complete discovery | multiple keyset pages, `after_cursor`, no offset, cursor loop/error bounds |
| Unbounded traversal | 1,005-page regression terminates only on server cursor exhaustion |
| Real Gamma traversal | 1,270 pages; 126,981 markets; terminal cursor; 700.262s; 0 retries |
| Fetch memory | page-bounded JSONL spool plus SQLite cursor/page/market uniqueness ledger |
| Observability | periodic page/count/elapsed/retry/cursor progress and terminal byte/hash evidence |
| Backoff | HTTP 429 + `Retry-After` retry path |
| HTTP reuse | one persistent session for the full traversal |
| Dynamic subscriptions | deterministic shards and subscribe/unsubscribe diff |
| Lifecycle | content revisions plus `new_market`/`market_resolved` catalog invalidation |
| Binary/negative-risk | two-token frame plus all relation-member requirement |
| Nautilus bootstrap | typed currency/time/price-size/minimum facts and reversible identity |
| Typed fees | ID/version/currency/rates/formula/exponent/quantum/rounding/effective provenance |
| Negative-risk solver | YES-only member set plus explicit YES/NO outcome pairs |
| Missing business facts | named incomplete fields and stable fail-closed reason codes |
| Invalid fact intervals | equal/reversed instrument and fee intervals normalize incomplete without catalog failure |
| Consumer fixture | binary + three-outcome negative-risk bootstrap/snapshot/resume flow |
| Recovery | disconnect gap, full `/books`, new epoch, checkpoint and post-checkpoint replay |
| Cross-process visibility | API starts first; separate writer adds catalog/books; all live reads update |
| Failed-event durability | checkpoint, invalid/missing/out-of-order event, restart remains fail closed |
| Event integrity | continuous cursor plus full event identity and canonical/raw hash tamper rejection |
| Book correctness | decimal, tick, checksum, duplicate, out-of-order, crossed update rejection |
| Resume/retention | cursor pages, `has_more`, expired-cursor failure, bounded replay storage |
| Long stability | repeated event application with bounded in-memory retention |
| Current-scale startup | 253,962-token upper bound; 508 ≤500-item messages; 32 WS groups |
| Atomic restart | failed partial refresh preserves the prior revision across process recovery |
| Verified retry | terminal spool endpoint/params/schema/count/size/hash validation and no-network reuse |
| Partial book coverage | missing tokens remain gaps/degraded while complete two-token markets become ready |
| Public facts | wallet/profile/transaction provenance and float rejection |
| API/OpenAPI | bootstrap, snapshot, events, checkpoint, health, gaps, public data |
| Scoped indexed reads | 1–100 markets; row offsets, one state snapshot transaction, event seeks |
| Index failure policy | legacy/missing, lagging, ahead, revision mismatch and payload/event tamper |
| Offline migration | deterministic catalog and checkpoint/event state index rebuild |
| Startup isolation | app construction and lightweight health do not deserialize catalog/replay events |
| Source policy | official public endpoints only; commercial/trial dependencies absent |

The final local handoff records exact focused/full test counts, lint, build, commit,
and worktree status after verification.

## Indexed-recovery verification

The 2026-08-09 indexed-recovery candidate passed 53 focused Polymarket live tests and
the complete 605-test MarketCow suite with zero failures/errors (21 existing skips).
Repository-wide Ruff, `git diff --check`, Python script compilation, and
`uv build --offline` all passed. The focused matrix includes the 100-market hard bound,
event-before-index crash window, cross-instance visibility, active recovery, coverage
gaps, checkpoint restart, state/catalog revision binding, payload/event tamper, and
offline deterministic index rebuild. Production data and the running service were not
modified or restarted; the included read-only measurement command is the handoff for
an operator-approved production-like latency/RSS run.
