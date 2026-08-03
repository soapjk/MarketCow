# Polymarket live local-candidate quality report

Date: 2026-08-03. Baseline: MarketCow `12fd4cd`.

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
| Backoff | HTTP 429 + `Retry-After` retry path |
| Dynamic subscriptions | deterministic shards and subscribe/unsubscribe diff |
| Lifecycle | content revisions plus `new_market`/`market_resolved` catalog invalidation |
| Binary/negative-risk | two-token frame plus all relation-member requirement |
| Recovery | disconnect gap, full `/books`, new epoch, checkpoint and post-checkpoint replay |
| Cross-process visibility | API starts first; separate writer adds catalog/books; all live reads update |
| Failed-event durability | checkpoint, invalid/missing/out-of-order event, restart remains fail closed |
| Event integrity | continuous cursor plus full event identity and canonical/raw hash tamper rejection |
| Book correctness | decimal, tick, checksum, duplicate, out-of-order, crossed update rejection |
| Resume/retention | cursor pages, `has_more`, expired-cursor failure, bounded replay storage |
| Long stability | repeated event application with bounded in-memory retention |
| Public facts | wallet/profile/transaction provenance and float rejection |
| API/OpenAPI | bootstrap, snapshot, events, checkpoint, health, gaps, public data |
| Source policy | official public endpoints only; commercial/trial dependencies absent |

The final Artifact records exact focused/full test counts, lint, build, commit, and
worktree status after verification.
