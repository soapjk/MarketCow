# Polymarket market fault isolation v1

Status: implemented candidate, not production-accepted.

## Boundary and users

The consumer is Tradude's read-only opportunity scanner and position monitor. MarketCow owns
market discovery, immutable facts, order books, the append-only event journal, projection health,
and read APIs. Tradude owns orders, fills, positions, capital, settlement tracking, and every
trading decision. MarketCow exposes no order, cancel, signing, wallet, account, or trading-control
operation. Real-order submission remains disabled and Tradude must not manage MarketCow's
lifecycle.

The authoritative inputs are Polymarket CLOB WebSocket frames, independently observed CLOB book
snapshots, the hash-pinned market/catalog/fee registry, and MarketCow's segmented append-only WAL.
All prices, sizes, rates, and quanta retain exact decimal-string semantics. Timestamps are UTC.

## Versions

- full sync and snapshots: `marketcow.polymarket.live.v3`
- scope discovery: `marketcow.polymarket.scope-discovery.v4`
- effective dynamic universe: `marketcow.polymarket.universe.v2`
- WebSocket: `marketcow.market-stream.v3`
- per-market snapshot: `marketcow.polymarket.market-snapshot.v1`

Scope artifacts remain hash-pinned `rust-live-scope.v4` inputs containing a
`marketcow.polymarket.universe.v1` candidate. The daemon publishes the effective v2 view after
atomically overlaying the same-boundary market health projection.

## Gap classification

| Evidence | Action |
| --- | --- |
| Downstream broadcast/cursor discontinuity with a complete verified WAL journal window | Replay every missing cursor from the durable journal, verify continuity, and continue without full sync. |
| Token fault mapped unambiguously to one catalog market, with catalog/tick/fee/relation generation still certain | Persist the fault, quarantine that market, publish a local control frame, and keep independently healthy markets available. |
| Both authoritative token books for a quarantined market pass identity, two-sidedness, tick, fee, relation, and sequence-boundary validation | Publish the recovered market atomically and require the consumer to validate its per-market snapshot before re-enabling it. |
| Unknown token/market ownership, missing WAL range, corrupt WAL, catalog/fee/tick/relation uncertainty, mixed generation, or unprovable atomic boundary | Publish `global_resync_required`, close the stream, and require full sync. |
| Healthy market count below `minimum_market_count` | Mark the whole scope unready and require full sync. |

MarketCow's WAL can repair only a downstream delivery gap for events that MarketCow durably
received. It cannot invent a venue event that never reached MarketCow. An upstream source gap is
therefore repaired with an authoritative two-token market snapshot or remains quarantined. Cursor
fabrication, event skipping, and stale-book continuation are forbidden.

The in-memory verified journal retains the latest 5,000 persisted events and the public broadcast
ring retains 4,096. At the observed 100-market rate this is approximately one minute of bounded
fast replay without retaining hundreds of megabytes of duplicated raw venue payload. Falling
outside that window is explicit cursor expiry and requires full-sync; the complete segmented WAL
remains append-only and authoritative. Ingress applies strict backpressure through a 2,048-batch
queue rather than dropping, fabricating, or silently skipping frames.

The transport's `upstream_connection_boundary` is always global even though the wire adapter emits
one token-shaped audit record per subscription. A token identifier on a connection-wide gap is not
evidence of market-local scope. Only an independently attributable market validation failure or an
explicitly injected market-local loss may enter the quarantine path.

## Per-event and per-market contract

Every ordinary market event includes:

- `cursor` (global durable cursor)
- `market_id`
- `market_sequence`
- `projection_generation`
- `catalog_revision`
- `event_id` and `event_revision`

`scope`, `full-sync`, and the per-market snapshot expose a health record containing:

- `projection_status`: `ready`, `recovering`, `temporarily_unavailable`, or `quarantined`
- `last_market_sequence`
- `gap_from` / `gap_to`
- `reason_code`, `retryable`, and `retry_after`
- `source_observed_at` and `last_recovered_at`
- `projection_generation`, `catalog_revision`, and `last_event_revision`

The full-sync snapshot contains only markets usable for new opportunities. It also exposes
`active_market_ids`, `quarantined_market_ids`, and all health records. Relations whose complete
member set is not active are omitted, preventing a partial negative-risk group from being treated
as executable.

`GET /v1/prediction-markets/polymarket/live/markets/{market_id}/snapshot` remains available for a
quarantined market and for a market removed from the scan universe. It returns stable
market/condition/token identities, both last known books, instrument and fee facts, any relation
that remains in the current catalog, global and market sequence boundaries, and health. A removed
market is retained in a separate monitoring projection and reports
`scan_universe_membership=false`, `reason_code=removed_from_scan_universe`, and
`usable_for_new_opportunities=false`; it never contributes to readiness or active counts.
`stable_identity_for_position_monitoring=true` allows Tradude to retain its own orders, fills,
positions, capital usage, and settlement state without treating the last book as an executable
quote. This endpoint is a last-known read model, not an independent promise of fresh held-position
quotes; Tradude remains the owner of position and settlement state.

## WebSocket controls

Local controls advance the same global cursor as ordinary events and set
`full_sync_required=false`:

- `market_quarantined`
- `market_recovery_started`
- `market_recovered`

Each carries global cursor, market sequence, projection/catalog revision, old/new projection
generation, affected/added/removed identities, reason, recovery boundary, and whether a per-market
snapshot is required. `universe_changed` remains the atomic replacement-generation boundary. The
periodic MarketCow-owned refresh controller observes a below-target effective universe, builds and
fully validates a replacement candidate, then atomically activates it; this is the
`market_replaced` equivalent at universe scope.

`global_resync_required` sets `full_sync_required=true` for uncertainty that cannot safely be
localized. The consumer must discard the scan projection and start again at a new full-sync
boundary.

## Hot-refresh staging

The single live owner prepares a requested universe generation before stopping the current
transport. Checkpoint/fork, catalog seed, book seed and candidate checkpoint run on a blocking
worker while HTTP continues to serve the last verified immutable projection. The owner pauses old
generation event application at one exact cursor while the connected transport applies bounded
backpressure; this prevents candidate and old-generation events from racing or mixing. Candidate
preparation failure leaves the transport and active generation unchanged.

After complete candidate validation, transport shutdown has a two-second bound. The ingress task
owns no WAL or projection state, so a stuck upstream close handshake is aborted after that bound;
the candidate writer is then swapped atomically and `universe_changed` requires consumer full-sync.
An activation client that times out must probe the hash-pinned active scope before deciding whether
to retry or restore its registered candidate; the controller treats that outcome as ambiguous, not
failed. A queued follow-up request is revalidated relative to the generation that is active when
the owner handles it.

## Availability and audit metrics

The live gate must report, per market and globally:

- quarantine, journal replay, replacement, recovery, and global-resync counts;
- local unavailable duration and maximum local unavailable duration;
- healthy-market count and `minimum_market_count` breaches;
- unaffected-market continuous cursor coverage and availability;
- overall scope availability, WAL/checkpoint identity, and read-only surface checks.

No live duration or process exit implies success. The one-hour acceptance gate remains outstanding
until it produces a reproducible result Artifact with zero unexplained resyncs and proves that
unaffected markets remain continuously usable during injected single-market failures.

## Release rollback boundary

An older binary must never be pointed at a WAL lineage after a newer binary has appended a record
whose canonical persistence schema it does not understand. The append-only hash chain correctly
rejects that combination as corrupt; deleting, rewriting, truncating, or rehashing the new records
would destroy the authoritative audit history and is forbidden.

Before a binary cutover, the MarketCow operator therefore records the checkpoint cursor, checkpoint
state hash, WAL anchor, active scope artifact, binary hash, and storage root, then creates a
generation-specific immutable storage fork at that verified boundary. A release rollback switches
the listener and single-writer lease to that fork. It does not reuse the storage lineage mutated by
the newer binary. A same-version process restart continues to use the current lineage and must
prove checkpoint/WAL recovery before readiness. Binary compatibility in the forward direction
(new reader over old lineage) does not imply reverse compatibility.
