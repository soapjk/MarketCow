# Polymarket authoritative refresh architecture review

Status: implementation paused after the failed `a904e13` strict 30-minute gate.

## Evidence

- The 12-minute gate crossed two periodic 200-book refreshes with zero failures.
- The 30-minute gate ran for 1,800.18 seconds and recorded eight failures across three recovery episodes.
- At 07:24, REST full books were assigned a common observation boundary of
  `07:24:19.516378Z`. Older queued WS deltas were retained as applied no-ops. A later WS delta at
  `07:24:21.925Z` then failed `best_bid_ask_source_mismatch` at cursors 1609146-1609147.
- The same pattern repeated at cursors 1629347-1629348 after the 07:29 REST refresh.
- At 07:39, an upstream connection boundary correctly opened durable source gaps and required a
  full resync. This is a genuine system-level fail-closed event, distinct from refresh ordering.

## Root architectural issue

Polymarket REST book responses and WS price-change frames do not expose a shared venue sequence or
transactional snapshot boundary. The current periodic refresh fetches 200 REST books, assigns an
application-level observation time, writes them into the live projection, and then drains WS frames.
Local cursor order and wall-clock timestamps cannot reconstruct venue causality across these two
independent transports.

Discarding all WS frames older than the REST receipt is not safe: a later incremental WS frame may
depend on one or more discarded intermediate deltas. Applying that later frame to the REST book can
therefore disagree with the frame's venue BBO even when its timestamp is newer. Adding more timestamp
exceptions cannot prove completeness.

The implementation also conflates two concerns:

1. proving that a quiet book was recently observed; and
2. replacing authoritative book state during recovery.

A periodic freshness check must not silently become a mixed-transport state replacement.

## Decision before further implementation

1. Periodic REST refresh becomes validation/freshness evidence only. It must not overwrite the live
   WS projection during normal `Running` state.
2. Book freshness evidence is tracked separately from the book's causal update lineage.
3. State replacement uses an explicit recovery epoch state machine:
   `Running -> Quiescing -> Recovering -> Validating -> AtomicPublish`.
4. On recovery, the old WS epoch is closed and cannot publish further deltas. A new epoch must obtain
   complete full books and establish its own subscription boundary before one atomic publication.
5. If the provider cannot supply a sequence-compatible REST/WS barrier, REST snapshots and old WS
   deltas must never be replayed together. Recovery must reconnect and rebuild from one coherent new
   transport epoch, remaining `ready=false` until complete.
6. Real upstream disconnects, cursor gaps, incomplete projections, and mixed epochs remain globally
   fail-closed. No safety check is relaxed.

## Required verification before another live gate

- Deterministic tests for refresh concurrent with queued and network-delayed WS frames without using
  wall-clock ordering as a correctness proof.
- Tests proving no old-epoch frame can publish after recovery begins.
- Atomic 200-book epoch publication and restart/replay determinism.
- Fault injection for REST partial failure, WS disconnect during recovery, slow consumer, and missing
  full books.
- A short multi-refresh gate before any new 30-minute gate.

MarketCow remains read-only. Real-order submission remains disabled and Tradude does not manage the
MarketCow lifecycle.
