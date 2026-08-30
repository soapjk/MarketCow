# Polymarket authoritative refresh architecture review

Status: architecture corrected locally after the failed `a904e13` strict 30-minute gate; live
candidate validation remains pending.

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

## Implemented correction

- `PolymarketRuntime::validate_full_book_observation` normalizes and compares one REST full book
  against the current WS-owned projection without invoking the single writer. It cannot change the
  projection hash, cursor, recent-event buffer, WAL, checkpoint, or public stream.
- A complete REST batch produces a separate immutable validation projection. Only exact matches
  receive a validation timestamp, and that projection is published only after the append-only
  lifecycle audit record is durably accepted.
- Effective freshness is the newer of the WS causal observation and an exact REST validation for
  the same token and scope. Consequently, a moving book can remain fresh through WS even if a REST
  comparison races; a quiet stale book must match REST exactly or the service fails closed on age.
- A malformed REST response or comparison failure cannot restart or overwrite a healthy WS state.
  It supplies no freshness, so repeated failure still reaches the existing fail-closed age gate.
- Scope/generation activation atomically clears the separate validation projection. Evidence from
  an old scope can never make a new scope ready.
- Historical WAL/checkpoint replay compatibility is retained for older builds that wrote periodic
  HTTP refresh events. New normal operation no longer emits those events.

Local verification at this revision:

- `cargo test --workspace`: passed (179 passed, 5 environment-gated integration tests ignored).
- `cargo clippy --workspace --all-targets -- -D warnings`: passed.
- Deterministic runtime tests prove exact validation and mismatch validation are non-mutating and
  that two WS deltas around REST validation retain contiguous WS-only cursors.

## Restart gate blocker discovered after implementation

The `1608751` release candidate did not reach LISTEN on 18872. Both the candidate and the exact
`a904e13` rollback binary rejected retained generation 17 with `corrupt WAL`; both restart loops were
booted out and the service remains fail-closed. No WAL or checkpoint was modified or removed.

The first cryptographically reproducible failure is between segments 1778832 and 1779432. The
successor header commits predecessor SHA-256
`7c7e9fc2fbc782155cfe156479a4dc114f080d9dd7db6432ce19f9febd075c00`, while the retained
predecessor now hashes to
`d40b17ea715e28b31749a101a174af48edb55242dc720b4182793a25dab67ecf`. File timestamps and
content show that records were appended to the predecessor after the successor anchor existed.

This reveals a second architectural issue: the Rust Polymarket "single writer" is enforced only by
an in-process mutex. Unlike the newer realtime durability layer, the Polymarket segmented WAL has
no cross-process advisory lock or writer lease. An overlapping cutover/restart can therefore retain
two append handles to the same generation and invalidate the immutable segment hash chain.

No further restart or WAL repair is permitted until the remediation design covers:

1. an OS-enforced exclusive writer lease held for the lifetime of a Polymarket runtime;
2. cutover fencing that proves the old writer exited and released the lease before the new writer
   opens the generation;
3. a non-destructive recovery path that preserves corrupt generation 17 as evidence and starts a
   new scope generation only from independently verifiable state;
4. fault tests for overlapping launchd processes, stale file descriptors, empty checkpoint-anchor
   segments, and crash during writer handoff.

The first remediation layer is now implemented locally: every `PolymarketRuntime` owns a private
`writer.lock` descriptor and acquires non-blocking `flock(LOCK_EX)` before WAL verification/open.
Candidate generations acquire a separate lease for their isolated root. A second runtime for the
same generation fails with `WriterAlreadyActive`; symlink or non-private lease substitution fails
closed. Dropping the old runtime explicitly releases the lease. Runtime tests cover overlap,
release/reacquire, and symlink substitution. This prevents future chain corruption but deliberately
does not bless or repair generation 17; its non-destructive recovery design remains pending.
