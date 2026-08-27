# Shadow, cutover and rollback runbook

Only the MarketCow operator may execute these steps. Tradude must never manage the service.
Real-order submission remains false at every step.

## Shadow

1. Pin binary commit, configuration hash, scope manifest/hash and worker revision.
2. Run `marketcow doctor`, `marketcow migrate --dry-run`, corpus verification and WAL verify.
3. Start Rust on a non-production loopback port. Python remains the only writer.
4. Replay identical events, diff every cursor/gap/recovery boundary and run the soak harness.
5. Store all results; a divergence or unavailable persistence makes readiness fail closed.

## Dual-run/read comparison

Dual-run means two readers and exactly one writer. Compare HTTP status/error code, cursor,
canonical hash, book checksum, gaps, tick and health. Never dual-write.

## Cutover (future Phase 7 gate)

Drain Rust and Python consumers; stop the Python writer; flush and hash legacy WAL; record a
cutover manifest; acquire the single-writer gate; enable Rust writer; wait for 8790 readiness;
then restore consumers. The included `cutover.sh` refuses execution because this repository
has not produced the raw r2, 24-hour and 7-day release evidence.

## Rollback

Stop Rust writer, flush/hash its tail, verify the exact cursor boundary and compatible schema,
start Python from that boundary, revalidate exact scope and append an audit event. Do not
delete WAL, reset cursor or relax freshness/gap checks. `rollback.sh` is similarly guarded.

## Recovery

On WAL tail corruption, stop at the last verified record and remain unready. On checkpoint
corruption, load the preceding verified checkpoint and replay WAL. Derived SQLite damage is
degraded-only and rebuilt from WAL; the realtime hot path never queries it.

