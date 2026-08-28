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

## Python provider workers

`marketcowd`, never Tradude, owns the optional local worker pool. Keep the pool disabled unless
all of these absolute-path settings are pinned:

```text
MARKETCOW_PYTHON_WORKER_EXECUTABLE=/absolute/path/to/python3
MARKETCOW_PYTHON_WORKER_SCRIPT=/absolute/path/to/python/marketcow_workers/worker.py
MARKETCOW_PYTHON_WORKER_REVISION=<immutable-revision>
MARKETCOW_PYTHON_WORKER_POOL_SIZE=2
```

The daemon clears the inherited environment before spawning workers, so PostgreSQL,
ClickHouse and admin credentials are not available to Python. Workers receive only the UDS
path and immutable revision, expose no public listener and never own migrations. The default
restart policy allows three restarts per 60-second sliding window with a one-second backoff;
configure its bounded values with `MARKETCOW_PYTHON_WORKER_MAX_RESTARTS`,
`MARKETCOW_PYTHON_WORKER_RESTART_WINDOW_SECONDS` and
`MARKETCOW_PYTHON_WORKER_RESTART_BACKOFF_MILLIS`. Exhaustion degrades the worker component
without restarting or stopping the HTTP/WAL platform. Check `/v1/health` and the
`marketcow_python_worker_*` metrics before enabling provider jobs.

Each worker also has an independent resource envelope. Defaults are 2048 MiB resident
memory, 900 seconds accumulated CPU, 256 file descriptors and zero-byte core dumps. Override
the first two with `MARKETCOW_PYTHON_WORKER_MEMORY_LIMIT_MIB` (128–16384) and
`MARKETCOW_PYTHON_WORKER_CPU_LIMIT_SECONDS` (1–86400). CPU, descriptor and core limits are
installed before `exec`; Linux additionally installs `RLIMIT_AS`. The supervisor checks RSS
every 250 milliseconds on all supported hosts and kills/reaps an over-limit worker. If RSS
cannot be measured, it kills the worker and records a fail-closed monitor failure instead of
running without enforcement.

Rust dispatch also requires an explicit policy for every worker capability. Defaults limit
SEC filing transforms to one in-flight task with a 1000 ms minimum interval, and CSV
inference to two in-flight tasks. Override the complete map atomically with JSON; omitted
capabilities fail closed and receive no lease:

```text
MARKETCOW_PYTHON_DISPATCH_POLICIES_JSON={"transform.sec_dividend_filing":{"max_in_flight":1,"minimum_interval_millis":1000},"transform.csv_inference":{"max_in_flight":2,"minimum_interval_millis":0}}
```

The last claim time is part of the authoritative job payload, so restart/recovery does not
reset the interval. `/v1/health` reports the effective non-secret policy map.

The supervisor deterministically assigns exactly one registered capability to each process
and passes it as `--capability`. The Python worker rejects unknown capabilities before opening
the UDS and advertises only its assigned capability during the nonce-bound handshake. Pool
size must be at least the number of configured capabilities, ensuring every policy has an
isolated executor. A restarted slot retains the same assignment.

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
