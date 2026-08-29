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

Dynamic LongPort instrument resolution is an opt-in third capability. It preserves the frozen
synchronous `POST /v1/instruments:resolve/query` contract while executing provider I/O only in
the isolated UDS worker. Rust checks registered mappings first, submits only missing symbols,
waits on bounded job-state notification, validates the content-addressed result, and is the only
process allowed to update Instrument Master. Enable it explicitly rather than adding it to the
default policies:

```text
MARKETCOW_PYTHON_DISPATCH_POLICIES_JSON={"transform.sec_dividend_filing":{"max_in_flight":1,"minimum_interval_millis":1000},"transform.csv_inference":{"max_in_flight":2,"minimum_interval_millis":0},"provider.longport.resolve_instruments":{"max_in_flight":1,"minimum_interval_millis":250}}
MARKETCOW_PYTHON_WORKER_POOL_SIZE=3
MARKETCOW_PYTHON_SECRET_REFERENCES_JSON={"provider.longport.resolve_instruments":"/run/marketcow/secrets/longport-resolver.json"}
```

The owner-only secret file is JSON with exactly `app_key`, `app_secret`, `access_token`, and an
optional boolean `enable_overnight`. Rust opens it with symlink following disabled and passes it
only as FD 3 to the LongPort capability process. The public API never accepts credentials, and
the worker never receives PostgreSQL, ClickHouse, admin-token, or other capability secrets.

## Native cached quote reads

The Rust daemon owns `POST /v1/quotes/query` and MCP `get_quotes` once
`MARKETCOW_CLICKHOUSE_DATABASE` is configured. It uses the existing loopback ClickHouse settings
`MARKETCOW_CLICKHOUSE_HOST`, `MARKETCOW_CLICKHOUSE_PORT`,
`MARKETCOW_CLICKHOUSE_USERNAME`, `MARKETCOW_CLICKHOUSE_PASSWORD`, and optional
`MARKETCOW_CLICKHOUSE_SECURE=true`. Production refuses to start without the database setting.

This boundary is deliberately cache-only: `refresh=true`, an explicit provider, or
`allow_fallback=true` is rejected and never calls Python or an upstream provider. ClickHouse
unavailability returns 503 instead of consulting SQLite. Missing symbols are returned as ordered
per-item `unavailable` errors, while duplicate requested symbols remain duplicated in the response.
The Rust quote writer remains disabled during this shadow milestone.

## Hyperliquid Rust realtime shadow

The Hyperliquid transport is disabled by default. To opt into a scoped shadow, provide an
explicit canonical-instrument-to-provider-coin map; the endpoint remains fixed to the
allow-listed `wss://api.hyperliquid.xyz/ws` and accepts no credentials:

```text
MARKETCOW_HYPERLIQUID_SHADOW_ENABLED=true
MARKETCOW_HYPERLIQUID_INSTRUMENTS_JSON={"BTC-PERP.HYPL":"BTC"}
MARKETCOW_HYPERLIQUID_MAX_SOURCE_DELAY_MILLIS=30000
```

Before binding the public Rust listener, `marketcowd` validates the mapping, opens the
single-writer WAL under `<storage-root>/hyperliquid`, restores its checkpoint/replay state and
starts the bounded Rust transport. One upstream frame is persisted and synced as one WAL batch
before its immutable projection can advance. Transport, owner, gateway-backpressure or
checkpoint failure becomes a non-resettable fail-closed state until a fresh process replays the
WAL. Inspect the bounded `snapshot` and `events` endpoints under
`/v1/market-data/providers/hyperliquid/shadow/`, its server-push-only `stream` WebSocket,
`/v1/health`, `/v1/readiness`, and `marketcow_hyperliquid_*` metrics. Filtered replay emits
sequence watermarks for non-matching events, preserving a contiguous cursor. Expired, ahead, or
lagged cursors require resync and never skip silently. Transport, gateway, and public-channel
depths are observable independently. When the opt-in is enabled, readiness requires the
Hyperliquid hub to be `ready`; do not bypass that gate.

The frozen unified WebSocket entry point accepts Hyperliquid without changing the default
Polymarket behavior:

```text
/v1/market-data/stream?provider=hyperliquid&after_cursor=<sequence>&instruments=<ids>&data_types=<types>
```

Omitting `provider` remains the Polymarket v1 contract; `provider=polymarket` is equivalent.
Unknown providers and Polymarket-only filter combinations fail closed with HTTP 422. The
provider-specific Hyperliquid shadow route remains a rollback-compatible alias during Phase 6.
Hyperliquid lifecycle transitions are fsync-appended to `audit.jsonl` with schema
`marketcow.lifecycle-audit.v1`; failure to append a transition makes the hub fail closed.

The realtime WAL maintains a derived `wal/cursor-index.json` using
`marketcow.realtime.sparse-index.v1`. Entries bind a cursor range to a segment, byte offset and
previous record hash; the whole index is SHA-256 sealed and atomically replaced with file and
directory fsync. The index is never authoritative: missing or malformed content is rebuilt from
the verified append-only WAL, while symlinks and unsafe permissions are rejected. This milestone
provides logarithmic boundary lookup but startup still performs a full WAL integrity scan; do not
claim history-independent startup until the checkpoint/segment manifest fast path is implemented
and fault-tested.

This is shadow evidence only. It does not enable a writer cutover, does not submit orders, and
does not authorize Tradude to start, stop, restart, or supervise MarketCow. LongPort remains an
owner-only UDS typed raw-push bridge; it has no Python public listener and is not yet wired to the
daemon streaming lifecycle.

The supervisor deterministically assigns exactly one registered capability to each process
and passes it as `--capability`. The Python worker rejects unknown capabilities before opening
the UDS and advertises only its assigned capability during the nonce-bound handshake. Pool
size must be at least the number of configured capabilities, ensuring every policy has an
isolated executor. A restarted slot retains the same assignment.

Provider secrets are optional capability-to-file references and are never accepted as secret
values in configuration. Configure only absolute, owner-only regular files (1–65536 bytes):

```text
MARKETCOW_PYTHON_SECRET_REFERENCES_JSON={"transform.sec_dividend_filing":"/run/marketcow/secrets/sec-provider"}
```

At preflight and again before every spawn, Rust opens each reference with symlink following
disabled, verifies that the file is owned by the MarketCow effective user and has no group or
other permissions, then passes only the matching capability's already-open descriptor as FD 3.
The child receives `MARKETCOW_PROVIDER_SECRET_FD=3`; neither secret content nor its path is
placed in the environment, command line, health response or logs. Unmapped processes receive
no secret descriptor. Rotate a secret by atomically replacing the owner-only file and allowing
the affected slot to restart through the bounded supervisor policy; never restart MarketCow
from Tradude.

## Staged MCP compatibility proxy

During Phase 2 shadow only, Rust may dispatch the 13 not-yet-native read-only MCP tools to the
legacy Python MCP endpoint. This does not transfer transport ownership back to Python: clients
connect only to Rust `/mcp`, and Rust performs the public boundary, protocol, size, Origin,
audit and contract checks. Configure an explicit loopback IP and distinct port:

```text
MARKETCOW_LEGACY_MCP_URL=http://127.0.0.1:8791/mcp
```

The URL must use uncredentialed plain HTTP, an explicit loopback IP/port, exact `/mcp` path and
must not point to the Rust listener. Rust disables environment proxies and redirects, uses a
one-second connect timeout and 20-second total timeout, and caps the streamed response at 1 MiB.
`tools/list` is accepted only when it contains exactly the frozen 14 names and every tool is
read-only/non-destructive. Rust replaces `service_health` with its native definition; the other
13 calls are response-envelope validated before returning. An unavailable, oversized or
contract-divergent legacy endpoint fails closed. Phase 5/7 cannot complete until those 13 tools
are native or worker-backed and this proxy setting is removed. Tradude must not start either
service.

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
