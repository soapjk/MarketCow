# Shared Polymarket `/events` tail-latency investigation

Date: 2026-08-20 (Asia/Shanghai)

## Scope and safety boundary

- Consumer: Tradude v48 shadow runner reading the shared MarketCow API at
  `127.0.0.1:8790`.
- Flow: bootstrap a fixed 100-market scope, bind a stable snapshot cursor, and
  repeatedly request `events?after_cursor=...&limit=1000` while health,
  bootstrap, and snapshot reads run concurrently.
- Data: the production Polymarket collector's immutable event JSONL, catalog
  files, and local SQLite/WAL state index. No market or account data is
  synthesized.
- Safety: the supplied v48 configuration has `mode: shadow`; its order canary
  is disabled. The verifier rejects any other mode, market count, base URL,
  timeout, or page limit. This work does not change Tradude unwind logic.
- Correctness boundary: event file SHA-256 checks, Pydantic validation,
  catalog-transition cursor expiry, stable-boundary checks, and fail-closed
  errors remain mandatory. A lock or unstable boundary never becomes a
  fabricated empty success.

## Reproduction and root cause

The pre-fix 8790 baseline used 100 explicit market IDs and produced roughly
0.94--1.03 MiB pages. In a short concurrent run, `/events` p99 and maximum were
3.423 seconds, above the 3-second target.

SQLite's old broad-scope plan searched `events_market_cursor` separately for
each market and then reported `USE TEMP B-TREE FOR ORDER BY`. Read-only timing
against the production index showed the cursor-order scan was consistently
faster for the 100-market scope:

| Cursor range | old market-index merge | cursor-order scan |
| --- | ---: | ---: |
| latest page | 33.700 ms | 0.303 ms |
| 1,000 | 3.159 ms | 0.240 ms |
| 100,000 | 52.740 ms | 0.281 ms |
| 1,000,000 | 54.883 ms | 0.292 ms |

The main failure mode was cumulative shared-path tail latency: the synchronous
route competed in Starlette's shared worker pool, broad market filtering did a
multi-range merge and temporary sort, and a near-1 MiB page was converted and
serialized again after the synchronous worker returned. Stable-boundary waits
could add several seconds on top. A 20-second SQLite busy timeout also exceeded
the consumer's 10-second transport deadline, so an exceptional lock could
strand a shared worker after the client had already abandoned it.

The production SQLite database was approximately 3.1 GiB with WAL enabled.
Direct WAL reads and the optimized page queries completed promptly; no
correlated SQLite busy/locked error or ten-second WAL/checkpoint wait was found.
The earlier slow diagnostic command was traced to an unconditional full-table
`COUNT(*)`, not a WAL lock.

## `production.error.log` timestamp correlation

Before the original API was stopped for replacement testing,
`/Users/androidjk/Library/Logs/MarketCow/production.error.log` had a last-write
timestamp of `2026-08-20T14:26:25+0800` and contained:

```text
error connecting in 'pool-1': connection timeout expired
```

Tradude attempt 2's `shadow.stderr.log` was born at 14:15:56 and last written
at `2026-08-20T14:38:59+0800`; its stack identifies `fetch_events` and an
`httpx.ReadTimeout`. The persisted cursor stopped at 9,642,659. Thus the two
symptoms occurred during the same run, but the database connection record
preceded the `/events` terminal failure by about 12 minutes 34 seconds.

The code-path evidence rules out that connection timeout as the direct cause:
`/events` uses `PolymarketLiveReadStore` over the local event files and SQLite,
whereas `pool-1` is the application's PostgreSQL connection pool used by other
services. It is evidence of concurrent host/service pressure, not evidence that
the events query waited on PostgreSQL. The evidence-backed conclusion is that
shared executor contention and avoidable query/model/serialization work caused
the tail, with stable-boundary waiting as a separately visible contributor;
neither PostgreSQL nor SQLite checkpoint locking was the direct failure source.

Reproduction references:

```sh
stat -f 'mtime=%Sm birth=%SB bytes=%z' -t '%Y-%m-%dT%H:%M:%S%z' \
  /Volumes/T9/projects/trade/tradude/.tradude-local/polymarket-live/\
supervised-shadow-v48-shared-api-formal-attempt2-20260820T140927CST/logs/shadow.stderr.log
rg -n 'connection timeout expired' \
  /Users/androidjk/Library/Logs/MarketCow/production.error.log
rg -n 'fetch_events|httpx.ReadTimeout' \
  /Volumes/T9/projects/trade/tradude/.tradude-local/polymarket-live/\
supervised-shadow-v48-shared-api-formal-attempt2-20260820T140927CST/logs/shadow.stderr.log
```

## Fix

- The shared API owns a four-worker `marketcow-polymarket-events` executor.
  Scope binding, stable-boundary read, event hash/model validation, and JSON
  serialization all execute there, isolating health/bootstrap/snapshot work.
- Broad scopes (eight or more IDs) scan the cursor primary key in natural page
  order. Narrow scopes retain the selective `(market_id, cursor)` index.
- The validated page is serialized exactly once in the dedicated executor and
  returned as immutable JSON bytes.
- SQLite busy wait is bounded to 500 ms and translated to the existing
  fail-closed `polymarket_state_index_lagging` response.
- `/events` emits per-request structured diagnostics and fixed-cardinality
  Prometheus metrics for executor queue, scope bootstrap, stable-boundary wait,
  SQLite query, model construction, JSON serialization, ASGI response write,
  total time, status/errors/timeouts, cursor/scope, and actual response bytes.
- The shared snapshot read boundary is 4.5 seconds because the 100-market
  collector can spend about four seconds between its common receive timestamp
  and atomic 200-book publication. `snapshot_json` still rechecks the exact
  serialized frame at the response edge, preserving Tradude's unchanged
  five-second fail-closed maximum.

## Verification

The formal one-hour report and final phase summary are populated only by a
completed run of:

```sh
PYTHONPATH=src .venv/bin/python scripts/verify_polymarket_shared_events_soak.py \
  --v48-config /Volumes/T9/projects/trade/tradude/.tradude-local/polymarket-live/\
supervised-shadow-v48-shared-api-config-20260820T140927CST/unified-shadow.yaml \
  --duration-seconds 3600 --api-pid 55034 --collector-pid 18847 \
  --output ops/verification/polymarket-shared-events-soak-final6-20260821.json
```

Automated checks:

```sh
PYTHONPATH=src .venv/bin/python -m unittest discover -v
uv run --group dev ruff check .
uv build
```

Final shared-API run: `2026-08-20T15:45:14.480726+00:00` through
`2026-08-20T16:45:14.990244+00:00` (3,600.805 seconds) on
`androidjkdeMac-mini.local`, macOS 26.6 arm64, Python 3.11.14. The v48
configuration SHA-256 was
`3e5b5bc8ea2db89233a7b3495e808ae8faeb711e198668d0853ab083e1d87661`.

- `/events`: 1,798 samples; p50 0.0146s, p95 0.3045s, p99 1.0265s,
  maximum 6.2021s; zero 10-second client timeouts; maximum consecutive
  unavailability 1.3062s.
- Cursor: 10,912,764 to 11,034,593; zero gaps, duplicate events, or integrity
  failures.
- Concurrent samples: health 865, bootstrap 575, snapshot 1,179. Every route
  returned successful responses and had zero client timeouts. Freshness-bound
  503 responses were retained rather than returning stale frames.
- Books: zero fail-closed frames among successful snapshots, zero integrity
  failures, maximum observed book age 4.610974s.
- Processes: API PID 69675 and collector PID 18847 were both alive at finish.
- Safety: shadow mode remained active and real order submission remained false.
- Structured traces: 1,798 records, of which 1,795 were successful and three
  were explicit stable-boundary 503 responses. Successful response size p99
  was 1,043,479 bytes and maximum was 1,048,221 bytes.
- Successful request phase p99/max: executor queue 1.832/7.659ms; SQLite
  347.854/857.080ms; model construction 738.665/1,734.089ms; JSON serialization
  18.711/59.726ms; response write 54.110/214.075ms; total
  993.568/6,141.067ms. Stable-boundary wait p99 was 0ms and maximum was
  5,994.377ms.

The original raw verifier report is preserved unchanged. It marked the aggregate
false because that version applied the 30-second availability condition to all
four routes. The work item applies that condition to `/events`; the concurrent
routes must respond and preserve fail-closed semantics. The audited assessment
uses the exact stated criterion, records the raw report SHA-256, and passed.

Final automated results:

- `PYTHONPATH=src .venv/bin/python -m unittest discover -v`: 672 tests passed,
  21 skipped. One earlier concurrent run had a pre-existing runtime-service
  quote timing failure; the isolated test and the subsequent serial full run
  both passed.
- `uv run --group dev ruff check .`: passed.
- `uv build`: source distribution and wheel built successfully.

## 2026-08-22 collector stability follow-up

Tradude's next preflight exposed a collector feedback loop rather than an
`/events` read-path regression. Both read APIs correctly returned fail-closed
503 responses because the oldest live book was repeatedly more than five
seconds old, even though the durable index remained structurally complete at
100 markets, 200 book tokens, and zero unresolved gaps.

Timestamped event inspection established the ordering failure: higher cursors
from a periodic REST refresh carried older `received_at` timestamps than lower
cursors already published by the WebSocket stream. Commit `45f6065` serialized
periodic refresh and reconnect recovery, but neither operation was coordinated
with WebSocket publication. A REST request could therefore start, receive newer
WebSocket state while in flight, and then overwrite it with its older snapshot.
The two-second refresh repeatedly wrote nearly all 200 books, amplified durable
book events, held the SQLite publication lock, and delayed the WebSocket event
loop. Lock waits reached 14--25 seconds and the resulting scheduling pressure
caused keepalive ping timeouts, reconnect recovery, and yet more full-state
publication. An empty `active_recovery_id` only proves that recovery is not
active; it does not satisfy the separate freshness boundary enforced by health.

Commit `a8db7a8` breaks that loop without weakening fail-closed behavior:

- An in-flight REST row is marked superseded and not published when a newer
  WebSocket generation or `received_at` value already exists.
- WebSocket arrays are durably published in bounded 32-item chunks, preserving
  order while limiting lock hold time and yielding to keepalive processing.
- A periodic refresh that spent an entire refresh interval waiting behind
  recovery is discarded and replaced by a fresh cycle.
- The default WebSocket connector retains 20-second pings but permits a bounded
  60-second pong timeout under transient local publication load.
- Slow WebSocket publish phases now record item count, emitted events, sync
  wait, publish time, and cursor.

After restarting the collector and both read APIs from the task worktree, the
first 200-token bootstrap fetched REST in 1.395 seconds and published in 0.074
seconds. Subsequent refresh publication was normally 0.02--0.06 seconds; no
timestamped `websocket_disconnected` event was observed after the restart.
A 60-second v48-frequency concurrent smoke run (not the formal one-hour
acceptance) produced 111 successful `/events` samples, p99 0.234223 seconds,
maximum 0.252672 seconds, zero ten-second timeouts, and zero unavailable time.
Cursor advanced from 14,880,453 to 14,883,295 with zero gaps, duplicates, or
integrity failures. Concurrent health, bootstrap, snapshot, and the secondary
read API all succeeded; maximum observed book age was 2.949552 seconds. Shadow
mode remained enabled and real order submission remained disabled.

## 2026-08-22 v49 failure and real-time architecture correction

The subsequent v49 formal run invalidated SQLite as a production hot-read
boundary even after the earlier query-plan and executor fixes. At approximately
17:28 Asia/Shanghai, two identical-cursor `/events` retries spent 12.57--12.58
seconds in SQLite; one complete request took 17.09 seconds, including 4.41
seconds of model construction. Another trace reached 21.42 seconds with 20.32
seconds in the SQLite phase. Tradude's unchanged ten-second timeout and retry
policy consequently exceeded 30 seconds of consecutive availability loss.

At the same timestamps the collector continued publishing. Its normal
200-token transactions completed in roughly 0.04--0.18 seconds, occasional
WebSocket batches in 0.25--2.77 seconds, and the logs contained no correlated
SQLite `busy` or `locked` error. The index was approximately 4.5 GiB and the
append-only event log approximately 54 GiB. The evidence supports external
volume page-in/random-read stalls, amplified by overlapping retries for the
same cursor, rather than WAL writer-lock contention. The PostgreSQL connection
timeouts in `production.error.log` remain a separate subsystem and do not occur
on the Polymarket event read code path.

Evidence:

- Tradude artifact root:
  `/Volumes/T9/projects/trade/tradude/.tradude-local/polymarket-live/supervised-shadow-v49-shared-api-formal-20260822T170211CST`
- Health monitor errors: `logs/health-monitor.stderr.log`
- Shadow request errors: `logs/shadow.stderr.log`
- MarketCow request traces:
  `/Users/androidjk/Library/Logs/MarketCow/events-soak.error.log`
- Collector phases:
  `/Users/androidjk/Library/Logs/MarketCow/polymarket-scoped.error.log`

The final correction removes durable storage from the real-time data path:

- The collector assigns the canonical cursor, updates memory, and immediately
  publishes the event to a loopback WebSocket hub.
- 8790 and 8791 independently consume that stream into bounded memory replay
  rings and live book projections. `/events`, health, and snapshot use those
  projections; `/events` records `sqlite_query_ms=0` and an explicit
  `memory_projection_ms` phase.
- JSONL and SQLite are written by one independent serial persistence thread in
  batches. The writer fsyncs the JSONL batch before committing its corresponding
  WAL transaction, preserving crash recovery without delaying publication.
- `published_cursor`, `persisted_cursor`, persistence lag, queue depth, stream
  connectivity, and persistence failure are separate watermarks. Persistence
  failure is observable but cannot apply backpressure to the real-time queue.
- Cursor gaps, stream disconnection, history-window expiry, incomplete books,
  stale books, or unresolved gaps remain explicit fail-closed states. Slow
  WebSocket consumers are disconnected rather than blocking the hub.

The v49 run covered only 1,522.74 seconds and failed; it is not acceptance
evidence. A new one-hour shadow run is required after deployment of this
architecture.

## 2026-08-23 v54 in-memory projection freshness failure

Tradude v54 confirmed that `/events` was using `memory_projection` with
`sqlite_query_ms=0`, but its tenth health sample failed closed after a combined
100-market bootstrap/snapshot read took 7.882 seconds. The returned frame
carried book ages from 6.427 to 8.667 seconds even though `/health` still
reported `index_ready`, a connected stream, zero gaps, and an empty persistence
queue. This was a separate in-process scheduling defect, not a regression to
the SQLite hot path.

Code inspection found three mutually reinforcing causes:

- bootstrap response-model validation and roughly 1 MiB JSON serialization ran
  on the API event loop;
- `/events` held the projection ingestion lock while calling
  `model_dump_json()` for every candidate event to bound response size;
- projection health checked structural completeness but not the actual book
  timestamps used by snapshot consumers.

The correction moves bootstrap/snapshot construction and encoding to a
dedicated frame executor and returns pre-encoded bytes, snapshots event and
market references under a short lock before expensive copying/serialization,
and rechecks both the current projection and the timestamps carried by the
exact serialized snapshot. Projection health now uses the same freshness
threshold and raises the existing retryable HTTP 503 boundary error when the
readable books are stale. JSONL and SQLite persistence remain asynchronous and
are not consulted by any of these real-time routes.
