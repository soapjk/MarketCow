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

## 2026-08-22 v49 formal-run storage-tail failure

The next formal shadow run lasted 1,522.74 seconds and did not pass. At
`2026-08-22T09:28:06.777305+00:00`, three retries from the same
`after_cursor=15045858` overlapped. Their SQLite phases took 20.316, 12.569,
and 12.580 seconds; subsequent payload reads and model validation took as much
as 4.408 seconds. The collector was still committing normally through this
window: the surrounding 200-token publications generally completed in
0.04--0.18 seconds and SQLite emitted no busy or locked error. This rules out a
writer transaction or WAL busy wait as the direct cause.

The evidence instead identifies an external-volume page-in stall on the
4.5-GiB `event_offsets` B-tree and 54-GiB `events.jsonl`, amplified when retries
ran the identical query on multiple executor workers. The same I/O and CPU/GIL
pressure delayed the concurrent bootstrap read. The supervisor's unrelated
terminal-session lifetime problem shortened orchestration coverage but did not
cause the API failure; its child processes continued until the MarketCow tail
event.

The follow-up fix maintains a bounded `recent_event_offsets` table containing
the last 50,000 cursor offsets and the exact canonical payload whose newline
hash is checked against the durable event log before commit. Current cursor
reads use that compact hot B-tree and validate the same event ID, cursor,
canonical/raw hashes, scope, and catalog-transition rules without seeking the
large JSONL file. A cursor older than the retained floor automatically uses the
original full index and event log, preserving complete historical pagination.
Identical shared-API reads are single-flight, and a storage operation exceeding
2.5 seconds returns the existing explicit fail-closed 503 while the underlying
query finishes; retries join it rather than multiplying I/O. No empty page is
synthesized on timeout.
