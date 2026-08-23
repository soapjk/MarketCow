# MarketCow atomic-freshness local runtime switch

Observed on 2026-08-23, Asia/Shanghai.

## Runtime identity

- Worktree: `/Volumes/T9/projects/marketcow-workitem-ff65fa86-9f6f-4f7a-948d-633270887332`
- Atomic freshness code/runtime commit: `703d263eda8357c1d5f13b194040118e912ddb5b`
- Stable Python: `/Users/androidjk/Library/Application Support/MarketCow/atomic-freshness-venv/bin/python`
- Collector: launchd `com.marketcow.polymarket.scoped`, PID 47435, started
  2026-08-23 15:08:09 +08:00, port 8794.
- Formal API: launchd `com.marketcow.events-soak`, PID 47641, started
  2026-08-23 15:08:38 +08:00, port 8790.
- Diagnostic API: launchd `com.marketcow.polymarket.read-api`, PID 47668,
  started 2026-08-23 15:08:43 +08:00, port 8791.
- All three launchd jobs report `state = running`, use the worktree above as
  their working directory, and have `KeepAlive=true`.
- Replaced plist backup:
  `/Users/androidjk/Library/Application Support/MarketCow/launchd-backup-20260823-atomic-freshness`.

## Exact 100-market scoped probes

Both `http://127.0.0.1:8790` and `http://127.0.0.1:8791` returned:

- scoped health HTTP 200, `index_ready`, `latest_state_ready=true`,
  `live_stream_connected=true`, `unresolved_gap_count=0`;
- `events_read_source=memory_projection` and `realtime_sqlite_query_ms=0`;
- full-sync HTTP 200 for 100 markets with 200 unique books.

Point-in-time full-sync readings after startup:

| Port | Maximum book age | Remaining freshness budget | Queue | Lag |
|---|---:|---:|---:|---:|
| 8790 | 946.167 ms | 4009.050 ms | 0 | 5 |
| 8791 | 1052.816 ms | 3902.415 ms | 27 | 7 |

Every successful full-sync exposed executor queue, lock wait, projection copy,
maximum book age, freshness check, frame build, JSON serialization, and
freshness headroom through `Server-Timing`.

## Short concurrent probe

For 30.283 seconds, each port received 75 requests apiece for health,
bootstrap, snapshot and full-sync over the exact 100-market scope:

- all 600 requests succeeded;
- maximum request latency was 118.441 ms on 8790 and 115.538 ms on 8791;
- consumer-observed successful snapshot/full-sync maximum book age was
  2.830012 seconds on 8790 and 2.877408 seconds on 8791;
- disconnect and unresolved gap maxima were zero;
- real-time SQLite query maximum was 0 ms and the only read source was
  `memory_projection`;
- maximum asynchronous persistence queue/lag was 8/80 on 8790 and 45/36 on
  8791.

The corrected current-cursor `/events` probe then ran for ten seconds per port:

| Port | Requests | Failures | Events | Cursor start | Cursor finish | Max latency |
|---|---:|---:|---:|---:|---:|---:|
| 8790 | 3480 | 0 | 729 | 19484736 | 19485465 | 47.081 ms |
| 8791 | 3630 | 0 | 770 | 19485502 | 19486272 | 25.923 ms |

The earlier one-hour report proves budget-exhausted reads return only retryable
HTTP 503 with `polymarket_snapshot_freshness_budget_exhausted`; it recorded no
critical HTTP 200 and a successful-response maximum book age of 4.68554 seconds.

## Recommended Tradude endpoint

Use `http://127.0.0.1:8790` for the formal one-hour shadow run. Port 8791 is the
diagnostic equivalent. The committed `canary.yaml` remains `mode: disabled`;
no real-order or real-trading process was started by this switch.

Recheck supervisor/process state:

```sh
for label in com.marketcow.polymarket.scoped com.marketcow.events-soak com.marketcow.polymarket.read-api; do
  launchctl print "gui/$(id -u)/$label" | rg 'state = running|pid =|program =|working directory ='
done
ps -p 47435,47641,47668 -o pid,lstart,etime,command
```
