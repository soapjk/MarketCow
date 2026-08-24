# MarketCow atomic health cursor runtime verification

Verified on 2026-08-24 (Asia/Shanghai).

## Runtime identity

- Running source commit: `f0e865fdee42810bc897777b4786fd28629e9462`.
- Worktree: `/Volumes/T9/projects/marketcow-workitem-collaboration-a672c3cb-77ce-4e17-aec1-0ac066a42d`.
- Port 8790: launchd `com.marketcow.events-soak`, PID 49796, started
  2026-08-24 14:25:39 +08:00.
- Port 8791: launchd `com.marketcow.polymarket.read-api`, PID 49803,
  started 2026-08-24 14:25:40 +08:00.
- Collector/8794 was not restarted: launchd `com.marketcow.polymarket.scoped`
  remained PID 50034, started 2026-08-24 08:35:38 +08:00.
- The launchd files were backed up locally at
  `/Users/androidjk/Library/Application Support/MarketCow/launchd-backup-20260824-atomic-health-cursor`.

Reproduce the runtime identity:

```sh
for label in com.marketcow.events-soak com.marketcow.polymarket.read-api com.marketcow.polymarket.scoped; do
  launchctl print "gui/$(id -u)/$label" | rg 'state = running|working directory =|pid ='
done
ps -p 49796,49803,50034 -o pid,lstart,etime,command
git rev-parse HEAD
```

## Exact-scope health verification

The exact scope is
`57bac2e63ea3015df414f874385f1080176cd771b047cb82e73b14e7cb6b45ef`.
The verifier sampled ports 8790 and 8791 concurrently, 200 responses per
port at 250 ms cadence. Every response was HTTP 200 and satisfied:

- `status=index_ready`;
- `market_count=100`, `token_count=200`, `book_token_count=200`, and
  `book_complete_market_count=100`;
- `persisted_cursor <= latest_cursor`;
- `persistence_lag_events == latest_cursor - persisted_cursor`;
- `projection_generation >= 1` and the exact ordered `scope_market_ids`;
- `unresolved_gap_count=0`, `live_stream_connected=true`, and
  `live_stream_disconnect_count=0`;
- `events_read_source=memory_projection` and `realtime_sqlite_query_ms=0`.

The report passed without failures. Port 8790 advanced from cursor 22252772
to 22253456; port 8791 advanced from 22252772 to 22253466. Maximum observed
lag was 16 on both ports.

Reproduce:

```sh
SCOPE_MANIFEST=/Volumes/T9/projects/trade/tradude-worktrees/polymarket-memory-stream-contract/.tradude-local/polymarket-live/v69-two-sided-scope-20260824T082100CST/scopes/57bac2e63ea3015df414f874385f1080176cd771b047cb82e73b14e7cb6b45ef/manifest.json
PYTHONPATH=src "/Users/androidjk/Library/Application Support/MarketCow/atomic-freshness-venv/bin/python" scripts/verify_polymarket_atomic_health.py \
  --scope-manifest "$SCOPE_MANIFEST" \
  --samples 200 \
  --interval-seconds 0.25 \
  --output artifacts/polymarket-atomic-health-f0e865f-20260824.json
jq '{passed,commit,scope_id,samples_per_port,interval_seconds,results}' artifacts/polymarket-atomic-health-f0e865f-20260824.json
```

## Authoritative log and order safety

The API-only switch did not restart the collector or open the event log for
repair/rewrite. Before the switch, `events.jsonl` had inode 30826139 and size
86713569514. After verification it had the same inode and size 86733405900,
consistent with append-only collector progress. The fixed 1 MiB block at
offset 86712520938 had SHA-256
`751fa0788d97bd4e18a7d1f4f69cb934700dc226704d4ab1210ce7e8b12585fa`
both before and after the switch, proving the pre-existing bytes at that
boundary were not overwritten.

Reproduce the preserved prefix block:

```sh
EVENT_LOG=/Volumes/T9/data/marketcow/production/prediction-markets/polymarket-live/events.jsonl
stat -f 'inode=%i size=%z' "$EVENT_LOG"
dd if="$EVENT_LOG" bs=1 skip=86712520938 count=1048576 2>/dev/null | shasum -a 256
```

Only the two MarketCow read APIs were restarted. No Tradude or order job was
created, modified, or restarted. The only observed Tradude launchd jobs for
this scope family invoke `run_shadow_acceptance_supervisor.py`; no v75 or
real-order service was active or enabled by this change.

## Automated tests

The merged current runtime contract and cursor fix passed all 142 relevant
Polymarket tests:

```sh
PYTHONPATH=src "/Users/androidjk/Library/Application Support/MarketCow/atomic-freshness-venv/bin/python" \
  -m unittest discover -s tests -p 'test_polymarket_live*.py' -v
```

Result: `Ran 142 tests in 6.700s` and `OK`.
