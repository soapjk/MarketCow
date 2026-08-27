# MarketCow exact scope b16f HTTP 503 root cause and runtime evidence

Observed and repaired on 2026-08-27 Asia/Shanghai. MarketCow remained a local,
read-only market-data service throughout this work. Tradude fail-closed checks
were not changed, no health/book data was synthesized, and no real-order path
was started.

## Exact scope and failure

- Scope: `b16f4fd2f867eb371b3c065791ec2071a263114e7ac31a4d83cb72cdd0556075`
- Both 8790 and 8791 reproduced HTTP 503 with code
  `polymarket_snapshot_freshness_budget_exhausted` and message
  `The scoped in-memory snapshot cannot retain the required delivery headroom`.
- The scope was recognized; this was not a retired/unknown scope response.
- The strict freshness rejection was correct fail-closed behavior, not the bug.

The pre-switch process inventory is in `pre-switch-processes.txt`. The old
supervisor/collector/8790/8791 PIDs were `40515/40549/40550/40551`, all started
from `/Volumes/T9/projects/marketcow`.

## Root cause

The 91 GB authoritative `events.jsonl` had reached about 24.8 million events.
The old asynchronous persistence worker fsynced the authoritative JSONL and
then updated the large derived SQLite index in the same worker loop. Derived
index batches took 18.55 to 40.26 seconds while the persistence queue grew to
roughly 49,000-56,000 records. At the same time, each hot event and book was
deep-copied for persistence.

The resulting disk/CPU contention made 32-item WebSocket publication batches
take about 1-3.3 seconds. Collector protocol pings timed out, reconnect recovery
amplified the load, and the two loopback API projections stopped receiving a
fresh enough 200-book boundary. A representative failing health trace recorded
`maximum_book_age_ms=139185.479`, `freshness_headroom_ms=-134770.456`, and HTTP
503. Thus 8790/8791 both correctly rejected the stale in-memory boundary.

Reproduction locators in the retained production stderr log:

- `/Users/androidjk/Library/Logs/MarketCow/production.error.log:545189`:
  derived persistence batch 27.01 seconds, queue depth 54,289.
- Same log at `545205`: derived persistence batch 34.52 seconds, queue depth
  55,152.
- Same log at `545217`: 32-item publication took 3.30 seconds.
- Same log at `544870`: HTTP 503 health trace with the 139-second maximum book
  age and negative freshness headroom.

Useful reproduction command:

```sh
rg -n 'websocket_publish_phase|polymarket_async_persistence_batch|freshness_headroom_ms.*status.:503' \
  "/Users/androidjk/Library/Logs/MarketCow/production.error.log"
```

## Repair

Commit `6946819a5b49524efd6760929a354c2f1968ed49` separates the authority and
the rebuildable derivative:

1. The authoritative append-only JSONL owns its own queue/worker and advances
   `persisted_cursor` immediately after fsync.
2. Derived SQLite updates run on an independent bounded queue/worker. If more
   than four durable batches accumulate, the derived index is explicitly
   isolated and reported instead of delaying authority or real-time delivery.
3. Immutable event/book model references replace unnecessary hot-path deep
   copies; mutable gap state remains copied.
4. Real-time APIs remain sourced from the process-local memory projection;
   no freshness budget, headroom threshold, or Tradude validation changed.

Implementation: `src/marketcow/polymarket_live.py:4955`, `:5028`, `:5170`,
`:5241`. Regression tests: `tests/test_polymarket_live_stream.py:116` and
`:158`.

The synthetic 100-token batch benchmark improved from 0.101125 seconds before
the patch to 0.021233 seconds after it. The new blocking-index regression test
proves an authoritative JSONL fsync completes while the derived SQLite worker
is deliberately held.

## Managed runtime switch

The owner-managed launchd service `com.marketcow.production` was switched by
setting `MARKETCOW_PROJECT_DIR`/`PYTHONPATH` in the local LaunchAgent to this
dedicated worktree and re-bootstraping launchd. No Tradude session started or
restarted MarketCow.

- Running code commit: `6946819a5b49524efd6760929a354c2f1968ed49`
- Launchd supervisor PID: `84686`
- Collector PID: `84708`
- Shared API / 8790 PID: `84709`
- Read API / 8791 PID: `84710`
- Child start: `2026-08-27 16:49:09 +0800`
- Child cwd/source:
  `/Volumes/T9/projects/marketcow-workitem-collaboration-68e9d09f-0713-4cc9-b8f3-c24c23ccbf`

The launch log records these children at
`/Users/androidjk/Library/Logs/MarketCow/production.log:36777-36779`.
Startup recovered the prior derived-index tail from cursor 24,852,526 to
24,852,643 (117 events). The first complete 200-book publication took 0.242837
seconds, then normal refresh publications stayed approximately 0.001-0.13
seconds during verification.

Runtime reproduction:

```sh
launchctl print gui/$(id -u)/com.marketcow.production
ps -p 84686,84708,84709,84710 -o pid=,ppid=,lstart=,command=
for pid in 84708 84709 84710; do lsof -a -p "$pid" -d cwd -Fn; done
git -C /Volumes/T9/projects/marketcow-workitem-collaboration-68e9d09f-0713-4cc9-b8f3-c24c23ccbf rev-parse HEAD
```

## Exact-scope acceptance

`exact-scope-verification.json` contains six rounds per endpoint (12 total),
all HTTP 200 and `index_ready`. Every round has exactly 100 markets, 200 books,
100 complete markets, tick consistency 200/200, gap 0, disconnect 0,
`events_read_source=memory_projection`, `realtime_sqlite_query_ms=0.0`, and no
derived-index error.

- 8790 cursors: 24,854,314 -> 24,856,360.
- 8791 cursors: 24,854,320 -> 24,856,381.
- `real_order_submission_enabled=false`.

Reproduce:

```sh
/Users/androidjk/Library/Application\ Support/MarketCow/atomic-freshness-venv/bin/python \
  scripts/verify_polymarket_exact_scope.py \
  --scope-manifest /Volumes/T9/projects/trade/tradude-worktrees/polymarket-memory-stream-contract/.tradude-local/polymarket-live/marketcow-production-20260827T143700CST/scopes/b16f4fd2f867eb371b3c065791ec2071a263114e7ac31a4d83cb72cdd0556075/manifest.json \
  --expected-scope-id b16f4fd2f867eb371b3c065791ec2071a263114e7ac31a4d83cb72cdd0556075 \
  --port 8790 --port 8791 --rounds 6 --round-interval-seconds 5 \
  --output artifacts/marketcow-exact-scope-b16f-20260827/exact-scope-verification.json
```

## Append-only authority proof

Two authenticated tail samples 10 seconds apart used the same event-log inode
`30826139`; size increased from `97,485,201,166` to `97,489,005,107` bytes and
cursor advanced from `24,858,486` to `24,859,368`. Both tail records passed
event identity, canonical payload hash, and raw payload hash verification.

Reproduce without SQLite:

```sh
PYTHONPATH=src /Users/androidjk/Library/Application\ Support/MarketCow/atomic-freshness-venv/bin/python - <<'PY'
from pathlib import Path
from marketcow.polymarket_live import _durable_event_log_tail, live_event_identity, content_sha256
p = Path('/Volumes/T9/data/marketcow/production/prediction-markets/polymarket-live/events.jsonl')
event, size = _durable_event_log_tail(p)
print(p.stat().st_ino, size, event.cursor,
      live_event_identity(event) == event.event_id,
      content_sha256(event.canonical_payload) == event.canonical_payload_sha256,
      content_sha256(event.raw_payload) == event.raw_payload_sha256)
PY
```

## Verification commands and results

```sh
PYTHONPATH=src /Users/androidjk/Library/Application\ Support/MarketCow/atomic-freshness-venv/bin/python \
  -m unittest tests.test_polymarket_live tests.test_polymarket_live_stream \
  tests.test_polymarket_live_read_api tests.test_launchd_startup -v
# Ran 148 tests in 10.232s -- OK

ruff check src/marketcow/polymarket_live.py tests/test_polymarket_live_stream.py
# All checks passed!
```
