# MarketCow exact 100-market repair and acceptance

Observed and repaired on 2026-08-26 (Asia/Shanghai). No Tradude trading process or real order submission was started.

## Current scope

- scope_id: `7073f089a3e110b95dd46e8434571d7a9347d2d9ea1c1571eb27e890c8f7b722`
- manifest: `/Volumes/T9/projects/trade/tradude-worktrees/polymarket-memory-stream-contract/.tradude-local/polymarket-live/marketcow-production-20260826T081500CST/scopes/7073f089a3e110b95dd46e8434571d7a9347d2d9ea1c1571eb27e890c8f7b722/manifest.json`
- registry: `/Volumes/T9/projects/trade/tradude-worktrees/polymarket-memory-stream-contract/.tradude-local/polymarket-live/marketcow-production-20260826T081500CST/scopes/7073f089a3e110b95dd46e8434571d7a9347d2d9ea1c1571eb27e890c8f7b722/registry.json`
- manifest SHA-256: `23b902a0653664756405ae5331db9a2e4de6781b161b50a97db94ad27bc4019f`
- registry SHA-256: `f7309b2e1ae163fff85725ce5c4eea477d6ce6756a7e5d1d593c819d238c4f65`
- manifest contains 100 unique market IDs.

## Repair

The 6.0 GiB derived `latest-state.sqlite3` failed `PRAGMA quick_check` with `database disk image is malformed`. MarketCow was stopped and the database plus WAL/SHM were moved, not deleted, to:

`/Volumes/T9/data/marketcow/production/prediction-markets/polymarket-live/recovery-backups/20260826T132122Z-derived-index/`

The corrupt database SHA-256 is `7f01bba7a7eee6206f4260dce5ea51aa1ac3070ea3db82ee66775314e7d436e8`.

The 85 GiB append-only `events.jsonl` remained untouched. A compact derived index was initialized at authenticated durable cursor `23543892`; its boundary event ID is `1535be4b1b735b006edafab9ab366792bfccd4d872dcb8b1f0d97615fe282183` and boundary line SHA-256 is `df4a5e94d5a28e975572196fac6664a89f4d7fb01f32f8ee6f3f85c9086f6aab`. Native startup tail recovery and the collector then populated the current scope.

Post-repair SQLite evidence:

- `PRAGMA quick_check`: `ok`
- `book_token_count=200`
- `book_complete_market_count=100`
- `unresolved_gap_count=0`
- observed persisted `latest_cursor=23557898`
- retained derived event-offset range: `23543892..23557898` (14,007 rows); earlier authoritative events remain in `events.jsonl` and the quarantined/rebuild artifacts.

## Running processes

- MarketCow commit: `384490691456487959306fd26646735a7415033c`
- supervisor PID: `53827`
- collector PID: `53850`
- shared API / 8790 PID: `53851`
- read API / 8791 PID: `53852`
- started: `2026-08-26 21:52:45 +0800`

## Exact-scope acceptance

Machine-readable report:

`/Volumes/T9/projects/marketcow-workitem-collaboration-7cf730b7-ba0c-489a-b1af-a216069c96/artifacts/exact-100-scope-verification-7073-20260826.json`

Result: `passed=true`, no failures.

| Port | Round | HTTP | Markets | Books | Complete | Tick | Gap | Disconnect | Cursor |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 8790 | 1 | 200 | 100 | 200 | 100 | 200/200 | 0 | 0 | 23555049 |
| 8790 | 2 | 200 | 100 | 200 | 100 | 200/200 | 0 | 0 | 23555982 |
| 8791 | 1 | 200 | 100 | 200 | 100 | 200/200 | 0 | 0 | 23555152 |
| 8791 | 2 | 200 | 100 | 200 | 100 | 200/200 | 0 | 0 | 23556056 |

All four observations reported `scope_exact=true`, `status=index_ready`, empty tick mismatch/mixed revision/revision binding failure lists, empty health mismatches, and `derived_index_error=null`.

Reproduction:

```sh
/Users/androidjk/Library/Application\ Support/MarketCow/atomic-freshness-venv/bin/python \
  scripts/verify_polymarket_exact_scope.py \
  --scope-manifest /Volumes/T9/projects/trade/tradude-worktrees/polymarket-memory-stream-contract/.tradude-local/polymarket-live/marketcow-production-20260826T081500CST/scopes/7073f089a3e110b95dd46e8434571d7a9347d2d9ea1c1571eb27e890c8f7b722/manifest.json \
  --expected-scope-id 7073f089a3e110b95dd46e8434571d7a9347d2d9ea1c1571eb27e890c8f7b722 \
  --port 8790 --port 8791 --rounds 2 --round-interval-seconds 5 \
  --output artifacts/exact-100-scope-verification-7073-20260826.json
```

## Trading safety

The launchd service starts only the MarketCow collector, shared API and Polymarket read API. Process inspection found no Tradude trading/execution/order-submission process. This repair invoked no trade, transfer or order API.
