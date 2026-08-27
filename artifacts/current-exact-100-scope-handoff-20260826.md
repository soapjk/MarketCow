# MarketCow current exact 100-market scope handoff

Observed on 2026-08-26 (Asia/Shanghai). This is a read-only handoff. No trading or order-submission process was started.

## Current scope

- scope_id: `7073f089a3e110b95dd46e8434571d7a9347d2d9ea1c1571eb27e890c8f7b722`
- manifest: `/Volumes/T9/projects/trade/tradude-worktrees/polymarket-memory-stream-contract/.tradude-local/polymarket-live/marketcow-production-20260826T081500CST/scopes/7073f089a3e110b95dd46e8434571d7a9347d2d9ea1c1571eb27e890c8f7b722/manifest.json`
- registry: `/Volumes/T9/projects/trade/tradude-worktrees/polymarket-memory-stream-contract/.tradude-local/polymarket-live/marketcow-production-20260826T081500CST/scopes/7073f089a3e110b95dd46e8434571d7a9347d2d9ea1c1571eb27e890c8f7b722/registry.json`
- manifest SHA-256: `23b902a0653664756405ae5331db9a2e4de6781b161b50a97db94ad27bc4019f`
- registry SHA-256: `f7309b2e1ae163fff85725ce5c4eea477d6ce6756a7e5d1d593c819d238c4f65`
- manifest declares 100 unique market IDs and registry_id `18b62988138f67a194ef4eafa5547885cc74a1a4f70476416ba8606bc0dabd42`.

The current scope is established by all of:

1. `MARKETCOW_POLYMARKET_SCOPE_MANIFEST` in `/Users/androidjk/Library/Application Support/MarketCow/production.env`.
2. `scope_selection_validated` in `/Users/androidjk/Library/Logs/MarketCow/production.log` (for example line 26059), which records the scope ID, manifest path/hash, 100 market IDs and token_count 200.
3. The collector PID command line, which contains the same 100 market IDs.

The older `/Volumes/T9/data/marketcow/production/prediction-markets/polymarket-live/scope-binding.json` points at `6ca68c...` and is stale relative to the active production environment/startup evidence; it must not be handed to Tradude as the current scope.

## Running revision and processes

- MarketCow commit: `384490691456487959306fd26646735a7415033c`
- main checkout: `/Volumes/T9/projects/marketcow`
- supervisor PID: `68989`
- collector PID: `69665`
- shared API / 8790 PID: `69667`
- read API / 8791 PID: `69668`
- children started: `2026-08-26 19:47:10 +0800`

All three child processes have cwd `/Volumes/T9/projects/marketcow`; that checkout resolves to the commit above. The commit time is `2026-08-26T08:20:24+08:00`, before the current process start.

## Acceptance status: not passed

The required live dual-endpoint acceptance cannot currently be reproduced:

- Six samples from `2026-08-26T12:51:26Z` through `12:51:54Z` returned HTTP 503 on both 8790 and 8791 with `polymarket_snapshot_freshness_budget_exhausted`.
- A prior same-session HTTP 200 health sample on both endpoints showed `index_ready`, market_count 100, token_count/book_token_count 200, book_complete_market_count 100, unresolved_gap_count 0 and cursor `23443346`, but also showed `live_stream_disconnect_count: 6` and `derived_index_error: DatabaseError:database disk image is malformed`. Therefore it does not satisfy disconnect 0.
- `artifacts/exact-100-scope-verification-20260826.json` is a failed verifier run: 8790 reset the connection and the launchd-managed stack subsequently restarted; it is failure evidence, not acceptance evidence.
- Consequently there is no current reproducible proof of dual HTTP 200, tick 200/200, disconnect 0 and cursor advancement. `corptie_work_item_report_acceptance` must not be called.

Reproduction:

```sh
for port in 8790 8791; do
  curl --noproxy '*' -sS -w '\nHTTP_STATUS=%{http_code}\n' \
    "http://127.0.0.1:$port/v1/prediction-markets/polymarket/live/health"
done
```

Once the runtime health issue is repaired, rerun:

```sh
/Users/androidjk/Library/Application\ Support/MarketCow/atomic-freshness-venv/bin/python \
  scripts/verify_polymarket_exact_scope.py \
  --scope-manifest /Volumes/T9/projects/trade/tradude-worktrees/polymarket-memory-stream-contract/.tradude-local/polymarket-live/marketcow-production-20260826T081500CST/scopes/7073f089a3e110b95dd46e8434571d7a9347d2d9ea1c1571eb27e890c8f7b722/manifest.json \
  --expected-scope-id 7073f089a3e110b95dd46e8434571d7a9347d2d9ea1c1571eb27e890c8f7b722 \
  --port 8790 --port 8791 --rounds 2 --round-interval-seconds 5 \
  --output artifacts/exact-100-scope-verification-7073.json
```

## Trading safety

No order submission was invoked. The launchd production program starts only the MarketCow collector, shared read API and Polymarket read API. The only Tradude-related running process observed was a dashboard process; no Tradude trading/order-submission process was started by this work item.
