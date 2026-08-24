# MarketCow Tradude v69 scope switch evidence

Observed 2026-08-24 CST/UTC. Acceptance was deliberately not reported because the
official CLOB book for market `3026106` became one-sided after Tradude generated the
manifest. MarketCow exposes the authoritative empty sides unchanged.

## Runtime binding

- Runtime code commit at launch: `eb36abc4e7ce5d6277e1d2410550047a1ef35fe2`.
- Scope ID: `57bac2e63ea3015df414f874385f1080176cd771b047cb82e73b14e7cb6b45ef`.
- Manifest SHA-256: `f4b9449096f11baeaf0f2146e3cf0188b08342b06a60c698fe433da01f6e2a79`.
- launchd PIDs after the switch: 8794 collector `50034`, 8791 read API `65572`,
  8790 API `65580`. Each listener was verified with `lsof` and each launchd job uses
  this worktree. The collector job contains the exact v69 manifest path.

Reproduce with:

```sh
for port in 8790 8791 8794; do lsof -nP -iTCP:$port -sTCP:LISTEN; done
launchctl print gui/$(id -u)/com.marketcow.polymarket.scoped
launchctl print gui/$(id -u)/com.marketcow.polymarket.read-api
launchctl print gui/$(id -u)/com.marketcow.events-soak
```

The checked-in launchd definitions are under `launchd/` beside this report.

## Two independent dual-end rounds

`full-sync-round-1.json` sampled each of 8790 and 8791 thirteen times. Both ports
returned only HTTP 200 / `index_ready`, exact 100 markets, 200 books, 100 complete
markets, zero unresolved gaps, zero disconnects, `memory_projection`, zero realtime
SQLite query milliseconds, and no tick mismatch. Cursors advanced from 21751416 to
21751756. The round failed only because the same two token books had an empty side.

`full-sync-round-2.json` repeated the same independent dual-end checks. Cursors advanced
from 21753215 to 21753477 and all non-two-sided criteria above remained valid. The same
two token books still had an empty side, so the round correctly failed.

Reproduce either round with:

```sh
python scripts/verify_polymarket_dynamic_tick_scope.py \
  --scope-manifest /Volumes/T9/projects/trade/tradude-worktrees/polymarket-memory-stream-contract/.tradude-local/polymarket-live/v69-two-sided-scope-20260824T082100CST/scopes/57bac2e63ea3015df414f874385f1080176cd771b047cb82e73b14e7cb6b45ef/manifest.json \
  --port 8790 --port 8791 --duration-seconds 15 \
  --output /tmp/marketcow-v69-verification.json
```

The verifier checks all 200 tokens for exact instrument `price_increment == book
tick_size` and non-empty bids/asks without synthesizing either side.

## Official CLOB blocker

`official-clob-blocker.json` records the direct `POST https://clob.polymarket.com/books`
result. Market `3026106` currently has:

- Yes token `383972...185184`: 0 bids, 4 asks, source hash `f85bd6...57dc5`.
- No token `376068...416855`: 4 bids, 0 asks, source hash `306604...e6d48`.

The hashes and empty sides match the MarketCow full-sync projection. Passing the missing
criterion would therefore require a new qualified scope or a real official book change;
filling or suppressing the sides would violate the acceptance contract.

## Hot path, append-only log, and order safety

Both dual-end rounds expose `derived_index_error="DatabaseError:database disk image is
malformed"` while continuing HTTP 200 from `events_read_source=memory_projection` with
`realtime_sqlite_query_ms=0.0`.

Across a two-second observation the authoritative event log retained inode `30826139`
and grew from 85168758503 to 85168815841 bytes. Its last cursors were contiguous 21754057
and 21754058. No replacement or truncation occurred.

The three running jobs are a scoped market-data collector and two read APIs. Runtime
OpenAPI lists every `/prediction-markets/polymarket/live` operation as GET only; there is
no order submission route or executor process. Thus real order submission is disabled
structurally for this MarketCow deployment.

## Verification commands completed

- `plutil -lint` passed for all three installed and checked-in launchd files.
- `pytest -q tests/test_polymarket_live_paper_scope.py tests/test_polymarket_live_stream.py tests/test_polymarket_live.py`
  passed: 135 tests and 9 subtests.
- Both runtime full-sync rounds executed successfully as probes but returned `passed=false`
  due solely to the authoritative two empty sides described above.
