# MarketCow exact 100-market scope switch evidence

Observed on 2026-08-27 Asia/Shanghai. All processes are local MarketCow
market-data processes; real order submission remained disabled.

## Immutable scope artifacts

- Scope ID: `b16f4fd2f867eb371b3c065791ec2071a263114e7ac31a4d83cb72cdd0556075`.
- Manifest: `/Volumes/T9/projects/trade/tradude-worktrees/polymarket-memory-stream-contract/.tradude-local/polymarket-live/marketcow-production-20260827T143700CST/scopes/b16f4fd2f867eb371b3c065791ec2071a263114e7ac31a4d83cb72cdd0556075/manifest.json`.
- Registry: `/Volumes/T9/projects/trade/tradude-worktrees/polymarket-memory-stream-contract/.tradude-local/polymarket-live/marketcow-production-20260827T143700CST/scopes/b16f4fd2f867eb371b3c065791ec2071a263114e7ac31a4d83cb72cdd0556075/registry.json`.
- Candidate snapshot: `/Volumes/T9/projects/trade/tradude-worktrees/polymarket-memory-stream-contract/.tradude-local/polymarket-live/marketcow-production-20260827T143700CST/candidates/28b5d1a51b5de922933b130a5e9ce93bba86606c5193c94b8caaad5edb7a71c1/candidates.json`.
- Manifest SHA-256: `517688afba3803b2fb073f065aa6861a4b0dedd19eb2a9930d95788cd55410c2`.
- Registry SHA-256: `31816b9883f189ddc8b31051c6c4f408e249163c998a0e904a4dabbd69b2c43e`.
- The manifest contains exactly 100 unique markets and excludes `3145213`.
  The current-time candidate check proved all selected markets active,
  accepting orders, open and not expired. Earliest `end_at_ns` is
  `1787875140000000000`.

The immutable previous runtime descriptor was retained, not deleted, at
`/Volumes/T9/data/marketcow/production/prediction-markets/polymarket-live/scope-runtime-history/7073f089a3e110b95dd46e8434571d7a9347d2d9ea1c1571eb27e890c8f7b722-20260827T144200CST.json` with SHA-256
`313e1e76d5764dad5539aca260ed906c7bf1329080763519e35dd0166911e8c9`.

## Official CLOB evidence

`official-clob-verification.json` is a second direct official
`POST https://clob.polymarket.com/books` probe after the generated selection
probe. At `checked_at_ns=1787813075384274000` it requested the exact 200 scope
tokens, received 200 books, and found 200 books with non-empty bids and asks.
Missing and non-two-sided counts are both zero. The raw response SHA-256 is
`f118df42e73f0a31035f53790aaf3245e2ee42c381b39ea832873faaabb1d93a`.

The generated `selection-report.json` independently records the final complete
official probe with response SHA-256
`d6908d39f1c3dca4a88c33c04ad26c16e5a2c9d5bc6519c3677330973f21d5c7`.
It also records `3145213` in the explicit exclusion set.

## Hosted runtime and dual-end verification

- MarketCow runtime commit: `6041e230ce1b325268ac25f2f00f47ad6c5bcd9c`.
- Tradude artifact/runtime commit: `286590196e5b82a8bfb0fb0053e3d394da8b029c`.
- Supervisor PID `40515`; collector/8794 PID `40549`; 8790 PID `40550`;
  8791 PID `40551`.
- Runtime descriptor identifies the new scope, binds manifest SHA-256
  `517688afba3803b2fb073f065aa6861a4b0dedd19eb2a9930d95788cd55410c2`,
  and records `real_order_submission_enabled=false`.

`8790-verification.json` and `8791-verification.json` each contain four
independent consecutive full-sync rounds. Every round passed HTTP 200 /
`index_ready`, exact 100 markets, 200 books, 100 complete markets, 200/200
instrument `price_increment == book tick_size`, gap 0, disconnect 0,
`events_read_source=memory_projection`, and `realtime_sqlite_query_ms=0`.
The 8790 cursor advanced `24459097 -> 24459762`; the 8791 cursor advanced
`24459097 -> 24459757`.

Two earlier deliberately strict combined probes are retained as
`dual-end-verification.json` and `dual-end-verification-pass.json`. They record
one fail-closed HTTP 503 freshness-headroom rejection rather than accepting a
stale frame. They are not used as passing evidence.

## Append-only authority and order safety

Before the scope switch the authoritative `events.jsonl` inode was `30826139`
and its observed size was `95739640201`. After the switch it retained inode
`30826139`. During a later three-second observation it grew from
`95784691530` to `95785711453` bytes while the SHA-256 of the fixed first 1 MiB
remained
`9f643f8292bd842a76919a4ba3dae168c36890f8593db5c2699c55dc83d13c98`.
The final two observed event cursors were contiguous `24463653` and `24463654`.
This is consistent with append-only growth and no replacement or truncation.

The runtime OpenAPI exposes the Polymarket live and dataset operations as GET
only. No Polymarket order/trade submission operation or executor process is
present, and the runtime descriptor explicitly disables real orders.

## Reproduction

The focused automated suite passed with `154 passed, 9 subtests passed in
22.63s`:

```sh
/tmp/marketcow-b16f-venv/bin/python -m pytest -q \
  tests/test_polymarket_scope_lifecycle.py \
  tests/test_polymarket_live_paper_scope.py \
  tests/test_polymarket_live_stream.py \
  tests/test_polymarket_live.py
```

```sh
PYTHONPATH=src /Users/androidjk/Library/Application\ Support/MarketCow/atomic-freshness-venv/bin/python \
  scripts/verify_polymarket_exact_scope.py \
  --scope-manifest /Volumes/T9/projects/trade/tradude-worktrees/polymarket-memory-stream-contract/.tradude-local/polymarket-live/marketcow-production-20260827T143700CST/scopes/b16f4fd2f867eb371b3c065791ec2071a263114e7ac31a4d83cb72cdd0556075/manifest.json \
  --expected-scope-id b16f4fd2f867eb371b3c065791ec2071a263114e7ac31a4d83cb72cdd0556075 \
  --port 8790 --rounds 4 --round-interval-seconds 4 \
  --output /tmp/marketcow-b16f-8790.json

PYTHONPATH=src /Users/androidjk/Library/Application\ Support/MarketCow/atomic-freshness-venv/bin/python \
  scripts/verify_polymarket_exact_scope.py \
  --scope-manifest /Volumes/T9/projects/trade/tradude-worktrees/polymarket-memory-stream-contract/.tradude-local/polymarket-live/marketcow-production-20260827T143700CST/scopes/b16f4fd2f867eb371b3c065791ec2071a263114e7ac31a4d83cb72cdd0556075/manifest.json \
  --expected-scope-id b16f4fd2f867eb371b3c065791ec2071a263114e7ac31a4d83cb72cdd0556075 \
  --port 8791 --rounds 4 --round-interval-seconds 4 \
  --output /tmp/marketcow-b16f-8791.json

jq '{scope_id,market_count:(.market_ids|length),contains_3145213:(.market_ids|index("3145213")!=null)}' \
  /Volumes/T9/projects/trade/tradude-worktrees/polymarket-memory-stream-contract/.tradude-local/polymarket-live/marketcow-production-20260827T143700CST/scopes/b16f4fd2f867eb371b3c065791ec2071a263114e7ac31a4d83cb72cdd0556075/manifest.json

jq '{received:(.coverage.received_token_ids|length),two_sided:(.coverage.two_sided_token_ids|length),missing:(.coverage.missing_token_ids|length),non_two_sided:(.coverage.non_two_sided_token_ids|length),complete:.coverage.complete,response_sha256:.coverage.response_sha256}' \
  artifacts/polymarket-exact-scope-b16f4fd2-20260827/official-clob-verification.json
```
