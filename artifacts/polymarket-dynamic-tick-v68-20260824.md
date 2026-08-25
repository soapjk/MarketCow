# MarketCow dynamic tick / instrument facts v68 verification

Generated on 2026-08-24 (Asia/Shanghai) for exact scope
`e583f5306111b5b8b336d95bb0bfd0872ca864417fed6abb2b981fe23a79e0ee`.

## Requirement and boundary

- Consumer: Tradude v68 strict preflight; MarketCow remains the authoritative
  market-data producer and does not weaken consumer validation.
- Flow: official Gamma instrument facts + official CLOB book/tick events ->
  MarketCow collector -> append-only event log -> loopback memory projection ->
  atomic scoped full-sync on ports 8790/8791.
- Data: official Polymarket Gamma/CLOB sources; tick values are normalized decimal
  strings, not binary floating-point amounts.
- Permission boundary: these processes collect and serve market data only. The
  runtime commands contain no execution/order submission capability; no Tradude
  scope, configuration, or true-order setting was changed.
- Acceptance boundary: exact 100-market scope, 200 books, 100 complete frames,
  no unresolved gap/disconnect, progressing cursor, memory projection only, and
  strict equality between every bootstrap instrument `price_increment` and both
  outcome books' `tick_size`.

## Runtime

- Running commit: `66199779dd5d52da8372119fbadd894e9247fcfa`
  (`163d5e096c3611d4ac29eed32ecba3547710b40b` contains the implementation;
  `6619977` adds the verifier).
- Collector PID 8141, read API PID 8143, main API PID 8145; all started
  2026-08-24 08:12:03 CST from this task worktree by the MarketCow owner.
- launchd jobs: `com.marketcow.polymarket.scoped`,
  `com.marketcow.polymarket.read-api`, and `com.marketcow.events-soak`.

Reproduce runtime ownership and commit:

```sh
git rev-parse HEAD
ps -p 8141,8143,8145 -o pid,ppid,lstart,command
launchctl list | rg 'com.marketcow.(polymarket.scoped|polymarket.read-api|events-soak)'
```

## Six-token before / after evidence

Before the fix, the same successful full-sync boundary observed at cursor
21714495 / projection_generation 6647330 returned these facts:

| Market | Instrument revision before | Facts | Tokens / live tick |
|---|---|---:|---|
| 1296001 | `10f044665871b13aa9eb6cc9e42f85a9271ed9528ccb87e7e46e8207ec4bfa08` | 0.01 | `685192…39187`, `451299…72683` / 0.001 |
| 1296002 | `5351c8b63c9475be3bb04a19ef01ef7fd16e7d0c6eea78d6254b2f80c08cfc19` | 0.01 | `151163…45354`, `111011…70759` / 0.001 |
| 1296004 | `5ae21d18b0cbb3c220bf401ea6b20f9bbbf4eaa9c20dbbf5a39bb211468fce08` | 0.01 | `561042…80360`, `653572…98250` / 0.001 |

After the fix, port 8790 returned:

| Market | Instrument revision after | Facts/book tick | Audited boundary |
|---|---|---:|---|
| 1296001 | `0f8f818ba0ef96a6bd04ac9da5accf128f1ccdc10a8af619bec058762d2f7e76` | 0.001 | cursor 21724623, generation 264 |
| 1296002 | `24a0e553563e7db9d3d1062307bf6aea88d2ff58c0235c3c8c3875b5b50f2cde` | 0.001 | cursor 21724657, generation 332 |
| 1296004 | `3903d5ac7103df90f9808c3f55e73bb6d56620ad2eb71d3d50abc2337716d137` | 0.001 | cursor 21724691, generation 400 |

All six books and all three dynamic provenance records expose tick_version
`34198d786c5ff128135dfebceb9132fad93b90368cc4e11da1ae74913069a823`.
The complete, unabridged token IDs, state revisions, provenance payload hashes,
and both ports' results are in
`artifacts/polymarket-dynamic-tick-v68-20260824.json`.

## Exact-scope runtime verification

```sh
PYTHONPATH=src /tmp/marketcow-tick-test.RDL0W6/bin/python \
  scripts/verify_polymarket_dynamic_tick_scope.py \
  --scope-manifest /Volumes/T9/projects/trade/tradude-worktrees/polymarket-memory-stream-contract/.tradude-local/polymarket-live/v68-current-scope-20260823T235000CST/scopes/e583f5306111b5b8b336d95bb0bfd0872ca864417fed6abb2b981fe23a79e0ee/manifest.json \
  --port 8790 --port 8791 --duration-seconds 15 \
  --output artifacts/polymarket-dynamic-tick-v68-20260824.json
```

Result: passed. Each port produced 13 successful samples. Port 8790 cursor
advanced 21726197 -> 21726349; port 8791 advanced over the same range. Every
sample had HTTP 200, exactly 100 scope markets, 200 books, 100 complete markets,
unresolved_gap_count=0, live_stream_disconnect_count=0,
events_read_source=memory_projection, realtime_sqlite_query_ms=0, and zero tick
mismatches across all 100 markets.

The production derived index was independently observed malformed
(`DatabaseError: database disk image is malformed`) in every health sample. The
successful live results therefore also reproduce that derived SQLite corruption
does not block the memory hot path or the authoritative event log.

## Automated regression and build

```sh
/tmp/marketcow-tick-test.RDL0W6/bin/ruff check src tests scripts
PYTHONPATH=src /tmp/marketcow-tick-test.RDL0W6/bin/python -m unittest discover -s tests -q
uv build --out-dir "$(mktemp -d /tmp/marketcow-tick-build.XXXXXX)"
```

Results: Ruff passed; 710 tests passed with 21 skipped; wheel and sdist built.
The dynamic-tick regression specifically proves the first per-token update fails
closed as mixed, and only the second matching token publishes a full-sync with
matching facts, tick_version, cursor, projection_generation and provenance.
