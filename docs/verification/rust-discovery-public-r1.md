# Direct Rust Discovery isolated probe r1

2026-09-06, U1 only, loopback 8795. Formal 8793/Paper not restarted.

Release SHA256: `42e92a9d9765847bb5ac969e6a65beadca4de43d91cfa0414196fc92fbed38c5`.
U1 workspace release tests/build exit 0 (`linux/logs/public-api-build-r3.log`).

Reproduction on an unused probe prefix:
`python3 scripts/check_u1_rust_discovery.py --binary-sha256 <release SHA>`.
This finite harness uses an independent bounded SQLite root and only authoritative
REST market reads; no Python API, catalog refresh, ledger, or trading calls.
The recorded command and resource samples are in
`/mnt/p44pro/marketcow-shadow-v3-runtime/linux/logs/public-discovery-smoke-r1.report.json`.

- Full-sync: 1000 markets, 4329221 bytes, SHA256
  `864fd0f33a440f76def78f2cd8e95237882fa8ab222550e703d72fddbb477972`.
- 1000 raw application WS messages, cursor 2643865 → 2644865, each strictly +1,
  typed market payload cursor equals frame next_cursor. Wire file SHA256
  `1731a92b795e93e11c98681c45f00e2c62052866fe172c31b2dba8c992ba0365`.
- Status before/after ready=true, gaps=0, same projection. This does not assert
  fresh/tradable books for all markets.
- Reached frame cap in 7.79s including close, not a sustained 20-second test.
- Sampled RSS 669596–760728 KiB; no steady-state memory claim.
- Collector stopped: MainPID=0, Result=success, ExecMainStatus=0.
  Durable cursor2645411, floor2634515, history67099894 bytes (<64MiB), integrity=ok,
  no JSONL. Original roots preserved.

## Independent consumer finding — compatibility not yet passed

Unmodified Tradude `MarketCowDiscoveryClient.fetch_full_sync` rejected the raw
baseline: relations[0] missing `schema_version`. MarketCow's legacy stored
DiscoveryRelation facts omit that field. Candidate now adds the required
`marketcow.polymarket.discovery-relation.v3` wire discriminator without changing
fact evidence. A local experiment adding only this field to the captured baseline
allowed the native decoder to parse all 1000 markets and 1000 captured deltas.
That modified-input experiment is **not** a new raw HTTP success. Rebuild and
repeat real HTTP/WS + unchanged consumer replay before declaring compatibility.

Source preparation seed: 1000 markets, 241 relations, settlement present=0;
SHA256 `ea7d9423c9f506c5bbb981dfd913ba568f9c05991bb7a8716c0c92c2feeed930`.
Existing metadata only; absent settlement remains null.
