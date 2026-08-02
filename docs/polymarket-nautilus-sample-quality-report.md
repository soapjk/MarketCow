# Polymarket Nautilus sample quality report

## Source review

- Dataset: `kinzikdza/polymarket-updown-microstructure`
- Immutable revision: `eb4e9fc794c059dd9bef69c98eb4d34e70a5bd83`
- License: CC BY 4.0
- Dataset card: real public CLOB snapshots and trade tape; Gamma-only resolutions;
  May 27–June 24, 2026 overall source coverage.
- Pinned files:
  - `slots.parquet`: `893785d09d7daaa1b94cd3a5bd46a968270531d4ae11c6c6523af1cbe9b0c912`
  - `book_snapshots.parquet`: `074c6f8f367c7fe1517a28adaa70d2fd98b0edf4167175bb8f9fb4c6c183dbb5`
  - `pm_trades.parquet`: `1378effac3f2d61807b5181155dfd66526cd0199ff1f317cbeab1b4f87ef00a6`
- Official fee contract capture SHA-256:
  `8e246189f6ca85b8b8782e1d76a769cf98672a98c4c63d8bbe6150d659db8d7c`.
- Official MIT-licensed CLOB v2 SDK rounding configuration, fixed at revision
  `f3e1a05f868a1fd0c34ef85dfc45c6ce78f5bb69`, SHA-256
  `0fd2d5020c1dd9b717788fc4f58d5a4ea28b790ad97170a7b4042b6e9864001f`;
  its `size: 2` rule supplies the audited `0.01` size increment.
- Official CLOB market metadata is cached locally per condition and content-hashed.

No PMData, Dome, PolymarketData, paid service, trial allowance, synthetic price,
interpolation, fabricated delta, cancellation, or queue event is used.

## Certified publication

- Dataset ID: `polymarket-updown-nautilus-sample-eb4e9fc`
- Manifest ID: `8581237cac6625cad8403dab249a5e5f8d699f1f59a8403dc85d782634825a25`
- Bootstrap ID: `f5361a524e1df3dfd7316fe039ffdb96fdc3404c0596db3f0a9e1641ace72034`
- Intended use: `nautilus_snapshot_replay`
- Markets: 20 resolved binary markets
- Tokens: 40, with reversible canonical instrument identities
- Materialized valid snapshots: 1,158
- Materialized public trade prints: 39,005
- Market-data coverage: 2026-05-29 01:45:00.250Z through
  2026-05-29 03:30:50.149Z
- Lifecycle: activation, expiration, and resolution are complete for every market
- Replay: snapshot-only, deterministic normalized sequence, absolute sizes
- Immutable parts:
  - books: `027ee7a1a61bbd9609db63c81e0c42d6ff9e243a49ebb236663cac960f0d4a21`
  - catalog: `3bd89f28c6abdcb558127f1ad80f086de38aeddcfb2b8a5ce6ecdca19024b301`
  - lifecycle: `c75eb329782d9b408bd271be1976e4a45498343a63a109e45893d028e04d64cd`
  - trades: `3ac8f31a7877d00a0b38ff367b9e9d379c5375f33448e4b264027be7c5ab0d53`

The source card warns that raw captures can be crossed. MarketCow keeps those raw
rows in the immutable pinned input, records per-token exclusion counts in the gap
ledger, and does not repair them. Certification requires every selected outcome
token to retain at least one valid, non-crossed, tick-aligned snapshot.

The local generated manifest, bootstrap, part paths, hashes, and checks are recorded
in the versioned delivery Artifact. Re-running against the same local inputs is
local-first and yields the same content-addressed Parquet parts.

## Consumer contract

Tradude should first read the certified manifest, then the bound bootstrap, and only
then download the declared Parquet parts. It must reject mismatched `dataset_id`,
`manifest_id`, `bootstrap_id`, part SHA-256, row payload SHA-256, unsupported replay
mode, or missing fee/rule facts. A copyable bootstrap, manifest, and minimal books
Parquet fixture are delivered under the local Artifact fixture directory.
