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
- Official MIT-licensed `exchange-fee-module`, fixed at revision
  `1a3c31c48275a9adceb039a05cfcf15aba4629bc`; the pinned executable excerpt
  SHA-256 is
  `910a2918cbf71f43db2a3ce8ccc7711d86c1de92c56d628abef8bf34a8acd13e`.
  Its public `matchOrders` contract takes `takerFeeAmount` as an operator-chosen
  `uint256`; it does not publish the operator's decimal tie-breaking algorithm.
- Official CLOB market metadata is cached locally per condition and content-hashed.

No PMData, Dome, PolymarketData, paid service, trial allowance, synthetic price,
interpolation, fabricated delta, cancellation, or queue event is used.

## Certified publication

- Dataset ID: `polymarket-updown-nautilus-sample-eb4e9fc`
- Manifest ID: `8dcf3a9705b633ab727bd6597ed2dec2ff90653febe287966c50a35a63f39949`
- Bootstrap ID: `ab29cbbecf1297338b8ac50f10022de26f5f690dc171a221bcf4c046f24982f8`
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
  - books: `d2ac15b23fc3b49c10c8e726fd935263d8cc272f972578d9b6ca9021d0bd0c2e`
  - catalog: `f662d44db48a95ed0549a3d28a3743e84d2381fed3ec305bbd9e23373a950c94`
  - lifecycle: `b6a3baa4a7107de51e164ede1a02c300929eb7fb82613a8d17d3c77e2dbf5b96`
  - trades: `accdf9e1e1a58f878360e9c5f2b15e67d24520659fd5e5be2dc916823f34b852`

The source card warns that raw captures can be crossed. MarketCow keeps those raw
rows in the immutable pinned input, records per-token exclusion counts in the gap
ledger, and does not repair them. Certification requires every selected outcome
token to retain at least one valid, non-crossed, tick-aligned snapshot.

The local generated manifest, bootstrap, part paths, hashes, and checks are recorded
in the versioned delivery Artifact. Re-running against the same local inputs is
local-first and yields the same content-addressed Parquet parts.

The published books part uses only canonical token-scoped snapshot payloads with
decimal-string levels. Vendor `yes_*`/`no_*` rows and their JSON numeric provenance
remain separately available in `raw_payload_json` and are independently hashed.
Certification parses all 1,158 public payloads, rejects JSON floats, verifies public
and raw hashes, and reconstructs every `state_checksum` using the documented
provider-neutral algorithm.

## Consumer contract

Tradude should first read the certified manifest, then the bound bootstrap, and only
then download the declared Parquet parts. It must reject mismatched `dataset_id`,
`manifest_id`, `bootstrap_id`, part SHA-256, row payload SHA-256, unsupported replay
mode, or missing fee/rule facts. A copyable bootstrap, manifest, and minimal books
Parquet fixture are delivered under the local Artifact fixture directory.

The fee schedule is intentionally machine-readable as
`rounding_mode=UNSPECIFIED`, `tie_semantics=unspecified`, and
`calculation_status=informational_only`. The formula, rate, exponent, and quantum
are source-backed, but this dataset is **not certified for executable PnL**.
Consumers must fail closed instead of choosing `ROUND_DOWN`, `ROUND_HALF_EVEN`, or
`ROUND_HALF_UP`. MarketCow's deterministic quantizer has golden boundary tests for
supported modes and rejects the sample schedule at half-quantum, sub-quantum,
exact-quantum, and representative 0.50/0.01 price-derived amounts.
