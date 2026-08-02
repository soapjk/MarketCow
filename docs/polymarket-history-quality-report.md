# Polymarket history local candidate quality report

Date: 2026-08-02. Contract: `marketcow.prediction_market.v1`.

## Delivered work packages

| ID | Evidence |
| --- | --- |
| MC-PM-HIST-001 | Gamma catalog normalizer emits two-token canonical identity plus versioned lifecycle/rules/fees and source revision. Missing rules fail. |
| MC-PM-HIST-002 | Data API and on-chain normalizers preserve transaction identity and decimal strings for fills/split/merge/convert/redemption/resolution. |
| MC-PM-HIST-003 | Four reviewed free dataset names, fixed 40-character revision, embedded revision URL, approved license, and SHA-256 gate. |
| MC-PM-HIST-004 | Public WebSocket append-only recorder/collector, checkpoints, reconnect snapshots, duplicate/out-of-order/gap handling, rejected-raw retention, and decimal protection. |
| MC-PM-HIST-005 | Explicit-schema immutable Parquet parts and content-addressed `marketcow.prediction_market.v1` draft manifest. |
| MC-PM-HIST-006 | Evidence-bearing checks and certified/rejected manifest; published reads revalidate certification and file hash. |

## Automated evidence

The focused suite covers:

- local hit before adapter invocation and tamper detection;
- missing historical coverage failure;
- dataset revision/license/hash and prohibited vendor policy;
- Gamma two-outcome identity, rules, fees, resolution, and raw revision;
- Data API/on-chain exact trade key reconciliation;
- rejection of binary floating-point price input;
- append/replay parity, checkpoint hashes, duplicates, sequence gaps, and snapshot
  recovery;
- crossed book and tick-alignment rejection;
- two outcome token coverage;
- immutable Parquet reuse and readable Parquet output;
- certified publication and rejected missing-on-chain reconciliation;
- public OpenAPI manifest/Parquet read contract and draft invisibility.

## Honest coverage boundary

No production dataset is certified by this implementation commit because the task
provided the source design document but no reviewed concrete dataset commit, license,
file SHA-256, or coverage window. Downloading a moving revision or inventing sample
data would violate the acceptance policy.

The tests use small deterministic real-schema fixtures to verify the pipeline. Before
Tradude uses a production manifest, an operator must provide a reviewed fixed source
configuration and pass the minimum real-data gate from
`historical-data-sources.md`: at least 20 resolved binary markets, two tokens per
market, complete lifecycle/24-hour coverage, book/hash invariants, and official/on-chain
reconciliation. Shortfall produces an error or rejected manifest, never synthetic L2.

## Source exclusions

No connector, configuration, dependency, or test refers to PMData, Dome, or
PolymarketData as an available source. No trial or paid service is required. Network
code is limited to official allowlisted hosts, the public WebSocket, and hash-pinned
Hugging Face files.
