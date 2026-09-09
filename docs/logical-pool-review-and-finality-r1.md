# Logical pool review and finality read proposal r1

2026-09-09. Research evidence, not strategy approval or a deployed finality API.

## Bounded current-pool result

Live health scope `54b2ad555edfd47bd1863fc71c7bb6443ca57a50c7433644b78ef3a78a4e4a12`
at 03:48–03:52 UTC supplied 250 IDs. Those exact records were read using primary-key
queries from U1 `linux/phase1/catalog-r1.sqlite`, verifying individual payload SHA.
This is current membership plus frozen September 4 metadata, not current orderability.
Titles/dates were used only for prescreening, without price or profitability input.

Selected five markets, four possible outcome-inclusion pairs:

| Antecedent | Consequent | Observation |
|---|---|---|
| 2587798 YES (ECB +50 or more) | 2587796 NO (not unchanged) | September ECB meeting |
| 2589856 YES (BOJ +25 bracket) | 2589855 NO (not unchanged) | September BOJ meeting |
| 2589857 YES (BOJ +50 or more) | 2589855 NO (not unchanged) | September BOJ meeting |
| 2589857 YES (BOJ +50 or more) | 2589856 NO (not +25 bracket) | September BOJ meeting |

These are proposed YES-to-NO set inclusions, not YES-to-YES threshold pairs.
The last three share markets and are not independent opportunities.
No pair has been approved, priced, subscribed or activated by this work.

Current raw capture: `/private/tmp/marketcow-logical-pool-rules-r1/`.
Each `<market_id>.raw` is the complete Gamma response; corresponding `.json` is
market-evidence.v1 with full description, condition and both outcome/token bindings.
`report.json` records URL, receipt time, byte count and SHA. All five HTTP 200,
24,752 total raw bytes, 8.018 seconds through final receipt (excludes final sleep).
Five GETs, serial with 1-second gaps, no redirects/retries, 15-second per-request
timeout, 180-second total deadline checked before requests, 256 KiB per raw body,
2 MiB aggregate retained-body cap. Reqwest may deliver one chunk beyond a retained
limit; this is explicitly not a hard network-byte cap. Midstream transport errors
propagate; this CLI does not promise partial-body preservation for those errors.

Within each bank group descriptions are identical. They specify one meeting and
one policy-rate change, upper bound if a rate range, explicit rounding, exclude
unrelated emergency changes, and use a common cancellation/postponement rule:
postponed meeting before the next scheduled meeting is used; otherwise No Change.
This is materially different from the earlier Arena first-recovery-check clause.
ECB scheduled end is 2026-09-10T11:59:00Z; BOJ 2026-09-18T15:59:00Z.
End dates do not promise payout by those dates: postponement may extend resolution.
Raw resolutionSource is empty; the description names and links the central bank.
No independent check of official meeting calendars was performed in this batch.
All five raw captures report closed=false and no UMA resolution status.
Consumer must review the text and exception semantics before approving relations.

Reproduce (new output directory required; makes five source GETs):

```
cargo run --offline -p marketcowd --example logical_pool_rules -- /absolute/new-output
```

## Authoritative settlement: minimal separate contract

Existing Gamma lifecycle observations are source reports, not chain finality.
Known ended example 1088482 has frozen observation SHA
`e98acc85124391b4387e5cbc5a3bdf126c6ecaebc9fbbd2e0a62c83c8eb2f4c2` and
condition `0x0f68c3a9e26d4c35f15fd26f9a6a049b2f1407261ad8981b98977a8afa9d087b`.
It reports Yes/No = 0/1, but remains `reported_unverified`; it is not an on-chain
test result. Its current endDate must not replace actual resolution evidence.

Primary references inspected:
- https://docs.polymarket.com/concepts/resolution
- https://github.com/gnosis/conditional-tokens-contracts/blob/master/contracts/ConditionalTokens.sol

CTF exposes getOutcomeSlotCount(condition), payoutDenominator(condition), and
payoutNumerators(condition,index). Nonzero denominator indicates reported CTF
resolution. Payouts are integer fractions, potentially fractional, not necessarily
one-hot. This fact alone does not verify the deployed contract, token binding,
collateral, block finality or the actual deployed oracle/adapter configuration.

Proposed logical operation `read_finality` (NO route deployed):

- Input: explicit chain profile ID/hash, condition ID, market ID, outcome-token
  binding evidence, and explicit request deadline/byte/call budgets.
- Operator chain profile must pin chain ID, authorized RPC endpoint reference,
  CTF deployment/code evidence, collateral and adapter conventions, supported
  finality policy, and resource limits. Never guess endpoints or private credentials.
- First verify RPC chain ID; obtain one finalized block number AND hash under the
  configured policy. Pin every state read to that block hash using supported
  canonical-block semantics. Unsupported finality or block pinning fails closed;
  do not substitute `latest` silently.
- Verify contract deployment/code binding and condition slot count; read denominator
  and every required numerator at the same block. Zero denominator is unresolved.
  Require all slots present and their nonnegative uint256 sum equals denominator.
- Verify outcome index/index-set, collection/position derivation and actual token
  identity against condition+collateral+parent collection. Negative-risk adapters
  and alternate collateral are explicit profiles; do not assume vanilla mapping.
- Response schema proposal `marketcow.polymarket.finality-evidence.v1`:
  `schema_version,market_id,condition_id,chain_profile_id,chain_profile_sha256,
  chain_id,ctf_address,block_number,block_hash,block_timestamp,observed_at,
  finality,status,outcomes,evidence,missing_facts`.
- `status`: unresolved / resolved_unverified / verified_final.
  `finality`: policy ID plus policy evidence references, never just a caller boolean.
  `outcomes`: outcome, token_id, index_set, numerator and denominator (decimal
  integer strings), collateral identity. Do not convert fractions through floats.
  `evidence`: each actual request method/params hash, response raw SHA/byte count,
  immutable raw reference and block binding; credential-free source reference.
- Only full verification may produce verified_final. RPC transport errors are not
  unresolved. Unsupported mapping or missing evidence yields explicit missing facts
  and cannot enter the settlement importer. observed_at is receipt time, not historic
  label_available_at; that historical field remains unknown without separate proof.
- Each condition can settle independently. No redeem/sendTransaction/account writes.

## Precise current gap

No matching eth_call/payout/RPC reader was found in current Rust, scripts or source
implementation. `polymarket_sources.py` lists `polygon-rpc.com` in a host allowlist;
that is NOT a configured endpoint, provider agreement or tested finality capability.
No approved chain-profile/RPC credential reference was established by this audit.
Consequently no on-chain call was made and the ended example remains unverified.
To run the minimal read path, supply/locate the actual RPC endpoint reference plus
deployment/adapter and finality policy above. No paid service or default RPC will
be selected automatically. This blocker does not prevent consumer rule review.

## Local reader implementation (subsequent same-day work)

`crates/marketcowd/src/source_finality_reader.rs` now implements injected-transport
RPC reading, not just a proposed wire: chain ID, finalized block, bytecode SHA,
hash-pinned canonical eth_call for binary slot count and payout vector, checked sum,
JSON-RPC response ID/error validation, aggregate byte/call budgets and raw evidence.
Selectors were independently calculated with local Keccak-256 from ABI signatures.
Eight offline tests pass (pinned success, per-stage failure, missing config/no call,
budgets, code mismatch, integer width, unresolved/fractional payouts, sum/RPC errors).

`examples/finality_read.rs` is an explicit-config Rust HTTPS transport, compile
checked but NOT run against any RPC. Config fields: rpc_endpoint, chain_id, contract,
code_sha256, finality_policy=`rpc_finalized_hash_pinned_v1`, condition_id. Endpoint
has no default; config path is passed instead of endpoint/secret on the command line.
Limits: 7 calls, 60 seconds total, 10 seconds/request, 32 KiB/response, 128 KiB total,
no retry/redirect. Reqwest may deliver a chunk beyond the application limit; this
does not claim a hard network cap. Failures produce read_failed, never unresolved.
Successful response raw bytes are preserved as UTF-8 JSON evidence; failure reports
do not currently preserve partial successful RPC evidence or failed HTTP bodies.

This is `ctf-observation.v1`, NOT the proposed finality-evidence.v1 verifier. It
supports binary uint256 values only within u128 arithmetic range (rejects larger),
does not verify token/collateral/adapter derivation or independent chain consensus,
and never outputs verified_final or settlement_import_allowed=true. Configuring a
bytecode hash cannot stand in for those missing checks. No production route added.

```
cargo test --offline -p marketcowd --example finality_reader_offline
cargo check --offline -p marketcowd --example finality_read
```

ECB current-source observation: `/private/tmp/marketcow-ecb-scope-evidence-r1/`.
fullsync.json SHA187a6376764d3fe73a9f9af4ba738aa186ac5542a6e9505b7dba3f2daadb8723,
39,383,239 bytes, cursor96777288, instance a9ab6b55491d44efb5e05b066f9542ff.
ecb-selected-r2.json is an exact selected-subtree derivative (NOT raw wire),
80,505 bytes/SHA7b6c52bb5cffd55b15b7f4419aa0ee9e131d2500c1f3cce70d680f7aee61d198.
Contains both NO books, market metadata, rule/fee/instrument versions and published
quality frames. At snapshot both active, missing/recovering lists empty; no WS ready
was observed. Fee calculation is informational_only/rounding UNSPECIFIED. Metadata
provenance is September 4, not a newly verified fee schedule. Do not authorize entry.
