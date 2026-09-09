# Strict logical strategy inputs — MarketCow v1 candidate

2026-09-09. New evidence route is **local only, not deployed**. Existing Live
8793 is unchanged. MarketCow publishes facts, not proof of logical implication,
profitability, settlement finality, or authorization to place orders.

## 1. Rule and lifecycle evidence

Candidate GET `/v1/prediction-markets/polymarket/research/market-evidence`
requires `market_id` (decimal, <=20 characters) and `expected_condition_id`
(0x + 64 hex). Unknown/duplicate query fields reject. Fixed Gamma market URL;
no arbitrary URL, retry, redirect, disk write or scope mutation. Separate single
request slot, one-second start spacing, 15 seconds and 256 KiB retained upstream
body cap. Network chunks may be allocated before checking their length; this is
not a hard process-memory ceiling. Existing history interface is unmodified.

Success schema `marketcow.polymarket.market-evidence.v1` required fields:
`market_id,condition_id,outcomes,source,source_url,observed_at,raw_complete,
raw_bytes,raw_sha256,raw_base64,rules,closed,accepting_orders,settlement,
fee_facts,execution_eligible,missing_facts` plus `schema_version`.

`outcomes=[{token_id,outcome}]` binds the source arrays positionally, with unique
identities. Rule fields: `question,description,resolution_source,
scheduled_end_at_source,observation_start,source_unavailable_extension,
interpretation,version_sha256`. Full description is preserved, including any
exception clauses. Missing structured fields remain null. No automatic parsing
of natural-language implication or deadlines. `version_sha256` hashes the
entire raw source response, conservatively invalidating on any source change;
it is **not** an independently identified authority rule revision.

Settlement fields: `source_reported_status,reported_payouts,finality,
finality_evidence,redeemable,label_available_at`. Reported payouts are accepted
only for an explicitly resolved source status and unique one-hot string vector
bound to outcomes/tokens. All other vectors remain null. `finality=unverified`,
`finality_evidence=null`, `redeemable=null`, `label_available_at=null` regardless.
This endpoint deliberately cannot certify chain finality from Gamma or its
prices. Each market is queried independently; no pair-level settlement barrier.

Non-200 upstream bodies use `marketcow.polymarket.market-evidence-rejection.v1`
with upstream status and complete raw bytes/hash, not an empty market. Transport,
size, timeout and identity/schema failures use
`marketcow.polymarket.market-evidence-error.v1` with `code`; they never produce
an execution-eligible response. Successful evidence is also always
`execution_eligible=false`: this input alone cannot authorize execution.

## 2. Existing realtime inputs

Use Live base `http://192.168.124.3:8793` and prefix
`/v1/prediction-markets/polymarket/live`:
`/full-sync?scope_id=...`, then `/stream?scope_id=...&after_cursor=...` via WS.
Use the existing strict Tradude decoder and fresh ready/confirmation baseline;
do not mix source instance/scope generations or use historical prices as depth.
The existing bootstrap/snapshot carry market instrument/fee facts and books;
existing event/confirmation/gap/terminal messages remain the source of dynamic
quality. Missing or informational fee facts must reject the affected trade.

Existing FeeSchedule contract (`polymarket_contracts.py`) has schedule ID/version,
currency, maker/taker rates, formula/exponent/quantum, rounding/tie semantics,
calculation status, effective interval and provenance. That schema is not proof
every current market has certified values. New raw-rule evidence does **not**
fill these fields from Gamma guesses; `fee_facts=null`. Runtime instrument-book
binding additionally checks tick version, projection generation and provenance.

Actual health at 2026-09-09T02:20:03Z: scope
`54b2ad555edfd47bd1863fc71c7bb6443ca57a50c7433644b78ef3a78a4e4a12`, 250 markets;
1831352 present, 1831353 absent. Four gaps are local facts, not an all-healthy
gate. No paired live-depth assertion is possible from this pool now.

To request coverage use the existing separately authorized hot-scope control
contract in `universe-hot-control-wire.md`: get actual status; submit an explicit
Live list containing both candidates, required dependencies and current position
protection, parent Discovery selection, expected scope/revision and expiry;
prepare does not publish, activation CAS does. Consumer retains its account,
clears old market context and installs fresh full-sync/ready. MarketCow does not
choose an eviction or automatically activate this pair.

## 3. Actual current rule captures

Local Rust two-GET capture, no service modifications:
`/private/tmp/marketcow-logical-rules-1831352-r1/` contains exact `.raw` and
projected `.json` for each ID. Source identity checked against prepared catalog.

| Market | Raw bytes | Raw SHA-256 |
|---|---:|---|
|1831352|4046|cfd4a17f455a5275bc94260e5231b3b070d95c49122ab4373fad1e98ab6e894d|
|1831353|3490|b9774b2281e644e00cd7c016dfdafebec7080b40799c78e2e3c9849762c8dd6f|

Both returned descriptions are equal. They reference Overall Text Arena score,
style control disabled, a September 30 2026 23:59 ET deadline, delayed checking
when the source is unavailable, and a No result for permanent unavailability.
Separate resolutionSource fields are null; the source references are in the
description. Observation start is not specified there. Independent check-time
and source outage behavior need strategy review; equal text alone does not
certify a shared observation process. Both captures currently report closed=false
and no resolved status. No final payout evidence was obtained. Capture is current,
not an as-of rule version. The JSON evidence carries actual observed_at.

## 4. Reproducible fixtures and verification

`cargo run --offline -p marketcowd --example market_evidence_capture -- fixtures <new absolute dir>`
generates four wrappers labelled `fixture_kind=synthetic`, each containing the
complete legal response. Names: complete-text, missing-description,
terminal-reported, rule-change. Even complete-text is not execution-eligible.
Shared generated originals: `/private/tmp/marketcow-market-evidence-fixtures-r1/`.
Terminal fixtures demonstrate reported status without finality, not successful
redemption. Rule-change demonstrates invalidation; actual fee-update wire is
still the existing Live contract, not this new route.

`TMPDIR=/tmp cargo test --offline -p marketcowd --bin marketcow-discovery-collector -- --test-threads=2`
passed 94 tests, 3 ignored. Four new projection tests cover source identity,
closed/prices non-finality, resolved still-unverified, missing/changed rule text.
No new route LAN deployment, full pair live run, chain finality adapter, or
strict-arbitrage execution is claimed by these tests.
