# Universe phase 1: implementation and activation boundary

> Current scope: [market-scope switching and software upgrades](universe-scope-and-upgrade-plan.md).
> Strategy-driven scope changes require online hot switching; incompatible software
> upgrades may stop services and rebuild market data. Read/admit evidence below
> does not imply online activation is implemented.

Status: U1 phase1-control-r1 installed on 2026-09-07 after direct user approval.
The authenticated listener is 127.0.0.1:18898 (SSH forwarding only). Existing 8793/8795 and Paper
are unchanged. This Python **control plane** never forwards real-time books and
does not import activation, collectors or accounts. Rust remains the data plane.

## Implemented

- Strict selection request/ASCII canonical hash, complete request digest.
- SQLite FULL-journal caller/request idempotency and clock fence. Permanent
  business refusals are stored too. Identical retries do not re-evaluate a new
  catalog; expired requests cannot renew their lease. Valid entries are not
  evicted. Store lifetime limits are pinned on first open; changes fail closed.
- Offline preparation from existing raw+normalized JSONL, count/hash/revision
  checks, input/output row and SQLite page budgets, atomic no-overwrite publish.
  Offset indexes avoid copying input bodies into SQLite. Excluded IDs/reasons
  remain in the prepared `omitted` table. Inputs are rehashed before publication.
- Source-backed field mapping: absent flags/date/metrics stay null; original
  event identity is required, not the old normalizer's market-id fallback.
  Relations use the final included identity set. Disagreeing relation copies
  reject preparation. Metrics require an explicit source unit mapping.
- Read-only prepared source; snapshot TTL with monotonic expiry, bounded active
  snapshots/readers, HMAC paging tokens bound to snapshot/page size, exact
  response byte budget. Restart requires a fresh snapshot.
- Loopback-only bearer caller scopes, request bytes/body deadline, no redirects
  or trust in forwarding headers. Opt-in server has no default deployment config.
- Read/admit only. Admission cannot collect, change an incumbent or activate.

## Deployed control endpoints

All three routes are on the independent configured control base:

- GET `/v1/prediction-markets/polymarket/catalog/snapshot?page_size=100`
- GET `/v1/prediction-markets/polymarket/catalog/page?snapshot_id=...&page_token=...&limit=100`
- POST `/v1/prediction-markets/polymarket/discovery-selections/admit`

`python -m marketcow.universe_control_server --config <absolute file>` requires
host/port, profile path+canonical SHA, prepared catalog path+file SHA, admission
SQLite path, caller-hash file, explicit incumbent, body timeout and row budget.
Bearer **values** are not command-line arguments or fixtures. Operator supplies
the dedicated consumer secret out of band; server config holds its SHA-256.

## Verification boundaries

Tests use synthetic frozen source files and two real ephemeral loopback HTTP
servers; they close their ports/threads. They are not U1 full-catalog evidence.
Run: `PYTHONPATH=src python3 -m pytest tests/test_universe*.py -q`.

U1 preparation verified raw 189118, normalized 189074, published 189073, source capture
2026-09-04. All 45 excluded source rows were independently located: 44 lack
the required binary tokens/outcomes and condition; one lacks event identity.
The preparation CLI deliberately reports source traversal as unverified; complete
coverage cannot be inferred from normalized count or a successful traversal of
the prepared subset. Missing source event IDs can further reduce the subset.

Actual U1 traversal covered 1891 pages, 189073 unique markets, 199980 unique
relations and 315377480 HTTP bytes in 105.286 seconds. Deterministic 10/100/1000
audit admissions passed (not strategy selections; each had zero external
dependencies). Identical retries survived a control-process restart; changed
content, bad authorization and mismatched paging limits were rejected.
Independent peer decoding of real records/errors remains outstanding.
Volume/liquidity are deliberately null because the source unit mapping remains
unverified. These are frozen metadata, not current whole-market live coverage.
Storage is bounded by configured entries/response sizes and source preparation
page budget, not a tested total disk bound including filesystem journal copies.

## Authorized U1 operation

Copy the ten `src/marketcow/universe*.py` modules, preparation script, relevant
tests and candidate profile to a versioned directory under
`/mnt/p44pro/marketcow-shadow-v3-runtime/linux/releases/`. They include source
code and remain private to U1's existing account/filesystem. No internet upload.
Read the existing bounded-discovery root's immutable manifest and source files;
write a NEW prepared metadata DB and admission DB under an operator-approved
`linux/phase1/` directory, preserving originals. Candidate bind `127.0.0.1:18898`
only, reached through existing SSH forwarding; do not touch 8793/8795 or Paper.

Before enabling, record approval for the shared `phase1-r3-test-v1` profile,
body/read/preparation budgets, new output paths, dedicated caller identity and
scopes, and the exact incumbent selection identity (or verified absence).
Do not put secrets in Channel messages. No implicit admission-to-activation.

New source/configuration-specific error codes (`clock_regression`,
`resource_profile_mismatch`, `catalog_source_invalid`, `request_size_exceeded`)
need peer agreement; this document does not declare a jointly frozen error set.

## Legacy incumbent operator shape

Operator config now also requires `legacy_incumbent`. Only a genuinely initial
installation uses both `legacy_incumbent: null` and
`expected_active_selection_id: null`. Existing Discovery uses:

```json
{
  "expected_active_selection_id": "legacy-discovery:<binding canonical SHA256>",
  "legacy_incumbent": {
    "manifest_path": "<absolute path to existing catalog.json>",
    "binding": {
      "schema_version": "marketcow.polymarket.legacy-incumbent-binding.v1",
      "origin": "legacy_discovery",
      "catalog_revision": "<manifest catalog revision>",
      "universe_revision": "<verified realtime_universe.universe_id>",
      "market_count": 1000,
      "market_ids_sha256": "<canonical SHA256 of the actual sorted ID array>"
    }
  }
}
```

This block documents field shapes, not valid deployment values. `market_count`
is checked against the actual manifest list, not silently forced to 1000.
The source universe hash is verified using the existing `content_sha256` of
the universe object without `universe_id`. IDs must already be sorted, unique,
nonempty ASCII. Binding hash uses the agreed narrowed ASCII canonical JSON.
The namespace prefix is outside the hash. No admission-generated identity is
claimed for this legacy object.

Startup and each new uncached admission re-read the manifest and compare all
binding facts and the expected ID. Drift rejects startup or yields incumbent
conflict; it never auto-adopts another list. Identical cached requests retain
their original outcome and are still not permission to activate. The prepared
catalog revision must match the pinned legacy binding. No manifest, selection,
collector or current generation is changed by these checks.
