# Same-version hot-scope control — candidate wire

2026-09-08. Implemented candidate, **not deployed or jointly frozen**. Replaces
no existing phase-1 wire. Software replacement remains a separate maintenance
operation; none of these routes starts systemd or copies a data root.

## Access and common behavior

Base remains the separately configured loopback control listener, reached over
authenticated SSH when remote. No management bearer on public 8793/8795. The
operator supplies an exact caller identity, bearer digest and explicit scopes.
Secrets do not appear in requests saved as fixtures, responses or logs.

Prefix: `/v1/prediction-markets/polymarket/hot-scopes`.

| Method and suffix | Caller scope | Effect |
| --- | --- | --- |
| GET `/status?pool=discovery\|live` | `hot.read` | Rust actual identity plus local candidate stages |
| POST `/discovery/prepare` | `hot.prepare` | Verify saved admission, prepare metadata/acquisition, no publication |
| POST `/live/prepare` | `hot.prepare` | Validate parent/protection, prepare metadata/acquisition, no publication |
| POST `/activate` | `hot.activate` | Actual Rust publication CAS |
| POST `/reconcile` | `hot.prepare` | Reconcile a lost receipt; may explicitly resume acquisition, never publication |
| POST `/retire` | `hot.retire` | Retire a nonactive candidate's exclusive resources |
| POST `/collect` | `hot.retire` | Bounded unreferenced resource cleanup, including legacy initial resources |

Strict JSON: unique keys, no nonfinite numbers, exact object schemas, no paths
or commands supplied by clients. POST bodies use the explicit admission byte
budget for Discovery preparation and 64 KiB for other commands. Control worker
concurrency is one; client cancellation does not release its slot early.
An uncertain response is nonretryable `runtime_state_requires_reconciliation`
(503), `reconcile_required=true`. Validation after submission may return
`runtime_operation_rejected` (409) and still require inspection. No automatic
publication retry. Authentication errors are 401/403, never an empty status.

## Discovery prepare

Reuse `marketcow.polymarket.discovery-prepare.v1` exactly:

```json
{
  "schema_version": "marketcow.polymarket.discovery-prepare.v1",
  "admission_request": "<the full phase-1 request object, not a string>",
  "admission_response_sha256": "<SHA256 of the exact saved admission response bytes>"
}
```

The strings above are notation, not a valid fixture. Admission must already
exist for this same caller and request. Its expected incumbent is compared with
the actual Rust Discovery universe binding, not a static startup selection.
Admission does not prepare or activate anything on its own.

## Live prepare

Exact fields, no extras:

```text
schema_version = marketcow.hot-live-prepare.v1
catalog_revision: SHA256
parent_selection_id: actual Discovery selection identity
market_ids: sorted unique ASCII IDs, 1..250
protected_market_ids: sorted unique subset of market_ids
protected_exceptions: [{market_id, reasons: sorted unique nonempty ASCII codes}]
policy_version: nonempty strategy policy identity
expires_ms: integer Unix milliseconds, bounded by operator candidate TTL
expected_scope_id: current actual Live scope ID
expected_revision: positive integer current actual revision
```

Exceptions must match exactly the requested IDs outside the parent Discovery
selection, and those IDs must be protected. MarketCow does not infer positions
or replace protection supplied by Tradude. Null source end dates stay null.
Candidate identity binds the complete materialized artifact; this request's
canonical digest is its Live selection binding. The candidate includes all
available dependency metadata, explicit missing dependencies and separately
declared acquired identities. No dates, settlement or books are fabricated.

## Prepare result

`marketcow.hot-scope-prepared.v1` fields:
`candidate_id,pool,selection_id,expires_ms,expected,requested_market_ids,
acquisition,publication_applied,actual` plus `schema_version`.

`expected={expected_scope_id,expected_revision}`; `publication_applied=false`.
`acquisition={all_installed:true,chunks:[{request_sha256,response_sha256,
metadata_count,acquisition_count}]}`. Bounded metadata chunks all finish before
publication can be prepared. Chunk receipt is installation, not proof that
every book arrived or is fresh. The old public scope remains active throughout.

An interrupted prepare reserves its candidate slot. An identical normal retry
returns the saved successful receipt only if prepared and unexpired; it does
not silently rerun an interrupted operation.

## Activate and reconcile

Activate exact fields:
`schema_version=marketcow.hot-scope-activate.v1,pool,candidate_id,
expected_scope_id,expected_revision,protected_market_ids`.

The candidate must belong to the caller, be prepared and unexpired, and retain
all protected IDs. Live's parent must still be the actual Discovery selection.
Rust validates current scope/revision and bounded predecessor capacity before
committing the control intent and publishing. The reply
`marketcow.hot-scope-activated.v1` carries `candidate_id,selection_id,actual,
new_full_sync_required=true`. Durable intent alone is never this receipt.

Reconcile exact fields:
`schema_version=marketcow.hot-scope-reconcile.v1,pool,candidate_id,
expected_scope_id,expected_revision`.
If the actual Rust identity matches, mark/return active without re-publishing.
Otherwise the caller's current expected identity must match; explicitly resume
the same identity-idempotent acquisition preparation if necessary. This route
never calls `publish_scope`. Expired candidates cannot resume preparation.

Clients install a new full-sync and native stream baseline. Live keeps its
ready/confirmation protocol; Discovery uses its own projection/delta protocol.
Old leases have a monotonic grace deadline; they never observe new membership.
No mixing cursors, market versions or confirmation baselines across bindings.

## Formal installation receipt (2026-09-08)

`hot-scope-r36-v1` is now installed on U1 (not merely staged).
Manifest SHA256 `004a87a3a5b353527d635664f051077acc5717bbc140cb579c169496d8172fc1`;
both running Rust children matched binary SHA256
`aec1db04448bd20c8fcbc3291deea7688bed74be35f8e4cbf23e46a9270c1b9f`.
Live LAN8793 and Discovery LAN8795 retained their roots and original250/1000
memberships. Loopback18898 authenticated hot status returned200 for both pools;
unauthenticated returned401. Dedicated caller now has the four hot scopes.
Live actual full-sync/WS advanced61948689→61949100 (411events,1ready);
Discovery8184815→8184816 (1delta). These finite probes do not prove new ranked
1000→250 activation or long-run reliability. No account was operated or reset.

Evidence directory on U1:
`/mnt/p44pro/marketcow-shadow-v3-runtime/linux/releases/hot-scope-r36-v1/installation`.
`receipt.json` SHA7131a15e1e49117c095a5925aa26c336cd1a1763260c5c5857a42db8688d346f;
`http-ws.json` SHA596d54da7ba5a0ed2a226f811973b6559e00b79fec3b24166f4dde43b189a0e9;
`control-status.json` SHA17f46bf7f0cf485a69cba6cf605780206a7ec787e3e57680b806dbe4009fea20.
The receipt preserves old shutdown exits(control143/Live1/Discovery0), not a
claim of clean old-service shutdown. New services started and supplied data.

## Retirement and resource collection

### Exact response boundaries (implementation r36)

The shared `tests/contracts/hot-scope/responses.json` is explicitly synthetic,
not an installed-service receipt. All keys below are required unless a union
branch says otherwise. A nullable value is not an omitted key.

`actual` common keys: `pool` (`live|discovery`), `revision` (positive integer),
`catalog_revision` (SHA256), `market_count` (nonnegative integer),
`source_readable` (boolean), `source_cursor`, `persisted_cursor`,
`retirement_submitted`, `retirement_persisted` (each nonnegative integer or null),
`admitted_market_ids` (string array or null), `referenced_market_ids` (string
array), `acquisition` (null or exactly `{tokens,sockets,retiring_shards}`, each
nonnegative integer). `sockets` reports acquisition slots; in REST mode these
are REST tasks, not a claim of open WebSockets. The inventories include retained
dependencies/predecessors and are not the selected-market list. Readability is
not all-market trading eligibility. Null cursors never authorize resume.

Live adds exactly `scope_id` and `stream_instance_id` (opaque strings).
Discovery instead adds `projection_id` and `universe_revision` (opaque strings);
its actual response does **not** contain `stream_instance_id` or `scope_id`.

Status is exactly `{schema_version:"marketcow.hot-scope-status.v1",pool,actual,
selection_id,candidate_id,candidates}`. Selection/candidate identities are nullable;
legacy Discovery may have selection but no candidate. Each candidate summary is
exactly `{candidate_id,stage,expires_ms}`; stages are `preparing`, `prepared`,
`active`, `retiring`. These are control records, not independently authoritative
publication receipts. `actual` remains the runtime identity authority.

Successful activation is exactly `{schema_version:"marketcow.hot-scope-activated.v1",
candidate_id,selection_id,actual,new_full_sync_required:true}`.
Reconcile has two success variants: (1) already published returns exactly
`{schema_version:"marketcow.hot-scope-reconciled.v1",stage:"active",actual,
candidate_id,selection_id,publication_applied_by_this_operation:false}`;
(2) preparation resumed returns the complete `marketcow.hot-scope-prepared.v1`
response documented above, **not** reconciled/active. Dispatch on schema, not
HTTP200 alone. A saved prepared receipt may contain an earlier actual snapshot;
activation must still perform current runtime CAS.

Hot-route errors use exactly `{schema_version:"marketcow.generation-operation-error.v1",
code,retryable,reconcile_required}`; no `request_id`, `details`, or fabricated
actual identity. Current classifications: `invalid_schema`400 before dispatch;
`runtime_operation_rejected`409 after dispatch; `runtime_state_requires_reconciliation`
503 on runtime/storage/transport exceptions; `resource_unavailable`429 with
retryable=true before dispatch. Authentication and explicit ControlError status/code
are preserved. `reconcile_required` indicates the worker was dispatched, not proof
that publication occurred. A transport loss with no response is also uncertain.
Never automatically repeat activate on 409/503 or a missing response; read status
and explicitly reconcile the original candidate. Some pre-dispatch 503 errors
have reconcile_required=false; status alone does not determine uncertainty.

Same-process Live switching preserves the source instance and cursor domain but
changes scope/revision. Obtain the new identity from authenticated status or
activation, install full-sync (scope_id/catalog_revision/projection_generation),
then request WS using that scope and baseline cursor. Ready binds through this
connection and contains instance/cursor/confirmation baseline, not scope/revision.
The current retirement error is exactly
`{"type":"error","code":"polymarket_scope_changed","message":"Scope retired; obtain current scope and new full-sync","retryable":true}`.
It contains no replacement identity or grace field. The server then attempts an
empty Close frame with a bounded send deadline; successful delivery/handshake is
not guaranteed. Grace is an operator setting, not a client wire lease timestamp.

Retire exact fields:
`schema_version=marketcow.hot-scope-retire.v1,pool,candidate_id`.
Active candidates cannot retire. Resources shared with the current scope or
another prepared candidate remain. Rust additionally protects active and
unexpired predecessor dependency references. A candidate is not deleted merely
because its retirement command was queued.

Collect exact fields:
`schema_version=marketcow.hot-scope-collect.v1,pool,expected_scope_id,
expected_revision`.
It reads Rust's actual admitted/reference inventory, retains nonexpired pending
candidates, and retires at most 4096 unreferenced metadata identities per call.
It also handles initial legacy resources not registered as a new candidate.
One pending receipt per pool is retained and subsequent explicit calls inspect
its actual disk/transport progress. No unbounded tombstone history.
New preparation is rejected while a collection intent is pending, so a retry
cannot delete identities that a newer candidate has acquired. Candidate
retirement also records its intent before sending the Rust command; explicit
retry and restart reconciliation distinguish queued and durable retirement.
Already-collected identities are not resubmitted as unknown removals.

`resources_released=false` is a pending operation, not failure or completion.
Source retirement is queued on the independent bounded persistence worker;
`retirement_submitted` and `retirement_persisted` are distinct. WS unsubscribe
ownership and REST late-response fencing/task references are separate facts.
Metadata cleanup does not delete bounded recent-event history or alter accounts.

## Operator configuration (all explicit)

The server `hot_operations` reference is `{path,sha256}`; SHA is the canonical
configuration digest. It is mutually exclusive with old `runtime_operations`
and `discovery_preparation` cold-process controls.

Exact profile keys:
`store_path,owner_lock,source_root,maximum_candidates,maximum_artifact_bytes,
maximum_row_bytes,maximum_source_bytes,maximum_relation_members,
maximum_metadata_tokens,maximum_dependency_markets,depth_quantities,
maximum_book_age_ms,candidate_ttl_seconds,legacy_discovery_universe,
legacy_selection_id,runtimes`.
`runtimes` has `live` and `discovery`; each has
`socket_path,maximum_bytes,timeout_seconds`. Paths are operator-only absolute
paths. Dedicated SQLite is not a Paper database. Every pool has bounded slots.

Each Rust collector opts in with private same-user Unix socket permissions 0600
under a 0700 directory, plus explicit `--scope-control-bytes`,
`--scope-control-timeout-seconds`, `--scope-state-file`, `--scope-state-bytes`,
`--scope-retire-grace-seconds`, `--acquisition-token-budget` and
`--acquisition-socket-budget`. The journal byte budget is separate from a
single command's byte limit: the existing Discovery seed alone is about 28 MB.
WS actor limits and REST task limits still apply; a technical token ceiling is
not a measured throughput guarantee. Formal values require an actual bound
operator artifact, not copied test settings.

## Current evidence and limits

U1 r32 workspace release tests/build passed; binary SHA
`69f540a5eccfab4271ffb385373181214d9950c798d17b7658e6cd79676a6907`.
Discovery r7 proved same-process publication and actual new-market updates.
r8 then restored the journal, published revision 5, consumed 1253 frames from
cursor 8240 to 9493, and retired 978 unreferenced markets. Final acquisition
inventory was 48 tokens / 3 REST tasks, zero retiring tasks; durable cursor
10954, history 67102800 bytes, clean exit 0. This is a ten-identity operational
sample, not a strategy-selected 1000 or a full throughput claim.

Live r1 published 250 → 250, revision 1 → 2 in one instance; the old stream
returned `polymarket_scope_changed`. New full-sync contained two authoritative
empty books for the added market. It then delivered 8754 events, 3881 confirmation
frames and one ready. The script nevertheless failed its 64 MiB evidence cap
while incorrectly waiting for an unchanged book to produce a future delta;
the report remains failed. Live r2 exposed a real managed-restart bug: the
Discovery bootstrap helper omitted Live's scope identity. The candidate fix
passes the active journal scope into bootstrap. U1 r36 regression/build passed,
binary `aec1db04448bd20c8fcbc3291deea7688bed74be35f8e4cbf23e46a9270c1b9f`.
Live r5 resumed revision 4, switched again, delivered 522 events / one ready /
one confirmation frame and stopped with exit 0. Its added market remained in
recovery; those actual facts were delivered rather than converted into books.
Final durable cursor was 8014932, tail SHA
`9e2a1508d2b080d09dccc7f361cf9306b59dfca8340dc95445473aac3568fd18`,
retained history 67107750 bytes. Sampled RSS was 3135512 KiB and HWM 3376292 KiB;
this is not a long-run memory guarantee. Outstanding recovery does not require
new network requests or all-market health during clean stop. Internal recovery
auditing does not reenable a disconnected public baseline.

All these runs used existing candidate roots, not formal services. Reports are
under U1 `linux/logs/universe-hot-{discovery,live}-r*.{log,report.json}` (Discovery
uses `-report.json`). No account state was changed.
These facts are not completion of strategy-selected 1000 → 250 → Paper flow.
