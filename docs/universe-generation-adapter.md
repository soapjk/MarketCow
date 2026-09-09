# Direct Rust generation adapter: implementation boundary

> 2026-09-08 corrected scope: same-version market-scope hot switching remains
> required. Cross-software-version seamless upgrades, old data-format compatibility
> and automatic software rollback are not required. Follow the
> [current design and backlog](universe-scope-and-upgrade-plan.md).
> Replacing a systemd unit is deployment, not market-scope hot switching. Evidence
> Historical evidence below must not be confused with the current section.

## Current same-version implementation, 2026-09-08

This section supersedes the historical stage notes below. Formal ports have not
been updated for this candidate. No actual strategy-selected 1000/250 hot switch
is claimed.

- Collector-managed private Unix socket commands now reach the actual running
  WS acquisition owner and publication registry; no process restart or Python
  market-data relay. Metadata closure and acquired token identities are separate.
- Added WS shards preserve incumbent subscriptions. Bounded supervised retries
  retain current membership. Retired/re-added market incarnations reject old
  actor results, transport frames and confirmations.
- Managed REST mode is implemented too, preserving formal Discovery's REST
  transport. Bounded independently scheduled batches share network permits;
  removed/re-added identities fence both late books and lifecycle results.
- Publication retirement is ordered on the existing bounded asynchronous disk
  queue. Scope control returns distinct submitted/persisted retirement tickets;
  wire/task ownership remains separately counted. Ordinary event publication
  does not wait for the control journal or disk worker.
- A private bounded scope journal retains current configuration, admitted
  metadata and pending retirement intent for restart. Startup reconciles it
  before opening listeners. A lost command response remains an unknown outcome,
  not a successful operation inferred from disk intent.
- `universe_rust_control.py`, `universe_hot_candidate.py`, and
  `universe_hot_operations.py` implement bounded command transport, same-runtime
  artifacts and authenticated prepare/activate/status/retire operations. The
  server has opt-in `/hot-scopes/*` routes; they cannot share an application with
  old cold-replacement controls. Admission can consult actual Rust membership
  instead of a permanently fixed legacy incumbent.

Real Discovery r8 restored its journal, published revision 5 in the same process,
streamed 1253 deltas and retired 978 unreferenced market identities. Transport
retirement reached zero; the final durable tail was checked and process exited 0.
Real Live r1/r3/r4 performed same-process 250 → 250 changes with explicit old
scope errors and new full-sync/ready. R2 exposed a missing Live scope in managed
startup; r34 fixed it and r3 actually restored revision 2 before publishing 3.
R4 delivered partial-scope recovery facts but shutdown auditing still failed.
U1 r36 regression/build passed; Live r5 then restored revision 4, activated a new
250 scope, delivered 522 events / one ready / one confirmation frame and stopped
cleanly with unresolved recovery facts preserved. Durable cursor 8014932 and
67107750 retained bytes were checked. New missing books remained explicitly
unusable; this was not a test of new-market trade eligibility.
Full details and failed-test boundaries are in
[hot control wire and evidence](universe-hot-control-wire.md).

Local universe tests: 130 passed, with one dependency deprecation warning.
These include real local transport/storage and explicitly synthetic operation
receipts, not all real upstream paths. External reconciliation and legacy
collection are implemented; collection is an explicit bounded operation.

Formal installation is staged at U1 `linux/releases/hot-scope-r36-v1`.
Manifest SHA `004a87a3a5b353527d635664f051077acc5717bbc140cb579c169496d8172fc1`;
binary SHA `aec1db04448bd20c8fcbc3291deea7688bed74be35f8e4cbf23e46a9270c1b9f`.
All 130 Python file hashes match the local candidate. Three unit artifacts
passed systemd verification; actual control application loading registered ten
routes without starting a listener. Dedicated hot SQLite was initialized, but
no scope was admitted or activated by this check. Formal unit links/processes
remain unchanged. Maintenance coordination was sent in
`channel_message:618f3a54-9773-4187-8bb7-a318f1ad79ce`; no current Paper pause
receipt has been received yet. Never substitute an old receipt.

`scripts/prepare_u1_hot_release.py` freezes artifacts only;
`scripts/install_u1_hot_release.py` performs explicit software replacement after
that coordination. The installer preserves old unit contents and reports
`http_ws_verified=false` until separate running-state evidence is obtained.

Still incomplete: outer authenticated management against actual Rust and consumer integration, formal
operator configuration/deployment, current catalog ranking facts, and Tradude's
end-to-end strategy-selected 1000 → quote → 250 flow. Follow the main backlog.

## Historical stage notes (not current runtime status)

### Same-version runtime work through r12

The following candidate code is implemented and release-tested on U1; it is not
deployed to formal 8793/8795 and is not yet a complete acquisition switch:

- `source_scope_registry.rs`: immutable request/connection scope leases, active
  revision CAS, a single retained predecessor, monotonic grace deadline, and
  rejection of stale concurrent candidates. No process or filesystem operations.
- Live `router_managed` / `PublicScopeControl`: validate a new range against the
  current in-memory metadata, share snapshot/client semaphores across ranges,
  publish a new range, and expire old WS with `polymarket_scope_changed`.
- Discovery `router_managed` / `DiscoveryScopeControl`: independent native
  projection/baseline caches with shared capacity, and native `resync_required`
  on predecessor retirement. No fabricated Live ready frame.
- Dispatcher and Pipeline can add/remove market actors without replacing the
  shared CPU/output budgets. Completion incarnation fences reject results from
  an old actor after the same market is removed and re-added.
- Writer token admission now checks the accumulated identity count, not merely
  each incoming batch's count; rejection leaves the previous map unchanged.

Real loopback HTTP/WS tests use explicitly synthetic market fixtures. They prove
same-listener publication changes including missing books; they do not prove
new upstream subscriptions. Old scope leases preserve their original identity.
U1 workspace release test/build r8 through r12 exited 0, including final r12
systemd Result=success/ExecMainStatus=0 and persistent exit-code 0. Final binary
SHA256: `f0986293397d3b1a74651d9f0d8758bd3a9cf2d5215e96826e566ecf0bd7bace`.
Logs: `linux/logs/universe-runtime-build-r12.log` and `.exit-code`.

Remaining wiring: acquisition-owner command intake, admitted new token/metadata
publication and ordered persistence binding, transport/confirmation subscription
updates, final reference-based retirement, and authenticated control requests.
The existing collector launcher still uses the non-managed router wrapper; it
does not expose activation. A passing build is not a formal hot-switch delivery.

The local `universe_generation.py` module provides a bounded durable **desired**
generation store. It does not replace the running collector or public router.
Never interpret `pending_runtime_application` as `active`.

Implemented: separate discovery/live budgets (1000/250 requested identities),
content-bound artifacts, sorted identities and separate dependencies, parent
selection/catalog binding, protected outside-parent exceptions, clock fence,
restart-safe CAS epochs (including ABA rejection), candidate expiration,
bounded retention and explicit rollback via a fresh CAS.

`register` is an internal supervisor operation after verification, not an
unauthenticated API for clients to self-certify preparation. Capacity estimates
and dependency budgets still come from validated admission. Registration does
not start collection. `select` records only desired state. Its SQLite operations
are outside the real-time publication path.

Still required before exposing activation:

- Translate admitted discovery/live artifacts into independently prepared Rust
  runtimes without mutating incumbent roots. Preserve exact selection and all
  required dependency identities; no all-healthy requirement.
- Apply one desired epoch to Rust dispatch and retire old WS generations with
  explicit scope-change semantics. Do not proxy books through Python.
- Return a separately verified running-generation receipt with fresh instance,
  full-sync/ready and actual public endpoint. Validate epoch on acknowledgement.
- On restart reconcile desired and running generation; a committed intent is
  not evidence of a completed switch. Define crash windows and compensation.
- Wire scoped authenticated management requests to verified admission and the
  supervisor; register/select are not presently exposed on port 18898.
- Test real preheat, failure preservation, rollback, client generation fencing
  and bounded resource retirement. Existing unit tests use synthetic metadata;
  they do not establish runtime switching success.

No remote deployment or incumbent changes were performed for this module.

## Runtime adapter and current evidence

`universe_systemd.py` implements registered-unit SHA verification, bounded
systemctl operations, atomic symlink replacement, and restoration/start of the
previous unit after publication failure. Starting the old unit is not a verified
healthy rollback receipt. `universe_runtime_command` holds a process owner lock
through the operation and durable acknowledgement; `reconcile` only probes.
Binding hashes include candidate/preheat unit hashes and endpoints, excluding
the generation ID (self-reference) and the incumbent (operation CAS precondition,
not candidate content). The pinned operation config and actual unit symlink
separately verify the incumbent. This permits a retained A after A→B without
pretending B was A's original predecessor. These modules are not yet mounted on
18898.

Live promotion requires an applied Discovery receipt, with no pending parent
transition. A desired parent alone is insufficient. All tools modifying desired
state must use the same owner lock as the publishing supervisor.

Discovery is probed with its native full-sync and advancing delta protocol,
not a fabricated Live ready frame. Runtime receipts explicitly distinguish
`identity_kind=discovery_projection` from `stream_instance`. Existing U1 8795
read-only probe: exact 1000 identities, projection
`0ddd3fefd83641aba809d387a626aefcfb5471c63e4ac76a1f5717770f83fc9b`,
cursor 6209347 → 6209348. This is incumbent protocol evidence, not a new switch.

`universe_source_plan` walks the prepared catalog with explicit dependency/token
budgets, keeps requested identities separate, and reports missing dependencies.
Actual U1 existing-plan parity (no state/listener writes):

| Pool | Requested | Pool-external dependencies | Total tokens | Relations |
| --- | ---: | ---: | ---: | ---: |
| Live | 250 | 531 | 1562 | 830 |
| Discovery | 1000 | 1873 | 5746 | 3114 |

Both had zero missing dependency identities. The Discovery traversal exceeded
the existing phase-1 4096-token admission profile; the second run used a larger
**read-only diagnostic** budget, not a changed production profile. Metadata
closure completeness must not be confused with realtime quote coverage.

Source-plan parity result hashes:
Live `5e410939de64e8940a0926ee5e560df4a246c4441308af40f8da68cb2996deb9`;
Discovery `c6dce9294ae368d1168be374d3afb72089cc57c815afe1f324aae84e28311679`.
U1 audit source is under `linux/universe-plan-audit-r1`; reproduce with
`audit_universe_source_plan.py --help` and explicit prepared catalog/file hashes
and budgets. These are existing lists, not newly ranked strategy selections.

The optional end-date candidate preserves explicit null rather than fabricating
or filtering dates. Known-date scope hashes remain unchanged; changing a date
to null changes the content hash. Missing fields remain invalid. Python tests
pass; Rust release validation runs on U1 before this candidate may be deployed.

## Direct live probe implementation

`universe_live_probe.py` now performs bounded real HTTP full-sync followed by
WS event replay/ready, with exact selected identities, catalog/scope and instance
binding. It rejects cursor regressions, mismatched event cursors and pre-ready
errors. Missing books are not a global readiness gate. This is a publication
identity probe, not a replacement for full book/hash/confirmation-version
consumer validation.

2026-09-07 local-to-U1 read-only probe on existing port 8793 succeeded:
instance `ef9ae9f82b804a72ad5d3d51d03b7404`, full-sync cursor `39447086`,
ready cursor `39448256`, existing exact 250 scope. The connection closed after
ready; no process restart, pool switch or account write. This verifies the probe
against the running Rust wire, not a newly prepared/switched generation.

## Cold-generation evidence, 2026-09-07

U1 workspace release regression/build r6 returned persistent exit 0 and systemd
Result=success/ExecMainStatus=0. Binary SHA256
`a3ef2127a46c61b322c25875d77036ed27f854ab1422b3d9134b36b45aa6b45b`.
Live preheat r3 used a **new empty state** and the incumbent 250 test selection:
instance `085536a4fa2e41b783260db7575d10bc`, HTTP full-sync 166 → WS ready 671.
After clean child stop, SQLite quick_check passed, exact hashed durable tail 671,
562 books, 5,405,626 recent-event bytes below 64 MiB, gap count 0. No all-healthy
gate: the sampled HTTP health still explicitly reported missing markets.
Report: U1 `linux/logs/universe-live-preheat-r3-report.json`.

Cold startup fixes are bounded empty-history reopening, initial missing tick as
a local unavailable/recovery fact, and recovery persistence before the first
book without inventing its receipt timestamp. Live/Discovery token identities
are now bound from the validated plan even without a Live dependency plan.

Discovery candidate preparation reads the immutable raw catalog with explicit
total/row/member budgets, verifies its hash and selected normalized evidence,
retains complete relation members with raw-group counts, and reuses the typed
settlement mapper. It does not copy historical books, silently omit closed
identities, or infer settlement from end dates. Selected acquisition remains
1000 identities/2000 tokens; 1873 external metadata dependencies and 5746 closure
tokens are separately reported, not subscribed as extra discovery identities.

U1 r7 release binary SHA256
`3e1a7c1acbde90a0c6ddf3a9486baa5f8a26b9368c39cd0044795da89729d0cb`
passed workspace release tests/build. Actual Discovery preheat r2 full-sync
contained the exact 1000 test identities and yielded native delta 260→261,
projection `ca73401d0314868fd4ca7b5b48aa98dbbb5755d8e884cec50b7b5239a9b9d0bb`.
Clean stop: durable cursor 1394, hashed recent-event tail, 9,056,915 history bytes,
SQLite quick_check ok. Report: `linux/logs/universe-discovery-preheat-r2-report.json`.
These are finite preheat/protocol identity checks, not full consumer validation,
new strategy selection or formal publication. Formal service PIDs remained
Live 280461 and Discovery 276977 during these checks.

## Opt-in management implementation (not yet enabled on U1)

`GenerationOperations` resolves only operator-registered config paths/hashes;
client payloads cannot supply units, executable paths or source roots. A single
owner lock covers desired CAS through actual preheat/publication/receipt. The
loopback control application can explicitly mount:

- GET `/v1/prediction-markets/polymarket/generations/status?pool=live|discovery`
  with `runtime.read`.
- POST `/v1/prediction-markets/polymarket/generations/apply` with
  `runtime.activate` and strict `marketcow.polymarket.generation-apply.v1`.
  Fields: schema_version, operation (activate/rollback/reconcile), pool,
  generation_id, expected (generation_id/epoch or null), protected_market_ids.

Catalog/admission credentials do not acquire these scopes implicitly. No routes
are mounted unless an explicit hash-bound runtime registry is supplied. Requests
are limited to 64 KiB, one runtime worker at a time; client disconnect does not
release ownership while the bounded operation continues. CAS retry is not a
second implicit deployment: clients must read desired/applied and reconcile.
Failures may leave desired intent pending and are not reported as an unchanged
incumbent or a successful rollback. Existing installed control stays unchanged.

Still open: admission-to-prepared generation registration and operator unit
construction, actual runtime publication/rollback with current Paper coordination,
consumer generation-bound account source migration, and bounded generation-root
retirement. Do not equate mounted `apply` code with this complete install flow.
