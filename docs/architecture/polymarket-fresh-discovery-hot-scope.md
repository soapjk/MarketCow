# MarketCow Polymarket 新鲜 Discovery 投影与热 Scope 激活方案

- Status: proposed (aligned with current runtime: `polymarket-discovery-v2.md`,
  `polymarket-scope-lifecycle.md`, `current-runtime.md`, and the Rust `global_cursor` /
  `projection_generation` / `UniverseChanged` contract)
- Date: 2026-09-03
- Consumer: Tradude observe-only Polymarket Shadow
- Public local endpoint: `http://127.0.0.1:8790`
- Safety boundary: market data only; no order submission, cancellation, signing, wallet or transfer API

## 1. Decision

MarketCow owns the complete data-plane problem for Polymarket Discovery: it builds a fresh
full-market lightweight baseline from live Polymarket sources, buffers and applies changes across
the bootstrap boundary, publishes one coherent cursor-bearing projection, retains bounded replay,
and rebuilds after an unprovable gap. Tradude never reconstructs that baseline from Polymarket or
from MarketCow persistence.

Tradude continues to own trading semantics. It consumes MarketCow facts to prefilter and rank
potential opportunities, publishes an exact desired Scope, and performs detailed strategy and
paper-execution evaluation after MarketCow activates that Scope. MarketCow does not compute edge,
profit, APY, capital release, strategy scores, or final rankings.

All Tradude traffic uses port `8790`. Ports `8795` and `8796` remain private implementation details
inside the MarketCow supervisor and are not consumer contracts.

## 2. Problem statement

The current hot Scope data plane is healthy and atomic, but the full-market Discovery boundary is
served from a disk-materialized snapshot that can remain readable long after it stops advancing.
The observed Discovery boundary was `1278934` while the authoritative hot projection had advanced
to approximately `1415106`. The API still reported Discovery materialization state `ready`, even
though all 1000 markets failed Tradude's readiness checks and 740 were rejected as stale.

The present distinction between “a materialized database can be opened” and “the projection is
current enough for live discovery” is not visible in the top-level readiness contract. This lets a
stale historical artifact masquerade as a live baseline.

The redesign must preserve the useful two-tier topology:

```text
full-market lightweight Discovery (~1000 markets)
        -> Tradude potential-opportunity selection
        -> bounded detailed hot Scope
        -> Tradude Shadow execution-quality evaluation
```

It must not solve the issue by putting strategy ranking in MarketCow or by making all 1000 markets
use the expensive detailed Shadow representation.

## 3. Ownership boundary

### 3.1 MarketCow owns

- Gamma/CLOB/WebSocket source access and source evidence;
- discovery-universe membership facts and canonical identities;
- fresh REST bootstrap of required books;
- buffering live changes while bootstrap is in progress;
- per-market ordering, global MarketCow cursor assignment and gap detection;
- the coherent in-memory Discovery projection;
- full-sync publication, bounded event replay and resync decisions;
- fee, tick, minimum size, lifecycle, outcome and relation facts;
- freshness and `ready/unready` truthfulness;
- candidate Scope validation, warmup, atomic generation activation and rollback;
- one unified public loopback gateway on `8790`.

### 3.2 MarketCow does not own

- opportunity thresholds or strategy parameters;
- gross/net edge, expected return or capital-release calculations;
- Top-N strategy ranking or incumbent score bonuses;
- paper fills, portfolio accounting or real trading;
- a compatibility adapter for the current private Discovery client contract.

## 4. Fresh baseline construction

### 4.1 Definition

A Discovery full-sync is a newly constructed current projection, not a persisted snapshot loaded
from a previous process. “Full-sync” describes an atomic consumer boundary; it does not require the
upstream provider to offer one atomic response.

MarketCow may fetch books in bounded batches, but it must prove that the published state can be
continued by the public delta stream without omission or reordering.

### 4.2 Bootstrap sequence

```text
1. Refresh and freeze one source-bound catalog/universe revision.
2. Open the required Polymarket live subscriptions and begin buffering input.
3. Fetch current books for every member in bounded batches.
4. For each market, reconcile the REST book with buffered events using source evidence.
5. Serialize accepted snapshots and deltas through one MarketCow ordering boundary.
6. Reject any market whose ordering, identity, tick, fee or gap status cannot be proven.
7. Publish the complete Discovery projection at boundary_cursor=N atomically.
8. Continue the same projection with events N+1, N+2, ... .
```

The exact provider reconciliation algorithm is an implementation choice, but its outcome must be
testable. If REST and WebSocket state cannot be ordered using provider timestamp/hash/revision and
MarketCow receipt evidence, the affected market is fail-closed; MarketCow must never guess.

### 4.3 Startup and recovery

On process start, MarketCow may read disk checkpoints only as recovery hints and audit evidence.
They cannot be published as live Discovery data until every required market has been refreshed and
reconciled against the new live subscription.

Before a fresh baseline exists:

```text
state = building
ready = false
HTTP full-sync = 503 discovery_projection_building
```

For a resumable transport interruption, MarketCow replays from its retained cursor window. For an
expired cursor, catalog boundary, unresolved source gap, or lost ordering proof, MarketCow builds a
new projection identity. During rebuild it either keeps the preceding projection explicitly marked
stale/unready for diagnostics or returns 503; it never labels that projection ready.

Disk remains valuable for audit, replay tests, restart acceleration and reconciliation. It is not
an authority for current opportunity discovery.

## 5. Public Discovery contract on 8790

### 5.1 Breaking vNext contract

Replace the current multi-call snapshot assembly contract with one explicit baseline plus stream:

```text
GET /v1/prediction-markets/polymarket/live/discovery/full-sync
WS  /v1/prediction-markets/polymarket/live/discovery/stream?after_cursor=N&projection_id=P
GET /v1/prediction-markets/polymarket/live/discovery/status
```

This is a private breaking change. Update all call sites together; do not retain schema probing,
optional old fields, or a compatibility branch. The three-endpoint shape is authoritative: it
collapses the current multi-call snapshot-assembly contract (which forces the consumer to remember
the binding between `snapshot_id`, pages, `metadata`, and `relations`) into one atomic consumer
boundary the consumer tracks with a single `projection_id` plus `global_cursor`. `metadata` and
`relations/{id}` are not separate endpoints here because they would re-expose the materialized
snapshot as a consumer data source; their content moves into `markets[]` / `relations[]` of the
full-sync and into the `relation_changed` stream events.

The route-level migration is fixed and is not a compatibility branch:

| Current (discovery v2) | Replaced by (this contract) | Fate |
|---|---|---|
| `GET .../discovery/snapshot` (multi-page assembly) | `GET .../discovery/full-sync` (paginated, one immutable boundary) | removed in the final delivery phase |
| `GET .../discovery/events?after_cursor=N` | `WS .../discovery/stream?after_cursor=N&projection_id=P` | removed |
| `GET .../discovery/metadata` | full-sync `markets[]` + `relations[]` | removed |
| `GET .../discovery/relations/{id}` | full-sync `relations[]` + stream `relation_changed` | removed |
| discovery state reported through `/v1/health` | dedicated `GET .../discovery/status` | health keeps only generic/primary-service state |

The full-sync may use immutable pagination if its bounded payload requires it. Every page must be
bound to the same retained `projection_id`, `catalog_revision`, `universe_revision`, and
`boundary_cursor`; pages cannot trigger materialization or observe different live boundaries.

Required envelope fields:

```text
schema_version
projection_id
catalog_revision
universe_revision
boundary_cursor
built_at
oldest_market_received_at
newest_market_received_at
ready
fail_closed_reason
unresolved_gap_count
depth_notionals
market_count
relation_count
markets[]
relations[]
```

`depth_notionals` is an array of decimal-base-outcome-size strings (e.g. `["10","50","100","500"]`),
carrying the same configured tiers as `MARKETCOW_POLYMARKET_DISCOVERY_DEPTH_NOTIONALS`. It is emitted
for context, not as a per-market value; each market's per-tier `buy_cost_at_notional` /
`sell_proceeds_at_notional` remain the authoritative costs/proceeds fields. Missing or unset tiers
yield an empty array, never a guessed default.

Each market exposes only source facts needed for broad screening: identity, lifecycle, two-sided
best prices, configured depth-tier costs/proceeds, fee schedule, tick/minimum size, data timestamps,
relation membership, `book_status`, and reason codes. Decimal values remain strings. Missing facts
remain null and cannot be serialized as plausible zeroes.

### 5.2 Readiness semantics

`ready=true` means all top-level invariants are satisfied at the published boundary:

- the projection was constructed in the current process/session from refreshed source data;
- catalog and discovery-universe revisions are checksum-bound;
- there is no unresolved global gap;
- the cursor can continue through retained stream events;
- projection age and build duration satisfy explicit configured limits;
- `oldest_market_received_at` is within the configured maximum market age at the boundary — a
  projection whose aggregate state is fresh but whose oldest market has silently stopped advancing
  is not ready (this is the exact failure the redesign exists to prevent);
- pagination and relation membership are internally consistent.

The configured limits above must be explicit. `MARKETCOW_POLYMARKET_CONSUMER_MAXIMUM_BOOK_AGE_SECONDS`
and `MARKETCOW_POLYMARKET_MINIMUM_DELIVERY_HEADROOM_SECONDS` govern the hot read path; the Discovery
baseline uses its own named age/build/duration limits, which are static configuration (not per-market
budgets) unless a scale-aware override is explicitly enabled. Missing values fail startup, and
fail-closed is preferred over silently widening an age limit when the baseline is large and slow.

Individual markets may be fail-closed without making the entire 1000-market projection unavailable,
provided their exact status and missing fields are published. Global ordering loss, a cross-revision
boundary, or an inability to serve one coherent baseline makes the entire projection unready.

`status.state=ready` must use the same definition as full-sync readiness. Introduce separate fields
for persistence/materializer health; opening a SQLite database is never sufficient for ready.

### 5.3 Delta stream

The stream publishes actual changes, not only invalidation notifications. Event types include:

```text
market_quote_changed
market_fail_closed
market_recovered
relation_changed
market_lifecycle_changed
projection_resync_required
```

Every ordinary frame includes the projection identity, exact next cursor, catalog/universe revision,
affected market/relation IDs, canonical payload and evidence hashes. `after_cursor=N` means strictly
after N. Duplicate, skipped or backward cursor delivery is forbidden.

The cursor is one process-independent, monotonically increasing global sequence (`global_cursor`),
shared by the current Rust data plane and extended by this Discovery redesign. A `projection_id` is a
label bound to one baseline boundary, not a separate cursor space: `after_cursor=N` remains
interpretable across a `projection_resync_required` or `universe_changed` because the consumer only
ever needs the largest cursor it has already seen. A consumer must never compare cursors across two
projections it has not both received from the same live stream.

Heartbeats do not advance the data cursor, but every heartbeat frame carries `last_cursor` — the
latest published data cursor — so a consumer that is synced to the boundary and observing a quiet
market can distinguish "connected and current" from "connected but stale" without any new event. The
stream also carries an explicit configured `heartbeat_interval` and `reconnect_backoff`, so
fail-closed does not become an unmonitored silent stall.

When replay cannot cover the requested boundary, MarketCow emits a typed resync frame and closes with
the documented close code. The consumer then requests a newly built or currently fresh full-sync.

## 6. Internal projection and persistence

The serving path reads the authoritative in-memory projection and bounded replay journal. HTTP
workers must not synchronously rebuild or scan the full disk store. Projection updates use copy-on-
write or an equivalent atomic publication mechanism so readers see either boundary N or N+1, never
a partial mixture.

Persistence runs asynchronously and records:

- raw/canonical source evidence;
- projection checkpoints;
- cursor journal and gap ledger;
- immutable full-sync evidence for audit;
- materializer lag and failure diagnostics.

Backpressure in persistence cannot stop WebSocket ingestion indefinitely. If required durable
evidence cannot remain within explicit bounded queues, MarketCow marks the affected state unready
and fails closed instead of continuing with misleading freshness.

## 7. Tradude selection handoff

Tradude publishes an immutable, atomically replaced local selection artifact using the existing
`tradude.prediction_market.scope_selection.v2` document. MarketCow validates but does not rerank it.
The artifact binds to:

```text
schema                          (fixed: tradude.prediction_market.scope_selection.v2)
market_ids                      (ordered, 1–100 explicit unique IDs)
discovery_snapshot_id           (the Discovery projection/boundary identity)
catalog_revision
relations[]                     (relation_id, member_market_ids, complete,
                                 actual_member_count, expected_member_count)
selection_evidence_sha256       (content-addressed over the selection body)
replaces_scope_id               (the content-addressed scope_id it supersedes)
```

MarketCow rejects a selection when its discovery projection/snapshot is unknown, catalog identity
differs, market identities are absent or not 1–100 unique, relation closure is broken
(`polymarket_scope_relation_incomplete`), the schema is unrecognized, or its age violates an explicit
activation policy. Those policy values must be configured; no business default is inferred. Rejections
use stable reason codes such as `polymarket_scope_selection_invalid`, never a generic failure label.

The watcher may continue using a local file inbox. Because the artifact is immutable and atomically
replaced (temp-file + `fsync` + `os.replace`), the watcher orders by `selection_evidence_sha256` and
`replaces_scope_id` — never by file mtime — so a rename racing an inotify read cannot reorder
generations. Tradude does not need the internal admin API and must not call port `8796`. MarketCow's
own activator may use internal services behind the supervisor.

## 8. Hot Scope activation

Membership changes are activated without restarting MarketCow or Tradude:

```text
selection generation N+1 received
        -> validate identities and relation closure
        -> keep generation N active
        -> subscribe/warm new and retained markets
        -> fetch and verify complete detailed books
        -> build atomic full-sync for N+1
        -> swap active generation at cursor boundary
        -> emit universe_changed to existing consumers
        -> retain N for bounded rollback/audit
```

The active generation is never edited in place. If warmup fails, generation N remains active and the
candidate is rejected with stable reason codes. There must be no interval in which an unready N+1 is
published as ready.

`universe_changed` (the existing Rust `UniverseChanged` frame) includes the stable logical
`universe_id`, old/new `projection_generation`, added/removed market IDs, `switch_boundary_cursor`,
and `full_sync_required`. Tradude obtains the new full-sync through `8790` and resumes after its
boundary on the global `global_cursor`. Because the cursor is a single global sequence, a consumer
does not need to re-derive a cursor space across a generation switch.

Normal selection membership changes must preserve the logical universe identity so a consumer can
hot-rebind. `full_sync_required=true` on `universe_changed` is a protocol requirement, not an
optimization: even when the N+1 detailed books were fully warmed before activation, Tradude still
issues a fresh full-sync to obtain a deterministic boundary rather than inferring one from deltas.
The logical `universe_id` staying put is what lets a consumer hot-rebind its in-memory identity
without re-establishing a session; it does not waive the full-sync handoff. Catalog revision changes
require a new explicit session epoch. MarketCow must publish a typed epoch-change control and a
complete replacement full-sync; it cannot silently reuse the old identity.

## 9. Observability

The unified status surface must separate these dimensions:

```text
source_connection_state
bootstrap_state
projection_ready
projection_id
boundary_cursor
latest_source_received_at
oldest_ready_market_received_at
projection_age_ms
ready / fail-closed market counts
unresolved_gap_count
replay_oldest_cursor
replay_latest_cursor
persistence_lag_events
last_materialization_error
active_scope_generation
scope_activation_state and duration
```

Stable reason codes replace generic “retrying” or “failed” labels. Dashboard poll frequency does not
define market-data freshness; status timestamps and cursor lag do.

## 10. Failure behavior

| Failure | Required behavior |
|---|---|
| No fresh startup baseline | full-sync 503; status `building`; no stale fallback |
| REST bootstrap partial failure | affected market or whole projection fail-closed according to explicit completeness contract |
| WebSocket gap during bootstrap | discard unprovable candidate boundary and retry |
| Cursor expired | typed resync; consumer fetches fresh full-sync |
| Catalog/relation revision changes | publish new projection identity; never mix revisions |
| Persistence blocked | expose lag; bound memory; fail closed when evidence guarantee is lost |
| Candidate Scope warmup fails | keep previous generation active |
| Unified gateway loses internal data plane | 503/1013 through `8790`; no consumer fallback to internal ports |
| Consumer receives 1013 / resync close | typed resync; consumer backoff and reconnect rhythm is configured (`reconnect_backoff`), never left to an unbounded hot loop |

## 11. Verification and acceptance

### 11.1 Contract tests

- fresh startup refuses persisted stale state;
- subscription-before-bootstrap buffering loses no event;
- REST/WebSocket race reconciliation is deterministic;
- full-sync pages share one immutable boundary;
- ready status becomes false when age/gap invariants fail;
- stream replay covers every cursor exactly once;
- expired replay produces typed resync;
- catalog/relation changes cannot cross projection boundaries;
- selection validation binds exact Discovery evidence;
- candidate failure leaves the active generation unchanged;
- successful activation emits `universe_changed` and serves matching full-sync;
- public consumer documentation and tests reference only `8790`.

### 11.2 End-to-end local acceptance

1. Start MarketCow without a usable checkpoint and observe `building`, never stale-ready.
2. Obtain a fresh 1000-market Discovery full-sync and record its boundary.
3. Apply live changes and verify a Tradude consumer reaches the current cursor without full reloads.
4. Change the Tradude selection while Shadow remains connected.
5. Verify MarketCow warms N+1 while N stays ready.
6. Verify atomic activation, one `universe_changed`, and a matching N+1 full-sync through `8790`.
7. Verify Tradude evaluates newly added markets without either service restarting.
8. Inject disconnect, cursor expiry, persistence blockage and catalog change independently and verify
   the documented fail-closed behavior.
9. Verify `MARKETCOW_REAL_ORDER_SUBMISSION_ENABLED=false` and that no order-capable endpoint or call
   is introduced.

All freshness, time, retry, queue and capacity thresholds used by acceptance must be explicit test or
production configuration. Missing values fail startup.

## 11.3 Fail-closed recovery contract

The redesign must stay honest without becoming frequently unavailable. Fail-closed is a correctness
choice, not an availability strategy, so its recovery path is a first-class contract rather than an
afterthought:

- Every fail-closed / unready / resync state must carry a stable `reason_code` and be surfaced in
  status. "Opening a SQLite database" is never a ready signal, but neither is "fail closed" an
  acceptable steady state — each state names why it failed and what restores it.
- Consumer reconnect and resync follow configured `reconnect_backoff` and `heartbeat_interval`; a
  consumer that is synced and observing a quiet market must be able to distinguish "current" from
  "stalled" (via heartbeat `last_cursor`), and a consumer that receives a resync close must not
  hot-loop.
- A fail-closed market is isolated per market (`book_status`, `missing_fields`, reason codes) and
  must not cascade into a whole-projection unready unless the invariant that failed is genuinely
  global (ordering loss, cross-revision boundary, coherent baseline loss).
- Recovery latencies are budgeted and observable: `projection_age_ms`, `replay_oldest_cursor` /
  `replay_latest_cursor`, and `persistence_lag_events` let an operator see whether a fail-closed state
  is actively recovering or wedged. A wedged fail-closed state (one that cannot name a restoring
  condition) is itself a defect and must be diagnosable as such.

Fail-closed is preferred to silent staleness, but a projection that fail-closes on every refresh
because of a single irreconcilable market is not "honest" — it is unproductive. The completeness
contract (Section 10) therefore decides per-market isolation vs whole-projection failure explicitly,
so one bad market does not take down all 1000.

## 12. Delivery sequence

1. Freeze the full-sync, stream, status and selection-evidence schemas under an immutable
   `schema_version` (not the ambiguous label "vNext"), and bind every route to it.
2. Implement the in-memory fresh bootstrap and buffered handoff behind tests.
3. Move Discovery serving authority away from stale disk materialization.
4. Implement bounded replay, resync and truthful readiness.
5. Update the selection watcher and generation activation evidence validation.
6. Coordinate the breaking Tradude client migration.
7. Run local fault-injection and long-duration observe-only acceptance.
8. Remove the obsolete private Discovery v2 serving path after all local call sites migrate — the
   `snapshot` / `events` / `metadata` / `relations/{id}` routes in the Section 5.1 mapping table,
   and the discovery-state fields carried through `/v1/health`, are deleted (not left as a
   compatibility branch) once the new `full-sync` / `stream` / `status` contract is the only consumer
   surface.

No phase may claim success by increasing stale-age limits, loading an old snapshot as current, hiding
cursor lag, or weakening source identity checks.
