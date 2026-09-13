# On-demand market acquisition

MarketCow can keep its read/control process resident while opening upstream
Polymarket sockets only for an active consumer. This is an operator mode, not a
change to market-scope semantics.

## Runtime profile

Enable all three collector arguments together:

```text
--acquisition-lease-required
--acquisition-lease-capacity 8
--acquisition-lease-max-seconds 120
```

This mode requires WebSocket input and the existing private scope-control
socket. Leases are process-local and intentionally disappear on restart. The
durable scope, catalog facts, event log and cursors retain their existing
ownership; a dead consumer cannot make acquisition restart by itself.

With no lease, the public process remains up but its full-sync/stream read is
unavailable, the replay cache is fenced, periodic confirmation membership is
empty, and all upstream acquisition shards are retired. An intentional stop
does not emit a recovery gap. Any real gap already present remains a real gap.

The first valid lease starts the complete eligible pool. `source_ready` remains
false until at least one subscribed market publishes an authoritative
post-resume full-book snapshot. This proves that the new connection generation
is producing data; it deliberately does not require every market to be healthy.
Consumers must then get a new full-sync and observe ready; an old cursor cannot
resume across a pause. Full-sync excludes each market's pre-resume book until
that market independently crosses the resume boundary.
The last release or expiry immediately makes the read surface unavailable and
starts bounded unsubscribe reconciliation. A new acquire received while that
unsubscribe is still completing waits for ownership to reach zero before
opening replacement shards.

## Private Rust commands

All commands retain the existing canonical JSON, same-user Unix socket,
expected scope/revision CAS and byte/deadline limits.

- `acquisition_lease_status`
- `acquire_acquisition_lease`: `lease_id`, `ttl_seconds`, and the sorted exact
  `market_ids` returned as `eligible_market_ids` by status.
- `renew_acquisition_lease`: existing `lease_id` and `ttl_seconds`.
- `release_acquisition_lease`: existing `lease_id`.

Acquire is idempotent only for the same lease ID and identical market set.
Renewing an expired/missing lease fails. Capacity and TTL are explicit. Status
reports active lease count, remaining milliseconds, eligibility and
`source_ready`; it never reports acquisition as ready merely because a lease
exists.

## Authenticated control routes

The existing loopback management service exposes the commands under the
dedicated `acquisition.lease` caller scope:

```text
GET  /v1/prediction-markets/polymarket/acquisition-leases/status?pool=live
POST /v1/prediction-markets/polymarket/acquisition-leases/acquire
POST /v1/prediction-markets/polymarket/acquisition-leases/renew
POST /v1/prediction-markets/polymarket/acquisition-leases/release
```

POST bodies use `marketcow.acquisition-lease.v1`, bind `pool`,
`expected_scope_id`, `expected_revision`, and a caller-local lease ID. Acquire
also includes the exact sorted eligible market list. The outer control hashes
the authenticated caller into the internal lease identity so one caller cannot
release another caller's lease through these routes.

The deployed U1 listener is `127.0.0.1:18898` and is intentionally not exposed
on the LAN. A remote consumer reaches it through an authenticated SSH local
forward and sends `Authorization: Bearer <caller secret>`. The server stores
only the bearer digest in the active release's `callers.json`; the existing
Tradude secret remains a mode-0600 operator file under `linux/phase1/` and must
be transferred into a consumer-owned mode-0600 file without logging its value.
The public data endpoints remain `192.168.124.3:8793` (Live) and `:8795`
(Discovery); the control plane is not a market-data proxy.

Read the current CAS values from:

```text
GET /v1/prediction-markets/polymarket/hot-scopes/status?pool=live
GET /v1/prediction-markets/polymarket/hot-scopes/status?pool=discovery
```

For Live, use `actual.scope_id` and `actual.revision`. For Discovery, use
`actual.projection_id` as `expected_scope_id` and use `actual.revision`.
Never reuse either value across a hot-scope change or service restart.

Acquire body (all fields required, no extras):

```json
{"schema_version":"marketcow.acquisition-lease.v1","pool":"live|discovery","expected_scope_id":"<current identity>","expected_revision":1,"lease_id":"<caller-local id>","ttl_seconds":120,"market_ids":["<exact sorted eligible ids>"]}
```

Renew removes `market_ids`; release removes both `market_ids` and
`ttl_seconds`. `lease_id` is 1--96 ASCII alphanumeric/`-_.:` characters at the
HTTP boundary. Acquire requires the exact sorted `eligible_market_ids` returned
by lease status; it is not a caller-selected subset.

Successful status/acquire/renew/release responses use
`marketcow.acquisition-lease-status.v1` and contain `required`,
`acquisition_enabled`, `active_leases`, `maximum_leases`,
`maximum_ttl_seconds`, `eligible_market_ids`, `leases`, and `source_ready`.
Each lease entry contains the caller-namespaced `lease_id`, `market_count`, and
`remaining_milliseconds`. HTTP operation failures use
`marketcow.generation-operation-error.v1` with `code`, `retryable`, and
`reconcile_required`; a lost/503 mutation response must be reconciled with
status, not blindly replayed.

The consumer sequence is: read hot-scope status, read lease status, acquire,
poll lease status until `source_ready=true`, fetch a fresh full-sync and ready,
renew before TTL, then release on orderly shutdown. A crash needs no cleanup;
TTL performs it. Hot scope changes still use the existing prepare/activate
protocol and require the consumer to bind the resulting scope/revision.

`source_ready` is connection-generation readiness, not an all-markets-health
gate. It becomes true after the first subscribed market publishes an
authoritative post-resume book. Full-sync omits every pre-resume book until that
market independently crosses the resume boundary; such markets remain locally
unavailable without blocking healthy markets or manufacturing a global gap.

## Validation boundary

Local tests cover lease capacity/idempotency/expiry, caller isolation, private
socket framing, authenticated HTTP routes, intentional pause without a new gap,
and old-replay rejection. Deployment validation must separately measure true
zero-consumer network traffic, 250/1000 cold-start readiness, expiry, rapid
release/reacquire, disconnect recovery while leased, and memory/socket bounds.

This mode does not claim that startup always takes tens of seconds. Connection
generation readiness includes upstream connection/subscription and the first
authoritative post-resume market book. End-to-end consumer readiness additionally
includes fresh full-sync plus its ready/delta handshake; individual markets that
have not refreshed remain locally unavailable. These times must be measured
separately for the active 250 and Discovery 1000 pools under deployed network
conditions.

The independent BTC research package is outside this lease lifecycle. Its
`btc_research_stream` opens its own explicitly bounded official WebSocket and
does not use the shared Live/Discovery pools. Its bounded 18898 rule/evidence
reads remain available while acquisition is paused because the resident control
service and catalog/evidence routes do not depend on a Live/Discovery lease.
