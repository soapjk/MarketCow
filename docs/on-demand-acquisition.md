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

The first valid lease starts the complete eligible pool. MarketCow remains
unready until every subscribed token has crossed the new resume boundary with
a verified full-book snapshot and no pending recovery. Consumers must then get
a new full-sync and observe ready; an old cursor cannot resume across a pause.
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

The consumer sequence is: read hot-scope status, read lease status, acquire,
poll lease status until `source_ready=true`, fetch a fresh full-sync and ready,
renew before TTL, then release on orderly shutdown. A crash needs no cleanup;
TTL performs it. Hot scope changes still use the existing prepare/activate
protocol and require the consumer to bind the resulting scope/revision.

## Validation boundary

Local tests cover lease capacity/idempotency/expiry, caller isolation, private
socket framing, authenticated HTTP routes, intentional pause without a new gap,
and old-replay rejection. Deployment validation must separately measure true
zero-consumer network traffic, 250/1000 cold-start readiness, expiry, rapid
release/reacquire, disconnect recovery while leased, and memory/socket bounds.

This mode does not claim that startup always takes tens of seconds. Readiness
time includes upstream connection and subscription, one authoritative full book
for every token, any targeted recovery, and the subsequent full-sync/ready
handshake. It must be measured separately for the active 250 and Discovery 1000
pools under the deployed network conditions.
