# Polymarket Discovery v3 consumer contract

Consumers use only the unified loopback API on port `8790`:

```text
GET /v1/prediction-markets/polymarket/live/discovery/full-sync
GET /v1/prediction-markets/polymarket/live/discovery/status
WS  /v1/prediction-markets/polymarket/live/discovery/stream?after_cursor=...&projection_id=...
```

Ports `8795` and `8796` are internal collector and Rust data-plane ports. They
are not consumer interfaces.

The long Gamma directory traversal is an explicit preparation operation:
`scripts/prepare_polymarket_discovery.py`. The resident collector never runs
that operation during startup. It loads only a previously published,
checksum-bound catalog/index/realtime-universe boundary and exits immediately
with an actionable error when the boundary is absent or invalid. Discovery is
an isolated module failure: the supervisor still starts the Rust data plane and
unified API, and the Discovery status endpoint reports the fail-closed state.

The full-sync schema is `marketcow.polymarket.discovery.v3`. It is one atomic,
non-paginated response. `ready=false`, a non-null `fail_closed_reason`, or a
positive `unresolved_gap_count` must stop consumer publication. The response
is rejected with HTTP 413 when it exceeds the configured byte limit.

Each market has an optional `settlement` fact with exactly these fields:

```text
resolution_source
rules_revision
redeemable
redeemable_at_ns
observed_at
evidence_sha256
```

MarketCow publishes this object only when all required source facts are
present and `redeemable == (redeemable_at_ns is not null)`. Otherwise it is
null; consumers must fail closed for settlement-dependent capital release and
must not infer a value.

The stream schema is `marketcow.polymarket.discovery-events.v3`. Every delta
item has one shape, `{type,payload}`. The discriminated payload types are:

- `market_update`: the latest complete Market object;
- `relation_update`: the latest complete Relation object;
- `universe_changed`: `{universe_revision}`.

Ordinary frames stay at the current cursor when idle or advance exactly once.
A projection change, gap, or expired boundary sets `resync_required`; consumers
discard the local projection and fetch a new full-sync. `universe_changed` is
never applied as an ordinary delta.
