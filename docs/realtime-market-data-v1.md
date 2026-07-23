# MarketCow realtime market data v1

Status: Phase 2 local candidate. WebSocket URL:
`ws://127.0.0.1:8790/v1/market-data/stream`.

## Lifecycle

The first client frame is normally a `subscribe`:

```json
{
  "type": "subscribe",
  "request_id": "sub-1",
  "instruments": ["AAPL.XNAS"],
  "data_types": ["quote", "trade", "bar", "order_book"],
  "bar_types": ["1-MINUTE"],
  "book_depth": 1
}
```

MarketCow validates every instrument against Instrument Master and requires an explicit
`provider:longport` mapping. Success returns `subscription_ack`; invalid input,
unavailable credentials, permission failures and unsupported instruments return the
machine-readable `stream_error` frame. `unsubscribe` removes only the named subscriptions.
At the provider boundary, quote and order-book filters share one LongPort Depth capability;
trade and bar filters share one LongPort Trade capability. Refcounts and rollback are
computed from these normalized capabilities, so partial unsubscribe and disconnect cannot
cancel a capability still required by another filter.

The server sends `stream_heartbeat` frames after 15 seconds without another frame by
default. Heartbeats report `last_sequence` but do not consume a market-event sequence.
The interval, per-client queue capacity and replay capacity are explicit settings:
`MARKETCOW_REALTIME_HEARTBEAT_SECONDS`, `MARKETCOW_REALTIME_QUEUE_CAPACITY`, and
`MARKETCOW_REALTIME_REPLAY_CAPACITY`.

## Sequence, reconnect and gaps

Market events use one monotonically increasing sequence within the process-unique
`stream_id`. Every subscribed connection receives every committed sequence: a matching
event is delivered in full; a non-matching event is represented by a machine-validated
`sequence_watermark` with reason `filtered`. A client must verify consecutive event or
watermark sequences, so normal filtering is distinguishable from transport loss.
The subscribe ack's `sequence` is the delivery baseline: the next event/watermark is
`sequence + 1`. For resume it equals the requested `resume_after`; replay is delivered
after the ack through `resume.replayed_through`.

On reconnect the client supplies both the prior `resume_stream_id` and `resume_after`.
The server replays one event or watermark for every retained sequence before new events.
It returns `replay_too_large` before ack if that replay cannot fit the bounded client queue,
and `gap_unrecoverable` if the stream ID changed, the
requested sequence is ahead, or the bounded replay buffer no longer contains the gap.
The client must then discard incremental state, fetch fresh instrument/book state where
applicable, and resubscribe without resume.

Per-client delivery queues are bounded. A full queue marks the connection
`slow_consumer`; the WebSocket closes with code 1013 and that reason. MarketCow never drops
an event from an apparently healthy connection and silently advances its sequence.

## LongPort mapping and state

LongPort `Depth` supplies v1 Quote bid/ask fields and L1 MBP order-book snapshots;
`Trade` supplies Trade events. Every snapshot's `baseline_sequence` equals its envelope
sequence. LongPort does not expose an MBO delta stream in this MVP, so MarketCow publishes
fresh L1 snapshots rather than manufacturing deltas. A gap invalidates the prior baseline.
When the
provider does not supply a trade ID, MarketCow deterministically hashes provider symbol,
event timestamp, price, volume, direction and position within the pushed batch. Unknown
direction maps to `NO_AGGRESSOR`.

Provider connection/subscription changes publish `stream_status`. Provider failures are
reported as `provider_unavailable` control errors; live data is never relabeled as another
source. LongPort Depth has no upstream event timestamp, so Quote uses MarketCow observation
time, sets `payload.ts_event_source=marketcow_observation`, and is marked degraded rather
than pretending that the timestamp was provider-assigned. Quote/trade financial values
remain decimal strings.

## One-minute bar rules

- Bars are trade-based, never quote-based.
- UTC windows are half-open `[minute, minute + 1 minute)`.
- `window_start` identifies the bucket; event `ts_event` and bar `window_end` are the
  exclusive window end. MarketCow publish time becomes Nautilus `ts_init`.
- Only non-empty periods produce bars; no synthetic zero-volume bars are created.
- Open/close follow provider event order; high/low are extrema; volume is summed.
- Bars are keyed by instrument and source, so one bar never mixes providers.
- Session is also part of the open-bucket key; regular, pre-market, post-market and
  overnight trades cannot be mixed into one bucket.
- The LongPort SDK's regular, pre-market and post-market trade pushes are included.
  Overnight is included only when `MARKETCOW_LONGPORT_ENABLE_OVERNIGHT=true`.
- A trade for an already closed minute is not used to revise the realtime bar. It is
  dropped with a degraded `stream_status` reason `late_trade_dropped`.
- Every open bucket owns a window-end timer. A bar is persisted and published at the end
  even when no later trade arrives; cross-minute trades and shutdown cancel the old timer.
- Completed bars are written to authoritative raw ClickHouse/WAL storage as `1m/raw`
  before publication. Canonical selection remains the existing asynchronous canonical
  pipeline; adjusted realtime bars are not synthesized.
- A persistence failure suppresses bar publication and emits degraded status
  `bar_persist_failed`.
- Provider switching is not performed in the Phase 2 MVP: LongPort is the only realtime
  source. A future provider switch must close the old source bucket rather than mix it.

On graceful shutdown MarketCow flushes open non-empty bars, closes LongPort, marks clients
with `server_shutdown`, and then closes the application service resources.

## Machine contracts

Schemas are served at `/v1/schemas/{name}`. Realtime control names are `subscribe`,
`unsubscribe`, `subscription_ack`, `stream_heartbeat`, `stream_error`, and
`sequence_watermark`. Market events
use the discriminated `event` schema and its quote, trade, bar and stream-status payloads.
