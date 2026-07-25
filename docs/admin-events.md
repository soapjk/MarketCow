# MarketCow administration event stream

`GET /v1/admin/events` provides sanitized, process-local administration summaries
as Server-Sent Events. It is for sub-second visualization, not durable accounting.

## Subscription

```text
GET /v1/admin/events?types=request.summary&after_sequence=42
Accept: text/event-stream
Last-Event-ID: 42
```

The query accepts a finite comma-separated event allow-list. `Last-Event-ID` and
`after_sequence` are combined by taking the greater sequence.

Every data event follows `marketcow.admin-event.v1`:

```json
{
  "schema_version": "marketcow.admin-event.v1",
  "event_id": "4c23...",
  "type": "request.summary",
  "source": "marketcow-api",
  "occurred_at": "2026-07-25T07:15:00Z",
  "sequence": 43,
  "payload": {
    "method": "GET",
    "route": "/v1/quotes/{symbol}",
    "status_family": "2xx",
    "duration_ms": 12.4,
    "exception": ""
  }
}
```

Raw URLs, query strings, bodies, cookies, authorization headers, tokens, and
credentials are not published.

## Delivery behavior

- Replay and each subscriber queue are bounded by the existing realtime capacity
  settings.
- A cursor older than the retained window produces `stream.gap`.
- A slow consumer loses oldest queued events and receives `stream.gap` with a
  dropped count.
- Idle streams receive `stream.heartbeat` with the current sequence watermark.
- Browser clients reconnect with exponential backoff and send the last sequence.
- Restart resets the process-local sequence and replay. Historical monitoring comes
  from Prometheus/Grafana instead.
