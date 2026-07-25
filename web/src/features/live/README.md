# Realtime visualization components

The live page is loaded as a separate JavaScript chunk so the ECharts runtime does
not increase the initial administration shell bundle.

Data handling is deliberately bounded:

- SSE events are resumed from the most recently observed sequence.
- Incoming events are queued and flushed to React every 250 ms rather than causing
  one render per request.
- `RollingEventBuffer` retains at most 2,000 request summaries.
- Charts display a fixed 60-second window.
- ECharts instances and `ResizeObserver` instances are disposed on unmount.
- Error rows retain only the newest 20 errors in the current window.
- Pause closes the SSE connection; clear releases the buffered event references.

`MarketCandle` and `OrderBookSnapshot` define the boundary for future K-line,
volume, and depth components without coupling the request-monitoring page to a
specific market-data provider.

The client-side P50/P95 values are display estimates over each one-second event
bucket. Durable and statistically authoritative latency quantiles remain in the
Prometheus/Grafana path.
