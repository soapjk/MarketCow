# Local Prometheus integration

MarketCow exposes aggregate request metrics at `/metrics`. The checked-in
`prometheus.yml` scrapes production every five seconds, giving the Grafana API
dashboard a normal 5–15 second visibility target without writing individual
requests to PostgreSQL or ClickHouse.

Example local invocation:

```bash
prometheus \
  --config.file=ops/prometheus/prometheus.yml \
  --storage.tsdb.path=/Volumes/T9/monitoring-services/prometheus-data \
  --storage.tsdb.retention.time=15d \
  --web.listen-address=127.0.0.1:9090
```

The data directory is local operational state and must not be committed. Grafana
provisioning registers `http://127.0.0.1:9090` as the `marketcow-prometheus`
datasource and loads `MarketCow API Observability`.

## Cardinality contract

- `method` is one of the finite HTTP method allow-list or `OTHER`.
- `route` is the FastAPI route template, never the raw URL. The process accepts at
  most 256 route labels and maps later values to `overflow`.
- `status_family` is one of `1xx` through `5xx`.
- User IDs, symbols, query strings, request bodies, and arbitrary URL segments are
  never metric labels.

Prometheus metrics are operational estimates, not an exact billing ledger. Process
restart resets the in-process counters; Prometheus retains already scraped samples.
