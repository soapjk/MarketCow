# Visualization module risk register

| Risk | Impact | Mitigation | Closure evidence |
| --- | --- | --- | --- |
| Grafana iframe login or cookie failure | dashboards appear blank | same-origin reverse proxy; explicit expired-login state; external-open fallback | browser integration test |
| Grafana Viewer can issue broader datasource queries | sensitive read exposure | dedicated DB role, restricted schema/views, query timeout and datasource separation | permission test and provisioning review |
| High-cardinality request labels | memory/storage growth | route templates and finite label allow-lists; prohibit raw URL, symbol, user ID | cardinality test |
| Slow live client blocks request handling | API degradation | bounded per-client queue, sampling, gap event, disconnect policy | slow-consumer load test |
| Realtime chart memory grows forever | browser instability | fixed rolling windows, batching, downsampling and disposal | 30-minute soak test |
| Secrets leak into UI/events | credential exposure | reuse sanitization, payload allow-list, response tests | redaction tests |
| Command double submission | duplicate work or mutation | idempotency keys, conflict responses, disabled pending state | API integration test |
| Frontend outage affects core API | data service outage | serve/deploy independently or isolate static route; fail-open observability | failure injection test |
| Grafana/Prometheus unavailable | partial blank experience | component-level errors and native health fallback | degradation E2E test |
| Event schema changes break clients | live page failure | envelope and payload versions; compatibility fixtures | contract test |
| Existing market stream is reused without authorization | unintended data access | separate admin subscription policy and authentication | authorization test |
| Multiple local dashboards become hard-coded | costly additions | server-owned dashboard registry | registry test |

## Architecture exit criteria

MCHR-44 is complete when:

- every data class has one primary path and a latency objective;
- first-release scope and explicit non-goals are documented;
- frontend, Grafana, control API, metrics, and realtime responsibilities are
  unambiguous;
- the initial event envelope and role matrix are documented;
- material risks have an owner task and objective closure evidence.

