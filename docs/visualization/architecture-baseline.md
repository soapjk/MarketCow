# MarketCow visualization and administration baseline

Status: accepted implementation baseline  
Scope: Plane `visulization_module`, MCHR-43 through MCHR-53

## Outcome

MarketCow will use one administration application with two visualization paths:

1. Grafana remains the read-only dashboard engine for inventory, historical
   coverage, data quality, and aggregated service metrics.
2. The administration frontend renders sub-second streams directly with Apache
   ECharts over a versioned WebSocket or SSE contract.

The administration frontend calls FastAPI for control-plane reads and commands.
It never connects directly to PostgreSQL, ClickHouse, or provider APIs.

```text
                                      +----------------------+
PostgreSQL / ClickHouse --------------> Grafana :3001        |
Prometheus / log backend -------------> historical/aggregate |
                                      +----------+-----------+
                                                 |
                                                 | iframe/link
                                                 v
+------------------+  HTTPS/JSON  +---------------+---------------+
| Browser admin UI |<------------>| MarketCow FastAPI control API |
| React + ECharts  |              +---------------+---------------+
+--------+---------+                              |
         ^                                        v
         | WebSocket/SSE                PostgreSQL control state
         +-------------------- bounded realtime event hub
```

## Current-state inventory

### Runtime

- FastAPI serves the MarketCow API.
- PostgreSQL owns transactional, metadata, fundamental, artifact, provider-health,
  and job state.
- ClickHouse owns raw and canonical market bars.
- The local WAL/spool handles bounded ClickHouse replay.
- A single local Grafana instance listens on `127.0.0.1:3001`.

### Existing visualization and observability

- `ops/grafana/dashboards/marketcow-data-inventory.json` is provisioned as a
  read-only inventory and quality dashboard.
- `ops/grafana/provision_local.py` creates a `marketcow_grafana` database role,
  grants PostgreSQL and ClickHouse read access, and writes local provisioning.
- Process-local bounded telemetry already covers storage, canonicalization,
  query, backup, replay, and operator behavior.
- `/v1/health`, `/v1/readiness`, and `/v1/snapshot` expose service state.
- There is no Prometheus-format request metrics endpoint yet.

### Existing control and streaming surfaces

- `/v1/admin/history-jobs` supports creation and listing.
- History jobs support detail, cancellation, and retry of failed items.
- Provider/source health and instrument lookup endpoints already exist.
- `/v1/market-data/stream` supplies versioned market-data WebSocket events with
  replay semantics, but it is not an authenticated administration event stream.
- A minimal server-rendered history jobs page exists; there is no standalone
  administration frontend.

## Information architecture

```text
MarketCow Administration
├── Overview
│   ├── runtime health and freshness
│   ├── recent jobs and provider failures
│   └── live request/connection summary
├── Dashboards
│   ├── MarketCow data inventory and quality
│   └── registered dashboards from other local services
├── Data
│   ├── instruments and coverage
│   ├── canonical/raw inspection
│   └── artifacts
├── Operations
│   ├── history jobs
│   ├── providers
│   └── guarded refresh/retry commands
├── Live
│   ├── request rate and latency
│   ├── active connections
│   └── latest sanitized failures
└── Administration
    ├── dashboard registry
    ├── audit events
    └── access policy
```

## Data paths and latency objectives

| Data class | Authoritative path | Presentation | Target visibility |
| --- | --- | --- | --- |
| Inventory and storage | PostgreSQL/ClickHouse | Grafana SQL | 30–60 seconds |
| Historical coverage and quality | ClickHouse canonical data | Grafana SQL | 30–60 seconds |
| Aggregated request metrics | Prometheus scrape | Grafana | 5–15 seconds |
| Searchable request/error history | log backend or ClickHouse | Grafana/Explore | under 30 seconds |
| Live request/connection samples | bounded event hub | ECharts via WS/SSE | P95 under 1 second on LAN |
| Control-plane state | PostgreSQL through FastAPI | native administration UI | request/response |
| Commands | authenticated FastAPI | native administration UI | request accepted promptly; progress asynchronous |

Raw requests must not be synchronously inserted into a business SQL table merely
to animate a chart. Monitoring is not a billing ledger; any future exact accounting
requirement needs a separate durable event design.

## Page and API responsibility matrix

| Page | Primary existing API | Required addition |
| --- | --- | --- |
| Overview | `/v1/health`, `/v1/readiness`, `/v1/snapshot` | compact admin summary |
| History jobs | `/v1/admin/history-jobs*` | pagination/filter consistency and audit |
| Providers | `/v1/sources/health` | normalized provider capabilities/status |
| Instruments | `/v1/instruments/search`, `/v1/instruments/{id}` | coverage summary |
| Dashboards | Grafana URL | dashboard registry API |
| Live | `/v1/market-data/stream` as reference | authenticated admin event stream |
| Audit | none | append-only audit query API |

## Realtime event contract draft

Every administration event uses this envelope:

```json
{
  "schema_version": "marketcow.admin-event.v1",
  "event_id": "01J...",
  "type": "request.summary",
  "source": "marketcow-api",
  "occurred_at": "2026-07-25T12:00:00.123Z",
  "sequence": 42,
  "payload": {}
}
```

Required protocol behavior:

- subscribe by a finite allow-listed event type;
- send heartbeat and sequence watermarks;
- retain only a bounded replay window;
- signal replay gaps instead of pretending delivery was complete;
- disconnect or sample slow consumers without blocking request handling;
- redact authorization, tokens, DSNs, filesystem paths, and request bodies;
- version the envelope independently from individual payload schemas.

SSE is preferred for server-to-browser, one-way summary streams. WebSocket is used
when subscription changes, replay commands, or existing realtime infrastructure
make bidirectional messages materially simpler.

## Roles and trust boundaries

| Role | Read dashboards | Read live stream | Run safe operations | Change configuration |
| --- | --- | --- | --- | --- |
| Viewer | yes | sanitized only | no | no |
| Operator | yes | yes | create/cancel/retry approved jobs | no |
| Admin | yes | yes | yes | local dashboard/access configuration |

- Grafana uses separate least-privilege read-only credentials.
- Browser clients never receive provider or database secrets.
- FastAPI authorizes every command; hiding a button is not authorization.
- Grafana iframe permissions and FastAPI permissions are separate controls.
- State-changing operations require CSRF protection where cookie auth is used,
  an idempotency or conflict strategy, and an audit record.

## Scope

### First release

- React and TypeScript application shell.
- Registered Grafana dashboard embedding/linking.
- Native history job, provider, instrument, and overview pages.
- Prometheus-style aggregate request metrics.
- Bounded administration event stream and ECharts live panels.
- Local Viewer/Operator/Admin authorization boundary and audit records.
- Automated tests, performance baselines, failure degradation, and operator docs.

### Explicit non-goals

- External deployment or hosted data transfer.
- Multi-tenant billing or exact request accounting.
- Replacing Grafana.
- A general-purpose dashboard builder.
- Persisting every realtime event.
- Editing Grafana dashboards from the MarketCow frontend.
- Automatic external alert delivery in the first release.

## Delivery dependencies

```text
MCHR-44 baseline
├── MCHR-45 frontend shell
├── MCHR-46 Grafana integration
├── MCHR-47 control API
└── MCHR-48 aggregate metrics
    └── MCHR-49 realtime gateway
        └── MCHR-50 ECharts components
            └── MCHR-51 product pages
                ├── MCHR-52 security hardening
                └── MCHR-53 verification and rollout
```

Security is designed in every phase; MCHR-52 is the explicit adversarial review
and closure task, not the first point at which authorization is considered.

