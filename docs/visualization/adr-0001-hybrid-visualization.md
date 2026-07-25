# ADR 0001: Use Grafana and native realtime panels together

Date: 2026-07-25
Status: accepted

## Context

MarketCow already provisions a useful Grafana inventory dashboard against
PostgreSQL and ClickHouse. The same machine uses one Grafana instance for multiple
local services. SQL polling is appropriate for inventory and history but is a poor
transport for sub-second request animation. MarketCow also needs operational
commands and domain workflows that do not belong inside Grafana.

## Decision

- Build a React + TypeScript administration frontend.
- Keep Grafana for historical, quality, inventory, and aggregate observability.
- Register Grafana dashboards centrally and embed or link them from the frontend.
- Use Apache ECharts for native, sub-second panels.
- Deliver live summaries through a bounded, versioned WebSocket/SSE channel.
- Expose aggregate request metrics for Prometheus-compatible collection.
- Keep all state-changing operations behind authenticated FastAPI endpoints.

## Why this option

- It preserves the existing dashboard investment.
- It avoids writing each request to SQL merely for visualization.
- It gives administration workflows normal application controls and feedback.
- ECharts supports incremental/dynamic data and future market-specific charts.
- The data paths can fail independently without taking down the core API.

## Rejected alternatives

### Use only Grafana with SQL polling

Rejected because sub-second refresh causes avoidable database/query load and still
does not provide a reliable per-event stream.

### Replace Grafana with a new dashboard platform

Rejected because it duplicates working inventory dashboards and does not remove
the need for instrumentation, authorization, and realtime transport.

### Use only Grafana Live

Not selected as the sole UI because management forms and domain workflows remain
awkward. Grafana Live or a streaming datasource may still be evaluated as an
optional transport for Grafana-native panels.

### Build a general dashboard designer

Rejected as out of scope. The first release uses a fixed dashboard registry and
purpose-built pages.

## Consequences

- The frontend has two visual rendering contexts: Grafana iframe/link and native
  ECharts panels.
- Authentication and theme behavior must be tested across the iframe boundary.
- Realtime events require strict bounding, redaction, and backpressure.
- Prometheus-compatible metrics add a runtime integration but remove SQL from the
  request monitoring hot path.
- The repository gains a frontend toolchain that needs its own tests and updates.
