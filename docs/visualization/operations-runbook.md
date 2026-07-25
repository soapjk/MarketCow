# Visualization and administration operations runbook

## Build and verify

```bash
./scripts/verify_visualization.sh
```

The command runs the complete Python test suite, Ruff, frontend type checking,
component tests, production build, ESLint, production dependency audit, and Git
whitespace validation.

## Local development

```bash
uv run marketcow --profile development start --host 127.0.0.1 --port 8792
cd web
npm run dev
```

Open `http://127.0.0.1:4173/admin/#/overview`. Vite proxies `/v1` to port 8792.

## Production-local build

```bash
cd web
npm ci
npm run build
cd ..
uv run marketcow --profile production start --host 127.0.0.1 --port 8790
```

When `web/dist/index.html` exists, FastAPI serves the built application at
`http://127.0.0.1:8790/admin/#/overview`. The API remains loopback-only unless the
operator uses the existing explicit non-loopback override.

## Staged enablement and rollback

All flags default to `true`; disable them independently in the local profile:

```dotenv
MARKETCOW_ADMIN_FRONTEND_ENABLED=true
MARKETCOW_ADMIN_GRAFANA_ENABLED=false
MARKETCOW_ADMIN_COMMANDS_ENABLED=false
MARKETCOW_ADMIN_LIVE_ENABLED=false
```

Recommended rollout:

1. enable the frontend while Grafana, commands, and live are disabled;
2. enable Grafana and validate iframe authentication;
3. enable commands for an Operator token and verify audit persistence;
4. enable live streaming and observe memory/connection metrics.

Setting commands to false makes all `/v1/admin/*` mutations return
`admin_commands_disabled`. Disabling live returns `admin_live_disabled`; disabling
Grafana returns an empty registry; disabling the frontend stops mounting `web/dist`.
None of these flags disables the core market-data API.

Rollback requires only changing the affected flag and restarting the local
MarketCow process. No database rollback is required. The append-only audit migration
may remain in place.

## Failure isolation

| Failure | Expected behavior | Operator action |
| --- | --- | --- |
| Grafana unavailable | frame shows loading/fallback; native pages continue | open Grafana directly and inspect port 3001 |
| Prometheus unavailable | historical request panels show no data; live page continues | inspect port 9090 and scrape target |
| SSE disconnected | page changes to retrying and resumes from sequence | inspect `/v1/admin/events`, connection count |
| SSE replay gap | client receives explicit `stream.gap` | use Grafana for historical interval |
| PostgreSQL unavailable | readiness and control pages degrade; core startup policy applies | run `marketcow doctor` |
| ClickHouse unavailable | coverage and inventory fail independently | inspect readiness and spool |
| Frontend build absent | `/admin` is not mounted; JSON API remains available | run `npm ci && npm run build` |

## Performance and resource baselines

- Prometheus scrape interval: 5 seconds.
- Grafana API dashboard refresh: 5 seconds.
- SSE visibility objective: LAN P95 under 1 second.
- SSE replay: configured runtime replay capacity, capped at 100,000.
- Per-client queue: configured runtime queue capacity, capped at 10,000.
- Default maximum SSE clients: 100.
- Browser event buffer: 2,000 records.
- Browser batch interval: 250 ms.
- Browser visible window: 60 seconds; error list: 20 records.
- Metrics route labels: at most 256 before overflow.
- Authentication sessions: at most 128, default lifetime eight hours.

Automated performance tests process 50,000 metric observations and 10,000 event
publishes within a conservative five-second CI ceiling, and verify bounded output
and replay memory. The frontend test feeds 20,000 events and proves retention remains
at 2,000.

## Backup and recovery

Dashboard JSON, Prometheus configuration, source code, and documentation are in the
repository. Grafana provisioning can be regenerated with
`ops/grafana/provision_local.py`. PostgreSQL backup includes
`admin_audit_event`; realtime replay, sessions, and in-process metrics are
intentionally ephemeral and reset after restart.
