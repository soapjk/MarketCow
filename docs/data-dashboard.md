# MarketCow data dashboard

MarketCow uses the existing local Grafana service for its data inventory and quality
dashboard. Grafana was selected because the machine already runs it and the official
ClickHouse plugin plus built-in PostgreSQL datasource provide provisioned, read-only
SQL dashboards without creating another frontend application.

The dashboard answers four groups of questions:

- Inventory: physical/logical row counts, unique symbols, markets, storage bytes and
  Artifact count.
- Distribution: raw/canonical/latest layers, market, interval, provider/source and
  the largest symbol histories.
- Coverage: first/last bar, span, ingest trend and filterable symbol/interval views.
- Quality: unexpected time gaps, superseded physical versions, raw/canonical storage
  shape, quote freshness and Artifact inventory.

## Local installation

Install the official ClickHouse datasource into the configured Grafana plugin path:

```shell
grafana cli \
  --homepath /opt/homebrew/opt/grafana/share/grafana \
  --pluginsDir /Volumes/T9/monitoring-services/grafana-plugins \
  plugins install grafana-clickhouse-datasource
```

Then provision dedicated read-only database users and the local datasource files:

```shell
uv run python ops/grafana/provision_local.py --env .env.production
```

The script writes secrets only to the local Grafana provisioning root, with mode
`0600`, and points the existing LaunchAgent at that root. Restart only the local
Grafana LaunchAgent after provisioning. The dashboard is available under the
`MarketCow` folder at `http://127.0.0.1:3001`.

## Continuity interpretation

The initial gap check is intentionally conservative and operates on canonical FINAL
bars. Intraday gaps are flagged above 1.5 expected intervals; daily gaps are flagged
above four calendar days so normal weekends are not false positives. This is not an
exchange-session completeness proof: holidays, lunch breaks, suspensions and symbols
with nonstandard sessions require a trading-calendar-aware completeness model in a
future iteration.

All queries are read-only. The dashboard never calls providers, refreshes quotes or
changes MarketCow data.

The dashboard is one part of the broader visualization and administration design.
The accepted boundary between Grafana, native administration pages, aggregate
metrics, and sub-second streaming panels is documented in
[`visualization/architecture-baseline.md`](visualization/architecture-baseline.md).

## Administration frontend embedding

The administration frontend reads its dashboard list from
`GET /v1/admin/dashboards`. MarketCow's inventory dashboard is registered by
default. Additional local dashboards are configured with
`MARKETCOW_DASHBOARD_REGISTRY_JSON`; entries contain a stable key, project, display
name, Grafana dashboard UID and slug, and optional panel, theme, variables, and sort
order. The server validates identifiers and returns only a Grafana-relative path.

The local provisioning command enables iframe rendering and keeps the Grafana login
cookie at `SameSite=Lax`. Grafana remains independently authenticated. The frontend
uses `VITE_GRAFANA_BASE_URL` (default `http://127.0.0.1:3001`) and offers a direct
Grafana link if the iframe does not load.

Example additional dashboard:

```dotenv
MARKETCOW_DASHBOARD_REGISTRY_JSON=[{"key":"api-traffic","project":"API Service","name":"API access","dashboard_uid":"api-traffic","slug":"api-traffic","sort_order":20}]
```
