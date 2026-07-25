# MarketCow administration frontend

## Development

```bash
npm install
npm run dev
```

The Vite server listens on `127.0.0.1:4173`; open `/admin/#/overview`. It proxies
`/v1` to the development MarketCow API at `127.0.0.1:8792`. Set
`VITE_API_BASE_URL` only when the API is served from another origin.

## Verification

```bash
npm run typecheck
npm test
npm run build
npm run lint
npm audit
```

Hash routes are intentional: a static local deployment can reload any page without
requiring a server-side history fallback. The application uses TanStack Query for
request caching and provides a bounded-retry SSE client foundation. The realtime
protocol itself is implemented by MCHR-49.

## Pages

- **Overview** refreshes the versioned control-plane summary every ten seconds.
- **Dashboards** renders server-registered, read-only Grafana frames.
- **Data** searches instruments and queries raw/canonical ClickHouse coverage.
- **Operations** manages durable history jobs and displays Provider health.
- **Live** consumes the bounded SSE stream and renders ECharts panels.
- **Settings** displays sanitized append-only administration audit events.

State-changing task actions show a confirmation, remain disabled while pending, and
surface the server result. Authorization is enforced by the API; page visibility is
not treated as a security boundary.
