# MarketCow administration frontend

## Development

```bash
npm install
npm run dev
```

The Vite server listens on `127.0.0.1:4173` and proxies `/v1` to the development
MarketCow API at `127.0.0.1:8792`. Set `VITE_API_BASE_URL` only when the API is
served from another origin.

## Verification

```bash
npm run typecheck
npm test
npm run build
npm run lint
npm audit --omit=dev
```

Hash routes are intentional: a static local deployment can reload any page without
requiring a server-side history fallback. The application uses TanStack Query for
request caching and provides a bounded-retry SSE client foundation. The realtime
protocol itself is implemented by MCHR-49.
