# Frozen migration contracts

The Phase 0 comparator covers the existing Python routes in `src/marketcow/api.py` and
`src/marketcow/polymarket_live_read_api.py`, the MCP envelope in
`src/marketcow/mcp_server.py`, the realtime fixture
`tests/fixtures/polymarket-live-provider-neutral-v2.json`, and the schemas documented in
`docs/market-data-v1.md`, `docs/realtime-market-data-v1.md` and
`docs/public-read-api.md`.

For the Polymarket sample the frozen machine errors are: validation, authentication,
authorization, idempotency conflict, upstream unavailable/invalid, freshness exhausted,
cursor gap, persistence unavailable, derived index degraded and internal invariant. The
shape is `{"detail":{"code":string,"message":string,"retryable":bool,"request_id":string}}`.

Compatibility compares status, JSON semantic value, error code, scope identity, cursor,
generation, applied flag, fail-closed reason, canonical payload hash, book checksum, tick
version, unresolved gaps, instrument revision and health. Volatile request/time fields are
validated by type and ordering rather than byte equality.

Repository boundaries are `InstrumentRepository`, `JobRepository`, `AuditRepository`,
`ArtifactManifestRepository`, `FundamentalRepository`, `MarketBarRepository`,
`QuoteRepository` and `MigrationCheckpointRepository`. Realtime apply/read code imports none
of them; therefore SQLite, PostgreSQL and ClickHouse cannot enter its hot path.

