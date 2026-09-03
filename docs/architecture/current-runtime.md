# MarketCow runtime architecture

MarketCow maintains one runtime architecture:

- PostgreSQL stores transactional data, metadata, fundamentals, control-plane state, and Artifact manifests.
- ClickHouse stores raw and canonical market bars and serves all online market-bar reads.
- The authoritative WAL/spool preserves failed ClickHouse writes for bounded replay.
- The canonical scheduler rebuilds canonical bars from acknowledged ClickHouse raw data.

Every `production`, `development`, and `test` profile requires both databases and an explicit allowed storage root. Startup fails before creating connections, directories, files, or threads when configuration is incomplete or escapes its root.

DuckDB, Warehouse adapters, shadow writes, and in-process storage fallback are not part of MarketCow. Historical data copies must be converted outside this repository before they are imported into PostgreSQL and ClickHouse.

## Polymarket final architecture

The production supervisor has one public listener, `127.0.0.1:8790`. The Python
process is the unified gateway for stocks, administration, discovery, MCP, and
all other existing APIs. Polymarket live HTTP and WebSocket requests under
`/v1/prediction-markets/polymarket/live/*` are forwarded without fallback to
the internal Rust data plane.

- Python retains the complete Gamma metadata catalog, but its internal
  discovery collector bootstraps and publishes only a checksum-bound, ranked
  realtime universe (1000 markets by default).
- Tradude alone ranks opportunities and publishes an exact selection.
- MarketCow validates books and facts, warms a new generation, and atomically
  activates it.
- `marketcowd` alone owns the active Polymarket scope, live projection, WAL,
  checkpoint, readiness, and stream publication.
- The retired Python fixed-scope collector and `latest-state.sqlite3` path are
  not production participants.

Ports `8795` (discovery stream) and `8796` (Rust data plane/control plane) are
loopback implementation details. Consumers use `8790` only.

The discovery API retains 4096 immutable snapshot generations by default. At
the 0.5-second materialization cadence this gives a consumer about 34 minutes
to finish pagination without crossing or expiring its bounded boundary.
