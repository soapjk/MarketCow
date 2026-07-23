# MarketCow runtime architecture

MarketCow maintains one runtime architecture:

- PostgreSQL stores transactional data, metadata, fundamentals, control-plane state, and Artifact manifests.
- ClickHouse stores raw and canonical market bars and serves all online market-bar reads.
- The authoritative WAL/spool preserves failed ClickHouse writes for bounded replay.
- The canonical scheduler rebuilds canonical bars from acknowledged ClickHouse raw data.

Every `production`, `development`, and `test` profile requires both databases and an explicit allowed storage root. Startup fails before creating connections, directories, files, or threads when configuration is incomplete or escapes its root.

DuckDB, Warehouse adapters, shadow writes, and in-process storage fallback are not part of MarketCow. Historical data copies must be converted outside this repository before they are imported into PostgreSQL and ClickHouse.
