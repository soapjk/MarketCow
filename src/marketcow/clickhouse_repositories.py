from __future__ import annotations

import re
import ipaddress
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import clickhouse_connect


CLICKHOUSE_MIGRATIONS = [
    (
        1,
        "raw and canonical market bar foundations",
        [
            """
            CREATE TABLE IF NOT EXISTS market_bar_raw (
                symbol String, market LowCardinality(String), interval LowCardinality(String),
                adjustment LowCardinality(String), bar_time DateTime64(3, 'UTC'),
                open Float64, high Float64, low Float64, close Float64,
                volume Float64, amount Nullable(Float64), source LowCardinality(String),
                source_sequence Nullable(String), observed_at DateTime64(3, 'UTC'),
                ingested_at DateTime64(3, 'UTC'), raw_artifact_id Nullable(String)
            ) ENGINE = ReplacingMergeTree(ingested_at)
            PARTITION BY toYYYYMM(bar_time)
            ORDER BY (symbol, interval, adjustment, source, bar_time)
            """,
            """
            CREATE TABLE IF NOT EXISTS market_bar_canonical (
                symbol String, market LowCardinality(String), interval LowCardinality(String),
                adjustment LowCardinality(String), bar_time DateTime64(3, 'UTC'),
                open Float64, high Float64, low Float64, close Float64,
                volume Float64, amount Nullable(Float64),
                selected_source LowCardinality(String), source_count UInt16,
                quality_status LowCardinality(String), version UInt64,
                observed_at DateTime64(3, 'UTC'), ingested_at DateTime64(3, 'UTC'),
                raw_artifact_id Nullable(String), updated_at DateTime64(3, 'UTC')
            ) ENGINE = ReplacingMergeTree(version)
            PARTITION BY toYYYYMM(bar_time)
            ORDER BY (symbol, interval, adjustment, bar_time)
            """,
        ],
    ),
]


class ClickHouseDatabase:
    """Explicit development/test ClickHouse lifecycle and schema boundary."""

    def __init__(
        self, host: str, port: int, database: str, username: str = "default",
        password: str = "", secure: bool = False,
    ) -> None:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", database):
            raise ValueError("ClickHouse database must be a simple identifier")
        try:
            loopback = ipaddress.ip_address(host).is_loopback
        except ValueError:
            loopback = host.lower() == "localhost"
        if not loopback:
            raise ValueError("ClickHouse foundation connections must use a loopback host")
        if not database.endswith(("_development", "_test")):
            raise ValueError("ClickHouse database must end in _development or _test")
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.secure = secure
        self.client: Optional[Any] = None

    def _connect(self, database: str) -> Any:
        return clickhouse_connect.get_client(
            host=self.host, port=self.port, username=self.username,
            password=self.password, database=database, secure=self.secure,
        )

    def open(self) -> None:
        bootstrap = self._connect("default")
        try:
            bootstrap.command(f"CREATE DATABASE IF NOT EXISTS `{self.database}`")
        finally:
            bootstrap.close()
        self.client = self._connect(self.database)
        try:
            if not self.client.ping():
                raise ConnectionError("ClickHouse health check failed")
            self.migrate()
        except Exception:
            self.close()
            raise

    def close(self) -> None:
        if self.client is not None:
            self.client.close()
            self.client = None

    def _require_client(self) -> Any:
        if self.client is None:
            raise RuntimeError("ClickHouse database is not open")
        return self.client

    def migrate(self) -> None:
        client = self._require_client()
        client.command(
            "CREATE TABLE IF NOT EXISTS schema_migrations ("
            "version UInt32, description String, applied_at DateTime64(3, 'UTC') DEFAULT now64(3)"
            ") ENGINE = MergeTree ORDER BY version"
        )
        applied = {row[0] for row in client.query(
            "SELECT version FROM schema_migrations"
        ).result_rows}
        for version, description, statements in CLICKHOUSE_MIGRATIONS:
            if version in applied:
                continue
            for statement in statements:
                client.command(statement)
            client.insert(
                "schema_migrations", [[version, description]],
                column_names=["version", "description"],
            )

    def diagnostics(self) -> Dict[str, Any]:
        client = self._require_client()
        version = client.query("SELECT version()").result_rows[0][0]
        tables = {row[0] for row in client.query(
            "SELECT name FROM system.tables WHERE database = currentDatabase()"
        ).result_rows}
        return {
            "status": "ok" if client.ping() else "unhealthy",
            "database": self.database,
            "version": version,
            "tables": sorted(tables),
        }


class ClickHouseMarketBarRepository:
    RAW_COLUMNS = [
        "symbol", "market", "interval", "adjustment", "bar_time", "open", "high",
        "low", "close", "volume", "amount", "source", "source_sequence",
        "observed_at", "ingested_at", "raw_artifact_id",
    ]
    CANONICAL_COLUMNS = [
        "symbol", "market", "interval", "adjustment", "bar_time", "open", "high",
        "low", "close", "volume", "amount", "selected_source", "source_count",
        "quality_status", "version", "observed_at", "ingested_at", "raw_artifact_id",
        "updated_at",
    ]

    def __init__(self, database: ClickHouseDatabase) -> None:
        self.database = database

    @staticmethod
    def _datetime(value: Any) -> datetime:
        if isinstance(value, datetime):
            return value
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)

    def _insert(
        self, table: str, columns: List[str], rows: List[Dict[str, Any]],
        batch_id: str = "",
    ) -> int:
        if not rows:
            return 0
        date_columns = {"bar_time", "observed_at", "ingested_at", "updated_at"}
        values = [[
            self._datetime(row.get(column)) if column in date_columns else row.get(column)
            for column in columns
        ] for row in rows]
        settings = {"insert_deduplication_token": batch_id} if batch_id else None
        self.database._require_client().insert(
            table, values, column_names=columns, settings=settings
        )
        return len(rows)

    def insert_raw_bars(self, rows: List[Dict[str, Any]], batch_id: str = "") -> int:
        return self._insert("market_bar_raw", self.RAW_COLUMNS, rows, batch_id)

    def insert_canonical_bars(
        self, rows: List[Dict[str, Any]], batch_id: str = ""
    ) -> int:
        return self._insert(
            "market_bar_canonical", self.CANONICAL_COLUMNS, rows, batch_id
        )

    def query_raw_bars(self, symbol: str, limit: int = 100) -> List[Dict[str, Any]]:
        result = self.database._require_client().query(
            "SELECT * FROM market_bar_raw FINAL WHERE symbol = {symbol:String} "
            "ORDER BY bar_time DESC LIMIT {limit:UInt32}",
            parameters={"symbol": symbol, "limit": limit},
        )
        return [dict(zip(result.column_names, row)) for row in result.result_rows]
