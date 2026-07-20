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
    (
        2,
        "canonical input fingerprint",
        [
            "ALTER TABLE market_bar_canonical ADD COLUMN IF NOT EXISTS "
            "input_fingerprint String DEFAULT '' AFTER quality_status",
        ],
    ),
    (
        3,
        "history adjustment contract fields",
        [
            "ALTER TABLE market_bar_raw ADD COLUMN IF NOT EXISTS "
            "raw_close Nullable(Float64) AFTER close",
            "ALTER TABLE market_bar_raw ADD COLUMN IF NOT EXISTS "
            "adjustment_factor Nullable(Float64) AFTER raw_close",
            "ALTER TABLE market_bar_canonical ADD COLUMN IF NOT EXISTS "
            "raw_close Nullable(Float64) AFTER close",
            "ALTER TABLE market_bar_canonical ADD COLUMN IF NOT EXISTS "
            "adjustment_factor Nullable(Float64) AFTER raw_close",
        ],
    ),
]


class ClickHouseDatabase:
    """Explicit development/test ClickHouse lifecycle and schema boundary."""

    def __init__(
        self, host: str, port: int, database: str, username: str = "default",
        password: str = "", secure: bool = False, connect_timeout: float = 2.0,
        read_timeout: float = 5.0,
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
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.client: Optional[Any] = None

    def _connect(self, database: str) -> Any:
        return clickhouse_connect.get_client(
            host=self.host, port=self.port, username=self.username,
            password=self.password, database=database, secure=self.secure,
            connect_timeout=self.connect_timeout, send_receive_timeout=self.read_timeout,
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
        "low", "close", "raw_close", "adjustment_factor", "volume", "amount",
        "source", "source_sequence",
        "observed_at", "ingested_at", "raw_artifact_id",
    ]
    CANONICAL_COLUMNS = [
        "symbol", "market", "interval", "adjustment", "bar_time", "open", "high",
        "low", "close", "raw_close", "adjustment_factor", "volume", "amount",
        "selected_source", "source_count",
        "quality_status", "input_fingerprint", "version", "observed_at", "ingested_at",
        "raw_artifact_id", "updated_at",
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

    def query_raw_batch(
        self, symbol: str, interval: str, adjustment: str, source: str,
        bar_times: List[Any],
    ) -> List[Dict[str, Any]]:
        if not bar_times:
            return []
        times = [self._datetime(value) for value in bar_times]
        result = self.database._require_client().query(
            "SELECT * FROM market_bar_raw FINAL WHERE symbol={symbol:String} "
            "AND interval={interval:String} AND adjustment={adjustment:String} "
            "AND source={source:String} AND bar_time IN {times:Array(DateTime64(3))} "
            "ORDER BY bar_time",
            parameters={"symbol": symbol, "interval": interval,
                        "adjustment": adjustment, "source": source, "times": times},
        )
        return [dict(zip(result.column_names, row)) for row in result.result_rows]

    def query_range(
        self, dataset: str, symbol: str, interval: str, adjustment: str,
        start: Any, end: Any, limit: int,
    ) -> tuple[List[Dict[str, Any]], bool]:
        if dataset not in {"raw", "canonical"}:
            raise ValueError("dataset must be raw or canonical")
        if not 1 <= limit <= 100000:
            raise ValueError("range limit must be between 1 and 100000")
        table = f"market_bar_{dataset}"
        suffix = ", source" if dataset == "raw" else ""
        result = self.database._require_client().query(
            f"SELECT * FROM {table} FINAL WHERE symbol={{symbol:String}} "
            "AND interval={interval:String} AND adjustment={adjustment:String} "
            "AND bar_time >= {start:DateTime64(3)} AND bar_time <= {end:DateTime64(3)} "
            f"ORDER BY bar_time{suffix} LIMIT {{fetch:UInt32}}",
            parameters={"symbol": symbol, "interval": interval,
                        "adjustment": adjustment, "start": self._datetime(start),
                        "end": self._datetime(end), "fetch": limit + 1},
        )
        rows = [dict(zip(result.column_names, row)) for row in result.result_rows]
        return rows[:limit], len(rows) > limit

    @staticmethod
    def _iso(value: Any) -> str:
        parsed = ClickHouseMarketBarRepository._datetime(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat()

    def get_canonical_price_bars(
        self, symbol: str, interval: str, adjustment: str, limit: int,
    ) -> List[Dict[str, Any]]:
        if not 1 <= limit <= 5000:
            raise ValueError("history limit must be between 1 and 5000")
        result = self.database._require_client().query(
            "SELECT symbol, interval, adjustment, bar_time, open, high, low, close, "
            "raw_close, adjustment_factor, volume, amount, selected_source, "
            "observed_at, ingested_at, "
            "raw_artifact_id, source_count, quality_status, version "
            "FROM market_bar_canonical FINAL WHERE symbol={symbol:String} "
            "AND interval={interval:String} AND adjustment={adjustment:String} "
            "ORDER BY bar_time DESC LIMIT {limit:UInt32}",
            parameters={"symbol": symbol, "interval": interval,
                        "adjustment": adjustment, "limit": limit},
        )
        rows = [dict(zip(result.column_names, row)) for row in result.result_rows]
        return self._map_canonical_rows(reversed(rows))

    def _map_canonical_rows(self, rows: Any) -> List[Dict[str, Any]]:
        mapped = []
        for row in rows:
            bar_time = self._datetime(row["bar_time"])
            if bar_time.tzinfo is None:
                bar_time = bar_time.replace(tzinfo=timezone.utc)
            bar_time = bar_time.astimezone(timezone.utc)
            mapped.append({
                "symbol": row["symbol"], "interval": row["interval"],
                "adjustment": row["adjustment"], "timestamp": int(bar_time.timestamp()),
                "bar_at": bar_time.isoformat(), "open": float(row["open"]),
                "high": float(row["high"]), "low": float(row["low"]),
                "close": float(row["close"]),
                "raw_close": (None if row["raw_close"] is None
                              else float(row["raw_close"])),
                "adjustment_factor": (None if row["adjustment_factor"] is None
                                      else float(row["adjustment_factor"])),
                "volume": float(row["volume"]),
                "amount": None if row["amount"] is None else float(row["amount"]),
                "source": row["selected_source"],
                "ingested_at": self._iso(row["ingested_at"]),
                "source_payload": {
                    "canonical": True, "selected_source": row["selected_source"],
                    "source_count": int(row["source_count"]),
                    "quality_status": row["quality_status"],
                    "version": int(row["version"]),
                    "observed_at": self._iso(row["observed_at"]),
                    "raw_artifact_id": row["raw_artifact_id"],
                },
            })
        return mapped

    def get_canonical_price_bars_range(
        self, symbol: str, interval: str, adjustment: str,
        start: str, end: str, limit: int,
    ) -> tuple[List[Dict[str, Any]], bool]:
        if not 1 <= limit <= 5000:
            raise ValueError("history limit must be between 1 and 5000")
        start_at = datetime.fromisoformat(start.replace("Z", "+00:00"))
        end_at = datetime.fromisoformat(end.replace("Z", "+00:00"))
        if start_at.tzinfo is None or end_at.tzinfo is None:
            raise ValueError("history range timestamps must include a timezone")
        start_at = start_at.astimezone(timezone.utc)
        end_at = end_at.astimezone(timezone.utc)
        if start_at > end_at:
            raise ValueError("history range start must not be after end")
        start_at = datetime.fromtimestamp(int(start_at.timestamp()), timezone.utc)
        end_at = datetime.fromtimestamp(int(end_at.timestamp()), timezone.utc)
        result = self.database._require_client().query(
            "SELECT symbol, interval, adjustment, bar_time, open, high, low, close, "
            "raw_close, adjustment_factor, volume, amount, selected_source, "
            "observed_at, ingested_at, raw_artifact_id, source_count, quality_status, "
            "version FROM market_bar_canonical FINAL WHERE symbol={symbol:String} "
            "AND interval={interval:String} AND adjustment={adjustment:String} "
            "AND bar_time >= {start:DateTime64(3)} AND bar_time <= {end:DateTime64(3)} "
            "ORDER BY bar_time ASC LIMIT {fetch:UInt32}",
            parameters={"symbol": symbol, "interval": interval,
                        "adjustment": adjustment, "start": start_at, "end": end_at,
                        "fetch": limit + 1},
        )
        rows = [dict(zip(result.column_names, row)) for row in result.result_rows]
        return self._map_canonical_rows(rows[:limit]), len(rows) > limit

    def get_raw_price_bars_range(
        self, symbol: str, interval: str, adjustment: str,
        start: str, end: str, limit: int,
        sources: Optional[List[str]] = None,
    ) -> tuple[List[Dict[str, Any]], bool]:
        if not 1 <= limit <= 5000:
            raise ValueError("raw history limit must be between 1 and 5000")
        start_at = datetime.fromisoformat(start.replace("Z", "+00:00"))
        end_at = datetime.fromisoformat(end.replace("Z", "+00:00"))
        if start_at.tzinfo is None or end_at.tzinfo is None:
            raise ValueError("history range timestamps must include a timezone")
        start_at = datetime.fromtimestamp(int(start_at.timestamp()), timezone.utc)
        end_at = datetime.fromtimestamp(int(end_at.timestamp()), timezone.utc)
        if start_at > end_at:
            raise ValueError("history range start must not be after end")
        source_filter = None if sources is None else sorted({
            str(value).strip() for value in sources if str(value).strip()
        })
        if source_filter is not None and len(source_filter) > 100:
            raise ValueError("raw history sources must contain at most 100 values")
        if source_filter == []:
            return [], False
        source_sql = ""
        parameters: Dict[str, Any] = {
            "symbol": symbol, "interval": interval, "adjustment": adjustment,
            "start": start_at, "end": end_at, "fetch": limit + 1,
        }
        if source_filter is not None:
            source_sql = " AND source IN {sources:Array(String)}"
            parameters["sources"] = source_filter
        result = self.database._require_client().query(
            "SELECT symbol, interval, adjustment, toUnixTimestamp(bar_time) AS timestamp, "
            "open, high, low, close, "
            "raw_close, adjustment_factor, volume, amount, source, source_sequence, "
            "toUnixTimestamp(observed_at) AS observed_timestamp, "
            "toUnixTimestamp(ingested_at) AS ingested_timestamp, raw_artifact_id "
            "FROM market_bar_raw FINAL "
            "WHERE symbol={symbol:String} AND interval={interval:String} "
            "AND adjustment={adjustment:String} AND bar_time >= {start:DateTime64(3)} "
            "AND bar_time <= {end:DateTime64(3)}" + source_sql +
            " ORDER BY bar_time ASC, source ASC LIMIT {fetch:UInt32}",
            parameters=parameters,
        )
        rows = [dict(zip(result.column_names, row)) for row in result.result_rows]
        mapped = []
        for row in rows[:limit]:
            timestamp = int(row.pop("timestamp"))
            observed_timestamp = int(row.pop("observed_timestamp"))
            ingested_timestamp = int(row.pop("ingested_timestamp"))
            mapped.append({
                **row, "timestamp": timestamp,
                "bar_at": datetime.fromtimestamp(timestamp, timezone.utc).isoformat(),
                "open": float(row["open"]), "high": float(row["high"]),
                "low": float(row["low"]), "close": float(row["close"]),
                "raw_close": None if row["raw_close"] is None else float(row["raw_close"]),
                "adjustment_factor": (None if row["adjustment_factor"] is None
                                      else float(row["adjustment_factor"])),
                "volume": float(row["volume"]),
                "amount": None if row["amount"] is None else float(row["amount"]),
                "observed_at": datetime.fromtimestamp(
                    observed_timestamp, timezone.utc
                ).isoformat(),
                "ingested_at": datetime.fromtimestamp(
                    ingested_timestamp, timezone.utc
                ).isoformat(),
                "source_payload": {},
            })
        return mapped, len(rows) > limit

    def get_canonical_price_bars_cross_section(
        self, interval: str, adjustment: str, bar_at: str, limit: int,
        symbols: Optional[List[str]] = None,
    ) -> tuple[List[Dict[str, Any]], bool]:
        if not 1 <= limit <= 5000:
            raise ValueError("cross-section limit must be between 1 and 5000")
        point = datetime.fromisoformat(bar_at.replace("Z", "+00:00"))
        if point.tzinfo is None:
            raise ValueError("cross-section bar_at must include a timezone")
        point = datetime.fromtimestamp(
            int(point.astimezone(timezone.utc).timestamp()), timezone.utc
        )
        symbol_filter = None if symbols is None else sorted(set(symbols))
        if symbol_filter is not None and len(symbol_filter) > 5000:
            raise ValueError("cross-section symbols must contain at most 5000 values")
        if symbol_filter == []:
            return [], False
        filter_sql = ""
        parameters: Dict[str, Any] = {
            "interval": interval, "adjustment": adjustment, "bar_at": point,
            "fetch": limit + 1,
        }
        if symbol_filter is not None:
            filter_sql = " AND symbol IN {symbols:Array(String)}"
            parameters["symbols"] = symbol_filter
        result = self.database._require_client().query(
            "SELECT symbol, interval, adjustment, bar_time, open, high, low, close, "
            "raw_close, adjustment_factor, volume, amount, selected_source, "
            "observed_at, ingested_at, raw_artifact_id, source_count, quality_status, "
            "version FROM market_bar_canonical FINAL WHERE interval={interval:String} "
            "AND adjustment={adjustment:String} AND bar_time={bar_at:DateTime64(3)}" +
            filter_sql + " ORDER BY symbol ASC LIMIT {fetch:UInt32}",
            parameters=parameters,
        )
        rows = [dict(zip(result.column_names, row)) for row in result.result_rows]
        return self._map_canonical_rows(rows[:limit]), len(rows) > limit
