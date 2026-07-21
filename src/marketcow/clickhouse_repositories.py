from __future__ import annotations

import hashlib
import ipaddress
import json
import re
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

import clickhouse_connect

from .bar_version import raw_content_rank, raw_content_version
from .canonical_selection import canonical_page_payload, with_effective_time


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
    (
        4,
        "deterministic raw equal-ingestion content rank",
        [
            "DROP TABLE IF EXISTS market_bar_raw_v3",
            "DROP TABLE IF EXISTS market_bar_raw_v4",
            """
            CREATE TABLE market_bar_raw_v4 (
                symbol String, market LowCardinality(String), interval LowCardinality(String),
                adjustment LowCardinality(String), bar_time DateTime64(3, 'UTC'),
                open Float64, high Float64, low Float64, close Float64,
                raw_close Nullable(Float64), adjustment_factor Nullable(Float64),
                volume Float64, amount Nullable(Float64), source LowCardinality(String),
                source_sequence Nullable(String), observed_at DateTime64(3, 'UTC'),
                ingested_at DateTime64(3, 'UTC'), raw_artifact_id Nullable(String),
                content_rank String, content_version UInt256
            ) ENGINE = ReplacingMergeTree(content_version)
            PARTITION BY toYYYYMM(bar_time)
            ORDER BY (symbol, interval, adjustment, source, bar_time)
            """,
            """
            INSERT INTO market_bar_raw_v4
            SELECT symbol, market, interval, adjustment, bar_time, open, high, low,
                   close, raw_close, adjustment_factor, volume, amount, source,
                   source_sequence, observed_at, ingested_at, raw_artifact_id, '',
                   bitShiftLeft(toUInt256(toUnixTimestamp64Milli(ingested_at)), 208)
            FROM market_bar_raw FINAL
            """,
            "RENAME TABLE market_bar_raw TO market_bar_raw_v3, "
            "market_bar_raw_v4 TO market_bar_raw",
            "DROP TABLE market_bar_raw_v3",
        ],
    ),
    (
        5,
        "direct V2 latest quote contract",
        [
            """
            CREATE TABLE IF NOT EXISTS market_quote_latest (
                symbol String, payload_json String,
                observed_at DateTime64(3, 'UTC'), ingested_at DateTime64(3, 'UTC'),
                source LowCardinality(String), content_rank String, content_version UInt256
            ) ENGINE = ReplacingMergeTree(content_version)
            ORDER BY symbol
            """,
        ],
    ),
]


class ClickHouseRepositoryError(RuntimeError):
    """Bounded direct-repository failure with no backend fallback or secret text."""


class ClickHouseDatabase:
    """Explicit V2 ClickHouse lifecycle and schema boundary."""

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
        if not database.endswith(("_production", "_development", "_test")):
            raise ValueError(
                "ClickHouse database must end in _production, _development or _test"
            )
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.secure = secure
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.client: Optional[Any] = None
        self.operation_lock = threading.RLock()

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

    def health_probe(self) -> bool:
        with self.operation_lock:
            return bool(self._require_client().ping())

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

    def pressure_probe(self) -> Dict[str, Any]:
        """Return bounded, read-only server pressure from the live target."""
        timeout = max(0.1, min(float(self.read_timeout), 30.0))
        settings = {"max_execution_time": timeout, "readonly": 1}
        client = self._connect(self.database)
        try:
            merges = client.query(
                "SELECT count() FROM system.merges WHERE database = currentDatabase()",
                settings=settings,
            ).result_rows
            disks = client.query(
                "SELECT sum(total_space), sum(free_space) FROM system.disks",
                settings=settings,
            ).result_rows
        finally:
            client.close()
        if len(merges) != 1 or len(merges[0]) != 1 or len(disks) != 1 or len(disks[0]) != 2:
            raise ClickHouseRepositoryError("ClickHouse pressure result is invalid")
        merge_queue = int(merges[0][0])
        total_bytes, free_bytes = int(disks[0][0]), int(disks[0][1])
        if merge_queue < 0 or total_bytes <= 0 or not 0 <= free_bytes <= total_bytes:
            raise ClickHouseRepositoryError("ClickHouse pressure values are invalid")
        return {
            "status": "observed", "merge_queue": merge_queue,
            "disk_used_ratio": round(1.0 - free_bytes / total_bytes, 6),
        }


class ClickHouseMarketBarRepository:
    RAW_COLUMNS = [
        "symbol", "market", "interval", "adjustment", "bar_time", "open", "high",
        "low", "close", "raw_close", "adjustment_factor", "volume", "amount",
        "source", "source_sequence",
        "observed_at", "ingested_at", "raw_artifact_id", "content_rank",
        "content_version",
    ]
    CANONICAL_COLUMNS = [
        "symbol", "market", "interval", "adjustment", "bar_time", "open", "high",
        "low", "close", "raw_close", "adjustment_factor", "volume", "amount",
        "selected_source", "source_count",
        "quality_status", "input_fingerprint", "version", "observed_at", "ingested_at",
        "raw_artifact_id", "updated_at",
    ]
    QUOTE_COLUMNS = [
        "symbol", "payload_json", "observed_at", "ingested_at", "source",
        "content_rank", "content_version",
    ]

    def __init__(self, database: ClickHouseDatabase) -> None:
        self.database = database

    def _query(self, statement: str, parameters: Optional[Dict[str, Any]] = None) -> Any:
        try:
            with self.database.operation_lock:
                return self.database._require_client().query(
                    statement, parameters=parameters or {}
                )
        except Exception as error:
            raise ClickHouseRepositoryError(
                f"ClickHouse query failed ({type(error).__name__})"
            ) from error

    def _client_insert(self, *args: Any, **kwargs: Any) -> None:
        try:
            with self.database.operation_lock:
                self.database._require_client().insert(*args, **kwargs)
        except Exception as error:
            raise ClickHouseRepositoryError(
                f"ClickHouse write failed ({type(error).__name__})"
            ) from error

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
        self._client_insert(
            table, values, column_names=columns, settings=settings
        )
        return len(rows)

    def insert_raw_bars(self, rows: List[Dict[str, Any]], batch_id: str = "") -> int:
        normalized = []
        for row in rows:
            content_rank = row.get("content_rank") or raw_content_rank(row)
            normalized.append({
                **row, "content_rank": content_rank,
                "content_version": row.get("content_version") or raw_content_version(
                    row["ingested_at"], content_rank
                ),
            })
        return self._insert("market_bar_raw", self.RAW_COLUMNS, normalized, batch_id)

    def insert_canonical_bars(
        self, rows: List[Dict[str, Any]], batch_id: str = ""
    ) -> int:
        return self._insert(
            "market_bar_canonical", self.CANONICAL_COLUMNS, rows, batch_id
        )

    @staticmethod
    def _market(symbol: str, provenance: Dict[str, Any]) -> str:
        if provenance.get("market"):
            return str(provenance["market"])
        if symbol.endswith((".SH", ".SZ", ".BJ")):
            return "CN"
        if symbol.endswith(".HK"):
            return "HK"
        return "US"

    def upsert_quote(self, row: Dict[str, Any]) -> None:
        symbol = str(row.get("symbol") or "").strip()
        source = str(row.get("source") or "").strip()
        ingested_at = row.get("ingested_at")
        observed_at = row.get("observed_at") or row.get("quote_at") or ingested_at
        if not symbol or not source or not ingested_at or not observed_at:
            raise ValueError(
                "quote requires symbol, source, observed_at, and ingested_at"
            )
        payload = json.dumps(
            row, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
        )
        rank = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        self._insert("market_quote_latest", self.QUOTE_COLUMNS, [{
            "symbol": symbol, "payload_json": payload,
            "observed_at": observed_at, "ingested_at": ingested_at,
            "source": source, "content_rank": rank,
            "content_version": raw_content_version(ingested_at, rank),
        }])

    def get_latest_quotes(self, symbols: Sequence[str]) -> List[Dict[str, Any]]:
        normalized = sorted({str(symbol).strip() for symbol in symbols if str(symbol).strip()})
        if not normalized:
            return []
        if len(normalized) > 5000:
            raise ValueError("latest quotes symbols must contain at most 5000 values")
        result = self._query(
            "SELECT symbol, argMax(payload_json, content_version) AS payload_json "
            "FROM market_quote_latest FINAL WHERE symbol IN {symbols:Array(String)} "
            "GROUP BY symbol ORDER BY symbol",
            {"symbols": normalized},
        )
        return [json.loads(row[1]) for row in result.result_rows]

    def prepare_raw_bars(
        self, symbol: str, interval: str, adjustment: str, source: str,
        ingested_at: str, bars: List[Dict[str, Any]],
        provenance: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        provenance = provenance or {}
        normalized = []
        for bar in bars:
            bar_time = bar.get("bar_at")
            if bar_time is None and bar.get("timestamp") is not None:
                bar_time = datetime.fromtimestamp(
                    int(bar["timestamp"]), timezone.utc
                ).isoformat()
            observed_at = provenance.get("observed_at") or bar_time
            normalized.append({
                "symbol": symbol, "market": self._market(symbol, provenance),
                "interval": interval, "adjustment": adjustment,
                "bar_time": bar_time, "open": bar.get("open"),
                "high": bar.get("high"), "low": bar.get("low"),
                "close": bar.get("close"), "raw_close": bar.get("raw_close"),
                "adjustment_factor": bar.get("adjustment_factor"),
                "volume": bar.get("volume"), "amount": bar.get("amount"),
                "source": source,
                "source_sequence": str(bar.get("source_sequence") or bar.get("timestamp")),
                "observed_at": observed_at, "ingested_at": ingested_at,
                "raw_artifact_id": provenance.get("raw_artifact_id"),
            })
        return normalized

    def upsert_price_bars(
        self, symbol: str, interval: str, adjustment: str, source: str,
        ingested_at: str, bars: List[Dict[str, Any]],
        provenance: Optional[Dict[str, Any]] = None,
    ) -> int:
        return self.insert_raw_bars(self.prepare_raw_bars(
            symbol, interval, adjustment, source, ingested_at, bars, provenance
        ))

    # Direct MarketBarRepository contract. The canonical-prefixed methods remain as
    # compatibility entry points for pre-blue/green offline tooling.
    def get_price_bars(
        self, symbol: str, interval: str, adjustment: str, limit: int
    ) -> List[Dict[str, Any]]:
        return self.get_canonical_price_bars(symbol, interval, adjustment, limit)

    def get_price_bars_range(
        self, symbol: str, interval: str, adjustment: str,
        start: str, end: str, limit: int,
    ) -> tuple[List[Dict[str, Any]], bool]:
        return self.get_canonical_price_bars_range(
            symbol, interval, adjustment, start, end, limit
        )

    def get_price_bars_page(
        self, symbol: str, interval: str, adjustment: str,
        start: str, end: str, page_size: int, after: Optional[int] = None,
    ) -> tuple[List[Dict[str, Any]], bool]:
        return self.get_canonical_price_bars_page(
            symbol, interval, adjustment, start, end, page_size, after
        )

    def get_price_bars_cross_section(
        self, interval: str, adjustment: str, bar_at: str, limit: int,
        symbols: Optional[Sequence[str]] = None,
    ) -> tuple[List[Dict[str, Any]], bool]:
        return self.get_canonical_price_bars_cross_section(
            interval, adjustment, bar_at, limit,
            None if symbols is None else list(symbols),
        )

    def get_price_bars_cross_section_page(
        self, interval: str, adjustment: str, bar_at: str, page_size: int,
        symbols: Optional[Sequence[str]] = None, after: Optional[str] = None,
    ) -> tuple[List[Dict[str, Any]], bool]:
        return self.get_canonical_price_bars_cross_section_page(
            interval, adjustment, bar_at, page_size,
            None if symbols is None else list(symbols), after,
        )

    def get_price_bars_matrix_page(
        self, interval: str, adjustment: str, bar_ats: Sequence[str],
        symbols: Sequence[str], page_size: int,
        after: Optional[tuple[int, str]] = None,
    ) -> tuple[List[Dict[str, Any]], bool]:
        return self.get_canonical_price_bars_matrix_page(
            interval, adjustment, list(bar_ats), list(symbols), page_size, after
        )

    def get_price_bar_as_of(
        self, symbol: str, interval: str, adjustment: str,
        as_of: str, max_lookback_seconds: int,
    ) -> Optional[Dict[str, Any]]:
        return self.get_canonical_price_bar_as_of(
            symbol, interval, adjustment, as_of, max_lookback_seconds
        )

    def get_price_bars_as_of_page(
        self, interval: str, adjustment: str, as_of: str,
        max_lookback_seconds: int, symbols: Sequence[str], page_size: int,
        after: Optional[str] = None,
    ) -> tuple[List[Dict[str, Any]], bool]:
        return self.get_canonical_price_bars_as_of_page(
            interval, adjustment, as_of, max_lookback_seconds,
            list(symbols), page_size, after,
        )

    def query_raw_bars(self, symbol: str, limit: int = 100) -> List[Dict[str, Any]]:
        result = self._query(
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
        result = self._query(
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
        result = self._query(
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
        result = self._query(
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
        result = self._query(
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

    def get_canonical_price_bars_page(
        self, symbol: str, interval: str, adjustment: str,
        start: str, end: str, page_size: int, after: Optional[int] = None,
    ) -> tuple[List[Dict[str, Any]], bool]:
        if not 1 <= page_size <= 5000:
            raise ValueError("history page_size must be between 1 and 5000")
        start_at = self._datetime(start).astimezone(timezone.utc)
        end_at = self._datetime(end).astimezone(timezone.utc)
        start_at = datetime.fromtimestamp(int(start_at.timestamp()), timezone.utc)
        end_at = datetime.fromtimestamp(int(end_at.timestamp()), timezone.utc)
        if start_at > end_at:
            raise ValueError("history range start must not be after end")
        after_sql = "" if after is None else " AND bar_time > {after:DateTime64(3)}"
        parameters: Dict[str, Any] = {
            "symbol": symbol, "interval": interval, "adjustment": adjustment,
            "start": start_at, "end": end_at, "fetch": page_size + 1,
        }
        if after is not None:
            parameters["after"] = datetime.fromtimestamp(after, timezone.utc)
        result = self._query(
            "SELECT symbol, interval, adjustment, bar_time, open, high, low, close, "
            "raw_close, adjustment_factor, volume, amount, selected_source, "
            "observed_at, ingested_at, raw_artifact_id, source_count, quality_status, "
            "version FROM market_bar_canonical FINAL WHERE symbol={symbol:String} "
            "AND interval={interval:String} AND adjustment={adjustment:String} "
            "AND bar_time >= {start:DateTime64(3)} AND bar_time <= {end:DateTime64(3)}" +
            after_sql + " ORDER BY bar_time ASC LIMIT {fetch:UInt32}",
            parameters=parameters,
        )
        rows = [dict(zip(result.column_names, row)) for row in result.result_rows]
        mapped = self._map_canonical_rows(rows[:page_size])
        for row in mapped:
            payload = row["source_payload"]
            row["source_payload"] = canonical_page_payload(
                row["source"], payload["observed_at"], payload["raw_artifact_id"]
            )
        return mapped, len(rows) > page_size

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
        result = self._query(
            "SELECT symbol, interval, adjustment, toUnixTimestamp(bar_time) AS timestamp, "
            "open, high, low, close, "
            "raw_close, adjustment_factor, volume, amount, source, source_sequence, "
            "toUnixTimestamp64Milli(observed_at) AS observed_millis, "
            "toUnixTimestamp64Milli(ingested_at) AS ingested_millis, raw_artifact_id, "
            "content_rank FROM (SELECT *, row_number() OVER (PARTITION BY symbol, "
            "interval, adjustment, source, bar_time ORDER BY ingested_at DESC, "
            "content_rank DESC) AS selected FROM market_bar_raw "
            "WHERE symbol={symbol:String} AND interval={interval:String} "
            "AND adjustment={adjustment:String} AND bar_time >= {start:DateTime64(3)} "
            "AND bar_time <= {end:DateTime64(3)}" + source_sql + ") WHERE selected=1" +
            " ORDER BY bar_time ASC, source ASC LIMIT {fetch:UInt32}",
            parameters=parameters,
        )
        rows = [dict(zip(result.column_names, row)) for row in result.result_rows]
        mapped = []
        for row in rows[:limit]:
            timestamp = int(row.pop("timestamp"))
            observed_millis = int(row.pop("observed_millis"))
            ingested_millis = int(row.pop("ingested_millis"))
            row.pop("content_rank")
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
                    observed_millis / 1000, timezone.utc
                ).isoformat(),
                "ingested_at": datetime.fromtimestamp(
                    ingested_millis / 1000, timezone.utc
                ).isoformat(),
                "source_payload": {},
            })
        return mapped, len(rows) > limit

    def get_raw_price_bars_page(
        self, symbol: str, interval: str, adjustment: str,
        start: str, end: str, page_size: int,
        sources: Optional[List[str]] = None,
        after: Optional[tuple[int, str]] = None,
    ) -> tuple[List[Dict[str, Any]], bool]:
        if not 1 <= page_size <= 5000:
            raise ValueError("raw history page_size must be between 1 and 5000")
        start_at = self._datetime(start).astimezone(timezone.utc)
        end_at = self._datetime(end).astimezone(timezone.utc)
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
        after_sql = ""
        parameters: Dict[str, Any] = {
            "symbol": symbol, "interval": interval, "adjustment": adjustment,
            "start": start_at, "end": end_at, "fetch": page_size + 1,
        }
        if source_filter is not None:
            source_sql = " AND source IN {sources:Array(String)}"
            parameters["sources"] = source_filter
        if after is not None:
            after_sql = (
                " AND (bar_time > {after_time:DateTime64(3)} OR "
                "(bar_time = {after_time:DateTime64(3)} AND source > {after_source:String}))"
            )
            parameters["after_time"] = datetime.fromtimestamp(after[0], timezone.utc)
            parameters["after_source"] = after[1]
        result = self._query(
            "SELECT symbol, interval, adjustment, toUnixTimestamp(bar_time) AS timestamp, "
            "open, high, low, close, raw_close, adjustment_factor, volume, amount, source, "
            "source_sequence, toUnixTimestamp64Milli(observed_at) AS observed_millis, "
            "toUnixTimestamp64Milli(ingested_at) AS ingested_millis, raw_artifact_id, "
            "content_rank FROM (SELECT *, row_number() OVER (PARTITION BY symbol, "
            "interval, adjustment, source, bar_time ORDER BY ingested_at DESC, "
            "content_rank DESC) AS selected FROM market_bar_raw "
            "WHERE symbol={symbol:String} AND interval={interval:String} "
            "AND adjustment={adjustment:String} AND bar_time >= {start:DateTime64(3)} "
            "AND bar_time <= {end:DateTime64(3)}" + source_sql + ") WHERE selected=1" +
            after_sql + " ORDER BY bar_time ASC, source ASC LIMIT {fetch:UInt32}",
            parameters=parameters,
        )
        rows = [dict(zip(result.column_names, row)) for row in result.result_rows]
        mapped = []
        for row in rows[:page_size]:
            timestamp = int(row.pop("timestamp"))
            observed_millis = int(row.pop("observed_millis"))
            ingested_millis = int(row.pop("ingested_millis"))
            row.pop("content_rank")
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
                    observed_millis / 1000, timezone.utc
                ).isoformat(),
                "ingested_at": datetime.fromtimestamp(
                    ingested_millis / 1000, timezone.utc
                ).isoformat(),
                "source_payload": {},
            })
        return mapped, len(rows) > page_size

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
        result = self._query(
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

    def get_canonical_price_bars_cross_section_page(
        self, interval: str, adjustment: str, bar_at: str, page_size: int,
        symbols: Optional[List[str]] = None, after: Optional[str] = None,
    ) -> tuple[List[Dict[str, Any]], bool]:
        if not 1 <= page_size <= 5000:
            raise ValueError("cross-section page_size must be between 1 and 5000")
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
            "fetch": page_size + 1,
        }
        if symbol_filter is not None:
            filter_sql += " AND symbol IN {symbols:Array(String)}"
            parameters["symbols"] = symbol_filter
        if after is not None:
            filter_sql += " AND symbol > {after:String}"
            parameters["after"] = after
        result = self._query(
            "SELECT symbol, interval, adjustment, bar_time, open, high, low, close, "
            "raw_close, adjustment_factor, volume, amount, selected_source, "
            "observed_at, ingested_at, raw_artifact_id, source_count, quality_status, "
            "version FROM market_bar_canonical FINAL WHERE interval={interval:String} "
            "AND adjustment={adjustment:String} AND bar_time={bar_at:DateTime64(3)}" +
            filter_sql + " ORDER BY symbol ASC LIMIT {fetch:UInt32}",
            parameters=parameters,
        )
        rows = [dict(zip(result.column_names, row)) for row in result.result_rows]
        mapped = self._map_canonical_rows(rows[:page_size])
        for row in mapped:
            payload = row["source_payload"]
            row["source_payload"] = canonical_page_payload(
                row["source"], payload["observed_at"], payload["raw_artifact_id"]
            )
        return mapped, len(rows) > page_size

    def get_canonical_price_bars_matrix_page(
        self, interval: str, adjustment: str, bar_ats: List[str],
        symbols: List[str], page_size: int,
        after: Optional[tuple[int, str]] = None,
    ) -> tuple[List[Dict[str, Any]], bool]:
        if not 1 <= page_size <= 5000:
            raise ValueError("matrix page_size must be between 1 and 5000")
        points = sorted({
            datetime.fromtimestamp(
                int(self._datetime(value).astimezone(timezone.utc).timestamp()),
                timezone.utc,
            ) for value in bar_ats
        })
        symbol_filter = sorted({str(value).strip() for value in symbols if str(value).strip()})
        if not 1 <= len(points) <= 100:
            raise ValueError("matrix bar_ats must contain between 1 and 100 values")
        if not 1 <= len(symbol_filter) <= 1000:
            raise ValueError("matrix symbols must contain between 1 and 1000 values")
        if len(points) * len(symbol_filter) > 100_000:
            raise ValueError("matrix request must contain at most 100000 cells")
        after_sql = ""
        parameters: Dict[str, Any] = {
            "interval": interval, "adjustment": adjustment, "bar_ats": points,
            "symbols": symbol_filter, "fetch": page_size + 1,
        }
        if after is not None:
            after_sql = (
                " AND (bar_time > {after_time:DateTime64(3)} OR "
                "(bar_time = {after_time:DateTime64(3)} AND symbol > {after_symbol:String}))"
            )
            parameters["after_time"] = datetime.fromtimestamp(after[0], timezone.utc)
            parameters["after_symbol"] = after[1]
        result = self._query(
            "SELECT symbol, interval, adjustment, bar_time, open, high, low, close, "
            "raw_close, adjustment_factor, volume, amount, selected_source, "
            "observed_at, ingested_at, raw_artifact_id, source_count, quality_status, "
            "version FROM market_bar_canonical FINAL WHERE interval={interval:String} "
            "AND adjustment={adjustment:String} "
            "AND bar_time IN {bar_ats:Array(DateTime64(3))} "
            "AND symbol IN {symbols:Array(String)}" + after_sql +
            " ORDER BY bar_time ASC, symbol ASC LIMIT {fetch:UInt32}",
            parameters=parameters,
        )
        rows = [dict(zip(result.column_names, row)) for row in result.result_rows]
        mapped = self._map_canonical_rows(rows[:page_size])
        for row in mapped:
            payload = row["source_payload"]
            row["source_payload"] = canonical_page_payload(
                row["source"], payload["observed_at"], payload["raw_artifact_id"]
            )
        return mapped, len(rows) > page_size

    def get_canonical_price_bar_as_of(
        self, symbol: str, interval: str, adjustment: str,
        as_of: str, max_lookback_seconds: int,
    ) -> Optional[Dict[str, Any]]:
        if not 1 <= max_lookback_seconds <= 31_536_000:
            raise ValueError("max_lookback_seconds must be between 1 and 31536000")
        point = self._datetime(as_of).astimezone(timezone.utc)
        point = datetime.fromtimestamp(int(point.timestamp()), timezone.utc)
        lower = datetime.fromtimestamp(
            int(point.timestamp()) - max_lookback_seconds, timezone.utc
        )
        result = self._query(
            "SELECT symbol, interval, adjustment, bar_time, open, high, low, close, "
            "raw_close, adjustment_factor, volume, amount, selected_source, "
            "observed_at, ingested_at, raw_artifact_id, source_count, quality_status, "
            "version FROM market_bar_canonical FINAL WHERE symbol={symbol:String} "
            "AND interval={interval:String} AND adjustment={adjustment:String} "
            "AND bar_time >= {lower:DateTime64(3)} AND bar_time <= {point:DateTime64(3)} "
            "ORDER BY bar_time DESC LIMIT 1",
            parameters={"symbol": symbol, "interval": interval,
                        "adjustment": adjustment, "lower": lower, "point": point},
        )
        if not result.result_rows:
            return None
        row = dict(zip(result.column_names, result.result_rows[0]))
        mapped = self._as_of_canonical_rows([row], point)
        return mapped[0]

    def get_canonical_price_bars_as_of_page(
        self, interval: str, adjustment: str, as_of: str,
        max_lookback_seconds: int, symbols: List[str], page_size: int,
        after: Optional[str] = None,
    ) -> tuple[List[Dict[str, Any]], bool]:
        if not 1 <= max_lookback_seconds <= 31_536_000:
            raise ValueError("max_lookback_seconds must be between 1 and 31536000")
        if not 1 <= page_size <= 1000:
            raise ValueError("as-of page_size must be between 1 and 1000")
        point = self._datetime(as_of).astimezone(timezone.utc)
        point = datetime.fromtimestamp(int(point.timestamp()), timezone.utc)
        lower = datetime.fromtimestamp(
            int(point.timestamp()) - max_lookback_seconds, timezone.utc
        )
        symbol_filter = sorted({str(value).strip() for value in symbols if str(value).strip()})
        if not 1 <= len(symbol_filter) <= 1000:
            raise ValueError("as-of symbols must contain between 1 and 1000 values")
        after_sql = "" if after is None else " AND symbol > {after:String}"
        parameters: Dict[str, Any] = {
            "interval": interval, "adjustment": adjustment, "lower": lower,
            "point": point, "symbols": symbol_filter, "fetch": page_size + 1,
        }
        if after is not None:
            parameters["after"] = after
        result = self._query(
            "SELECT symbol, interval, adjustment, bar_time, open, high, low, close, "
            "raw_close, adjustment_factor, volume, amount, selected_source, "
            "observed_at, ingested_at, raw_artifact_id, source_count, quality_status, "
            "version FROM (SELECT *, row_number() OVER (PARTITION BY symbol "
            "ORDER BY bar_time DESC) AS selected FROM market_bar_canonical FINAL "
            "WHERE interval={interval:String} AND adjustment={adjustment:String} "
            "AND bar_time >= {lower:DateTime64(3)} AND bar_time <= {point:DateTime64(3)} "
            "AND symbol IN {symbols:Array(String)}) WHERE selected=1" + after_sql +
            " ORDER BY symbol ASC LIMIT {fetch:UInt32}",
            parameters=parameters,
        )
        rows = [dict(zip(result.column_names, row)) for row in result.result_rows]
        return self._as_of_canonical_rows(rows[:page_size], point), len(rows) > page_size

    def _as_of_canonical_rows(
        self, rows: Any, point: datetime,
    ) -> List[Dict[str, Any]]:
        mapped = self._map_canonical_rows(rows)
        for row in mapped:
            payload = row["source_payload"]
            row["source_payload"] = canonical_page_payload(
                row["source"], payload["observed_at"], payload["raw_artifact_id"]
            )
        return [with_effective_time(row, point) for row in mapped]
