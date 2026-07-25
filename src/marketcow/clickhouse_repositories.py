from __future__ import annotations

import hashlib
import ipaddress
import json
import math
import re
import threading
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence

import clickhouse_connect

from .bar_version import raw_content_rank, raw_content_version
from .canonical_selection import canonical_page_payload, with_effective_time
from .migration_policy import validate_migration_history


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
        "direct latest quote contract",
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
    (
        6,
        "raw history ingestion identity",
        [
            "ALTER TABLE market_bar_raw ADD COLUMN IF NOT EXISTS "
            "ingestion_id String DEFAULT '' AFTER raw_artifact_id",
        ],
    ),
    (
        7,
        "daily market adjustment factors",
        [
            """
            CREATE TABLE IF NOT EXISTS market_adjustment_factor (
                symbol String, trade_date Date, adjustment_factor Decimal128(18),
                source LowCardinality(String), observed_at DateTime64(3, 'UTC'),
                ingested_at DateTime64(3, 'UTC'), raw_artifact_id Nullable(String),
                ingestion_id String, content_rank String, content_version UInt256
            ) ENGINE = ReplacingMergeTree(content_version)
            PARTITION BY toYYYYMM(trade_date)
            ORDER BY (symbol, source, trade_date)
            """,
        ],
    ),
    (
        8,
        "explicit price adjustment contract fields",
        [
            "ALTER TABLE market_bar_raw ADD COLUMN IF NOT EXISTS "
            "factor_applicability LowCardinality(String) DEFAULT '' AFTER adjustment_factor",
            "ALTER TABLE market_bar_raw ADD COLUMN IF NOT EXISTS "
            "corporate_action_factor Nullable(Decimal128(18)) AFTER factor_applicability",
            "ALTER TABLE market_bar_raw ADD COLUMN IF NOT EXISTS "
            "applied_adjustment_multiplier Nullable(Decimal128(18)) "
            "AFTER corporate_action_factor",
            "ALTER TABLE market_bar_raw ADD COLUMN IF NOT EXISTS "
            "adjustment_reference_date Nullable(Date) "
            "AFTER applied_adjustment_multiplier",
            "ALTER TABLE market_bar_raw ADD COLUMN IF NOT EXISTS "
            "reference_factor Nullable(Decimal128(18)) AFTER adjustment_reference_date",
            "ALTER TABLE market_bar_raw ADD COLUMN IF NOT EXISTS "
            "factor_source Nullable(String) AFTER reference_factor",
            "ALTER TABLE market_bar_raw ADD COLUMN IF NOT EXISTS "
            "factor_artifact_id Nullable(String) AFTER factor_source",
            "ALTER TABLE market_bar_raw ADD COLUMN IF NOT EXISTS "
            "factor_as_of Nullable(DateTime64(3, 'UTC')) AFTER factor_artifact_id",
            "ALTER TABLE market_bar_canonical ADD COLUMN IF NOT EXISTS "
            "factor_applicability LowCardinality(String) DEFAULT '' AFTER adjustment_factor",
            "ALTER TABLE market_bar_canonical ADD COLUMN IF NOT EXISTS "
            "corporate_action_factor Nullable(Decimal128(18)) AFTER factor_applicability",
            "ALTER TABLE market_bar_canonical ADD COLUMN IF NOT EXISTS "
            "applied_adjustment_multiplier Nullable(Decimal128(18)) "
            "AFTER corporate_action_factor",
            "ALTER TABLE market_bar_canonical ADD COLUMN IF NOT EXISTS "
            "adjustment_reference_date Nullable(Date) "
            "AFTER applied_adjustment_multiplier",
            "ALTER TABLE market_bar_canonical ADD COLUMN IF NOT EXISTS "
            "reference_factor Nullable(Decimal128(18)) AFTER adjustment_reference_date",
            "ALTER TABLE market_bar_canonical ADD COLUMN IF NOT EXISTS "
            "factor_source Nullable(String) AFTER reference_factor",
            "ALTER TABLE market_bar_canonical ADD COLUMN IF NOT EXISTS "
            "factor_artifact_id Nullable(String) AFTER factor_source",
            "ALTER TABLE market_bar_canonical ADD COLUMN IF NOT EXISTS "
            "factor_as_of Nullable(DateTime64(3, 'UTC')) AFTER factor_artifact_id",
        ],
    ),
]


class ClickHouseRepositoryError(RuntimeError):
    """Bounded direct-repository failure with no backend fallback or secret text."""


def canonical_json_value(value: Any) -> Any:
    """Normalize ClickHouse values without repr fallbacks or information loss."""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="strict")
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("canonical Decimal values must be finite")
        return format(value, "f")
    if isinstance(value, datetime):
        if value.tzinfo is None:
            # clickhouse-connect returns DateTime64(..., 'UTC') as naive datetime.
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("canonical float values must be finite")
        return value
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, (list, tuple)):
        return [canonical_json_value(item) for item in value]
    if isinstance(value, dict):
        normalized: Dict[str, Any] = {}
        for raw_key, raw_value in value.items():
            key = canonical_json_value(raw_key)
            if not isinstance(key, str):
                raise TypeError("canonical JSON object keys must be text")
            if key in normalized:
                raise ValueError("canonical JSON object keys collide after decoding")
            normalized[key] = canonical_json_value(raw_value)
        return normalized
    raise TypeError(f"unsupported canonical JSON value type: {type(value).__name__}")


class ClickHouseDatabase:
    """Explicit ClickHouse lifecycle and schema boundary."""

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
        applied = validate_migration_history(
            client.query(
                "SELECT version, description FROM schema_migrations"
            ).result_rows,
            CLICKHOUSE_MIGRATIONS,
            "ClickHouse",
        )
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
        "factor_applicability", "corporate_action_factor",
        "applied_adjustment_multiplier", "adjustment_reference_date",
        "reference_factor", "factor_source", "factor_artifact_id", "factor_as_of",
        "source", "source_sequence",
        "observed_at", "ingested_at", "raw_artifact_id", "ingestion_id",
        "content_rank", "content_version",
    ]
    CANONICAL_COLUMNS = [
        "symbol", "market", "interval", "adjustment", "bar_time", "open", "high",
        "low", "close", "raw_close", "adjustment_factor", "volume", "amount",
        "factor_applicability", "corporate_action_factor",
        "applied_adjustment_multiplier", "adjustment_reference_date",
        "reference_factor", "factor_source", "factor_artifact_id", "factor_as_of",
        "selected_source", "source_count",
        "quality_status", "input_fingerprint", "version", "observed_at", "ingested_at",
        "raw_artifact_id", "updated_at",
    ]
    QUOTE_COLUMNS = [
        "symbol", "payload_json", "observed_at", "ingested_at", "source",
        "content_rank", "content_version",
    ]
    ADJUSTMENT_FACTOR_COLUMNS = [
        "symbol", "trade_date", "adjustment_factor", "source", "observed_at",
        "ingested_at", "raw_artifact_id", "ingestion_id", "content_rank",
        "content_version",
    ]

    def __init__(self, database: ClickHouseDatabase) -> None:
        self.database = database

    @staticmethod
    def _range_time(value: Any, name: str) -> datetime:
        parsed = (
            value
            if isinstance(value, datetime)
            else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        )
        if parsed.tzinfo is None:
            raise ValueError(f"{name} must include a timezone")
        return parsed.astimezone(timezone.utc)

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
        date_columns = {
            "bar_time", "observed_at", "ingested_at", "updated_at", "factor_as_of",
        }
        values = [[
            (
                self._datetime(row.get(column))
                if column in date_columns and row.get(column) is not None
                else (
                    date.fromisoformat(str(row.get(column)))
                    if column in {"trade_date", "adjustment_reference_date"}
                    and row.get(column) is not None
                    and not isinstance(row.get(column), date)
                    else row.get(column)
                )
            )
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
        # Keep the deterministic tie-break inside the low 208 bits reserved by
        # raw_content_version so ingestion time always dominates older content.
        rank = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:52]
        computed_version = raw_content_version(ingested_at, rank)
        # Quote versions created before the rank-width fix may occupy timestamp
        # bits. Serialize read/compare/write so a fresh quote always supersedes
        # those rows while identical retries remain no-ops.
        with self.database.operation_lock:
            current = self.database._require_client().query(
                "SELECT argMax(payload_json, content_version), max(content_version) "
                "FROM market_quote_latest WHERE symbol = {symbol:String}",
                parameters={"symbol": symbol},
            ).result_rows[0]
            current_payload, current_version = current
            if current_payload == payload:
                return
            version = max(computed_version, int(current_version or 0) + 1)
            self._insert("market_quote_latest", self.QUOTE_COLUMNS, [{
                "symbol": symbol, "payload_json": payload,
                "observed_at": observed_at, "ingested_at": ingested_at,
                "source": source, "content_rank": rank,
                "content_version": version,
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
                "factor_applicability": bar.get("factor_applicability") or "",
                "corporate_action_factor": bar.get("corporate_action_factor"),
                "applied_adjustment_multiplier": bar.get(
                    "applied_adjustment_multiplier"
                ),
                "adjustment_reference_date": bar.get(
                    "adjustment_reference_date"
                ),
                "reference_factor": bar.get("reference_factor"),
                "factor_source": bar.get("factor_source"),
                "factor_artifact_id": bar.get("factor_artifact_id"),
                "factor_as_of": bar.get("factor_as_of"),
                "volume": bar.get("volume"), "amount": bar.get("amount"),
                "source": source,
                "source_sequence": str(bar.get("source_sequence") or bar.get("timestamp")),
                "observed_at": observed_at, "ingested_at": ingested_at,
                "raw_artifact_id": provenance.get("raw_artifact_id"),
                "ingestion_id": str(provenance.get("ingestion_id") or ""),
            })
        return normalized

    def upsert_price_bars(
        self, symbol: str, interval: str, adjustment: str, source: str,
        ingested_at: str, bars: List[Dict[str, Any]],
        provenance: Optional[Dict[str, Any]] = None,
    ) -> int:
        provenance = provenance or {}
        return self.insert_raw_bars(
            self.prepare_raw_bars(
                symbol, interval, adjustment, source, ingested_at, bars,
                provenance,
            ),
            batch_id=str(provenance.get("ingestion_id") or ""),
        )

    def upsert_adjustment_factors(
        self, symbol: str, source: str, ingested_at: str,
        factors: List[Dict[str, Any]],
        provenance: Optional[Dict[str, Any]] = None,
    ) -> int:
        provenance = provenance or {}
        normalized = []
        for factor in factors:
            trade_date = date.fromisoformat(str(factor["trade_date"])).isoformat()
            value = Decimal(str(factor["adjustment_factor"]))
            if not value.is_finite() or value <= 0:
                raise ValueError("adjustment_factor must be finite and greater than zero")
            rank_payload = json.dumps(
                {
                    "symbol": symbol, "trade_date": trade_date,
                    "adjustment_factor": format(value, "f"), "source": source,
                    "raw_artifact_id": provenance.get("raw_artifact_id"),
                },
                ensure_ascii=False, sort_keys=True, separators=(",", ":"),
            )
            content_rank = hashlib.sha256(
                rank_payload.encode("utf-8")
            ).hexdigest()[:52]
            normalized.append({
                "symbol": symbol, "trade_date": trade_date,
                "adjustment_factor": value, "source": source,
                "observed_at": provenance.get("observed_at") or ingested_at,
                "ingested_at": ingested_at,
                "raw_artifact_id": provenance.get("raw_artifact_id"),
                "ingestion_id": str(provenance.get("ingestion_id") or ""),
                "content_rank": content_rank,
                "content_version": raw_content_version(ingested_at, content_rank),
            })
        return self._insert(
            "market_adjustment_factor", self.ADJUSTMENT_FACTOR_COLUMNS, normalized,
            str(provenance.get("ingestion_id") or ""),
        )

    def get_adjustment_factors(
        self, symbol: str, start_date: str, end_date: str, source: str = "",
    ) -> List[Dict[str, Any]]:
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
        if start > end:
            raise ValueError("adjustment factor date range must be ordered")
        source_filter = " AND source={source:String}" if source else ""
        result = self._query(
            "SELECT symbol,trade_date,adjustment_factor,source,observed_at,"
            "ingested_at,raw_artifact_id,ingestion_id "
            "FROM market_adjustment_factor FINAL "
            "WHERE symbol={symbol:String} AND trade_date>={start:Date} "
            "AND trade_date<={end:Date}"
            f"{source_filter} "
            "ORDER BY trade_date,source",
            {
                "symbol": symbol, "start": start, "end": end,
                **({"source": source} if source else {}),
            },
        )
        return [dict(zip(result.column_names, row)) for row in result.result_rows]

    def get_raw_ingestion_receipt(
        self, ingestion_id: str
    ) -> Optional[Dict[str, Any]]:
        normalized = str(ingestion_id).strip()
        if not normalized:
            raise ValueError("ingestion_id is required")
        result = self._query(
            """
            SELECT count(), min(toUnixTimestamp64Milli(bar_time)),
                   max(toUnixTimestamp64Milli(bar_time)),
                   argMax(raw_artifact_id, content_version)
            FROM market_bar_raw FINAL
            WHERE ingestion_id={ingestion_id:String}
            """,
            {"ingestion_id": normalized},
        )
        row = result.result_rows[0]
        count = int(row[0] or 0)
        if count == 0:
            return None
        return {
            "ingestion_id": normalized, "row_count": count,
            "first_bar_at_ms": int(row[1]), "last_bar_at_ms": int(row[2]),
            "raw_artifact_id": row[3],
        }

    def list_raw_ingestion_receipts(
        self, limit: int = 10000
    ) -> List[Dict[str, Any]]:
        if not 1 <= int(limit) <= 100000:
            raise ValueError("limit must be between 1 and 100000")
        result = self._query(
            """
            SELECT ingestion_id,count(),
                   min(toUnixTimestamp64Milli(bar_time)),
                   max(toUnixTimestamp64Milli(bar_time)),
                   argMax(raw_artifact_id,content_version)
            FROM market_bar_raw FINAL
            WHERE ingestion_id <> ''
            GROUP BY ingestion_id ORDER BY ingestion_id LIMIT {limit:UInt32}
            """,
            {"limit": int(limit)},
        )
        return [{
            "ingestion_id": str(row[0]), "row_count": int(row[1]),
            "first_bar_at_ms": int(row[2]), "last_bar_at_ms": int(row[3]),
            "raw_artifact_id": row[4],
        } for row in result.result_rows]

    def list_adjustment_contract_candidates(
        self, limit: int = 10000
    ) -> List[Dict[str, Any]]:
        if not 1 <= int(limit) <= 100000:
            raise ValueError("limit must be between 1 and 100000")
        result = self._query(
            "SELECT * FROM market_bar_raw FINAL "
            "WHERE factor_applicability='' OR adjustment='adjusted' "
            "ORDER BY symbol,interval,adjustment,source,bar_time "
            "LIMIT {limit:UInt32}",
            {"limit": int(limit)},
        )
        return [
            dict(zip(result.column_names, row)) for row in result.result_rows
        ]

    def adjustment_contract_audit(self) -> List[Dict[str, Any]]:
        result = self._query(
            "SELECT adjustment,factor_applicability,count() AS row_count "
            "FROM market_bar_raw FINAL "
            "GROUP BY adjustment,factor_applicability "
            "ORDER BY adjustment,factor_applicability"
        )
        return [{
            "adjustment": str(row[0]),
            "factor_applicability": str(row[1]),
            "row_count": int(row[2]),
        } for row in result.result_rows]

    def get_canonical_ingestion_coverage(
        self, ingestion_ids: Sequence[str]
    ) -> Dict[str, Any]:
        normalized = sorted({
            str(value).strip() for value in ingestion_ids if str(value).strip()
        })
        if not normalized:
            raise ValueError("ingestion_ids must not be empty")
        if len(normalized) > 10000:
            raise ValueError("ingestion_ids must contain at most 10000 values")
        result = self._query(
            """
            SELECT count() AS raw_rows,
                   countIf(c.symbol != '') AS canonical_rows,
                   min(toUnixTimestamp64Milli(r.bar_time)) AS first_bar_at_ms,
                   max(toUnixTimestamp64Milli(r.bar_time)) AS last_bar_at_ms,
                   countIf(
                       c.symbol != '' AND (
                           NOT isFinite(c.open) OR NOT isFinite(c.high)
                           OR NOT isFinite(c.low) OR NOT isFinite(c.close)
                           OR c.open <= 0 OR c.high <= 0 OR c.low <= 0
                           OR c.close <= 0 OR c.volume < 0
                           OR c.low > least(c.open,c.close)
                           OR c.high < greatest(c.open,c.close)
                           OR c.low > c.high
                       )
                   ) AS canonical_invalid_ohlc_rows,
                   countIf(
                       c.symbol != '' AND c.low > 0
                       AND c.high / c.low >= 100
                   ) AS canonical_abnormal_price_rows
            FROM (
                SELECT DISTINCT symbol,interval,adjustment,bar_time
                FROM market_bar_raw FINAL
                WHERE ingestion_id IN {ingestion_ids:Array(String)}
            ) r
            LEFT JOIN market_bar_canonical FINAL c
              ON c.symbol=r.symbol AND c.interval=r.interval
             AND c.adjustment=r.adjustment AND c.bar_time=r.bar_time
            """,
            {"ingestion_ids": normalized},
        )
        row = result.result_rows[0]
        return {
            "ingestion_ids": normalized,
            "raw_rows": int(row[0] or 0),
            "canonical_rows": int(row[1] or 0),
            "first_bar_at_ms": None if row[2] is None else int(row[2]),
            "last_bar_at_ms": None if row[3] is None else int(row[3]),
            "canonical_invalid_ohlc_rows": (
                int(row[4] or 0) if len(row) > 4 else 0
            ),
            "canonical_abnormal_price_rows": (
                int(row[5] or 0) if len(row) > 5 else 0
            ),
        }

    def get_canonical_ingestion_quality(
        self, ingestion_ids: Sequence[str]
    ) -> List[Dict[str, Any]]:
        normalized = sorted({
            str(value).strip() for value in ingestion_ids if str(value).strip()
        })
        if not normalized or len(normalized) > 10000:
            raise ValueError(
                "ingestion_ids must contain between 1 and 10000 values"
            )
        result = self._query(
            """
            SELECT r.ingestion_id,
                   count() AS raw_rows,
                   countIf(c.symbol != '') AS canonical_rows,
                   countIf(
                       c.symbol != '' AND (
                           NOT isFinite(c.open) OR NOT isFinite(c.high)
                           OR NOT isFinite(c.low) OR NOT isFinite(c.close)
                           OR c.open <= 0 OR c.high <= 0 OR c.low <= 0
                           OR c.close <= 0 OR c.volume < 0
                           OR c.low > least(c.open,c.close)
                           OR c.high < greatest(c.open,c.close)
                           OR c.low > c.high
                       )
                   ) AS canonical_invalid_ohlc_rows
            FROM (
                SELECT DISTINCT ingestion_id,symbol,interval,adjustment,bar_time
                FROM market_bar_raw FINAL
                WHERE ingestion_id IN {ingestion_ids:Array(String)}
            ) r
            LEFT JOIN market_bar_canonical FINAL c
              ON c.symbol=r.symbol AND c.interval=r.interval
             AND c.adjustment=r.adjustment AND c.bar_time=r.bar_time
            GROUP BY r.ingestion_id
            ORDER BY r.ingestion_id
            """,
            {"ingestion_ids": normalized},
        )
        return [{
            "ingestion_id": str(row[0]),
            "raw_rows": int(row[1] or 0),
            "canonical_rows": int(row[2] or 0),
            "canonical_invalid_ohlc_rows": int(row[3] or 0),
        } for row in result.result_rows]

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
            "raw_close, adjustment_factor, factor_applicability, "
            "corporate_action_factor, applied_adjustment_multiplier, "
            "adjustment_reference_date, reference_factor, factor_source, "
            "factor_artifact_id, factor_as_of, volume, amount, selected_source, "
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

    def get_symbol_coverage(self, symbol: str) -> List[Dict[str, Any]]:
        result = self._query(
            """
            SELECT layer, interval, adjustment, min(bar_time) AS first_bar,
                   max(bar_time) AS last_bar, count() AS row_count,
                   groupUniqArray(source) AS sources
            FROM (
                SELECT 'raw' AS layer, interval, adjustment, bar_time, source
                FROM market_bar_raw FINAL WHERE symbol={symbol:String}
                UNION ALL
                SELECT 'canonical' AS layer, interval, adjustment, bar_time,
                       selected_source AS source
                FROM market_bar_canonical FINAL WHERE symbol={symbol:String}
            )
            GROUP BY layer, interval, adjustment
            ORDER BY layer, interval, adjustment
            """,
            parameters={"symbol": symbol},
        )
        rows = []
        for raw in result.result_rows:
            row = dict(zip(result.column_names, raw))
            rows.append({
                "layer": str(row["layer"]),
                "interval": str(row["interval"]),
                "adjustment": str(row["adjustment"]),
                "first_bar": self._iso(row["first_bar"]),
                "last_bar": self._iso(row["last_bar"]),
                "row_count": int(row["row_count"]),
                "sources": sorted(str(source) for source in row["sources"]),
            })
        return rows

    def _map_canonical_rows(self, rows: Any) -> List[Dict[str, Any]]:
        mapped = []
        for raw_row in rows:
            row = canonical_json_value(raw_row)
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
                **self._map_adjustment_contract(row),
                "volume": float(row["volume"]),
                "amount": None if row["amount"] is None else float(row["amount"]),
                "source": row["selected_source"],
                "selected_source": row["selected_source"],
                "quality_status": row["quality_status"],
                "version": int(row["version"]),
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

    def _map_adjustment_contract(self, row: Dict[str, Any]) -> Dict[str, Any]:
        def decimal_text(name: str) -> Optional[str]:
            value = row.get(name)
            if value is None:
                return None
            return format(Decimal(str(value)), "f")

        reference_date = row.get("adjustment_reference_date")
        factor_as_of = row.get("factor_as_of")
        return {
            "factor_applicability": str(row.get("factor_applicability") or ""),
            "corporate_action_factor": decimal_text("corporate_action_factor"),
            "applied_adjustment_multiplier": decimal_text(
                "applied_adjustment_multiplier"
            ),
            "adjustment_reference_date": (
                None if reference_date is None else str(reference_date)
            ),
            "reference_factor": decimal_text("reference_factor"),
            "factor_source": row.get("factor_source"),
            "factor_artifact_id": row.get("factor_artifact_id"),
            "factor_as_of": (
                None if factor_as_of is None else self._iso(factor_as_of)
            ),
        }

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
            "raw_close, adjustment_factor, factor_applicability, "
            "corporate_action_factor, applied_adjustment_multiplier, "
            "adjustment_reference_date, reference_factor, factor_source, "
            "factor_artifact_id, factor_as_of, volume, amount, selected_source, "
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
            "raw_close, adjustment_factor, factor_applicability, "
            "corporate_action_factor, applied_adjustment_multiplier, "
            "adjustment_reference_date, reference_factor, factor_source, "
            "factor_artifact_id, factor_as_of, volume, amount, selected_source, "
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
            "raw_close, adjustment_factor, factor_applicability, "
            "corporate_action_factor, applied_adjustment_multiplier, "
            "adjustment_reference_date, reference_factor, factor_source, "
            "factor_artifact_id, factor_as_of, volume, amount, source, source_sequence, "
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
                **self._map_adjustment_contract(row),
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
            "open, high, low, close, raw_close, adjustment_factor, "
            "factor_applicability, corporate_action_factor, "
            "applied_adjustment_multiplier, adjustment_reference_date, "
            "reference_factor, factor_source, factor_artifact_id, factor_as_of, "
            "volume, amount, source, "
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
                **self._map_adjustment_contract(row),
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
            "raw_close, adjustment_factor, factor_applicability, "
            "corporate_action_factor, applied_adjustment_multiplier, "
            "adjustment_reference_date, reference_factor, factor_source, "
            "factor_artifact_id, factor_as_of, volume, amount, selected_source, "
            "observed_at, ingested_at, raw_artifact_id, source_count, quality_status, "
            "version FROM market_bar_canonical FINAL WHERE interval={interval:String} "
            "AND adjustment={adjustment:String} AND bar_time={bar_at:DateTime64(3)}" +
            filter_sql + " ORDER BY symbol ASC LIMIT {fetch:UInt32}",
            parameters=parameters,
        )
        rows = [dict(zip(result.column_names, row)) for row in result.result_rows]
        return self._map_canonical_rows(rows[:limit]), len(rows) > limit

    def get_canonical_dataset_identity(
        self, symbol: str, interval: str, adjustment: str, start: str, end: str
    ) -> Dict[str, Any]:
        start_at = self._range_time(start, "start")
        end_at = self._range_time(end, "end")
        result = self._query(
            """
            SELECT toUnixTimestamp64Milli(bar_time), toString(open), toString(high),
                   toString(low), toString(close), toString(raw_close),
                   toString(adjustment_factor), factor_applicability,
                   toString(corporate_action_factor),
                   toString(applied_adjustment_multiplier),
                   toString(adjustment_reference_date), toString(reference_factor),
                   factor_source, factor_artifact_id, toString(factor_as_of),
                   toString(volume), toString(amount),
                   selected_source, source_count, quality_status, input_fingerprint,
                   toString(version), toUnixTimestamp64Milli(observed_at),
                   toUnixTimestamp64Milli(ingested_at), raw_artifact_id
            FROM market_bar_canonical FINAL
            WHERE symbol={symbol:String} AND interval={interval:String}
              AND adjustment={adjustment:String}
              AND bar_time >= {start:DateTime64(3)} AND bar_time <= {end:DateTime64(3)}
            ORDER BY bar_time ASC
            """,
            parameters={
                "symbol": symbol, "interval": interval, "adjustment": adjustment,
                "start": start_at, "end": end_at,
            },
        )
        canonical_rows = canonical_json_value(result.result_rows)
        max_ingested_millis = max(
            (int(row[-2]) for row in canonical_rows), default=0
        )
        content_hash = "sha256:" + hashlib.sha256(json.dumps(
            canonical_rows, separators=(",", ":"), ensure_ascii=True
        ).encode()).hexdigest()
        identity = canonical_json_value({
            "symbol": symbol, "interval": interval, "adjustment": adjustment,
            "start": start_at.isoformat(), "end": end_at.isoformat(),
            "row_count": len(canonical_rows),
            "max_ingested_millis": max_ingested_millis,
            "content_hash": content_hash,
        })
        digest = hashlib.sha256(json.dumps(
            identity, sort_keys=True, separators=(",", ":")
        ).encode()).hexdigest()
        return {
            **identity, "canonical_version": str(identity["max_ingested_millis"]),
            "snapshot_id": digest[:32],
        }

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
            "raw_close, adjustment_factor, factor_applicability, "
            "corporate_action_factor, applied_adjustment_multiplier, "
            "adjustment_reference_date, reference_factor, factor_source, "
            "factor_artifact_id, factor_as_of, volume, amount, selected_source, "
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
            "raw_close, adjustment_factor, factor_applicability, "
            "corporate_action_factor, applied_adjustment_multiplier, "
            "adjustment_reference_date, reference_factor, factor_source, "
            "factor_artifact_id, factor_as_of, volume, amount, selected_source, "
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
            "raw_close, adjustment_factor, factor_applicability, "
            "corporate_action_factor, applied_adjustment_multiplier, "
            "adjustment_reference_date, reference_factor, factor_source, "
            "factor_artifact_id, factor_as_of, volume, amount, selected_source, "
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
            "raw_close, adjustment_factor, factor_applicability, "
            "corporate_action_factor, applied_adjustment_multiplier, "
            "adjustment_reference_date, reference_factor, factor_source, "
            "factor_artifact_id, factor_as_of, volume, amount, selected_source, "
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
