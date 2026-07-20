from __future__ import annotations

import hashlib
import json
import os
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Sequence

import duckdb


MANIFEST_VERSION = "storage-v2.parquet-manifest.v1"
SCHEMA_VERSION = 1
MAX_READ_ROWS = 100000
DATA_COLUMNS = (
    "symbol", "interval", "adjustment", "timestamp", "bar_at", "open", "high",
    "low", "close", "raw_close", "adjustment_factor", "volume", "amount", "source",
    "ingested_at", "source_url", "observed_at", "raw_response_locator", "raw_path",
    "raw_artifact_id", "payload_json", "source_sequence", "content_rank",
)
BUSINESS_KEY = ("symbol", "interval", "adjustment", "timestamp", "source")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_hash(value: Any) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _month_bounds(year: int, month: int) -> tuple[int, int]:
    if not 1970 <= year <= 9998 or not 1 <= month <= 12:
        raise ValueError("archive year/month is out of bounds")
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    end = datetime(year + (month == 12), 1 if month == 12 else month + 1, 1,
                   tzinfo=timezone.utc)
    return int(start.timestamp()), int(end.timestamp())


def _utc_iso(value: Any) -> str:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("archive ingested_at must include timezone")
    return parsed.astimezone(timezone.utc).isoformat()


class ParquetColdArchive:
    """Development-only atomic and verifiable market-bar Parquet archive."""

    def __init__(
        self, database_path: Path, archive_root: Path, storage_root: Path,
        profile: str = "development", fault_hook: Callable[[str], None] | None = None,
    ) -> None:
        if profile != "development":
            raise ValueError("cold archive is development-only")
        self.database_path = Path(database_path)
        self.storage_root = Path(storage_root).resolve()
        self.archive_root = Path(archive_root).resolve()
        try:
            self.archive_root.relative_to(self.storage_root)
        except ValueError as error:
            raise ValueError("archive root must remain inside development storage root") from error
        self.archive_root.mkdir(parents=True, exist_ok=True)
        self.staging_root = self.archive_root / ".staging"
        self.staging_root.mkdir(exist_ok=True)
        self.fault_hook = fault_hook

    @staticmethod
    def _market_expression() -> str:
        return ("CASE WHEN symbol LIKE '%.SH' OR symbol LIKE '%.SZ' OR "
                "symbol LIKE '%.BJ' THEN 'CN' WHEN symbol LIKE '%.HK' "
                "THEN 'HK' ELSE 'US' END")

    def _partition(self, market: str, interval: str, source: str,
                   year: int, month: int) -> Path:
        if market not in {"CN", "HK", "US"}:
            raise ValueError("market must be CN, HK, or US")
        for name, value in (("interval", interval), ("source", source)):
            if not value or len(value) > 64 or any(char not in "-_ .abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789" for char in value):
                raise ValueError(f"archive {name} is invalid")
        return (self.archive_root / f"market={market}" / f"interval={interval}" /
                f"source={source}" / f"year={year:04d}" / f"month={month:02d}")

    def _select_sql(self) -> str:
        columns = ", ".join(DATA_COLUMNS)
        return (
            f"SELECT {columns} FROM market_price_bar WHERE {self._market_expression()}=? "
            "AND interval=? AND source=? AND timestamp>=? AND timestamp<? "
            "ORDER BY symbol, interval, adjustment, timestamp, source"
        )

    def export_partition(self, market: str, interval: str, source: str,
                         year: int, month: int) -> Dict[str, Any]:
        start, end = _month_bounds(year, month)
        partition = self._partition(market, interval, source, year, month)
        params = [market, interval, source, start, end]
        with duckdb.connect(str(self.database_path), read_only=True) as source_db:
            rows = source_db.execute(self._select_sql(), params).fetchall()
        if not rows:
            raise ValueError("archive partition has no rows")
        logical_rows = [dict(zip(DATA_COLUMNS, row)) for row in rows]
        logical_checksum = _json_hash(logical_rows)
        key_checksum = _json_hash([
            [row[column] for column in BUSINESS_KEY] for row in logical_rows
        ])
        artifact_id = logical_checksum[:24]
        final = partition / artifact_id
        if final.exists():
            return self.verify(final)
        staging = self.staging_root / f"{artifact_id}-{uuid.uuid4().hex}"
        staging.mkdir()
        parquet = staging / "data.parquet"
        manifest_path = staging / "manifest.json"
        try:
            with duckdb.connect(str(self.database_path), read_only=True) as source_db:
                source_db.execute("CREATE TEMP TABLE archive_export AS " + self._select_sql(), params)
                target = str(parquet).replace("'", "''")
                source_db.execute(
                    f"COPY archive_export TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD)"
                )
                schema = [
                    {"name": row[0], "type": row[1]}
                    for row in source_db.execute("DESCRIBE archive_export").fetchall()
                ]
            with parquet.open("rb") as handle:
                os.fsync(handle.fileno())
            if self.fault_hook:
                self.fault_hook("after_parquet")
            parquet_checksum = _sha256(parquet)
            ingested = sorted(_utc_iso(row["ingested_at"]) for row in logical_rows)
            manifest = {
                "manifest_version": MANIFEST_VERSION,
                "schema_version": SCHEMA_VERSION,
                "artifact_id": artifact_id,
                "dataset": "market_price_bar_raw",
                "partition": {"market": market, "interval": interval, "source": source,
                              "year": year, "month": month},
                "business_key": list(BUSINESS_KEY),
                "row_count": len(logical_rows),
                "logical_checksum": logical_checksum,
                "business_key_checksum": key_checksum,
                "parquet_sha256": parquet_checksum,
                "parquet_bytes": parquet.stat().st_size,
                "logical_json_bytes": len(json.dumps(logical_rows, default=str).encode("utf-8")),
                "schema": schema,
                "watermark": {
                    "min_timestamp": min(row["timestamp"] for row in logical_rows),
                    "max_timestamp": max(row["timestamp"] for row in logical_rows),
                    "min_ingested_at": ingested[0], "max_ingested_at": ingested[-1],
                },
            }
            manifest["manifest_payload_sha256"] = _json_hash(manifest)
            encoded = json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2).encode()
            with manifest_path.open("wb") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            if self.fault_hook:
                self.fault_hook("after_manifest")
            partition.mkdir(parents=True, exist_ok=True)
            try:
                os.replace(staging, final)
            except OSError:
                if final.exists():
                    shutil.rmtree(staging, ignore_errors=True)
                    return self.verify(final)
                raise
            directory_fd = os.open(partition, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            if self.fault_hook:
                self.fault_hook("after_publish")
            return self.verify(final)
        finally:
            if staging.exists():
                shutil.rmtree(staging, ignore_errors=True)

    def verify(self, artifact: Path) -> Dict[str, Any]:
        artifact = Path(artifact).resolve()
        try:
            artifact.relative_to(self.archive_root)
        except ValueError as error:
            raise ValueError("archive artifact escapes archive root") from error
        manifest_path, parquet = artifact / "manifest.json", artifact / "data.parquet"
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ValueError("archive manifest is missing or corrupt") from error
        if manifest.get("manifest_version") != MANIFEST_VERSION:
            raise ValueError("unsupported archive manifest version")
        unsigned_manifest = {
            key: value for key, value in manifest.items()
            if key != "manifest_payload_sha256"
        }
        if _json_hash(unsigned_manifest) != manifest.get("manifest_payload_sha256"):
            raise ValueError("archive manifest checksum mismatch")
        if manifest.get("schema_version") != SCHEMA_VERSION:
            raise ValueError("unsupported archive schema version")
        if manifest.get("business_key") != list(BUSINESS_KEY):
            raise ValueError("archive business key schema mismatch")
        if not parquet.is_file() or _sha256(parquet) != manifest.get("parquet_sha256"):
            raise ValueError("archive parquet checksum mismatch")
        with duckdb.connect() as con:
            target = str(parquet).replace("'", "''")
            description = con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{target}', hive_partitioning=false)"
            ).fetchall()
            columns = [row[0] for row in description]
            rows = con.execute(
                f"SELECT * FROM read_parquet('{target}', hive_partitioning=false) ORDER BY "
                "symbol, interval, adjustment, timestamp, source"
            ).fetchall()
        actual_schema = [{"name": row[0], "type": row[1]} for row in description]
        if actual_schema != manifest.get("schema"):
            raise ValueError("archive parquet schema metadata mismatch")
        if columns != list(DATA_COLUMNS) or len(rows) != manifest.get("row_count"):
            raise ValueError(
                f"archive parquet schema or row count mismatch: {columns!r}/{len(rows)}"
            )
        logical_rows = [dict(zip(DATA_COLUMNS, row)) for row in rows]
        if _json_hash(logical_rows) != manifest.get("logical_checksum"):
            raise ValueError("archive logical checksum mismatch")
        keys = [[row[column] for column in BUSINESS_KEY] for row in logical_rows]
        if _json_hash(keys) != manifest.get("business_key_checksum"):
            raise ValueError("archive business key checksum mismatch")
        return {**manifest, "artifact_path": str(artifact), "status": "verified"}

    def query(self, artifact: Path, where: str = "", params: Sequence[Any] = (),
              limit: int = 5000) -> List[Dict[str, Any]]:
        if not 1 <= limit <= MAX_READ_ROWS:
            raise ValueError(f"archive query limit must be between 1 and {MAX_READ_ROWS}")
        self.verify(artifact)
        parquet = Path(artifact).resolve() / "data.parquet"
        target = str(parquet).replace("'", "''")
        allowed = {"", "symbol=?", "adjustment=?", "symbol=? AND adjustment=?"}
        if where not in allowed:
            raise ValueError("archive query predicate is not allowed")
        clause = f" WHERE {where}" if where else ""
        with duckdb.connect() as con:
            cursor = con.execute(
                f"SELECT * FROM read_parquet('{target}', hive_partitioning=false){clause} "
                "ORDER BY "
                "symbol, interval, adjustment, timestamp, source LIMIT ?",
                [*params, limit],
            )
            columns = [item[0] for item in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def read_for_backfill(self, artifact: Path) -> List[Dict[str, Any]]:
        manifest = self.verify(artifact)
        count = int(manifest["row_count"])
        if count > MAX_READ_ROWS:
            raise ValueError("archive exceeds bounded backfill read limit")
        return self.query(artifact, limit=max(1, count))
