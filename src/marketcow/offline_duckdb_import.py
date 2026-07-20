"""Offline-only, read-only validation and extraction of legacy DuckDB copies.

This module is intentionally excluded from every online import closure.  It does not
assemble the V2 factory or connect to PostgreSQL/ClickHouse; BG-013 consumes its
bounded extraction interface later.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import stat
import sys
import time
from typing import Any, Iterator, Sequence

import duckdb


SCHEMA_VERSION = "marketcow.offline-duckdb.v1"
SUPPORTED_MIGRATIONS = (2, 3, 4)
SUPPORTED_DUCKDB_MAJOR = 1
SUPPORTED_SCHEMA_SHA256 = "32acc64a6afe6fa2d25d5c9a0e0d5b54f8f07a5b870f6fc65b2323f1d9a13d12"
SOURCE_LABELS = frozenset({"development-copy", "test-fixture"})
PRODUCTION_TOKENS = frozenset({"prod", "production", "live"})
ALLOWED_TABLES = (
    "baostock_snapshot",
    "economic_calendar_event",
    "economic_indicator_latest",
    "earnings_calendar_event",
    "financial_statement_rows",
    "fundamental_snapshot",
    "fundamental_snapshot_history",
    "funnel_metrics",
    "ingestion_runs",
    "market_price_bar",
    "market_quote_latest",
    "market_quote_observation",
    "provider_health",
    "raw_artifact_manifest",
    "tdx_financial_snapshot",
    "tdx_financial_snapshot_history",
    "tushare_data_row",
    "tushare_request",
    "validation_result",
)
_SAFE_TABLE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


class OfflineDuckDBError(RuntimeError):
    """Bounded, non-sensitive importer error with a stable machine code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code

    def document(self) -> dict[str, str]:
        return {"code": self.code, "message": str(self)}


@dataclass(frozen=True)
class ImportLimits:
    max_file_bytes: int = 2 * 1024 * 1024 * 1024
    max_rows: int = 100_000
    batch_rows: int = 1_000
    memory_mb: int = 256
    timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        if not 1 <= self.max_file_bytes <= 64 * 1024**3:
            raise ValueError("max_file_bytes outside supported bound")
        if not 1 <= self.max_rows <= 10_000_000:
            raise ValueError("max_rows outside supported bound")
        if not 1 <= self.batch_rows <= min(self.max_rows, 10_000):
            raise ValueError("batch_rows outside supported bound")
        if not 16 <= self.memory_mb <= 4096:
            raise ValueError("memory_mb outside supported bound")
        if not 0.1 <= self.timeout_seconds <= 3600:
            raise ValueError("timeout_seconds outside supported bound")


def _has_symlink(path: Path) -> bool:
    candidate = Path(path.anchor)
    for part in path.parts[1:]:
        candidate /= part
        try:
            if stat.S_ISLNK(candidate.lstat().st_mode):
                return True
        except FileNotFoundError:
            return False
    return False


def _safe_source(allowed_root: Path, source: Path, source_label: str) -> tuple[Path, Path]:
    if source_label not in SOURCE_LABELS:
        raise OfflineDuckDBError("source_label_rejected", "source must be an explicit isolated copy")
    root = Path(allowed_root)
    candidate = Path(source)
    if not root.is_absolute() or not candidate.is_absolute():
        raise OfflineDuckDBError("path_rejected", "allowed root and source must be absolute")
    if _has_symlink(root) or _has_symlink(candidate):
        raise OfflineDuckDBError("symlink_rejected", "symbolic links are not allowed")
    try:
        resolved_root = root.resolve(strict=True)
        resolved = candidate.resolve(strict=True)
    except (FileNotFoundError, OSError):
        raise OfflineDuckDBError("path_rejected", "source or allowed root is unavailable") from None
    if not resolved_root.is_dir() or not resolved.is_file() or resolved_root not in resolved.parents:
        raise OfflineDuckDBError("containment_rejected", "source must be a regular file below the allowed root")
    tokens = {part.lower() for part in resolved.parts}
    if tokens & PRODUCTION_TOKENS:
        raise OfflineDuckDBError("production_rejected", "production-identified paths are forbidden")
    return resolved_root, resolved


def _file_sha256(path: Path, max_bytes: int) -> tuple[str, int]:
    size = path.stat().st_size
    if size <= 0 or size > max_bytes:
        raise OfflineDuckDBError("file_size_rejected", "source size is outside the configured bound")
    digest = hashlib.sha256()
    consumed = 0
    try:
        with path.open("rb") as source:
            while chunk := source.read(1024 * 1024):
                consumed += len(chunk)
                if consumed > max_bytes:
                    raise OfflineDuckDBError("file_size_rejected", "source exceeds the configured bound")
                digest.update(chunk)
    except OfflineDuckDBError:
        raise
    except OSError:
        raise OfflineDuckDBError("source_read_failed", "source copy could not be read") from None
    return digest.hexdigest(), consumed


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (datetime, date, Decimal)):
        return str(value)
    if isinstance(value, bytes):
        return {"bytes_sha256": hashlib.sha256(value).hexdigest(), "byte_size": len(value)}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    return str(value)


class OfflineDuckDBImporter:
    """Validate and extract one explicitly supplied immutable legacy copy."""

    def __init__(
        self,
        *,
        allowed_root: Path,
        source: Path,
        source_label: str,
        limits: ImportLimits | None = None,
        clock=time.monotonic,
    ) -> None:
        self.allowed_root, self.source = _safe_source(allowed_root, source, source_label)
        self.source_label = source_label
        self.limits = limits or ImportLimits()
        self.clock = clock

    def _connect(self):
        try:
            connection = duckdb.connect(str(self.source), read_only=True)
            connection.execute("SET enable_external_access = false")
            connection.execute("SET autoinstall_known_extensions = false")
            connection.execute("SET autoload_known_extensions = false")
            connection.execute("SET threads = 1")
            connection.execute(f"SET memory_limit = '{self.limits.memory_mb}MB'")
            return connection
        except Exception:
            raise OfflineDuckDBError("duckdb_open_failed", "source is not a readable supported DuckDB copy") from None

    def _deadline(self) -> float:
        return self.clock() + self.limits.timeout_seconds

    def _check_deadline(self, deadline: float) -> None:
        if self.clock() > deadline:
            raise OfflineDuckDBError("timeout", "offline operation exceeded its configured time bound")

    def inspect(self) -> dict[str, Any]:
        before = self.source.stat()
        digest, byte_size = _file_sha256(self.source, self.limits.max_file_bytes)
        deadline = self._deadline()
        try:
            with self._connect() as connection:
                self._check_deadline(deadline)
                version_text = str(connection.execute("SELECT version() ").fetchone()[0])
                match = re.search(r"v?(\d+)\.", version_text)
                if not match or int(match.group(1)) != SUPPORTED_DUCKDB_MAJOR:
                    raise OfflineDuckDBError("duckdb_version_rejected", "DuckDB engine version is unsupported")
                tables = tuple(sorted(row[0] for row in connection.execute("SHOW TABLES").fetchall()))
                missing = sorted(set(ALLOWED_TABLES) - set(tables))
                unexpected = sorted(set(tables) - set(ALLOWED_TABLES) - {"schema_migrations"})
                if missing or unexpected or "schema_migrations" not in tables:
                    raise OfflineDuckDBError("schema_rejected", "source table inventory is incompatible")
                object_types = connection.execute(
                    "SELECT table_name, table_type FROM information_schema.tables "
                    "WHERE table_schema = 'main' ORDER BY table_name"
                ).fetchall()
                if tuple(name for name, _ in object_types) != tables or any(
                    kind != "BASE TABLE" for _, kind in object_types
                ):
                    raise OfflineDuckDBError("schema_rejected", "source objects must be physical base tables")
                schema_rows = connection.execute(
                    "SELECT table_name,column_name,data_type,is_nullable,ordinal_position "
                    "FROM information_schema.columns WHERE table_schema='main' "
                    "ORDER BY table_name,ordinal_position"
                ).fetchall()
                schema_sha256 = hashlib.sha256(
                    json.dumps([list(row) for row in schema_rows], separators=(",", ":")).encode()
                ).hexdigest()
                if schema_sha256 != SUPPORTED_SCHEMA_SHA256:
                    raise OfflineDuckDBError("schema_rejected", "source column schema is incompatible")
                migrations = tuple(
                    int(row[0])
                    for row in connection.execute(
                        "SELECT version FROM schema_migrations ORDER BY version"
                    ).fetchall()
                )
                if migrations != SUPPORTED_MIGRATIONS:
                    raise OfflineDuckDBError("migration_rejected", "source migration version is unsupported")
                counts: dict[str, int] = {}
                total = 0
                for table in ALLOWED_TABLES:
                    self._check_deadline(deadline)
                    count = int(connection.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0])
                    total += count
                    if count > self.limits.max_rows or total > self.limits.max_rows:
                        raise OfflineDuckDBError("row_limit_rejected", "source row count exceeds the configured bound")
                    counts[table] = count
        except OfflineDuckDBError:
            raise
        except Exception:
            raise OfflineDuckDBError("schema_read_failed", "source schema could not be validated") from None
        after = self.source.stat()
        if (before.st_size, before.st_mtime_ns, before.st_ino) != (after.st_size, after.st_mtime_ns, after.st_ino):
            raise OfflineDuckDBError("source_changed", "source changed during validation")
        binding = {
            "file_sha256": digest,
            "byte_size": byte_size,
            "migrations": list(migrations),
            "schema_sha256": schema_sha256,
            "tables": counts,
        }
        fingerprint = hashlib.sha256(
            json.dumps(binding, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return {
            "schema": SCHEMA_VERSION,
            "status": "validated",
            "source": f"duckdb-copy://{fingerprint[:16]}",
            "source_label": self.source_label,
            "source_fingerprint": fingerprint,
            "file_sha256": digest,
            "byte_size": byte_size,
            "duckdb_major": SUPPORTED_DUCKDB_MAJOR,
            "schema_sha256": schema_sha256,
            "migrations": list(migrations),
            "tables": counts,
            "limits": {
                "max_file_bytes": self.limits.max_file_bytes,
                "max_rows": self.limits.max_rows,
                "batch_rows": self.limits.batch_rows,
                "memory_mb": self.limits.memory_mb,
                "timeout_seconds": self.limits.timeout_seconds,
            },
        }

    def batches(self, table: str) -> Iterator[list[dict[str, Any]]]:
        if table not in ALLOWED_TABLES or not _SAFE_TABLE.fullmatch(table):
            raise OfflineDuckDBError("table_rejected", "requested table is not extractable")
        manifest = self.inspect()
        expected = manifest["source_fingerprint"]
        deadline = self._deadline()
        emitted = 0
        try:
            with self._connect() as connection:
                columns = [row[1] for row in connection.execute(f"PRAGMA table_info('{table}')").fetchall()]
                cursor = connection.execute(f'SELECT * FROM "{table}" LIMIT {self.limits.max_rows + 1}')
                while True:
                    self._check_deadline(deadline)
                    rows = cursor.fetchmany(self.limits.batch_rows)
                    if not rows:
                        break
                    emitted += len(rows)
                    if emitted > self.limits.max_rows:
                        raise OfflineDuckDBError("row_limit_rejected", "extraction exceeds the configured row bound")
                    yield [
                        {column: _json_value(value) for column, value in zip(columns, row)}
                        for row in rows
                    ]
        except OfflineDuckDBError:
            raise
        except Exception:
            raise OfflineDuckDBError("extract_failed", "bounded extraction failed") from None
        if self.inspect()["source_fingerprint"] != expected:
            raise OfflineDuckDBError("source_changed", "source changed during extraction")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="marketcow-offline-duckdb")
    parser.add_argument("command", choices=("validate", "extract"))
    parser.add_argument("--allowed-root", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--source-label", required=True, choices=sorted(SOURCE_LABELS))
    parser.add_argument("--table", choices=ALLOWED_TABLES)
    parser.add_argument("--max-file-bytes", type=int, default=ImportLimits.max_file_bytes)
    parser.add_argument("--max-rows", type=int, default=ImportLimits.max_rows)
    parser.add_argument("--batch-rows", type=int, default=ImportLimits.batch_rows)
    parser.add_argument("--memory-mb", type=int, default=ImportLimits.memory_mb)
    parser.add_argument("--timeout-seconds", type=float, default=ImportLimits.timeout_seconds)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
        limits = ImportLimits(
            max_file_bytes=args.max_file_bytes,
            max_rows=args.max_rows,
            batch_rows=args.batch_rows,
            memory_mb=args.memory_mb,
            timeout_seconds=args.timeout_seconds,
        )
        importer = OfflineDuckDBImporter(
            allowed_root=Path(args.allowed_root),
            source=Path(args.source),
            source_label=args.source_label,
            limits=limits,
        )
        if args.command == "validate":
            result: dict[str, Any] = importer.inspect()
        else:
            if not args.table:
                raise OfflineDuckDBError("table_required", "extract requires an explicit table")
            batches = list(importer.batches(args.table))
            result = {
                "schema": SCHEMA_VERSION,
                "status": "extracted",
                "table": args.table,
                "row_count": sum(map(len, batches)),
                "batches": batches,
            }
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 0
    except (OfflineDuckDBError, ValueError) as error:
        payload = error.document() if isinstance(error, OfflineDuckDBError) else {
            "code": "invalid_limits", "message": "resource limits are invalid"
        }
        print(json.dumps({"schema": SCHEMA_VERSION, "status": "rejected", "error": payload}, sort_keys=True), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
