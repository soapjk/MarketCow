"""Offline-only, read-only validation and extraction of legacy DuckDB copies.

This module is intentionally excluded from every online import closure.  It does not
assemble the V2 factory or connect to PostgreSQL/ClickHouse; BG-013 consumes its
bounded extraction interface later.
"""

from __future__ import annotations

import argparse
import io
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
import multiprocessing
from pathlib import Path
import re
import stat
import sys
import time
from typing import Any, BinaryIO, Iterator, Sequence

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
    max_value_bytes: int = 256 * 1024
    max_row_bytes: int = 512 * 1024
    max_batch_bytes: int = 4 * 1024 * 1024
    max_output_bytes: int = 256 * 1024 * 1024

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
        if not 1 <= self.max_value_bytes <= 16 * 1024**2:
            raise ValueError("max_value_bytes outside supported bound")
        if not self.max_value_bytes <= self.max_row_bytes <= 32 * 1024**2:
            raise ValueError("max_row_bytes outside supported bound")
        if not self.max_row_bytes <= self.max_batch_bytes <= 64 * 1024**2:
            raise ValueError("max_batch_bytes outside supported bound")
        if not self.max_batch_bytes <= self.max_output_bytes <= 2 * 1024**3:
            raise ValueError("max_output_bytes outside supported bound")


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

    def _inspect_unisolated(self) -> dict[str, Any]:
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
                "max_value_bytes": self.limits.max_value_bytes,
                "max_row_bytes": self.limits.max_row_bytes,
                "max_batch_bytes": self.limits.max_batch_bytes,
                "max_output_bytes": self.limits.max_output_bytes,
            },
        }

    def _validate_value_widths(self, connection, table: str, columns: list[str]) -> None:
        widths: list[str] = []
        for column in columns:
            escaped = column.replace('"', '""')
            expression = f'octet_length(encode(CAST("{escaped}" AS VARCHAR)))'
            widths.append(f"COALESCE({expression},0)")
            maximum = connection.execute(
                f'SELECT max({expression}) FROM "{table}"'
            ).fetchone()[0]
            if maximum is not None and int(maximum) > self.limits.max_value_bytes:
                raise OfflineDuckDBError("value_limit_rejected", "a source value exceeds the configured byte bound")
        maximum_row = connection.execute(
            f'SELECT max({" + ".join(widths)}) FROM "{table}"'
        ).fetchone()[0]
        if maximum_row is not None and int(maximum_row) > self.limits.max_row_bytes:
            raise OfflineDuckDBError("row_bytes_rejected", "a source row exceeds the configured byte bound")

    def _bounded_row(self, columns: list[str], row: tuple[Any, ...]) -> tuple[dict[str, Any], int]:
        document: dict[str, Any] = {}
        for column, value in zip(columns, row):
            normalized = _json_value(value)
            value_size = len(json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode())
            if value_size > self.limits.max_value_bytes:
                raise OfflineDuckDBError("value_limit_rejected", "a source value exceeds the configured byte bound")
            document[column] = normalized
        row_size = len(json.dumps(document, sort_keys=True, separators=(",", ":")).encode())
        if row_size > self.limits.max_row_bytes:
            raise OfflineDuckDBError("row_bytes_rejected", "a source row exceeds the configured byte bound")
        return document, row_size

    def _batches_unisolated(self, table: str) -> Iterator[list[dict[str, Any]]]:
        if table not in ALLOWED_TABLES or not _SAFE_TABLE.fullmatch(table):
            raise OfflineDuckDBError("table_rejected", "requested table is not extractable")
        manifest = self._inspect_unisolated()
        expected = manifest["source_fingerprint"]
        deadline = self._deadline()
        emitted = 0
        try:
            with self._connect() as connection:
                columns = [row[1] for row in connection.execute(f"PRAGMA table_info('{table}')").fetchall()]
                self._validate_value_widths(connection, table, columns)
                cursor = connection.execute(f'SELECT * FROM "{table}" LIMIT {self.limits.max_rows + 1}')
                fetch_rows = min(
                    self.limits.batch_rows,
                    max(1, self.limits.max_batch_bytes // self.limits.max_row_bytes),
                )
                while True:
                    self._check_deadline(deadline)
                    rows = cursor.fetchmany(fetch_rows)
                    if not rows:
                        break
                    emitted += len(rows)
                    if emitted > self.limits.max_rows:
                        raise OfflineDuckDBError("row_limit_rejected", "extraction exceeds the configured row bound")
                    batch: list[dict[str, Any]] = []
                    batch_bytes = 0
                    for row in rows:
                        document, row_bytes = self._bounded_row(columns, row)
                        batch_bytes += row_bytes
                        if batch_bytes > self.limits.max_batch_bytes:
                            raise OfflineDuckDBError(
                                "batch_bytes_rejected", "a source batch exceeds the configured byte bound"
                            )
                        batch.append(document)
                    yield batch
        except OfflineDuckDBError:
            raise
        except Exception:
            raise OfflineDuckDBError("extract_failed", "bounded extraction failed") from None
        if self._inspect_unisolated()["source_fingerprint"] != expected:
            raise OfflineDuckDBError("source_changed", "source changed during extraction")

    @staticmethod
    def _record_bytes(record: dict[str, Any]) -> bytes:
        return json.dumps(record, sort_keys=True, separators=(",", ":")).encode() + b"\n"

    def _stream_worker(self, command: str, table: str | None, sender) -> None:
        """Child-process worker. Each send blocks until the parent drains the bounded pipe."""
        try:
            manifest = self._inspect_unisolated()
            manifest_record = self._record_bytes({"type": "manifest", **manifest})
            sent_bytes = len(manifest_record)
            if sent_bytes > self.limits.max_output_bytes:
                raise OfflineDuckDBError("output_limit_rejected", "stream exceeds the configured output byte bound")
            sender.send_bytes(manifest_record)
            data_digest = hashlib.sha256()
            rows = 0
            batches = 0
            if command == "extract":
                if table is None:
                    raise OfflineDuckDBError("table_required", "extract requires an explicit table")
                for batch in self._batches_unisolated(table):
                    record = self._record_bytes(
                        {"type": "batch", "table": table, "sequence": batches, "rows": batch}
                    )
                    if len(record) > self.limits.max_batch_bytes:
                        raise OfflineDuckDBError(
                            "batch_bytes_rejected", "serialized batch exceeds the configured byte bound"
                        )
                    sent_bytes += len(record)
                    if sent_bytes > self.limits.max_output_bytes:
                        raise OfflineDuckDBError(
                            "output_limit_rejected", "stream exceeds the configured output byte bound"
                        )
                    data_digest.update(record)
                    sender.send_bytes(record)
                    rows += len(batch)
                    batches += 1
            terminal = {
                "type": "complete",
                "schema": SCHEMA_VERSION,
                "status": "complete",
                "command": command,
                "table": table,
                "row_count": rows,
                "batch_count": batches,
                "data_sha256": data_digest.hexdigest(),
                "payload_bytes": sent_bytes,
                "source_fingerprint": manifest["source_fingerprint"],
            }
            terminal_record = self._record_bytes(terminal)
            if sent_bytes + len(terminal_record) > self.limits.max_output_bytes:
                raise OfflineDuckDBError("output_limit_rejected", "stream exceeds the configured output byte bound")
            sender.send_bytes(terminal_record)
        except OfflineDuckDBError as error:
            sender.send_bytes(self._record_bytes({
                "type": "failed", "schema": SCHEMA_VERSION, "status": "failed", "error": error.document()
            }))
        except BaseException:
            sender.send_bytes(self._record_bytes({
                "type": "failed", "schema": SCHEMA_VERSION, "status": "failed",
                "error": {"code": "worker_failed", "message": "isolated offline worker failed"},
            }))
        finally:
            sender.close()

    @staticmethod
    def _terminate(process) -> None:
        if process.is_alive():
            process.terminate()
        process.join(timeout=1)
        if process.is_alive():
            process.kill()
            process.join(timeout=1)

    def stream(self, command: str, table: str | None, output: BinaryIO) -> int:
        """Run DuckDB work in a killable process and forward bounded NDJSON records."""
        if command not in {"validate", "extract"}:
            raise OfflineDuckDBError("command_rejected", "offline command is unsupported")
        if command == "extract" and (table not in ALLOWED_TABLES or not _SAFE_TABLE.fullmatch(table or "")):
            raise OfflineDuckDBError("table_rejected", "requested table is not extractable")
        context = multiprocessing.get_context("fork")
        receiver, sender = context.Pipe(duplex=False)
        process = context.Process(target=self._stream_worker, args=(command, table, sender))
        process.start()
        sender.close()
        deadline = time.monotonic() + self.limits.timeout_seconds
        terminal = False
        status = 2
        def write(record: bytes) -> None:
            try:
                output.write(record)
            except TypeError:
                output.write(record.decode())
            output.flush()
        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not receiver.poll(remaining):
                    self._terminate(process)
                    write(self._record_bytes({
                        "type": "failed", "schema": SCHEMA_VERSION, "status": "failed",
                        "error": {"code": "timeout", "message": "offline operation exceeded its hard time bound"},
                    }))
                    return 2
                try:
                    record = receiver.recv_bytes(self.limits.max_batch_bytes + self.limits.max_row_bytes)
                except (EOFError, OSError):
                    break
                write(record)
                try:
                    kind = json.loads(record)["type"]
                except (KeyError, json.JSONDecodeError, UnicodeDecodeError):
                    kind = "invalid"
                if kind in {"complete", "failed"}:
                    terminal = True
                    status = 0 if kind == "complete" else 2
                    break
        finally:
            receiver.close()
            self._terminate(process)
        if not terminal:
            write(self._record_bytes({
                "type": "failed", "schema": SCHEMA_VERSION, "status": "failed",
                "error": {"code": "incomplete_stream", "message": "offline stream ended without a terminal record"},
            }))
            return 2
        return status

    def inspect(self) -> dict[str, Any]:
        """Return a validated manifest through the same hard-timeout worker boundary."""
        output = io.BytesIO()
        status = self.stream("validate", None, output)
        records = [json.loads(line) for line in output.getvalue().splitlines()]
        if status != 0:
            error = records[-1].get("error", {}) if records else {}
            raise OfflineDuckDBError(
                str(error.get("code", "worker_failed")),
                str(error.get("message", "isolated offline worker failed")),
            )
        if len(records) != 2 or records[0].get("type") != "manifest" or records[1].get("type") != "complete":
            raise OfflineDuckDBError("incomplete_stream", "manifest stream is incomplete")
        return {key: value for key, value in records[0].items() if key != "type"}


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
    parser.add_argument("--max-value-bytes", type=int, default=ImportLimits.max_value_bytes)
    parser.add_argument("--max-row-bytes", type=int, default=ImportLimits.max_row_bytes)
    parser.add_argument("--max-batch-bytes", type=int, default=ImportLimits.max_batch_bytes)
    parser.add_argument("--max-output-bytes", type=int, default=ImportLimits.max_output_bytes)
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
            max_value_bytes=args.max_value_bytes,
            max_row_bytes=args.max_row_bytes,
            max_batch_bytes=args.max_batch_bytes,
            max_output_bytes=args.max_output_bytes,
        )
        importer = OfflineDuckDBImporter(
            allowed_root=Path(args.allowed_root),
            source=Path(args.source),
            source_label=args.source_label,
            limits=limits,
        )
        output = getattr(sys.stdout, "buffer", sys.stdout)
        return importer.stream(args.command, args.table, output)
    except (OfflineDuckDBError, ValueError) as error:
        payload = error.document() if isinstance(error, OfflineDuckDBError) else {
            "code": "invalid_limits", "message": "resource limits are invalid"
        }
        print(json.dumps({"schema": SCHEMA_VERSION, "status": "rejected", "error": payload}, sort_keys=True), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
