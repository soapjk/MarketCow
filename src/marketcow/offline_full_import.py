"""BG-013 synthetic, checkpointed full import from BG-012 streams.

The module is offline-only.  It consumes a verified legacy-copy stream and writes
only explicitly supplied disposable PostgreSQL/ClickHouse targets.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import tempfile
from typing import Any, Callable, Mapping, Sequence

from psycopg import sql
from psycopg.types.json import Jsonb

from .clickhouse_writer import normalize_bar
from .clickhouse_shadow import _market
from .local_backfill import Domain, POSTGRES_DOMAINS
from .offline_duckdb_import import OfflineDuckDBImporter, _has_symlink
from .postgres_migrations import POSTGRES_TRANSACTION_DOMAINS
from .postgres_migrations import POSTGRES_MIGRATIONS
from .clickhouse_repositories import CLICKHOUSE_MIGRATIONS


FULL_IMPORT_VERSION = "storage-v2.offline-full-import.v1"
MAX_ERRORS = 50


@dataclass(frozen=True)
class FullImportTargets:
    root: Path
    allowed_root: Path
    postgres: Any
    clickhouse: Any
    writer: Any
    canonical_builder: Any
    profile: str = "test"
    artifact_source_root: Path | None = None


def _canonical(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _canonical(item) for key, item in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat(timespec="milliseconds")
    if isinstance(value, float):
        return format(value, ".15g")
    if isinstance(value, str) and "T" in value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is not None:
                return parsed.astimezone(timezone.utc).isoformat(timespec="milliseconds")
        except ValueError:
            pass
    return value


def _digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(_canonical(value), sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _atomic(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=".full-import-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(json.dumps(value, sort_keys=True, indent=2).encode())
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


class _StageSink:
    def __init__(self, destination: Path, expected_table: str, expected_fingerprint: str) -> None:
        self.destination = destination
        self.temporary = destination.with_suffix(".partial")
        self.expected_table = expected_table
        self.expected_fingerprint = expected_fingerprint
        self.output = self.temporary.open("wb")
        self.digest = hashlib.sha256()
        self.sequence = 0
        self.rows = 0
        self.complete = False
        self.manifest = False
        self.error = ""

    def write(self, payload: bytes | str) -> None:
        if isinstance(payload, str):
            payload = payload.encode()
        record = json.loads(payload)
        kind = record.get("type")
        if kind == "manifest":
            if self.manifest or self.sequence or self.complete or record.get("source_fingerprint") != self.expected_fingerprint:
                raise ValueError("stream source fingerprint mismatch")
            self.manifest = True
        elif kind == "batch":
            if not self.manifest or self.complete or record.get("table") != self.expected_table or record.get("sequence") != self.sequence:
                raise ValueError("stream batch identity mismatch")
            self.digest.update(payload)
            self.sequence += 1
            self.rows += len(record.get("rows", []))
        elif kind == "complete":
            if (
                not self.manifest
                or self.complete
                or record.get("source_fingerprint") != self.expected_fingerprint
                or record.get("table") != self.expected_table
                or record.get("row_count") != self.rows
                or record.get("batch_count") != self.sequence
                or record.get("data_sha256") != self.digest.hexdigest()
            ):
                raise ValueError("stream terminal verification failed")
            self.complete = True
        elif kind == "failed":
            self.error = str(record.get("error", {}).get("code", "stream_failed"))[:100]
        else:
            raise ValueError("unknown stream record")
        self.output.write(payload)

    def flush(self) -> None:
        self.output.flush()

    def publish(self, status: int) -> None:
        try:
            self.output.flush()
            os.fsync(self.output.fileno())
        finally:
            self.output.close()
        if status != 0 or not self.complete or self.error:
            self.temporary.unlink(missing_ok=True)
            raise ValueError("source stream did not complete")
        os.replace(self.temporary, self.destination)
        directory = os.open(self.destination.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)


class OfflineFullImport:
    """Bounded full-only import; incremental catch-up is deliberately absent."""

    def __init__(
        self,
        source: OfflineDuckDBImporter,
        targets: FullImportTargets,
        fault_hook: Callable[[str, str], None] | None = None,
    ) -> None:
        if targets.profile not in {"production", "development", "test"}:
            raise ValueError("full import profile is invalid")
        root = Path(targets.root)
        allowed = Path(targets.allowed_root)
        if root.is_symlink() or allowed.is_symlink():
            raise ValueError("full import roots must not be symlinks")
        self.root, self.allowed = root.resolve(), allowed.resolve()
        try:
            self.root.relative_to(self.allowed)
        except ValueError as error:
            raise ValueError("full import root escapes allowed root") from error
        identifiers = (self.root.name, str(targets.postgres.schema), str(targets.clickhouse.database))
        expected_suffix = targets.profile
        if not all(item.lower().endswith(expected_suffix) for item in identifiers):
            raise ValueError(
                "full import targets must match the explicit profile; "
                "development/test-only unless production is explicit"
            )
        self.artifact_source_root = None
        if targets.artifact_source_root is not None:
            artifact_root = Path(targets.artifact_source_root)
            if artifact_root.is_symlink() or not artifact_root.is_dir():
                raise ValueError("artifact source root is invalid")
            self.artifact_source_root = artifact_root.resolve()
        self.source = source
        self.targets = targets
        self.fault_hook = fault_hook
        self.state = self.root / ".offline-full-import"
        self.stage = self.state / "stage"
        self.checkpoint_path = self.state / "checkpoint.json"
        self.report_path = self.state / "report.json"

    def _target_ids(self) -> dict[str, str]:
        return {
            "postgres_schema": str(self.targets.postgres.schema),
            "clickhouse_database": str(self.targets.clickhouse.database),
        }

    @staticmethod
    def _sign(checkpoint: dict[str, Any]) -> None:
        checkpoint.pop("checksum", None)
        checkpoint["checksum"] = _digest(checkpoint)

    def _save(self, checkpoint: dict[str, Any]) -> None:
        self._sign(checkpoint)
        _atomic(self.checkpoint_path, checkpoint)

    def _load(self, fingerprint: str) -> dict[str, Any]:
        if self.checkpoint_path.exists():
            checkpoint = json.loads(self.checkpoint_path.read_text())
            unsigned = dict(checkpoint)
            checksum = unsigned.pop("checksum", None)
            if checksum != _digest(unsigned) or checkpoint.get("version") != FULL_IMPORT_VERSION:
                raise ValueError("full import checkpoint is invalid")
            if checkpoint.get("source_fingerprint") != fingerprint or checkpoint.get("targets") != self._target_ids():
                raise ValueError("full import checkpoint binding mismatch")
            return checkpoint
        binding = {"source_fingerprint": fingerprint, "targets": self._target_ids(), "version": FULL_IMPORT_VERSION}
        checkpoint = {
            **binding,
            "run_id": _digest(binding),
            "phase": "staging",
            "domains": {},
            "errors": [],
        }
        self._save(checkpoint)
        return checkpoint

    def _stage_table(self, table: str, fingerprint: str) -> Path:
        destination = self.stage / f"{table}.ndjson"
        if destination.exists():
            self._verify_stage(destination, table, fingerprint)
            return destination
        self.stage.mkdir(parents=True, exist_ok=True)
        sink = _StageSink(destination, table, fingerprint)
        try:
            status = self.source.stream("extract", table, sink)
            sink.publish(status)
        except Exception:
            if not sink.output.closed:
                sink.output.close()
            sink.temporary.unlink(missing_ok=True)
            raise
        return destination

    @staticmethod
    def _verify_stage(path: Path, table: str, fingerprint: str) -> dict[str, Any]:
        digest = hashlib.sha256()
        sequence = rows = 0
        terminal = None
        manifest = False
        with path.open("rb") as source:
            for payload in source:
                record = json.loads(payload)
                if record["type"] == "manifest":
                    if manifest or sequence or terminal or record["source_fingerprint"] != fingerprint:
                        raise ValueError("staged manifest fingerprint mismatch")
                    manifest = True
                if record["type"] == "batch":
                    if not manifest or terminal or record["table"] != table or record["sequence"] != sequence:
                        raise ValueError("staged batch sequence mismatch")
                    digest.update(payload)
                    sequence += 1
                    rows += len(record["rows"])
                elif record["type"] == "complete":
                    if terminal:
                        raise ValueError("duplicate staged terminal")
                    terminal = record
                elif record["type"] == "failed":
                    raise ValueError("staged stream contains failure")
        if not manifest or not terminal or (
            terminal.get("source_fingerprint") != fingerprint
            or terminal.get("table") != table
            or terminal.get("batch_count") != sequence
            or terminal.get("row_count") != rows
            or terminal.get("data_sha256") != digest.hexdigest()
        ):
            raise ValueError("staged stream terminal mismatch")
        return {"batches": sequence, "rows": rows, "checksum": digest.hexdigest()}

    @classmethod
    def _stage_batches(cls, path: Path, table: str, fingerprint: str):
        cls._verify_stage(path, table, fingerprint)
        with path.open("rb") as source:
            for payload in source:
                record = json.loads(payload)
                if record["type"] == "batch":
                    yield record["sequence"], record["rows"]

    def _upsert_pg(self, domain: Domain, rows: Sequence[Mapping[str, Any]]) -> None:
        if not rows:
            return
        columns = list(rows[0])
        updates = [name for name in columns if name not in domain.key]
        statement = sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}").format(
            sql.Identifier(domain.table),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
            sql.SQL(", ").join(map(sql.Identifier, domain.key)),
            sql.SQL(", ").join(
                sql.SQL("{}=EXCLUDED.{}").format(sql.Identifier(name), sql.Identifier(name)) for name in updates
            ),
        )
        with self.targets.postgres.connection() as connection:
            values = []
            for row in rows:
                item = []
                for name in columns:
                    value = row.get(name)
                    if name in domain.json_columns:
                        value = Jsonb(json.loads(value) if isinstance(value, str) else value or {})
                    item.append(value)
                values.append(item)
            with connection.cursor() as cursor:
                cursor.executemany(statement, values)

    @staticmethod
    def _raw(row: Mapping[str, Any]) -> dict[str, Any]:
        timestamp = int(row["timestamp"])
        return normalize_bar("raw", {
            "symbol": row["symbol"], "market": _market(str(row["symbol"]), {}),
            "interval": row["interval"], "adjustment": row["adjustment"],
            "bar_time": row.get("bar_at") or datetime.fromtimestamp(timestamp, timezone.utc).isoformat(),
            "open": row["open"], "high": row["high"], "low": row["low"], "close": row["close"],
            "raw_close": row.get("raw_close"), "adjustment_factor": row.get("adjustment_factor"),
            "volume": row["volume"], "amount": row.get("amount"), "source": row["source"],
            "source_sequence": row.get("source_sequence") or str(timestamp),
            "observed_at": row.get("observed_at") or row["ingested_at"],
            "ingested_at": row["ingested_at"], "raw_artifact_id": row.get("raw_artifact_id"),
        })

    def _artifact_row(self, row: dict[str, Any]) -> dict[str, Any]:
        declared_path = Path(str(row["storage_path"]))
        if declared_path.is_absolute() and self.artifact_source_root is not None:
            try:
                data_index = declared_path.parts.index("data")
            except ValueError as error:
                raise ValueError("artifact body has no stable data-relative path") from error
            source_path = (
                self.artifact_source_root / Path(*declared_path.parts[data_index + 1:])
            ).resolve()
            containment_root = self.artifact_source_root
        else:
            source_path = (self.source.allowed_root / declared_path).resolve()
            containment_root = self.source.allowed_root
        try:
            source_path.relative_to(containment_root)
        except ValueError as error:
            raise ValueError("artifact body escapes source root") from error
        if _has_symlink(source_path) or not source_path.is_file():
            raise ValueError("artifact body is unavailable")
        declared_size = int(row["byte_size"])
        artifact_limit = min(self.source.limits.max_output_bytes, 1024 * 1024 * 1024)
        if declared_size < 0 or declared_size > artifact_limit or source_path.stat().st_size != declared_size:
            raise ValueError("artifact body checksum mismatch")
        destination = self.root / "artifacts" / row["artifact_id"]
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            digest = hashlib.sha256()
            size = 0
            with destination.open("rb") as existing:
                while chunk := existing.read(1024 * 1024):
                    size += len(chunk)
                    digest.update(chunk)
            if size != declared_size or digest.hexdigest() != row["sha256"]:
                raise ValueError("existing artifact body mismatch")
        else:
            temporary = destination.with_suffix(".partial")
            digest = hashlib.sha256()
            size = 0
            with source_path.open("rb") as source, temporary.open("wb") as output:
                while chunk := source.read(1024 * 1024):
                    size += len(chunk)
                    if size > artifact_limit:
                        raise ValueError("artifact body exceeds size bound")
                    digest.update(chunk)
                    output.write(chunk)
                output.flush()
                os.fsync(output.fileno())
            if size != declared_size or digest.hexdigest() != row["sha256"]:
                temporary.unlink(missing_ok=True)
                raise ValueError("artifact body checksum mismatch")
            os.replace(temporary, destination)
            descriptor = os.open(destination.parent, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        return {**row, "storage_path": f"artifact://{row['artifact_id']}"}

    def _import_pg_domain(self, checkpoint: dict[str, Any], domain: Domain, fingerprint: str) -> None:
        state = checkpoint["domains"].setdefault(domain.table, {"batch": 0, "rows": 0, "done": False})
        stage_path = self._stage_table(domain.table, fingerprint)
        stage_meta = self._verify_stage(stage_path, domain.table, fingerprint)
        for index, batch in self._stage_batches(stage_path, domain.table, fingerprint):
            if index < state["batch"]:
                continue
            if domain.table == "raw_artifact_manifest":
                batch = [self._artifact_row(dict(row)) for row in batch]
            if self.fault_hook:
                self.fault_hook("before_write", domain.table)
            self._upsert_pg(domain, batch)
            if self.fault_hook:
                self.fault_hook("after_write", domain.table)
            state["batch"] = index + 1
            state["rows"] += len(batch)
            if self.fault_hook:
                self.fault_hook("before_checkpoint", domain.table)
            self._save(checkpoint)
            if self.fault_hook:
                self.fault_hook("after_checkpoint", domain.table)
        state["done"] = True
        state["checksum"] = stage_meta["checksum"]
        if state["rows"] != stage_meta["rows"]:
            raise RuntimeError("PostgreSQL staged row count mismatch")
        self._save(checkpoint)

    def _import_market(self, checkpoint: dict[str, Any], fingerprint: str) -> None:
        name = "market_bar_raw"
        state = checkpoint["domains"].setdefault(name, {"batch": 0, "rows": 0, "done": False, "ranges": {}})
        stage_path = self._stage_table("market_price_bar", fingerprint)
        stage_meta = self._verify_stage(stage_path, "market_price_bar", fingerprint)
        for index, batch in self._stage_batches(stage_path, "market_price_bar", fingerprint):
            if index < state["batch"]:
                continue
            rows = [self._raw(row) for row in batch]
            if self.fault_hook:
                self.fault_hook("before_write", name)
            outcome = self.targets.writer.write("raw", rows)
            if not outcome.get("acknowledged") or not outcome.get("verified") or outcome.get("written") != len(rows):
                raise RuntimeError("ClickHouse raw batch was not fully acknowledged")
            for row in rows:
                key = "\x1f".join((row["symbol"], row["interval"], row["adjustment"]))
                boundary = state["ranges"].setdefault(key, {"start": row["bar_time"], "end": row["bar_time"]})
                boundary["start"] = min(boundary["start"], row["bar_time"])
                boundary["end"] = max(boundary["end"], row["bar_time"])
            if self.fault_hook:
                self.fault_hook("after_write", name)
            state["batch"] = index + 1
            state["rows"] += len(rows)
            if self.fault_hook:
                self.fault_hook("before_checkpoint", name)
            self._save(checkpoint)
            if self.fault_hook:
                self.fault_hook("after_checkpoint", name)
        if state["rows"] != stage_meta["rows"]:
            raise RuntimeError("ClickHouse staged row count mismatch")
        for key, boundary in sorted(state["ranges"].items()):
            symbol, interval, adjustment = key.split("\x1f")
            result = self.targets.canonical_builder.rebuild(
                symbol, interval, adjustment, boundary["start"], boundary["end"], 100_000
            )
            if result.get("status") != "ok" or result.get("truncated"):
                raise RuntimeError("canonical full-range rebuild failed")
        state["done"] = True
        checkpoint["domains"]["market_bar_canonical"] = {
            "rows": "verified-during-reconcile", "done": True,
        }
        self._save(checkpoint)

    def _native_domains(self, checkpoint: dict[str, Any]) -> None:
        config = {"source_fingerprint": checkpoint["source_fingerprint"], "mode": "full-only"}
        config_hash = hashlib.sha256(
            json.dumps(config, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        now = datetime.now(timezone.utc)
        with self.targets.postgres.connection() as connection:
            connection.execute(
                "INSERT INTO runtime_config_version VALUES (%s,1,'test',%s,%s,%s,%s,'offline-full-import') "
                "ON CONFLICT (config_id,config_sha256) DO NOTHING",
                (checkpoint["run_id"], FULL_IMPORT_VERSION, Jsonb(config), config_hash, now),
            )
            connection.execute(
                "INSERT INTO migration_checkpoint "
                "(run_id,domain,shard,revision,status,source_watermark,target_watermark,cursor_json,evidence_json,updated_at) "
                "VALUES (%s,'full-import','',1,'completed',%s,%s,%s,%s,%s) "
                "ON CONFLICT (run_id,domain,shard) DO NOTHING",
                (checkpoint["run_id"], checkpoint["source_fingerprint"], checkpoint["source_fingerprint"],
                 Jsonb({"domains": len(POSTGRES_DOMAINS)}),
                 Jsonb({"run_id": checkpoint["run_id"], "source": checkpoint["source_fingerprint"]}), now),
            )
        checkpoint["domains"]["runtime_config_version"] = {"rows": 1, "done": True}
        checkpoint["domains"]["migration_checkpoint"] = {"rows": 1, "done": True}
        self._save(checkpoint)

    def _spool_empty(self) -> bool:
        spool = self.targets.writer.spool
        diagnostics = spool.diagnostics()
        intents, more_intents = spool._bounded_files(spool.intents, 1000)
        processing, more_processing = spool._bounded_files(spool.processing_intents, 1000)
        quarantined, more_quarantine = spool._bounded_files(spool.quarantine, 1000)
        return not (
            diagnostics["pending"] or diagnostics["failed"] or quarantined
            or diagnostics["truncated"] or intents or processing or more_intents
            or more_processing or more_quarantine
        )

    def _reconcile(self, checkpoint: dict[str, Any], fingerprint: str) -> dict[str, Any]:
        domains = []
        mismatches = []
        for domain in POSTGRES_DOMAINS:
            expected = [
                row
                for _, batch in self._stage_batches(
                    self.stage / f"{domain.table}.ndjson", domain.table, fingerprint
                )
                for row in batch
            ]
            if domain.table == "raw_artifact_manifest":
                expected = [self._artifact_row(dict(row)) for row in expected]
            columns = list(expected[0]) if expected else []
            with self.targets.postgres.connection() as connection:
                actual = list(connection.execute(sql.SQL("SELECT {} FROM {} ORDER BY {}").format(
                    sql.SQL(", ").join(map(sql.Identifier, columns)), sql.Identifier(domain.table),
                    sql.SQL(", ").join(map(sql.Identifier, domain.key)),
                )).fetchall()) if columns else []
            actual_docs = [dict(row) for row in actual]
            for row in actual_docs:
                for name in domain.json_columns:
                    if name in row:
                        row[name] = row[name] or {}
            for row in expected:
                for name in domain.json_columns:
                    if isinstance(row.get(name), str):
                        row[name] = json.loads(row[name])
            expected_checksum = _digest(expected)
            actual_checksum = _digest(actual_docs)
            ok = len(expected) == len(actual_docs) and expected_checksum == actual_checksum
            domains.append({
                "domain": domain.table, "rows": len(expected),
                "key_content_checksum": actual_checksum,
                "expected_checksum": expected_checksum,
                "status": "ok" if ok else "mismatch",
            })
            if not ok:
                mismatches.append({"domain": domain.table, "reason": "row/key/content/PIT checksum"})
        client = self.targets.clickhouse._require_client()
        source_raw = [
            self._raw(row)
            for _, batch in self._stage_batches(
                self.stage / "market_price_bar.ndjson", "market_price_bar", fingerprint
            )
            for row in batch
        ]
        raw_columns = list(self.targets.writer.repository.RAW_COLUMNS)
        raw_actual_rows = client.query(
            "SELECT " + ",".join(raw_columns) + " FROM market_bar_raw FINAL "
            "ORDER BY symbol,interval,adjustment,bar_time,source"
        ).result_rows
        raw_actual = [dict(zip(raw_columns, row)) for row in raw_actual_rows]
        source_raw = sorted(
            source_raw,
            key=lambda row: (row["symbol"], row["interval"], row["adjustment"], str(row["bar_time"]), row["source"]),
        )
        expected_canonical = self.targets.canonical_builder.build_rows(source_raw, [])[0]
        canonical_columns = list(self.targets.writer.repository.CANONICAL_COLUMNS)
        canonical_actual_rows = client.query(
            "SELECT " + ",".join(canonical_columns) + " FROM market_bar_canonical FINAL "
            "ORDER BY symbol,interval,adjustment,bar_time"
        ).result_rows
        canonical_actual = [dict(zip(canonical_columns, row)) for row in canonical_actual_rows]
        raw_rows = len(raw_actual)
        canonical_rows = len(canonical_actual)
        expected_raw = checkpoint["domains"]["market_bar_raw"]["rows"]
        expected_raw_checksum, raw_checksum = _digest(source_raw), _digest(raw_actual)
        expected_canonical_checksum = _digest(expected_canonical)
        canonical_checksum = _digest(canonical_actual)
        raw_ok = raw_rows == expected_raw and expected_raw_checksum == raw_checksum
        canonical_ok = len(expected_canonical) == canonical_rows and expected_canonical_checksum == canonical_checksum
        if not raw_ok:
            mismatches.append({"domain": "market_bar_raw", "reason": "FINAL content/provenance checksum"})
        if not canonical_ok:
            mismatches.append({"domain": "market_bar_canonical", "reason": "FINAL selection/content checksum"})
        domains.extend([
            {"domain": "market_bar_raw", "rows": raw_rows, "key_content_checksum": raw_checksum,
             "expected_checksum": expected_raw_checksum, "status": "ok" if raw_ok else "mismatch"},
            {"domain": "market_bar_canonical", "rows": canonical_rows,
             "key_content_checksum": canonical_checksum,
             "expected_checksum": expected_canonical_checksum,
             "status": "ok" if canonical_ok else "mismatch"},
        ])
        if not self._spool_empty():
            mismatches.append({"domain": "spool", "reason": "pending intent or WAL"})
        with self.targets.postgres.connection() as connection:
            runtime = list(connection.execute(
                "SELECT config_id,version,profile,schema_version,config_json,config_sha256,observed_at,actor "
                "FROM runtime_config_version WHERE config_id=%s ORDER BY config_sha256",
                (checkpoint["run_id"],),
            ).fetchall())
            migration = list(connection.execute(
                "SELECT run_id,domain,shard,revision,status,source_watermark,target_watermark,cursor_json,evidence_json "
                "FROM migration_checkpoint WHERE run_id=%s ORDER BY domain,shard",
                (checkpoint["run_id"],),
            ).fetchall())
        runtime_checksum, migration_checksum = _digest(runtime), _digest(migration)
        domains.extend([
            {"domain": "runtime_config_version", "rows": len(runtime),
             "key_content_checksum": runtime_checksum, "status": "ok" if len(runtime) == 1 else "mismatch"},
            {"domain": "migration_checkpoint", "rows": len(migration),
             "key_content_checksum": migration_checksum, "status": "ok" if len(migration) == 1 else "mismatch"},
        ])
        if len(runtime) != 1:
            mismatches.append({"domain": "runtime_config_version", "reason": "native control row count"})
        if len(migration) != 1:
            mismatches.append({"domain": "migration_checkpoint", "reason": "native control row count"})
        return {"status": "ok" if not mismatches else "mismatch", "domains": domains, "mismatches": mismatches[:MAX_ERRORS]}

    def run(self) -> dict[str, Any]:
        checkpoint: dict[str, Any] | None = None
        self.targets.postgres.migrate()
        self.targets.clickhouse.migrate()
        with self.targets.postgres.connection() as connection:
            pg_versions = {row["version"] for row in connection.execute(
                "SELECT version FROM schema_migrations"
            ).fetchall()}
        ch_versions = {int(row[0]) for row in self.targets.clickhouse._require_client().query(
            "SELECT version FROM schema_migrations"
        ).result_rows}
        if pg_versions != {item[0] for item in POSTGRES_MIGRATIONS}:
            raise ValueError("PostgreSQL migration version mismatch")
        if ch_versions != {item[0] for item in CLICKHOUSE_MIGRATIONS}:
            raise ValueError("ClickHouse migration version mismatch")
        manifest = self.source.inspect()
        fingerprint = manifest["source_fingerprint"]
        self.state.mkdir(parents=True, exist_ok=True)
        with (self.state / "import.lock").open("a+") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            try:
                if not self.checkpoint_path.exists():
                    with self.targets.postgres.connection() as connection:
                        occupied = sum(
                            int(connection.execute(
                                sql.SQL("SELECT count(*) AS count FROM {}").format(sql.Identifier(domain))
                            ).fetchone()["count"])
                            for domain in POSTGRES_TRANSACTION_DOMAINS
                        )
                    client = self.targets.clickhouse._require_client()
                    occupied += int(client.query(
                        "SELECT (SELECT count() FROM market_bar_raw FINAL) + "
                        "(SELECT count() FROM market_bar_canonical FINAL)"
                    ).result_rows[0][0])
                    if occupied:
                        raise ValueError("full import targets must be empty for a new run")
                checkpoint = self._load(fingerprint)
                for domain in POSTGRES_DOMAINS:
                    self._import_pg_domain(checkpoint, domain, fingerprint)
                self._import_market(checkpoint, fingerprint)
                self._native_domains(checkpoint)
                if self.source.inspect()["source_fingerprint"] != fingerprint:
                    raise RuntimeError("source fingerprint changed during full import")
                reconciliation = self._reconcile(checkpoint, fingerprint)
                if reconciliation["status"] != "ok":
                    failed = ",".join(item["domain"] for item in reconciliation["mismatches"])
                    raise RuntimeError(f"full import reconciliation failed: {failed[:200]}")
                expected_domains = set(POSTGRES_TRANSACTION_DOMAINS) | {
                    "market_bar_raw", "market_bar_canonical",
                }
                if set(checkpoint["domains"]) != expected_domains:
                    raise RuntimeError("full import domain inventory is incomplete")
                checkpoint["phase"] = "complete"
                self._save(checkpoint)
                report = {
                    "version": FULL_IMPORT_VERSION,
                    "status": "complete",
                    "run_id": checkpoint["run_id"],
                    "source_fingerprint": fingerprint,
                    "targets": self._target_ids(),
                    "domains": reconciliation["domains"],
                    "mismatches": [],
                    "spool_pending": 0,
                    "incremental_catchup": "not_started_bg014",
                }
                _atomic(self.report_path, report)
                return report
            except Exception as error:
                if checkpoint is not None:
                    checkpoint["errors"] = (checkpoint.get("errors", []) + [type(error).__name__])[-MAX_ERRORS:]
                    self._save(checkpoint)
                raise
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
