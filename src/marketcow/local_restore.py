from __future__ import annotations

import fcntl
import json
import os
import shutil
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Mapping

from .local_backup import (
    LocalStorageBackup,
    REQUIRED_COMPONENTS,
    _assert_no_sensitive,
    _fsync_dir,
    _hash,
    _json,
    _utc,
    unseal_cursor_key,
)


RESTORE_VERSION = "storage-v2.local-restore.v1"
COMPONENT_ORDER = (
    "postgresql", "clickhouse", "duckdb", "cold_archive", "spool", "cursor_key",
)
SUPPORTED = {
    "postgresql": {("logical-json", "postgresql-schema-v1"),
                   ("logical-json", "postgres-16")},
    "clickhouse": {("logical-json", "clickhouse-schema-v1"),
                   ("logical-json", "clickhouse-25.8")},
    "duckdb": {("duckdb-file", "1")},
    "cold_archive": {("parquet-tree", "manifest-v1")},
    "spool": {("wal-tree", "spool-v1")},
    "cursor_key": {("sealed-secret", "cursor-v1")},
}


@dataclass(frozen=True)
class RestoreTargets:
    root: Path
    postgres: Any = None
    clickhouse: Any = None
    profile: str = "development"
    allowed_root: Path | None = None


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(dir=path.parent, prefix=".restore-")
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(json.dumps(value, sort_keys=True, indent=2).encode())
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_dir(path.parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


class LocalStorageRestore:
    """Fail-closed, checkpointed empty-environment restore drill."""

    def __init__(self, backup: LocalStorageBackup, targets: RestoreTargets,
                 clock: Callable[[], float] = time.monotonic) -> None:
        if targets.profile not in {"development", "test"}:
            raise ValueError("Storage V2 restore is development/test-only")
        supplied = Path(targets.root)
        if supplied.is_symlink():
            raise ValueError("restore target must not be a symlink")
        self.root = supplied.resolve()
        allowed_root = Path(targets.allowed_root).resolve() if targets.allowed_root else None
        if allowed_root is None:
            raise ValueError("restore target requires an explicit allowed root")
        try:
            self.root.relative_to(allowed_root)
        except ValueError as error:
            raise ValueError("restore target escapes allowed root") from error
        if (not self.root.name.endswith(("development", "test")) or
                "production" in self.root.name.lower()):
            raise ValueError("restore target must be an isolated development/test root")
        if (targets.postgres is not None and
                not str(targets.postgres.schema).endswith(("_development", "_test"))):
            raise ValueError("PostgreSQL restore schema must be development/test isolated")
        if (targets.clickhouse is not None and
                not str(targets.clickhouse.database).endswith(("_development", "_test"))):
            raise ValueError("ClickHouse restore database must be development/test isolated")
        self.backup = backup
        self.targets = targets
        self.clock = clock
        self.state_root = self.root / ".storage-v2-restore"
        self.checkpoint_path = self.state_root / "checkpoint.json"

    @staticmethod
    def _sign_checkpoint(checkpoint: Dict[str, Any]) -> None:
        checkpoint.pop("checksum", None)
        checkpoint["checksum"] = _hash(_json(checkpoint))

    @staticmethod
    def _validate_checkpoint(checkpoint: Mapping[str, Any]) -> None:
        unsigned = dict(checkpoint)
        checksum = unsigned.pop("checksum", None)
        if checksum != _hash(_json(unsigned)):
            raise ValueError("restore checkpoint checksum mismatch")
        completed = checkpoint.get("completed")
        if (not isinstance(completed, list) or
                completed != list(COMPONENT_ORDER[:len(completed)])):
            raise ValueError("restore checkpoint component order is corrupt")

    def restore(self, artifacts: Iterable[Path], fault_hook: Any = None) -> Dict[str, Any]:
        started = self.clock()
        chain = self._preflight(tuple(artifacts))
        self.root.mkdir(parents=True, exist_ok=True)
        self.state_root.mkdir(exist_ok=True)
        lock_path = self.state_root / "restore.lock"
        with lock_path.open("a+") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            try:
                checkpoint = self._load_or_initialize(chain)
                final_artifact, manifest = chain[-1]
                components = {item["name"]: item for item in manifest["components"]}
                for name in COMPONENT_ORDER:
                    if name in checkpoint["completed"]:
                        continue
                    if fault_hook:
                        fault_hook("before", name)
                    self._restore_component(name, final_artifact, components[name])
                    if fault_hook:
                        fault_hook("after_write", name)
                    checkpoint["completed"].append(name)
                    self._sign_checkpoint(checkpoint)
                    _atomic_json(self.checkpoint_path, checkpoint)
                    if fault_hook:
                        fault_hook("after_checkpoint", name)
                report = self._report(chain, checkpoint, max(0.0, self.clock() - started))
                _atomic_json(self.state_root / "report.json", report)
                return report
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    def _preflight(self, artifacts: tuple[Path, ...]):
        if not artifacts:
            raise ValueError("restore requires a backup bundle")
        chain = []
        previous = None
        for index, artifact in enumerate(artifacts):
            manifest = self.backup.verify(artifact)
            mode = manifest["mode"]
            if index == 0 and mode != "full":
                raise ValueError("restore chain must begin with a full backup")
            if index and (mode != "incremental" or
                          manifest.get("base_backup_id") != previous):
                raise ValueError("incremental backup chain is incomplete or out of order")
            if set(item["name"] for item in manifest["components"]) != REQUIRED_COMPONENTS:
                raise ValueError("restore component set is incomplete")
            for item in manifest["components"]:
                if (item.get("kind"), item.get("version")) not in SUPPORTED[item["name"]]:
                    raise ValueError("unsupported restore component version")
            if chain and _utc(manifest["snapshot_at"]) < _utc(chain[-1][1]["snapshot_at"]):
                raise ValueError("incremental backup snapshots are out of order")
            chain.append((Path(artifact).resolve(), manifest))
            previous = manifest["backup_id"]
        existing = set()
        if self.root.exists():
            existing = {item.name for item in self.root.iterdir()}
        if self.checkpoint_path.exists():
            if self.state_root.is_symlink() or self.checkpoint_path.is_symlink():
                raise ValueError("restore checkpoint must not be symlinked")
            checkpoint = json.loads(self.checkpoint_path.read_text())
            self._validate_checkpoint(checkpoint)
            allowed = {".storage-v2-restore"}
            completed = set(checkpoint.get("completed", ()))
            completed_order = checkpoint.get("completed", ())
            next_name = (COMPONENT_ORDER[len(completed_order)]
                         if len(completed_order) < len(COMPONENT_ORDER) else None)
            if "duckdb" in completed or next_name == "duckdb":
                allowed.add("warehouse")
            if "cold_archive" in completed or next_name == "cold_archive":
                allowed.add("archive")
            if "spool" in completed or next_name == "spool":
                allowed.add("spool")
            if "cursor_key" in completed or next_name == "cursor_key":
                allowed.add(".market-bar-cursor.key")
            if not existing.issubset(allowed):
                raise ValueError("restore target contains untracked data")
        elif existing:
            raise ValueError("restore target must be empty")
        if not self.checkpoint_path.exists():
            self._require_empty_databases()
        return chain

    def _require_empty_databases(self) -> None:
        if self.targets.postgres is not None:
            database = self.targets.postgres
            with database.pool.connection() as connection:
                count = connection.execute(
                    "SELECT count(*) AS count FROM information_schema.tables "
                    "WHERE table_schema=%s", [database.schema],
                ).fetchone()
                value = count["count"] if isinstance(count, dict) else count[0]
                if value:
                    raise ValueError("PostgreSQL restore target must be empty")
        if self.targets.clickhouse is not None:
            database = self.targets.clickhouse
            client = database._require_client()
            if client.query("SHOW TABLES").result_rows:
                raise ValueError("ClickHouse restore target must be empty")

    def _load_or_initialize(self, chain):
        ids = [manifest["backup_id"] for _, manifest in chain]
        if self.checkpoint_path.exists():
            checkpoint = json.loads(self.checkpoint_path.read_text())
            self._validate_checkpoint(checkpoint)
            if checkpoint.get("version") != RESTORE_VERSION or checkpoint.get("chain") != ids:
                raise ValueError("restore checkpoint does not match backup chain")
            return checkpoint
        checkpoint = {"version": RESTORE_VERSION, "chain": ids, "completed": []}
        self._sign_checkpoint(checkpoint)
        _atomic_json(self.checkpoint_path, checkpoint)
        return checkpoint

    @staticmethod
    def _component_files(artifact: Path, component: Mapping[str, Any]):
        for item in component["files"]:
            yield item, artifact / item["path"]

    def _restore_component(self, name: str, artifact: Path,
                           component: Mapping[str, Any]) -> None:
        if name == "postgresql":
            self._restore_postgres(artifact, component)
        elif name == "clickhouse":
            self._restore_clickhouse(artifact, component)
        elif name == "cursor_key":
            self._restore_cursor(artifact, component)
        else:
            destination = {
                "duckdb": self.root / "warehouse",
                "cold_archive": self.root / "archive",
                "spool": self.root / "spool" / "clickhouse",
            }[name]
            self._restore_tree(artifact, component, destination)

    def _restore_tree(self, artifact: Path, component: Mapping[str, Any],
                      destination: Path) -> None:
        staging = self.state_root / f"stage-{component['name']}"
        if staging.exists():
            shutil.rmtree(staging)
        staging.mkdir()
        for item, source in self._component_files(artifact, component):
            relative = Path(item["path"]).relative_to("components", component["name"])
            target = staging / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(source.read_bytes())
            os.chmod(target, int(item["mode"], 8))
        if destination.exists():
            if self._tree_digest(destination) == self._tree_digest(staging):
                shutil.rmtree(staging)
                return
            raise ValueError(f"restore destination for {component['name']} is non-empty")
        destination.parent.mkdir(parents=True, exist_ok=True)
        os.replace(staging, destination)
        _fsync_dir(destination.parent)

    @staticmethod
    def _tree_digest(root: Path):
        result = []
        for path in sorted(root.rglob("*")):
            if path.is_symlink():
                raise ValueError("restore destination must not contain symlinks")
            if path.is_file():
                try:
                    path.resolve().relative_to(root.resolve())
                except ValueError as error:
                    raise ValueError("restore destination escapes target") from error
                result.append((path.relative_to(root).as_posix(), path.read_bytes()))
        return result

    def _restore_cursor(self, artifact: Path, component: Mapping[str, Any]) -> None:
        files = list(self._component_files(artifact, component))
        if len(files) != 1:
            raise ValueError("cursor backup must contain exactly one key")
        plaintext = unseal_cursor_key(files[0][1].read_bytes(), self.backup.wrapping_key)
        target = self.root / ".market-bar-cursor.key"
        if target.exists():
            if target.read_bytes() != plaintext or target.stat().st_mode & 0o777 != 0o600:
                raise ValueError("existing cursor key does not match restore")
            return
        descriptor, temporary = tempfile.mkstemp(dir=self.root, prefix=".cursor-restore-")
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(plaintext)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temporary, 0o600)
            os.replace(temporary, target)
            _fsync_dir(self.root)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def _restore_postgres(self, artifact: Path, component: Mapping[str, Any]) -> None:
        database = self.targets.postgres
        if database is None:
            raise ValueError("PostgreSQL restore target is required")
        from psycopg import sql
        from psycopg.types.json import Jsonb

        payload = json.loads(next(self._component_files(artifact, component))[1].read_text())
        database.migrate()
        with database.pool.connection() as connection:
            connection.execute(sql.SQL("SET LOCAL search_path TO {}, public").format(
                sql.Identifier(database.schema)))
            for table, rows in payload.items():
                if table == "schema_migrations" or not rows:
                    continue
                columns = list(rows[0])
                query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT DO NOTHING").format(
                    sql.Identifier(table), sql.SQL(",").join(map(sql.Identifier, columns)),
                    sql.SQL(",").join(sql.Placeholder() for _ in columns),
                )
                for row in rows:
                    values = [Jsonb(row[col]) if isinstance(row[col], (dict, list))
                              else row[col] for col in columns]
                    connection.execute(query, values)

    def _restore_clickhouse(self, artifact: Path, component: Mapping[str, Any]) -> None:
        database = self.targets.clickhouse
        if database is None:
            raise ValueError("ClickHouse restore target is required")
        payload = json.loads(next(self._component_files(artifact, component))[1].read_text())
        database.migrate()
        client = database._require_client()
        for table, content in payload.items():
            if table == "schema_migrations" or not content["rows"]:
                continue
            types = {row[0]: row[1] for row in client.query(
                f"DESCRIBE TABLE `{table}`").result_rows}
            rows = [[self._clickhouse_value(value, types[column])
                     for column, value in zip(content["columns"], row)]
                    for row in content["rows"]]
            client.insert(table, rows, column_names=content["columns"])

    @staticmethod
    def _clickhouse_value(value: Any, kind: str) -> Any:
        if value is None:
            return None
        if kind.startswith("DateTime") and isinstance(value, str):
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed
        if kind.startswith("UInt") and isinstance(value, str) and value.isdigit():
            return int(value)
        return value

    def _report(self, chain, checkpoint, elapsed: float) -> Dict[str, Any]:
        manifest = chain[-1][1]
        report = {
            "report_version": RESTORE_VERSION,
            "status": "complete",
            "backup_chain": checkpoint["chain"],
            "steps": list(checkpoint["completed"]),
            "elapsed_seconds": round(elapsed, 6),
            "rto_target_seconds": 3600,
            "rpo": manifest["rpo_assumption"],
            "snapshot_at": manifest["snapshot_at"],
            "watermark": manifest["cross_component_watermark"],
            "components": [{"name": item["name"], "kind": item["kind"],
                            "version": item["version"]}
                           for item in manifest["components"]],
            "canonical_boundary": manifest["canonical_boundary"],
            "manual_steps": ["run local contract gate against disposable targets"],
            "rollback": "discard disposable restore root and database instances",
        }
        _assert_no_sensitive(report, "restore report")
        return report
