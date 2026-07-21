"""BG-016 pure PostgreSQL/ClickHouse local backup and restore contract."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from .local_backup import BackupComponent, LocalStorageBackup, _hash, _json
from .local_restore import LocalStorageRestore, RestoreTargets
from .offline_incremental_catchup import CATCHUP_VERSION
from .postgres_migrations import POSTGRES_TRANSACTION_DOMAINS


V2_BACKUP_MANIFEST_VERSION = "storage-v2.pg-ch-backup-manifest.v1"
V2_RESTORE_VERSION = "storage-v2.pg-ch-restore.v1"
V2_COMPONENT_ORDER = (
    "postgresql",
    "clickhouse",
    "artifact_archive",
    "authoritative_spool",
    "scheduler_state",
    "config_version",
    "migration_watermark",
    "cursor_key",
)
V2_REQUIRED_COMPONENTS = frozenset(V2_COMPONENT_ORDER)
V2_SUPPORTED = {
    "postgresql": {("logical-json", "postgres-16-v2-18-domains")},
    "clickhouse": {("logical-json", "clickhouse-25.8-raw-canonical")},
    "artifact_archive": {("artifact-parquet-tree", "artifact-parquet-v1")},
    "authoritative_spool": {("wal-intent-tree", "authoritative-spool-v2")},
    "scheduler_state": {("scheduler-tree", "canonical-scheduler-v1")},
    "config_version": {("logical-json", "v2-config-version-v1")},
    "migration_watermark": {("logical-json", CATCHUP_VERSION)},
    "cursor_key": {("sealed-secret", "cursor-v1")},
}
V2_TREE_DESTINATIONS = {
    "artifact_archive": "artifacts",
    "authoritative_spool": "spool/clickhouse",
    "scheduler_state": "canonical-scheduler",
    "config_version": ".storage-v2/config",
    "migration_watermark": ".storage-v2/migration",
}


def capture_v2_postgresql(database: Any, captured_at: str) -> BackupComponent:
    component = BackupComponent.postgresql(database, captured_at)
    return BackupComponent(
        "postgresql", "logical-json", "postgres-16-v2-18-domains",
        component.files, component.watermark,
    )


def capture_v2_clickhouse(database: Any, captured_at: str) -> BackupComponent:
    component = BackupComponent.clickhouse(database, captured_at)
    return BackupComponent(
        "clickhouse", "logical-json", "clickhouse-25.8-raw-canonical",
        component.files, component.watermark, True,
    )


def _logical_document(component: BackupComponent) -> dict[str, Any]:
    if set(component.files) != {"logical.json"}:
        raise ValueError(f"backup component {component.name} must contain logical.json")
    try:
        value = json.loads(component.files["logical.json"])
    except (UnicodeError, json.JSONDecodeError):
        raise ValueError(f"backup component {component.name} JSON is invalid") from None
    if not isinstance(value, dict):
        raise ValueError(f"backup component {component.name} JSON must be an object")
    return value


def _utc(value: Any) -> str:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        # ClickHouse DateTime64 columns are declared UTC but its client returns
        # naive datetime/string values for these result rows.
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


class V2LocalBackup(LocalStorageBackup):
    """Development/test-only bundle containing no DuckDB component."""

    def __init__(self, backup_root: Path, storage_root: Path, wrapping_key: bytes,
                 profile: str = "development") -> None:
        super().__init__(
            backup_root, storage_root, wrapping_key, profile,
            required_components=V2_REQUIRED_COMPONENTS,
            manifest_version=V2_BACKUP_MANIFEST_VERSION,
        )

    @staticmethod
    def _validate_components(components: Mapping[str, BackupComponent]) -> None:
        for name, expected in V2_SUPPORTED.items():
            component = components[name]
            if (component.kind, component.version) not in expected:
                raise ValueError(f"unsupported V2 backup component {name}")
        pg = _logical_document(components["postgresql"])
        if set(pg) - {"schema_migrations"} != set(POSTGRES_TRANSACTION_DOMAINS):
            raise ValueError("PostgreSQL backup must cover exactly 18 authority domains")
        clickhouse = _logical_document(components["clickhouse"])
        if not {"market_bar_raw", "market_bar_canonical"}.issubset(clickhouse):
            raise ValueError("ClickHouse backup must contain raw and canonical tables")
        raw = clickhouse["market_bar_raw"]
        canonical = clickhouse["market_bar_canonical"]
        if not isinstance(raw, dict) or not isinstance(canonical, dict):
            raise ValueError("ClickHouse backup table payload is invalid")
        config = _logical_document(components["config_version"])
        if not isinstance(config.get("version"), str) or not config["version"]:
            raise ValueError("V2 configuration version is missing")
        watermark = _logical_document(components["migration_watermark"])
        unsigned = dict(watermark)
        checksum = unsigned.pop("checksum", None)
        if (
            watermark.get("version") != CATCHUP_VERSION
            or watermark.get("phase") != "complete"
            or checksum != _hash(_json(unsigned))
            or not watermark.get("source_high_watermark", {}).get("source_fingerprint")
            or watermark.get("lag") != 0
        ):
            raise ValueError("BG-014 verified raw watermark is invalid")
        columns = list(raw.get("columns", ()))
        rows = list(raw.get("rows", ()))
        try:
            ingested_index = columns.index("ingested_at")
            actual_raw_max = max((_utc(row[ingested_index]) for row in rows), default=None)
        except (ValueError, IndexError, TypeError):
            raise ValueError("ClickHouse raw watermark cannot be derived") from None
        declared_raw_max = watermark.get("verified_raw_max_ingested_at")
        boundary = watermark.get("canonical_rebuild_through")
        if actual_raw_max != (_utc(declared_raw_max) if declared_raw_max else None):
            raise ValueError("BG-014 raw watermark does not match ClickHouse FINAL payload")
        if boundary != declared_raw_max:
            raise ValueError("canonical rebuild boundary does not match verified raw watermark")
        canonical_columns = list(canonical.get("columns", ()))
        canonical_rows = list(canonical.get("rows", ()))
        if canonical_rows:
            try:
                ingested_index = canonical_columns.index("ingested_at")
                if max(_utc(row[ingested_index]) for row in canonical_rows) > _utc(boundary):
                    raise ValueError("canonical payload exceeds verified raw watermark")
            except (ValueError, IndexError, TypeError):
                raise ValueError("ClickHouse canonical watermark cannot be derived") from None
        spool_files = components["authoritative_spool"].files
        scheduler_files = components["scheduler_state"].files
        if not spool_files or not scheduler_files:
            raise ValueError("WAL/intent and scheduler state are required")

    def create(self, components: Iterable[BackupComponent], snapshot_at: str,
               mode: str = "full", base_backup_id: str | None = None,
               fault_hook: Any = None) -> dict[str, Any]:
        normalized = {component.name: component for component in components}
        if set(normalized) != V2_REQUIRED_COMPONENTS:
            raise ValueError("pure PG/CH backup component set mismatch")
        self._validate_components(normalized)
        result = super().create(normalized.values(), snapshot_at, mode, base_backup_id, fault_hook)
        if any(item["name"] == "duckdb" for item in result["components"]):
            raise ValueError("DuckDB is forbidden in a V2 online backup")
        return result

    def verify(self, artifact: Path) -> dict[str, Any]:
        manifest = super().verify(artifact)
        if tuple(item["name"] for item in sorted(
            manifest["components"], key=lambda item: V2_COMPONENT_ORDER.index(item["name"])
        )) != V2_COMPONENT_ORDER:
            raise ValueError("pure PG/CH backup component inventory mismatch")
        artifact_root = Path(manifest["artifact_path"])
        reconstructed = {}
        for item in manifest["components"]:
            files = {}
            for entry in item["files"]:
                relative = Path(entry["path"]).relative_to("components", item["name"])
                if item["name"] == "cursor_key" and relative.suffix == ".sealed":
                    relative = relative.with_suffix("")
                source = artifact_root / entry["path"]
                files[relative.as_posix()] = LocalStorageRestore._verified_component_bytes(
                    source, entry,
                )
            reconstructed[item["name"]] = BackupComponent(
                item["name"], item["kind"], item["version"], files,
                item["watermark"], bool(item.get("canonical_rebuildable")),
            )
        self._validate_components(reconstructed)
        return manifest


class V2LocalRestore(LocalStorageRestore):
    """Checkpointed empty-root restore for the pure PG/CH component graph."""

    def __init__(self, backup: V2LocalBackup, targets: RestoreTargets, clock=None) -> None:
        kwargs = {} if clock is None else {"clock": clock}
        super().__init__(
            backup, targets,
            component_order=V2_COMPONENT_ORDER,
            supported=V2_SUPPORTED,
            tree_destinations=V2_TREE_DESTINATIONS,
            restore_version=V2_RESTORE_VERSION,
            **kwargs,
        )

    def record_v2_verification(self, results: Mapping[str, Any]) -> dict[str, Any]:
        required = {
            "postgres_18_domains", "postgres_pit", "clickhouse_raw_final",
            "clickhouse_canonical", "market_api_contract", "pagination_cursor_cache",
            "artifact_parquet_query", "spool_replay_once", "canonical_boundary",
        }
        if set(results) != required or any(value != "ok" for value in results.values()):
            raise ValueError("V2 restored-target verification is incomplete")
        return self.record_verification(results)
