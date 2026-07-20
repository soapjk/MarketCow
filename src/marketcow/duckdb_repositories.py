from __future__ import annotations

import hashlib
import json
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from .repositories import ArtifactManifestRepository, Repositories
from .storage import Warehouse


class LocalArtifactStore:
    """Filesystem artifact bodies with manifests persisted by DuckDB."""

    def __init__(self, manifest_repository: ArtifactManifestRepository):
        self._manifest_repository = manifest_repository

    def write_json(
        self,
        folder: Path,
        dataset: str,
        payload: Any,
        source: str,
        source_url: str,
        raw_response_locator: str,
        observed_at: str,
        ingested_at: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        artifact_id = uuid.uuid4().hex
        folder.mkdir(parents=True, exist_ok=True)
        stamp = ingested_at.replace(":", "").replace("+", "_").replace(".", "")
        path = folder / f"{dataset}-{stamp}-{artifact_id[:8]}.json"
        body = {
            "artifact_id": artifact_id,
            "dataset": dataset,
            "source": source,
            "source_url": source_url,
            "observed_at": observed_at,
            "ingested_at": ingested_at,
            "raw_response_locator": raw_response_locator,
            "metadata": metadata or {},
            "payload": payload,
        }
        encoded = json.dumps(
            body, ensure_ascii=False, allow_nan=False, sort_keys=True
        ).encode("utf-8")
        path.write_bytes(encoded)
        manifest = {
            "artifact_id": artifact_id,
            "dataset": dataset,
            "source": source,
            "source_url": source_url,
            "observed_at": observed_at,
            "ingested_at": ingested_at,
            "raw_response_locator": raw_response_locator,
            "storage_path": str(path),
            "sha256": hashlib.sha256(encoded).hexdigest(),
            "byte_size": len(encoded),
            "metadata_json": json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True),
        }
        self.save_artifact(manifest)
        return manifest

    def save_artifact(self, row: Dict[str, Any]) -> None:
        self._manifest_repository.save_artifact(row)

    def save_artifacts(self, rows: List[Dict[str, Any]]) -> int:
        return self._manifest_repository.save_artifacts(rows)

    def artifact_paths(self) -> set[str]:
        return self._manifest_repository.artifact_paths()

    def list_artifacts(self, dataset: str = "", limit: int = 100) -> List[Dict[str, Any]]:
        return self._manifest_repository.list_artifacts(dataset, limit)

    def latest_artifact(
        self, dataset: str, metadata_key: str = "", metadata_value: str = ""
    ) -> Optional[Dict[str, Any]]:
        return self._manifest_repository.latest_artifact(dataset, metadata_key, metadata_value)


class Stage1FundamentalRepository:
    """Route migrated fundamentals to PostgreSQL and remaining datasets to DuckDB."""

    POSTGRES_METHODS = {
        "replace_fundamentals",
        "query_fundamentals",
        "replace_statement_rows",
        "get_statement_rows",
        "upsert_baostock",
        "get_baostock",
        "replace_tdx_period",
        "get_tdx",
        "tdx_coverage",
        "get_tdx_history",
        "save_validation_results",
        "get_validation_results",
        "rebuild_validation_results",
        "rebuild_funnel_metrics",
        "query_funnel_metrics",
    }

    def __init__(self, postgres_repository: Any, duckdb_repository: Warehouse):
        self._postgres_repository = postgres_repository
        self._duckdb_repository = duckdb_repository

    def __getattr__(self, name: str) -> Any:
        repository = (
            self._postgres_repository
            if name in self.POSTGRES_METHODS
            else self._duckdb_repository
        )
        return getattr(repository, name)


class RepositoryResources:
    def __init__(self, resources: List[Any]) -> None:
        self.resources = resources

    def close(self) -> None:
        for resource in reversed(self.resources):
            resource.close()
        self.resources.clear()


def create_duckdb_repositories(warehouse: Warehouse) -> Repositories:
    """Build the compatibility backend while each data domain is migrated separately."""

    return Repositories(
        metadata=warehouse,
        fundamentals=warehouse,
        market_bars=warehouse,
        artifacts=LocalArtifactStore(warehouse),
    )


def create_stage1_repositories(settings: Any, warehouse: Warehouse) -> tuple[Repositories, Any]:
    """Assemble opt-in development backends while DuckDB remains the primary."""

    settings.validate_runtime_isolation()
    warehouse.canonical_source_priority = tuple(settings.clickhouse_source_priority)
    if settings.metadata_backend == "duckdb" and not settings.clickhouse_enabled:
        return create_duckdb_repositories(warehouse), None
    resources: List[Any] = []
    metadata: Any = warehouse
    fundamentals: Any = warehouse
    artifacts: Any = LocalArtifactStore(warehouse)
    market_bars: Any = warehouse
    try:
        if settings.metadata_backend == "postgres":
            from .postgres_repositories import PostgresDatabase, PostgresMetadataRepository

            postgres = PostgresDatabase(settings.postgres_dsn, settings.postgres_schema)
            postgres.open()
            resources.append(postgres)
            metadata = PostgresMetadataRepository(postgres)
            fundamentals = Stage1FundamentalRepository(metadata, warehouse)
            artifacts = LocalArtifactStore(metadata)
        if settings.clickhouse_enabled:
            from .clickhouse_repositories import (
                ClickHouseDatabase,
                ClickHouseMarketBarRepository,
            )
            from .clickhouse_shadow import ShadowMarketBarRepository
            from .clickhouse_canonical import CanonicalMarketBarBuilder
            from .clickhouse_writer import LocalClickHouseSpool, ReliableClickHouseWriter

            clickhouse = ClickHouseDatabase(
                settings.clickhouse_host, settings.clickhouse_port,
                settings.clickhouse_database, settings.clickhouse_username,
                settings.clickhouse_password, settings.clickhouse_secure,
                settings.clickhouse_connect_timeout, settings.clickhouse_read_timeout,
            )
            clickhouse.open()
            resources.append(clickhouse)
            spool = LocalClickHouseSpool(
                settings.clickhouse_spool_path, settings.storage_root,
                settings.clickhouse_spool_quota_bytes,
                settings.clickhouse_spool_warning_ratio,
            )
            from .spool_operator import SpoolOperator
            SpoolOperator(spool).migrate_legacy(1000)
            clickhouse_repository = ClickHouseMarketBarRepository(clickhouse)
            writer = ReliableClickHouseWriter(
                clickhouse_repository, spool,
                settings.clickhouse_batch_size,
            )
            canonical = CanonicalMarketBarBuilder(
                clickhouse_repository, writer, settings.clickhouse_source_priority,
                settings.clickhouse_canonical_rel_tol,
                settings.clickhouse_canonical_abs_tol,
            )
            market_bars = ShadowMarketBarRepository(
                warehouse, writer, canonical,
                settings.market_bar_read_backend == "clickhouse_canonical",
                settings.raw_market_bar_read_backend == "clickhouse_raw",
                settings.clickhouse_auto_canonical,
                settings.clickhouse_auto_canonical_limit,
            )
            if settings.clickhouse_background_canonical:
                from .clickhouse_scheduler import BackgroundCanonicalScheduler

                scheduler_clickhouse = ClickHouseDatabase(
                    settings.clickhouse_host, settings.clickhouse_port,
                    settings.clickhouse_database, settings.clickhouse_username,
                    settings.clickhouse_password, settings.clickhouse_secure,
                    settings.clickhouse_connect_timeout, settings.clickhouse_read_timeout,
                )
                scheduler_clickhouse.open()
                resources.append(scheduler_clickhouse)
                scheduler_repository = ClickHouseMarketBarRepository(scheduler_clickhouse)
                scheduler_writer = ReliableClickHouseWriter(
                    scheduler_repository, spool, settings.clickhouse_batch_size,
                )
                scheduler_canonical = CanonicalMarketBarBuilder(
                    scheduler_repository, scheduler_writer,
                    settings.clickhouse_source_priority,
                    settings.clickhouse_canonical_rel_tol,
                    settings.clickhouse_canonical_abs_tol,
                )
                scheduler = BackgroundCanonicalScheduler(
                    scheduler_canonical, spool, settings.clickhouse_auto_canonical_limit,
                    settings.clickhouse_scheduler_queue_cap,
                    settings.clickhouse_scheduler_scan_limit,
                    settings.clickhouse_scheduler_poll_seconds,
                    settings.clickhouse_scheduler_backoff_base_seconds,
                    settings.clickhouse_scheduler_backoff_max_seconds,
                    settings.clickhouse_scheduler_max_attempts,
                )
                resources.append(scheduler)
                market_bars.background_scheduler = scheduler
                writer.on_raw_replayed = scheduler.enqueue_replayed_rows
    except Exception:
        RepositoryResources(resources).close()
        raise
    return Repositories(
        metadata=metadata, fundamentals=fundamentals, market_bars=market_bars,
        artifacts=artifacts,
    ), RepositoryResources(resources)
