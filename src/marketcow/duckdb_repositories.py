from __future__ import annotations

from typing import Any, List

from .artifact_store import LocalArtifactStore
from .repositories import Repositories
from .storage import Warehouse


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
    from .telemetry import Telemetry, instrument_duckdb_market_bars
    telemetry = getattr(warehouse, "telemetry", None) or Telemetry(
        clickhouse_enabled=settings.clickhouse_enabled
    )
    instrument_duckdb_market_bars(warehouse, telemetry)
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
            spool.telemetry = telemetry
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
            market_bars.telemetry = spool.telemetry
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
