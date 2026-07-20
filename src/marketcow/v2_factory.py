from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Callable, Mapping

from .clickhouse_canonical import CanonicalMarketBarBuilder
from .clickhouse_repositories import ClickHouseDatabase, ClickHouseMarketBarRepository
from .clickhouse_scheduler import create_canonical_scheduler
from .clickhouse_writer import LocalClickHouseSpool, ReliableClickHouseWriter
from .config import Settings
from .postgres_migrations import POSTGRES_TRANSACTION_DOMAINS
from .postgres_repositories import PostgresDatabase, PostgresRepository
from .v2_observability import V2Telemetry
from .health import V2_HEALTH_SCHEMA


@dataclass(frozen=True)
class V2FactoryDependencies:
    """Injectable constructors keep startup order and failure cleanup testable."""

    postgres_database: Callable[..., Any] = PostgresDatabase
    postgres_repository: Callable[..., Any] = PostgresRepository
    clickhouse_database: Callable[..., Any] = ClickHouseDatabase
    clickhouse_repository: Callable[..., Any] = ClickHouseMarketBarRepository
    telemetry: Callable[..., Any] = V2Telemetry
    spool: Callable[..., Any] = LocalClickHouseSpool
    writer: Callable[..., Any] = ReliableClickHouseWriter
    canonical_builder: Callable[..., Any] = CanonicalMarketBarBuilder
    canonical_scheduler: Callable[..., Any] = create_canonical_scheduler


class V2OnlineRepositories:
    """Own the complete PostgreSQL/ClickHouse V2 online resource graph."""

    def __init__(
        self, *, postgres_database: Any, postgres_repository: Any,
        clickhouse_database: Any, market_bars: Any, telemetry: Any, spool: Any,
        writer: Any, canonical_builder: Any, canonical_scheduler: Any,
        scheduler_clickhouse_database: Any = None,
        scheduler_market_bars: Any = None, scheduler_writer: Any = None,
    ) -> None:
        self.postgres_database = postgres_database
        self.postgres = postgres_repository
        self.transaction_domains: Mapping[str, Any] = MappingProxyType({
            domain: postgres_repository for domain in POSTGRES_TRANSACTION_DOMAINS
        })
        self.clickhouse_database = clickhouse_database
        self.market_bars = market_bars
        self.telemetry = telemetry
        self.spool = spool
        self.writer = writer
        self.canonical_builder = canonical_builder
        self.canonical_scheduler = canonical_scheduler
        self.scheduler_clickhouse_database = scheduler_clickhouse_database
        self.scheduler_market_bars = scheduler_market_bars
        self.scheduler_writer = scheduler_writer
        self._closed = False

    def close(self) -> None:
        """Idempotently close owned resources in strict reverse creation order."""
        if self._closed:
            return
        self._closed = True
        errors = []
        for resource in (
            self.canonical_scheduler,
            self.scheduler_clickhouse_database,
            self.clickhouse_database,
            self.postgres_database,
        ):
            close = getattr(resource, "close", None)
            if close is None:
                continue
            try:
                close()
            except Exception as error:  # close every earlier resource before reporting
                errors.append(error)
        if errors:
            raise RuntimeError("V2 resource shutdown failed") from errors[0]

    @staticmethod
    def _safe_status(call: Callable[[], Any], logical_id: str) -> dict[str, Any]:
        try:
            call()
            return {"status": "healthy", "logical_id": logical_id}
        except Exception:
            return {"status": "unavailable", "logical_id": logical_id,
                    "reason": "dependency_probe_failed"}

    def health_snapshot(self) -> dict[str, Any]:
        """Bounded logical-only dependency snapshot; never exposes connection details."""
        pg = self._safe_status(
            lambda: self.postgres_database.health_probe() or
            (_ for _ in ()).throw(RuntimeError("probe failed")),
            f"postgresql://{self.postgres_database.schema}",
        )
        main = self._safe_status(
            lambda: self.clickhouse_database._require_client().ping() or
            (_ for _ in ()).throw(RuntimeError("ping failed")),
            f"clickhouse://{self.clickhouse_database.database}",
        )
        try:
            wal_raw = self.spool.diagnostics(1000)
            usage = wal_raw.get("quota", {})
            total = max(1, int(usage.get("bytes", 0)) + int(usage.get("free_bytes", 0)))
            wal = {
                "status": "healthy", "pending": int(wal_raw.get("pending", 0)),
                "failed": int(wal_raw.get("failed", 0)),
                "replayed": int(wal_raw.get("replayed", 0)),
                "quarantine": len(self.spool._bounded_files(self.spool.quarantine, 1000)[0]),
                "oldest_pending_lag_seconds": float(
                    wal_raw.get("oldest_pending_lag_seconds", 0)
                ),
                "truncated": bool(wal_raw.get("truncated")),
                "disk_used_ratio": round(1.0 - int(usage.get("free_bytes", 0)) / total, 6),
            }
        except Exception:
            wal = {"status": "unavailable", "reason": "wal_probe_failed"}
        if self.canonical_scheduler is None:
            scheduler = {"status": "disabled", "enabled": False}
            scheduler_ch = {"status": "disabled", "enabled": False}
        else:
            try:
                raw_scheduler = self.canonical_scheduler.diagnostics()
                scheduler = {
                    "status": "healthy", "enabled": True,
                    "paused": bool(raw_scheduler.get("paused")),
                    "thread_alive": bool(raw_scheduler.get("thread_alive")),
                    "pending": int(raw_scheduler.get("pending", 0)),
                    "failed": int(raw_scheduler.get("failed", 0)),
                    "backlog_truncated": bool(raw_scheduler.get("backlog_truncated")),
                    "oldest_lag_seconds": float(
                        raw_scheduler.get("oldest_lag_seconds", 0)
                    ),
                    "invalid": int(raw_scheduler.get("invalid", 0)),
                }
            except Exception:
                scheduler = {"status": "unavailable", "enabled": True,
                             "reason": "canonical_probe_failed"}
            scheduler_ch = self._safe_status(
                lambda: self.scheduler_clickhouse_database._require_client().ping() or
                (_ for _ in ()).throw(RuntimeError("ping failed")),
                f"clickhouse://{self.scheduler_clickhouse_database.database}",
            )
            scheduler_ch["enabled"] = True
        pressure = {"status": "missing"}
        try:
            snapshot = self.telemetry.snapshot()
            values = {}
            for metric in snapshot.get("metrics", []):
                if metric.get("name") == "clickhouse_pressure":
                    values[metric.get("labels", {}).get("kind")] = metric.get("value")
            if {"merge_queue", "disk_used_ratio"} <= values.keys():
                pressure = {"status": "observed", "merge_queue": values["merge_queue"],
                            "disk_used_ratio": values["disk_used_ratio"]}
        except Exception:
            pressure = {"status": "missing"}
        return {"schema": V2_HEALTH_SCHEMA, "components": {
            "postgresql": pg, "clickhouse_main": main, "authoritative_wal": wal,
            "canonical_scheduler": scheduler, "clickhouse_scheduler": scheduler_ch,
            "clickhouse_pressure": pressure,
        }}

    def __enter__(self) -> "V2OnlineRepositories":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


def create_v2_online_repositories(
    settings: Settings, dependencies: V2FactoryDependencies | None = None,
) -> V2OnlineRepositories:
    """Build the sole V2 online PG/CH graph after a side-effect-free preflight."""
    settings.validate_v2_preflight()
    deps = dependencies or V2FactoryDependencies()
    created: list[Any] = []
    try:
        postgres_database = deps.postgres_database(
            settings.postgres_dsn, settings.postgres_schema,
            connect_timeout=settings.postgres_connect_timeout,
            read_timeout=settings.postgres_read_timeout,
        )
        created.append(postgres_database)
        postgres_database.open()
        postgres_repository = deps.postgres_repository(postgres_database)

        clickhouse_database = deps.clickhouse_database(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            database=settings.clickhouse_database,
            username=settings.clickhouse_username,
            password=settings.clickhouse_password,
            secure=settings.clickhouse_secure,
            connect_timeout=settings.clickhouse_connect_timeout,
            read_timeout=settings.clickhouse_read_timeout,
        )
        created.append(clickhouse_database)
        clickhouse_database.open()
        market_bars = deps.clickhouse_repository(clickhouse_database)

        telemetry = deps.telemetry(clickhouse_enabled=True)
        spool = deps.spool(
            settings.clickhouse_spool_path,
            settings.storage_root,
            quota_bytes=settings.clickhouse_spool_quota_bytes,
            quota_warning_ratio=settings.clickhouse_spool_warning_ratio,
        )
        spool.telemetry = telemetry
        writer = deps.writer(
            market_bars, spool, batch_size=settings.clickhouse_batch_size
        )
        scheduler_clickhouse_database = None
        scheduler_market_bars = None
        scheduler_writer = None
        canonical_builder = None
        if settings.clickhouse_background_canonical:
            scheduler_clickhouse_database = deps.clickhouse_database(
                host=settings.clickhouse_host,
                port=settings.clickhouse_port,
                database=settings.clickhouse_database,
                username=settings.clickhouse_username,
                password=settings.clickhouse_password,
                secure=settings.clickhouse_secure,
                connect_timeout=settings.clickhouse_connect_timeout,
                read_timeout=settings.clickhouse_read_timeout,
            )
            created.append(scheduler_clickhouse_database)
            scheduler_clickhouse_database.open()
            scheduler_market_bars = deps.clickhouse_repository(
                scheduler_clickhouse_database
            )
            scheduler_writer = deps.writer(
                scheduler_market_bars, spool, batch_size=settings.clickhouse_batch_size
            )
            canonical_builder = deps.canonical_builder(
                scheduler_market_bars,
                scheduler_writer,
                source_priority=settings.clickhouse_source_priority,
                rel_tol=settings.clickhouse_canonical_rel_tol,
                abs_tol=settings.clickhouse_canonical_abs_tol,
            )
        else:
            canonical_builder = deps.canonical_builder(
                market_bars,
                writer,
                source_priority=settings.clickhouse_source_priority,
                rel_tol=settings.clickhouse_canonical_rel_tol,
                abs_tol=settings.clickhouse_canonical_abs_tol,
            )
        canonical_scheduler = deps.canonical_scheduler(
            settings.clickhouse_background_canonical,
            builder=canonical_builder, spool=spool,
            canonical_limit=settings.clickhouse_auto_canonical_limit,
            queue_cap=settings.clickhouse_scheduler_queue_cap,
            scan_limit=settings.clickhouse_scheduler_scan_limit,
            poll_seconds=settings.clickhouse_scheduler_poll_seconds,
            backoff_base_seconds=settings.clickhouse_scheduler_backoff_base_seconds,
            backoff_max_seconds=settings.clickhouse_scheduler_backoff_max_seconds,
            max_attempts=settings.clickhouse_scheduler_max_attempts,
        )
        if canonical_scheduler is not None:
            created.append(canonical_scheduler)
            canonical_scheduler.bind_writer(writer)
        return V2OnlineRepositories(
            postgres_database=postgres_database,
            postgres_repository=postgres_repository,
            clickhouse_database=clickhouse_database,
            market_bars=market_bars,
            telemetry=telemetry,
            spool=spool,
            writer=writer,
            canonical_builder=canonical_builder,
            canonical_scheduler=canonical_scheduler,
            scheduler_clickhouse_database=scheduler_clickhouse_database,
            scheduler_market_bars=scheduler_market_bars,
            scheduler_writer=scheduler_writer,
        )
    except Exception:
        for resource in reversed(created):
            close = getattr(resource, "close", None)
            if close is not None:
                try:
                    close()
                except Exception:
                    pass
        raise
