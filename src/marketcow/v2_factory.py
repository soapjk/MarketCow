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
from .telemetry import Telemetry


@dataclass(frozen=True)
class V2FactoryDependencies:
    """Injectable constructors keep startup order and failure cleanup testable."""

    postgres_database: Callable[..., Any] = PostgresDatabase
    postgres_repository: Callable[..., Any] = PostgresRepository
    clickhouse_database: Callable[..., Any] = ClickHouseDatabase
    clickhouse_repository: Callable[..., Any] = ClickHouseMarketBarRepository
    telemetry: Callable[..., Any] = Telemetry
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
        self._closed = False

    def close(self) -> None:
        """Idempotently close owned resources in strict reverse creation order."""
        if self._closed:
            return
        self._closed = True
        errors = []
        for resource in (
            self.canonical_scheduler,
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
            settings.postgres_dsn, settings.postgres_schema
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
        canonical_builder = deps.canonical_builder(
            market_bars,
            writer,
            source_priority=settings.clickhouse_source_priority,
            rel_tol=settings.clickhouse_canonical_rel_tol,
            abs_tol=settings.clickhouse_canonical_abs_tol,
        )
        canonical_scheduler = deps.canonical_scheduler(
            settings.clickhouse_background_canonical,
            builder=canonical_builder,
            spool=spool,
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
