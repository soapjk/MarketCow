from __future__ import annotations

import ipaddress
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    database_path: Path
    raw_path: Path
    host: str = "127.0.0.1"
    port: int = 8790
    tushare_token: str = ""
    tushare_base_url: str = "https://fastapic.stockai888.top"
    tushare_realtime_url: str = "https://realtime.stockai888.top"
    tushare_min_interval: float = 0.5
    profile: str = "production"
    metadata_backend: str = "duckdb"
    postgres_dsn: str = ""
    postgres_schema: str = "marketcow_development"
    clickhouse_enabled: bool = False
    clickhouse_host: str = "127.0.0.1"
    clickhouse_port: int = 8123
    clickhouse_database: str = "marketcow_development"
    clickhouse_username: str = "default"
    clickhouse_password: str = ""
    clickhouse_secure: bool = False
    clickhouse_batch_size: int = 5000
    clickhouse_spool_path: Path = Path("data-development/spool/clickhouse")
    storage_root: Path = Path("data")
    clickhouse_connect_timeout: float = 2.0
    clickhouse_read_timeout: float = 5.0
    clickhouse_source_priority: tuple[str, ...] = (
        "tushare", "sina", "eastmoney", "yahoo_chart", "baostock"
    )
    clickhouse_canonical_rel_tol: float = 1e-6
    clickhouse_canonical_abs_tol: float = 1e-9
    market_bar_read_backend: str = "duckdb"
    raw_market_bar_read_backend: str = "duckdb"
    clickhouse_auto_canonical: bool = False
    clickhouse_auto_canonical_limit: int = 50000
    market_bar_cache_freshness_seconds: int = 900

    @classmethod
    def from_env(cls, profile: str | None = None) -> "Settings":
        profile = (profile or os.getenv("MARKETCOW_PROFILE", "production")).strip().lower()
        if profile not in {"production", "development"}:
            raise ValueError("MARKETCOW_PROFILE must be production or development")
        load_dotenv(Path.cwd() / (".env." + profile), override=False)
        load_dotenv(Path.cwd() / ".env", override=False)
        default_data_dir = "data" if profile == "production" else "data-development"
        default_port = "8790" if profile == "production" else "8791"
        data_root = Path(
            os.getenv("MARKETCOW_HOME", str(Path.cwd() / default_data_dir))
        ).expanduser()
        database_path = Path(
            os.getenv("MARKETCOW_DB", str(data_root / "warehouse/market_data.duckdb"))
        ).expanduser()
        raw_path = Path(
            os.getenv("MARKETCOW_RAW", str(data_root / "raw"))
        ).expanduser()
        return cls(
            database_path=database_path,
            raw_path=raw_path,
            host=os.getenv("MARKETCOW_HOST", "127.0.0.1"),
            port=int(os.getenv("MARKETCOW_PORT", default_port)),
            tushare_token=os.getenv("TUSHARE_TOKEN", ""),
            tushare_base_url=os.getenv("TUSHARE_BASE_URL", "https://fastapic.stockai888.top").rstrip("/"),
            tushare_realtime_url=os.getenv("TUSHARE_REALTIME_URL", "https://realtime.stockai888.top").rstrip("/"),
            tushare_min_interval=float(os.getenv("TUSHARE_MIN_INTERVAL", "0.5")),
            profile=profile,
            metadata_backend=os.getenv("MARKETCOW_METADATA_BACKEND", "duckdb").strip().lower(),
            postgres_dsn=os.getenv("MARKETCOW_POSTGRES_DSN", "").strip(),
            postgres_schema=os.getenv(
                "MARKETCOW_POSTGRES_SCHEMA", "marketcow_development"
            ).strip(),
            clickhouse_enabled=os.getenv(
                "MARKETCOW_CLICKHOUSE_ENABLED", "false"
            ).strip().lower() in {"1", "true", "yes", "on"},
            clickhouse_host=os.getenv("MARKETCOW_CLICKHOUSE_HOST", "127.0.0.1").strip(),
            clickhouse_port=int(os.getenv("MARKETCOW_CLICKHOUSE_PORT", "8123")),
            clickhouse_database=os.getenv(
                "MARKETCOW_CLICKHOUSE_DATABASE", "marketcow_development"
            ).strip(),
            clickhouse_username=os.getenv(
                "MARKETCOW_CLICKHOUSE_USERNAME", "default"
            ).strip(),
            clickhouse_password=os.getenv("MARKETCOW_CLICKHOUSE_PASSWORD", ""),
            clickhouse_secure=os.getenv(
                "MARKETCOW_CLICKHOUSE_SECURE", "false"
            ).strip().lower() in {"1", "true", "yes", "on"},
            clickhouse_batch_size=int(os.getenv("MARKETCOW_CLICKHOUSE_BATCH_SIZE", "5000")),
            clickhouse_spool_path=Path(os.getenv(
                "MARKETCOW_CLICKHOUSE_SPOOL", str(data_root / "spool/clickhouse")
            )).expanduser(),
            storage_root=data_root,
            clickhouse_connect_timeout=float(os.getenv(
                "MARKETCOW_CLICKHOUSE_CONNECT_TIMEOUT", "2.0"
            )),
            clickhouse_read_timeout=float(os.getenv(
                "MARKETCOW_CLICKHOUSE_READ_TIMEOUT", "5.0"
            )),
            clickhouse_source_priority=tuple(value.strip() for value in os.getenv(
                "MARKETCOW_CLICKHOUSE_SOURCE_PRIORITY",
                "tushare,sina,eastmoney,yahoo_chart,baostock",
            ).split(",") if value.strip()),
            clickhouse_canonical_rel_tol=float(os.getenv(
                "MARKETCOW_CLICKHOUSE_CANONICAL_REL_TOL", "1e-6"
            )),
            clickhouse_canonical_abs_tol=float(os.getenv(
                "MARKETCOW_CLICKHOUSE_CANONICAL_ABS_TOL", "1e-9"
            )),
            market_bar_read_backend=os.getenv(
                "MARKETCOW_MARKET_BAR_READ_BACKEND", "duckdb"
            ).strip().lower(),
            raw_market_bar_read_backend=os.getenv(
                "MARKETCOW_RAW_MARKET_BAR_READ_BACKEND", "duckdb"
            ).strip().lower(),
            clickhouse_auto_canonical=os.getenv(
                "MARKETCOW_CLICKHOUSE_AUTO_CANONICAL", "false"
            ).strip().lower() in {"1", "true", "yes", "on"},
            clickhouse_auto_canonical_limit=int(os.getenv(
                "MARKETCOW_CLICKHOUSE_AUTO_CANONICAL_LIMIT", "50000"
            )),
            market_bar_cache_freshness_seconds=int(os.getenv(
                "MARKETCOW_MARKET_BAR_CACHE_FRESHNESS_SECONDS", "900"
            )),
        )

    def validate_runtime_isolation(self) -> None:
        if not 1 <= self.market_bar_cache_freshness_seconds <= 86400:
            raise ValueError("market bar cache freshness must be between 1 and 86400 seconds")
        if not 1 <= self.clickhouse_auto_canonical_limit <= 100000:
            raise ValueError("automatic canonical limit must be between 1 and 100000")
        if self.clickhouse_auto_canonical:
            if self.profile != "development":
                raise ValueError("automatic canonical rebuild is development-only")
            if not self.clickhouse_enabled:
                raise ValueError("automatic canonical rebuild requires MARKETCOW_CLICKHOUSE_ENABLED")
        if self.market_bar_read_backend not in {"duckdb", "clickhouse_canonical"}:
            raise ValueError(
                "MARKETCOW_MARKET_BAR_READ_BACKEND must be duckdb or clickhouse_canonical"
            )
        if self.market_bar_read_backend == "clickhouse_canonical":
            if self.profile != "development":
                raise ValueError("ClickHouse canonical reads are development-only")
            if not self.clickhouse_enabled:
                raise ValueError(
                    "ClickHouse canonical reads require MARKETCOW_CLICKHOUSE_ENABLED"
                )
        if self.raw_market_bar_read_backend not in {"duckdb", "clickhouse_raw"}:
            raise ValueError(
                "MARKETCOW_RAW_MARKET_BAR_READ_BACKEND must be duckdb or clickhouse_raw"
            )
        if self.raw_market_bar_read_backend == "clickhouse_raw":
            if self.profile != "development":
                raise ValueError("ClickHouse raw reads are development-only")
            if not self.clickhouse_enabled:
                raise ValueError(
                    "ClickHouse raw reads require MARKETCOW_CLICKHOUSE_ENABLED"
                )
        if self.metadata_backend not in {"duckdb", "postgres"}:
            raise ValueError("MARKETCOW_METADATA_BACKEND must be duckdb or postgres")
        if self.metadata_backend == "postgres":
            if self.profile != "development":
                raise ValueError("PostgreSQL metadata backend is development-only during Stage 1")
            if not self.postgres_dsn:
                raise ValueError("MARKETCOW_POSTGRES_DSN is required for PostgreSQL metadata")
            if not self.postgres_schema.endswith(("_development", "_test")):
                raise ValueError("development PostgreSQL schema must end in _development or _test")
        if self.clickhouse_enabled:
            if self.profile != "development":
                raise ValueError("ClickHouse is development-only during the storage foundation stage")
            try:
                loopback = ipaddress.ip_address(self.clickhouse_host).is_loopback
            except ValueError:
                loopback = self.clickhouse_host.lower() == "localhost"
            if not loopback:
                raise ValueError("development ClickHouse host must be loopback")
            if not self.clickhouse_database.endswith(("_development", "_test")):
                raise ValueError(
                    "development ClickHouse database must end in _development or _test"
                )
            if not 1000 <= self.clickhouse_batch_size <= 50000:
                raise ValueError("ClickHouse batch size must be between 1000 and 50000")
            if not 0.1 <= self.clickhouse_connect_timeout <= 30:
                raise ValueError("ClickHouse connect timeout must be between 0.1 and 30 seconds")
            if not 0.1 <= self.clickhouse_read_timeout <= 30:
                raise ValueError("ClickHouse read timeout must be between 0.1 and 30 seconds")
            if not self.clickhouse_source_priority or len(set(
                self.clickhouse_source_priority
            )) != len(self.clickhouse_source_priority):
                raise ValueError("ClickHouse source priority must be non-empty and unique")
            if not 0 <= self.clickhouse_canonical_rel_tol <= 0.01:
                raise ValueError("canonical relative tolerance must be between 0 and 0.01")
            if not 0 <= self.clickhouse_canonical_abs_tol <= 1:
                raise ValueError("canonical absolute tolerance must be between 0 and 1")
            storage_root = self.storage_root.resolve()
            root_name = storage_root.name.lower()
            if "development" not in root_name and not root_name.endswith(("_test", "-test")):
                raise ValueError(
                    "ClickHouse storage root must be explicitly named development or test"
                )
            spool = self.clickhouse_spool_path.resolve()
            if spool != storage_root and storage_root not in spool.parents:
                raise ValueError("ClickHouse spool must stay within development storage root")
        if self.profile != "development":
            return
        production_db = (Path.cwd() / "data/warehouse/market_data.duckdb").resolve()
        production_raw = (Path.cwd() / "data/raw").resolve()
        if self.port == 8790:
            raise ValueError("development profile must not use the production port 8790")
        if self.database_path.resolve() == production_db or self.raw_path.resolve() == production_raw:
            raise ValueError("development profile must not use the default production data paths")
