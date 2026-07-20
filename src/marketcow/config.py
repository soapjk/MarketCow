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
        )

    def validate_runtime_isolation(self) -> None:
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
            production_spool = (Path.cwd() / "data").resolve()
            spool = self.clickhouse_spool_path.resolve()
            if spool == production_spool or production_spool in spool.parents:
                raise ValueError("development ClickHouse spool must not use production data")
        if self.profile != "development":
            return
        production_db = (Path.cwd() / "data/warehouse/market_data.duckdb").resolve()
        production_raw = (Path.cwd() / "data/raw").resolve()
        if self.port == 8790:
            raise ValueError("development profile must not use the production port 8790")
        if self.database_path.resolve() == production_db or self.raw_path.resolve() == production_raw:
            raise ValueError("development profile must not use the default production data paths")
