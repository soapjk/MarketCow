from __future__ import annotations

import ipaddress
import os
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    database_path: Path | None
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
    clickhouse_background_canonical: bool = False
    clickhouse_scheduler_queue_cap: int = 1000
    clickhouse_scheduler_scan_limit: int = 100
    clickhouse_scheduler_poll_seconds: float = 1.0
    clickhouse_scheduler_backoff_base_seconds: float = 1.0
    clickhouse_scheduler_backoff_max_seconds: float = 60.0
    clickhouse_scheduler_max_attempts: int = 10
    clickhouse_spool_quota_bytes: int = 1073741824
    clickhouse_spool_warning_ratio: float = 0.8
    market_bar_cache_freshness_seconds: int = 900
    market_bar_cursor_secret: str = ""
    market_bar_cursor_ttl_seconds: int = 3600
    runtime_architecture: str = "legacy"
    postgres_dsn_ref: str = ""
    clickhouse_password_ref: str = ""
    postgres_connect_timeout: float = 2.0
    postgres_read_timeout: float = 5.0
    v2_allowed_root: Path | None = None
    runtime_config_schema: str = "marketcow.legacy-runtime-config.v1"

    @classmethod
    def from_env(cls, profile: str | None = None) -> "Settings":
        profile = (profile or os.getenv("MARKETCOW_PROFILE", "production")).strip().lower()
        profiles = {"production", "development", "v2-development", "v2-test"}
        if profile not in profiles:
            raise ValueError(
                "MARKETCOW_PROFILE must be production or development, or v2-development or v2-test"
            )
        load_dotenv(Path.cwd() / (".env." + profile), override=False)
        load_dotenv(Path.cwd() / ".env", override=False)
        v2 = profile.startswith("v2-")
        default_data_dir = {
            "production": "data",
            "development": "data-development",
            "v2-development": "data-v2-development",
            "v2-test": "data-v2-test",
        }[profile]
        default_port = {
            "production": "8790", "development": "8791",
            "v2-development": "8792", "v2-test": "8793",
        }[profile]
        data_root = Path(
            os.getenv("MARKETCOW_HOME", str(Path.cwd() / default_data_dir))
        ).expanduser()
        configured_database = os.getenv("MARKETCOW_DB")
        database_path = (
            Path(configured_database).expanduser() if configured_database else
            None if v2 else data_root / "warehouse/market_data.duckdb"
        )
        raw_path = Path(
            os.getenv("MARKETCOW_RAW", str(data_root / "raw"))
        ).expanduser()
        postgres_dsn_ref = os.getenv(
            "MARKETCOW_POSTGRES_DSN_REF", "MARKETCOW_POSTGRES_DSN" if v2 else ""
        ).strip()
        clickhouse_password_ref = os.getenv(
            "MARKETCOW_CLICKHOUSE_PASSWORD_REF",
            "MARKETCOW_CLICKHOUSE_PASSWORD" if v2 else "",
        ).strip()
        v2_allowed_root_value = os.getenv("MARKETCOW_V2_ALLOWED_ROOT", "").strip()
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
            metadata_backend=os.getenv(
                "MARKETCOW_METADATA_BACKEND", "postgres" if v2 else "duckdb"
            ).strip().lower(),
            postgres_dsn=os.getenv(
                postgres_dsn_ref if v2 else "MARKETCOW_POSTGRES_DSN", ""
            ).strip(),
            postgres_schema=os.getenv(
                "MARKETCOW_POSTGRES_SCHEMA",
                "marketcow_test" if profile == "v2-test" else "marketcow_development",
            ).strip(),
            clickhouse_enabled=os.getenv(
                "MARKETCOW_CLICKHOUSE_ENABLED", "true" if v2 else "false"
            ).strip().lower() in {"1", "true", "yes", "on"},
            clickhouse_host=os.getenv("MARKETCOW_CLICKHOUSE_HOST", "127.0.0.1").strip(),
            clickhouse_port=int(os.getenv("MARKETCOW_CLICKHOUSE_PORT", "8123")),
            clickhouse_database=os.getenv(
                "MARKETCOW_CLICKHOUSE_DATABASE",
                "marketcow_test" if profile == "v2-test" else "marketcow_development",
            ).strip(),
            clickhouse_username=os.getenv(
                "MARKETCOW_CLICKHOUSE_USERNAME", "default"
            ).strip(),
            clickhouse_password=os.getenv(
                clickhouse_password_ref if v2 else "MARKETCOW_CLICKHOUSE_PASSWORD", ""
            ),
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
                "MARKETCOW_MARKET_BAR_READ_BACKEND",
                "clickhouse_canonical" if v2 else "duckdb",
            ).strip().lower(),
            raw_market_bar_read_backend=os.getenv(
                "MARKETCOW_RAW_MARKET_BAR_READ_BACKEND",
                "clickhouse_raw" if v2 else "duckdb",
            ).strip().lower(),
            clickhouse_auto_canonical=os.getenv(
                "MARKETCOW_CLICKHOUSE_AUTO_CANONICAL", "false"
            ).strip().lower() in {"1", "true", "yes", "on"},
            clickhouse_auto_canonical_limit=int(os.getenv(
                "MARKETCOW_CLICKHOUSE_AUTO_CANONICAL_LIMIT", "50000"
            )),
            clickhouse_background_canonical=os.getenv(
                "MARKETCOW_CLICKHOUSE_BACKGROUND_CANONICAL", "false"
            ).strip().lower() in {"1", "true", "yes", "on"},
            clickhouse_scheduler_queue_cap=int(os.getenv(
                "MARKETCOW_CLICKHOUSE_SCHEDULER_QUEUE_CAP", "1000")),
            clickhouse_scheduler_scan_limit=int(os.getenv(
                "MARKETCOW_CLICKHOUSE_SCHEDULER_SCAN_LIMIT", "100")),
            clickhouse_scheduler_poll_seconds=float(os.getenv(
                "MARKETCOW_CLICKHOUSE_SCHEDULER_POLL_SECONDS", "1.0")),
            clickhouse_scheduler_backoff_base_seconds=float(os.getenv(
                "MARKETCOW_CLICKHOUSE_SCHEDULER_BACKOFF_BASE_SECONDS", "1.0")),
            clickhouse_scheduler_backoff_max_seconds=float(os.getenv(
                "MARKETCOW_CLICKHOUSE_SCHEDULER_BACKOFF_MAX_SECONDS", "60.0")),
            clickhouse_scheduler_max_attempts=int(os.getenv(
                "MARKETCOW_CLICKHOUSE_SCHEDULER_MAX_ATTEMPTS", "10")),
            clickhouse_spool_quota_bytes=int(os.getenv(
                "MARKETCOW_CLICKHOUSE_SPOOL_QUOTA_BYTES", "1073741824")),
            clickhouse_spool_warning_ratio=float(os.getenv(
                "MARKETCOW_CLICKHOUSE_SPOOL_WARNING_RATIO", "0.8")),
            market_bar_cache_freshness_seconds=int(os.getenv(
                "MARKETCOW_MARKET_BAR_CACHE_FRESHNESS_SECONDS", "900"
            )),
            market_bar_cursor_secret=os.getenv(
                "MARKETCOW_MARKET_BAR_CURSOR_SECRET", ""
            ),
            market_bar_cursor_ttl_seconds=int(os.getenv(
                "MARKETCOW_MARKET_BAR_CURSOR_TTL_SECONDS", "3600"
            )),
            runtime_architecture=(
                "postgres_clickhouse_v2" if v2 else
                os.getenv("MARKETCOW_RUNTIME_ARCHITECTURE", "legacy").strip().lower()
            ),
            postgres_dsn_ref=postgres_dsn_ref,
            clickhouse_password_ref=clickhouse_password_ref,
            postgres_connect_timeout=float(os.getenv(
                "MARKETCOW_POSTGRES_CONNECT_TIMEOUT", "2.0"
            )),
            postgres_read_timeout=float(os.getenv(
                "MARKETCOW_POSTGRES_READ_TIMEOUT", "5.0"
            )),
            v2_allowed_root=(
                Path(v2_allowed_root_value).expanduser()
                if v2 and v2_allowed_root_value else None
            ),
            runtime_config_schema=(
                "marketcow.v2-runtime-config.v1" if v2 else
                "marketcow.legacy-runtime-config.v1"
            ),
        )

    def validate_runtime_isolation(self) -> None:
        if self.profile in {"v2-development", "v2-test"}:
            self.validate_v2_preflight()
            return
        if self.database_path is None:
            raise ValueError("legacy runtime requires a DuckDB database path")
        if not 1 <= self.market_bar_cache_freshness_seconds <= 86400:
            raise ValueError("market bar cache freshness must be between 1 and 86400 seconds")
        if self.market_bar_cursor_secret:
            from .market_bar_cursor import validate_explicit_secret
            validate_explicit_secret(self.market_bar_cursor_secret)
        if not 60 <= self.market_bar_cursor_ttl_seconds <= 86400:
            raise ValueError("market bar cursor TTL must be between 60 and 86400 seconds")
        if not 1 <= self.clickhouse_auto_canonical_limit <= 100000:
            raise ValueError("automatic canonical limit must be between 1 and 100000")
        if self.clickhouse_auto_canonical:
            if self.profile != "development":
                raise ValueError("automatic canonical rebuild is development-only")
            if not self.clickhouse_enabled:
                raise ValueError("automatic canonical rebuild requires MARKETCOW_CLICKHOUSE_ENABLED")
        if self.clickhouse_background_canonical:
            if self.profile != "development":
                raise ValueError("background canonical scheduler is development-only")
            if not self.clickhouse_enabled:
                raise ValueError("background canonical scheduler requires MARKETCOW_CLICKHOUSE_ENABLED")
            if self.clickhouse_auto_canonical:
                raise ValueError("background and synchronous automatic canonical are mutually exclusive")
        if not 1 <= self.clickhouse_scheduler_queue_cap <= 10000:
            raise ValueError("scheduler queue cap must be between 1 and 10000")
        if not 1 <= self.clickhouse_scheduler_scan_limit <= 1000:
            raise ValueError("scheduler scan limit must be between 1 and 1000")
        if not 0.05 <= self.clickhouse_scheduler_poll_seconds <= 60:
            raise ValueError("scheduler poll seconds must be between 0.05 and 60")
        if not (0.01 <= self.clickhouse_scheduler_backoff_base_seconds <=
                self.clickhouse_scheduler_backoff_max_seconds <= 3600):
            raise ValueError("scheduler backoff bounds are invalid")
        if not 1 <= self.clickhouse_scheduler_max_attempts <= 100:
            raise ValueError("scheduler max attempts must be between 1 and 100")
        if not 1048576 <= self.clickhouse_spool_quota_bytes <= 1099511627776:
            raise ValueError("spool quota must be between 1 MiB and 1 TiB")
        if not 0.5 <= self.clickhouse_spool_warning_ratio < 1:
            raise ValueError("spool warning ratio must be between 0.5 and 1")
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

    def validate_v2_preflight(self) -> None:
        """Pure validation: reject unsafe V2 configuration before any resource side effect."""
        if self.profile not in {"v2-development", "v2-test"}:
            raise ValueError("V2 preflight requires a V2 development or test profile")
        if self.runtime_architecture != "postgres_clickhouse_v2":
            raise ValueError("V2 runtime architecture must be postgres_clickhouse_v2")
        if self.runtime_config_schema != "marketcow.v2-runtime-config.v1":
            raise ValueError("V2 runtime configuration schema is invalid")
        if self.database_path is not None:
            raise ValueError("V2 online configuration must not define a DuckDB path")
        if self.metadata_backend != "postgres":
            raise ValueError("V2 metadata backend must be PostgreSQL")
        if not self.clickhouse_enabled:
            raise ValueError("V2 requires ClickHouse")
        if self.market_bar_read_backend != "clickhouse_canonical":
            raise ValueError("V2 canonical reads must use ClickHouse")
        if self.raw_market_bar_read_backend != "clickhouse_raw":
            raise ValueError("V2 raw reads must use ClickHouse")
        if self.port == 8790 or "production" in self.profile:
            raise ValueError("V2 local profile must not use a production identity")
        if not self._loopback(self.host):
            raise ValueError("V2 local service host must be loopback")

        if self.v2_allowed_root is None:
            raise ValueError("V2 requires an explicit allowed root")
        allowed_root = self.v2_allowed_root.resolve()
        root = self.storage_root.resolve()
        if root != allowed_root and allowed_root not in root.parents:
            raise ValueError("V2 storage root escapes its allowed root")
        root_name = root.name.lower()
        expected_suffix = "development" if self.profile == "v2-development" else "test"
        if not root_name.endswith(expected_suffix):
            raise ValueError("V2 storage root must match the isolated profile")
        for path, label in ((self.raw_path, "raw"),
                            (self.clickhouse_spool_path, "ClickHouse spool")):
            resolved = path.resolve()
            if resolved != root and root not in resolved.parents:
                raise ValueError(f"V2 {label} path must stay within the isolated storage root")

        if not self._secret_reference(self.postgres_dsn_ref):
            raise ValueError("V2 PostgreSQL DSN must use a valid environment reference")
        if not self._secret_reference(self.clickhouse_password_ref):
            raise ValueError("V2 ClickHouse credential must use a valid environment reference")
        if not self.postgres_dsn:
            raise ValueError("V2 requires PostgreSQL credentials through its environment reference")
        if not self.clickhouse_password.strip():
            raise ValueError("V2 requires ClickHouse credentials through its environment reference")
        parsed = urlsplit(self.postgres_dsn)
        if parsed.scheme not in {"postgres", "postgresql"} or not parsed.hostname:
            raise ValueError("V2 PostgreSQL target is invalid")
        if not self._loopback(parsed.hostname):
            raise ValueError("V2 PostgreSQL target must be loopback")
        database_suffix = "_development" if self.profile == "v2-development" else "_test"
        postgres_database = parsed.path.lstrip("/").lower()
        if not postgres_database.endswith(database_suffix):
            raise ValueError("V2 PostgreSQL database must match the isolated profile")
        if not self.postgres_schema.lower().endswith(database_suffix):
            raise ValueError("V2 PostgreSQL schema must match the isolated profile")
        if not self._loopback(self.clickhouse_host):
            raise ValueError("V2 ClickHouse target must be loopback")
        if not self.clickhouse_database.lower().endswith(database_suffix):
            raise ValueError("V2 ClickHouse database must match the isolated profile")
        for value, label in (
            (self.postgres_connect_timeout, "PostgreSQL connect timeout"),
            (self.postgres_read_timeout, "PostgreSQL read timeout"),
            (self.clickhouse_connect_timeout, "ClickHouse connect timeout"),
            (self.clickhouse_read_timeout, "ClickHouse read timeout"),
        ):
            if not 0.1 <= value <= 30:
                raise ValueError(f"V2 {label} must be between 0.1 and 30 seconds")

    @staticmethod
    def _loopback(host: str) -> bool:
        try:
            return ipaddress.ip_address(host).is_loopback
        except ValueError:
            return host.lower() == "localhost"

    @staticmethod
    def _secret_reference(value: str) -> bool:
        return bool(re.fullmatch(r"[A-Z][A-Z0-9_]{2,127}", value))
