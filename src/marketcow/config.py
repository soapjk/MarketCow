from __future__ import annotations

import ipaddress
import os
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from dotenv import load_dotenv

PROFILES = frozenset({"production", "development", "test"})


@dataclass(frozen=True)
class Settings:
    raw_path: Path
    storage_root: Path
    allowed_root: Path | None
    postgres_dsn: str
    clickhouse_password: str
    host: str = "127.0.0.1"
    port: int = 8790
    profile: str = "production"
    postgres_dsn_ref: str = "MARKETCOW_POSTGRES_DSN"
    postgres_schema: str = "marketcow_production"
    postgres_connect_timeout: float = 2.0
    postgres_read_timeout: float = 5.0
    clickhouse_host: str = "127.0.0.1"
    clickhouse_port: int = 8123
    clickhouse_database: str = "marketcow_production"
    clickhouse_username: str = "default"
    clickhouse_password_ref: str = "MARKETCOW_CLICKHOUSE_PASSWORD"
    clickhouse_secure: bool = False
    clickhouse_connect_timeout: float = 2.0
    clickhouse_read_timeout: float = 5.0
    clickhouse_batch_size: int = 5000
    clickhouse_spool_path: Path = Path("data-production/spool/clickhouse")
    clickhouse_source_priority: tuple[str, ...] = (
        "longport", "tushare", "sina", "eastmoney", "yahoo_chart", "baostock"
    )
    clickhouse_canonical_rel_tol: float = 1e-6
    clickhouse_canonical_abs_tol: float = 1e-9
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
    tushare_token: str = ""
    tushare_base_url: str = "https://fastapic.stockai888.top"
    tushare_realtime_url: str = "https://realtime.stockai888.top"
    tushare_min_interval: float = 0.5
    longport_app_key: str = ""
    longport_app_secret: str = ""
    longport_access_token: str = ""
    longport_enable_overnight: bool = False
    quote_cache_ttl_seconds: float = 60.0
    quote_stale_max_seconds: float = 604800.0
    quote_refresh_workers: int = 8
    quote_persistence_queue_size: int = 256
    quote_persistence_shutdown_seconds: float = 5.0
    sec_user_agent: str = "MarketCow toczx@outlook.com"

    @classmethod
    def from_env(cls, profile: str | None = None) -> "Settings":
        profile = (profile or os.getenv("MARKETCOW_PROFILE", "production")).strip().lower()
        if profile not in PROFILES:
            raise ValueError("MARKETCOW_PROFILE must be production, development or test")
        load_dotenv(Path.cwd() / f".env.{profile}", override=False)
        load_dotenv(Path.cwd() / ".env", override=False)
        suffix = {"production": "production", "development": "development", "test": "test"}[profile]
        default_port = {"production": "8790", "development": "8792", "test": "8793"}[profile]
        root = Path(os.getenv("MARKETCOW_HOME", str(Path.cwd() / f"data-{suffix}"))).expanduser()
        allowed_value = os.getenv("MARKETCOW_ALLOWED_ROOT", "").strip()
        dsn_ref = os.getenv("MARKETCOW_POSTGRES_DSN_REF", "MARKETCOW_POSTGRES_DSN").strip()
        password_ref = os.getenv(
            "MARKETCOW_CLICKHOUSE_PASSWORD_REF", "MARKETCOW_CLICKHOUSE_PASSWORD"
        ).strip()
        return cls(
            raw_path=Path(os.getenv("MARKETCOW_RAW", str(root / "raw"))).expanduser(),
            storage_root=root,
            allowed_root=Path(allowed_value).expanduser() if allowed_value else None,
            postgres_dsn=os.getenv(dsn_ref, "").strip(),
            clickhouse_password=os.getenv(password_ref, ""),
            host=os.getenv("MARKETCOW_HOST", "127.0.0.1"),
            port=int(os.getenv("MARKETCOW_PORT", default_port)),
            profile=profile,
            postgres_dsn_ref=dsn_ref,
            postgres_schema=os.getenv("MARKETCOW_POSTGRES_SCHEMA", f"marketcow_{suffix}").strip(),
            postgres_connect_timeout=float(os.getenv("MARKETCOW_POSTGRES_CONNECT_TIMEOUT", "2.0")),
            postgres_read_timeout=float(os.getenv("MARKETCOW_POSTGRES_READ_TIMEOUT", "5.0")),
            clickhouse_host=os.getenv("MARKETCOW_CLICKHOUSE_HOST", "127.0.0.1").strip(),
            clickhouse_port=int(os.getenv("MARKETCOW_CLICKHOUSE_PORT", "8123")),
            clickhouse_database=os.getenv("MARKETCOW_CLICKHOUSE_DATABASE", f"marketcow_{suffix}").strip(),
            clickhouse_username=os.getenv("MARKETCOW_CLICKHOUSE_USERNAME", "default").strip(),
            clickhouse_password_ref=password_ref,
            clickhouse_secure=_bool_env("MARKETCOW_CLICKHOUSE_SECURE", False),
            clickhouse_connect_timeout=float(os.getenv("MARKETCOW_CLICKHOUSE_CONNECT_TIMEOUT", "2.0")),
            clickhouse_read_timeout=float(os.getenv("MARKETCOW_CLICKHOUSE_READ_TIMEOUT", "5.0")),
            clickhouse_batch_size=int(os.getenv("MARKETCOW_CLICKHOUSE_BATCH_SIZE", "5000")),
            clickhouse_spool_path=Path(os.getenv(
                "MARKETCOW_CLICKHOUSE_SPOOL", str(root / "spool/clickhouse")
            )).expanduser(),
            clickhouse_source_priority=tuple(x.strip() for x in os.getenv(
                "MARKETCOW_CLICKHOUSE_SOURCE_PRIORITY",
                "longport,tushare,sina,eastmoney,yahoo_chart,baostock",
            ).split(",") if x.strip()),
            clickhouse_canonical_rel_tol=float(os.getenv("MARKETCOW_CLICKHOUSE_CANONICAL_REL_TOL", "1e-6")),
            clickhouse_canonical_abs_tol=float(os.getenv("MARKETCOW_CLICKHOUSE_CANONICAL_ABS_TOL", "1e-9")),
            clickhouse_auto_canonical=_bool_env("MARKETCOW_CLICKHOUSE_AUTO_CANONICAL", False),
            clickhouse_auto_canonical_limit=int(os.getenv("MARKETCOW_CLICKHOUSE_AUTO_CANONICAL_LIMIT", "50000")),
            clickhouse_background_canonical=_bool_env("MARKETCOW_CLICKHOUSE_BACKGROUND_CANONICAL", False),
            clickhouse_scheduler_queue_cap=int(os.getenv("MARKETCOW_CLICKHOUSE_SCHEDULER_QUEUE_CAP", "1000")),
            clickhouse_scheduler_scan_limit=int(os.getenv("MARKETCOW_CLICKHOUSE_SCHEDULER_SCAN_LIMIT", "100")),
            clickhouse_scheduler_poll_seconds=float(os.getenv("MARKETCOW_CLICKHOUSE_SCHEDULER_POLL_SECONDS", "1.0")),
            clickhouse_scheduler_backoff_base_seconds=float(os.getenv("MARKETCOW_CLICKHOUSE_SCHEDULER_BACKOFF_BASE_SECONDS", "1.0")),
            clickhouse_scheduler_backoff_max_seconds=float(os.getenv("MARKETCOW_CLICKHOUSE_SCHEDULER_BACKOFF_MAX_SECONDS", "60.0")),
            clickhouse_scheduler_max_attempts=int(os.getenv("MARKETCOW_CLICKHOUSE_SCHEDULER_MAX_ATTEMPTS", "10")),
            clickhouse_spool_quota_bytes=int(os.getenv("MARKETCOW_CLICKHOUSE_SPOOL_QUOTA_BYTES", "1073741824")),
            clickhouse_spool_warning_ratio=float(os.getenv("MARKETCOW_CLICKHOUSE_SPOOL_WARNING_RATIO", "0.8")),
            market_bar_cache_freshness_seconds=int(os.getenv("MARKETCOW_MARKET_BAR_CACHE_FRESHNESS_SECONDS", "900")),
            market_bar_cursor_secret=os.getenv("MARKETCOW_MARKET_BAR_CURSOR_SECRET", ""),
            market_bar_cursor_ttl_seconds=int(os.getenv("MARKETCOW_MARKET_BAR_CURSOR_TTL_SECONDS", "3600")),
            tushare_token=os.getenv("TUSHARE_TOKEN", ""),
            tushare_base_url=os.getenv("TUSHARE_BASE_URL", "https://fastapic.stockai888.top").rstrip("/"),
            tushare_realtime_url=os.getenv("TUSHARE_REALTIME_URL", "https://realtime.stockai888.top").rstrip("/"),
            tushare_min_interval=float(os.getenv("TUSHARE_MIN_INTERVAL", "0.5")),
            longport_app_key=os.getenv(
                "MARKETCOW_LONGPORT_APP_KEY",
                os.getenv("LONGBRIDGE_APP_KEY", os.getenv("LONGPORT_APP_KEY", "")),
            ).strip(),
            longport_app_secret=os.getenv(
                "MARKETCOW_LONGPORT_APP_SECRET",
                os.getenv("LONGBRIDGE_APP_SECRET", os.getenv("LONGPORT_APP_SECRET", "")),
            ).strip(),
            longport_access_token=os.getenv(
                "MARKETCOW_LONGPORT_ACCESS_TOKEN",
                os.getenv("LONGBRIDGE_ACCESS_TOKEN", os.getenv("LONGPORT_ACCESS_TOKEN", "")),
            ).strip(),
            longport_enable_overnight=_bool_env(
                "MARKETCOW_LONGPORT_ENABLE_OVERNIGHT",
                _bool_env("LONGBRIDGE_ENABLE_OVERNIGHT", _bool_env("LONGPORT_ENABLE_OVERNIGHT", False)),
            ),
            quote_cache_ttl_seconds=float(os.getenv("MARKETCOW_QUOTE_CACHE_TTL_SECONDS", "60")),
            quote_stale_max_seconds=float(os.getenv("MARKETCOW_QUOTE_STALE_MAX_SECONDS", "604800")),
            quote_refresh_workers=int(os.getenv("MARKETCOW_QUOTE_REFRESH_WORKERS", "8")),
            quote_persistence_queue_size=int(os.getenv(
                "MARKETCOW_QUOTE_PERSISTENCE_QUEUE_SIZE", "256"
            )),
            quote_persistence_shutdown_seconds=float(os.getenv(
                "MARKETCOW_QUOTE_PERSISTENCE_SHUTDOWN_SECONDS", "5.0"
            )),
            sec_user_agent=os.getenv(
                "MARKETCOW_SEC_USER_AGENT", "MarketCow toczx@outlook.com"
            ).strip(),
        )

    def validate_runtime_isolation(self) -> None:
        self.validate_preflight()

    def validate_preflight(self) -> None:
        if self.profile not in PROFILES:
            raise ValueError("runtime profile is invalid")
        if not self._loopback(self.host):
            raise ValueError("service host must be loopback")
        if self.profile == "production" and self.port != 8790:
            raise ValueError("production profile must use port 8790")
        if self.profile != "production" and self.port == 8790:
            raise ValueError("local profile must not use production port 8790")
        if self.allowed_root is None:
            raise ValueError("runtime requires an explicit allowed root")
        allowed = self.allowed_root.resolve()
        root = self.storage_root.resolve()
        if root != allowed and allowed not in root.parents:
            raise ValueError("storage root escapes its allowed root")
        if not root.name.lower().endswith(self.profile):
            raise ValueError("storage root must match the active profile")
        for path, label in ((self.raw_path, "raw"), (self.clickhouse_spool_path, "spool")):
            resolved = path.resolve()
            if resolved != root and root not in resolved.parents:
                raise ValueError(f"{label} path escapes the storage root")
        if not self._secret_reference(self.postgres_dsn_ref) or not self.postgres_dsn:
            raise ValueError("PostgreSQL DSN environment reference is missing")
        if not self._secret_reference(self.clickhouse_password_ref) or not self.clickhouse_password.strip():
            raise ValueError("ClickHouse credential environment reference is missing")
        parsed = urlsplit(self.postgres_dsn)
        if parsed.scheme not in {"postgres", "postgresql"} or not parsed.hostname or not self._loopback(parsed.hostname):
            raise ValueError("PostgreSQL target must be a loopback PostgreSQL URI")
        suffix = f"_{self.profile}"
        if not parsed.path.lstrip("/").lower().endswith(suffix):
            raise ValueError("PostgreSQL database must match the active profile")
        if not self.postgres_schema.lower().endswith(suffix):
            raise ValueError("PostgreSQL schema must match the active profile")
        if not self._loopback(self.clickhouse_host) or not self.clickhouse_database.lower().endswith(suffix):
            raise ValueError("ClickHouse target must be loopback and match the active profile")
        for value in (self.postgres_connect_timeout, self.postgres_read_timeout,
                      self.clickhouse_connect_timeout, self.clickhouse_read_timeout):
            if not 0.1 <= value <= 30:
                raise ValueError("database timeouts must be between 0.1 and 30 seconds")
        if not 1000 <= self.clickhouse_batch_size <= 50000:
            raise ValueError("ClickHouse batch size must be between 1000 and 50000")
        if not self.clickhouse_source_priority or len(set(self.clickhouse_source_priority)) != len(self.clickhouse_source_priority):
            raise ValueError("ClickHouse source priority must be non-empty and unique")
        if not 1 <= self.quote_persistence_queue_size <= 10000:
            raise ValueError("quote persistence queue size must be between 1 and 10000")
        if not 0.1 <= self.quote_persistence_shutdown_seconds <= 30:
            raise ValueError("quote persistence shutdown must be between 0.1 and 30 seconds")

    @staticmethod
    def _loopback(host: str) -> bool:
        try:
            return ipaddress.ip_address(host).is_loopback
        except ValueError:
            return host.lower() == "localhost"

    @staticmethod
    def _secret_reference(value: str) -> bool:
        return bool(re.fullmatch(r"[A-Z][A-Z0-9_]{2,127}", value))


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}
