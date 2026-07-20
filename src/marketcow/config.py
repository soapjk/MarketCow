from __future__ import annotations

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
        )

    def validate_runtime_isolation(self) -> None:
        if self.profile != "development":
            return
        production_db = (Path.cwd() / "data/warehouse/market_data.duckdb").resolve()
        production_raw = (Path.cwd() / "data/raw").resolve()
        if self.port == 8790:
            raise ValueError("development profile must not use the production port 8790")
        if self.database_path.resolve() == production_db or self.raw_path.resolve() == production_raw:
            raise ValueError("development profile must not use the default production data paths")
