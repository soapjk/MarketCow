from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    database_path: Path
    raw_path: Path
    host: str = "127.0.0.1"
    port: int = 8790

    @classmethod
    def from_env(cls) -> "Settings":
        data_root = Path(
            os.getenv("MARKETCOW_HOME", str(Path.cwd() / "data"))
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
            port=int(os.getenv("MARKETCOW_PORT", "8790")),
        )
