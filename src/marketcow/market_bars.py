from __future__ import annotations

from typing import Any, Dict, List, Optional

from .clickhouse_repositories import ClickHouseMarketBarRepository
from .clickhouse_writer import ReliableClickHouseWriter


class AuthoritativeWriteError(RuntimeError):
    """Bounded online write failure; callers must never use another storage backend."""


class AuthoritativeMarketBarRepository:
    """Direct ClickHouse reads with authoritative WAL-backed raw writes."""

    def __init__(
        self, repository: ClickHouseMarketBarRepository,
        writer: ReliableClickHouseWriter, telemetry: Any = None,
        background_scheduler: Any = None,
    ) -> None:
        self.repository = repository
        self.writer = writer
        self.telemetry = telemetry
        self.background_scheduler = background_scheduler

    def __getattr__(self, name: str) -> Any:
        return getattr(self.repository, name)

    def upsert_price_bars(
        self, symbol: str, interval: str, adjustment: str, source: str,
        ingested_at: str, bars: List[Dict[str, Any]],
        provenance: Optional[Dict[str, Any]] = None,
    ) -> int:
        rows = self.repository.prepare_raw_bars(
            symbol, interval, adjustment, source, ingested_at, bars, provenance
        )
        result = self.writer.write("raw", rows)
        if not result.get("acknowledged") or not result.get("verified"):
            status = str(result.get("status") or "write_not_acknowledged")[:80]
            raise AuthoritativeWriteError(
                f"ClickHouse authoritative write did not complete ({status})"
            )
        return int(result.get("written", len(rows)))
