from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class ProviderHistoryCapability:
    schema_version: int
    provider: str
    interval: str
    maximum_rows_per_request: int
    planning_row_budget: int
    rows_per_trading_day: int
    minimum_split_seconds: int
    range_end_semantics: str
    limit_confidence: str
    capability_source: str
    calendar_name: str | None = None
    supports_pagination: bool = False

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


_TUSHARE_ROWS_PER_DAY = {
    "1m": 242,
    "2m": 122,
    "5m": 50,
    "15m": 18,
    "30m": 10,
    "60m": 6,
    "1h": 6,
}


def provider_history_capability(
    provider: str, interval: str, instrument_id: str = ""
) -> ProviderHistoryCapability:
    provider = provider.strip().lower()
    interval = interval.strip().lower()
    if provider == "tushare":
        rows_per_day = _TUSHARE_ROWS_PER_DAY.get(interval)
        if rows_per_day is None:
            raise ValueError("unsupported Tushare history interval")
        return ProviderHistoryCapability(
            schema_version=1,
            provider=provider,
            interval=interval,
            # The compatible endpoint does not publish a dependable hard cap.
            # Treat 5,000 as a fail-closed operational ceiling and plan at 60%.
            maximum_rows_per_request=5000,
            planning_row_budget=3000,
            rows_per_trading_day=rows_per_day,
            minimum_split_seconds=86400,
            range_end_semantics="exclusive",
            limit_confidence="operational_ceiling",
            capability_source="stockai888-probe-2026-07-25",
            calendar_name="XSHG",
        )
    if provider == "yahoo":
        mic = instrument_id.rsplit(".", 1)[-1].upper()
        if mic in {"XSHG", "XSHE", "XBSE"}:
            session_minutes, calendar_name = 242, "XSHG"
        elif mic == "XHKG":
            session_minutes, calendar_name = 332, "XHKG"
        else:
            session_minutes, calendar_name = 392, "XNYS"
        seconds = {
            "1m": 60, "2m": 120, "5m": 300, "15m": 900,
            "30m": 1800, "60m": 3600, "90m": 5400, "1h": 3600,
        }.get(interval)
        rows_per_day = (
            (session_minutes * 60 + seconds - 1) // seconds
            if seconds else 1 if interval in {
                "1d", "5d", "1wk", "1mo", "3mo"
            } else None
        )
        if rows_per_day is None:
            raise ValueError("unsupported Yahoo history interval")
        return ProviderHistoryCapability(
            1, provider, interval, 10000, 6000, rows_per_day, 86400,
            "exclusive", "documented", "provider-retention-contract",
            calendar_name,
        )
    if provider == "hyperliquid":
        return ProviderHistoryCapability(
            1, provider, interval, 5000, 4000, 0, 60, "exclusive",
            "documented", "provider-api-contract", None,
        )
    raise ValueError("unsupported history provider")
