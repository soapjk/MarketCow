from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable
from zoneinfo import ZoneInfo

from .history_capabilities import provider_history_capability
from .history_shards import _exchange_calendar, history_ingestion_identity


class HistoryCoverageUnproven(ValueError):
    pass


def _position(bar: Dict[str, Any]) -> str:
    return str(bar.get("bar_at") or bar.get("timestamp") or "").strip()


def assess_history_response(
    provider: str, interval: str, bars: Iterable[Dict[str, Any]],
    range_start: datetime | None = None, range_end: datetime | None = None,
    exempt_session_dates: Iterable[str] = (),
    instrument_id: str = "",
) -> Dict[str, Any]:
    capability = provider_history_capability(provider, interval, instrument_id)
    positions = [_position(bar) for bar in bars]
    nonempty = [value for value in positions if value]
    unique = set(nonempty)
    reasons = []
    if len(unique) != len(nonempty):
        reasons.append("duplicate_positions")
    parsed = []
    for value in nonempty:
        try:
            parsed.append(
                datetime.fromtimestamp(float(value), timezone.utc)
                if value.replace(".", "", 1).isdigit()
                else datetime.fromisoformat(value.replace("Z", "+00:00"))
                .astimezone(timezone.utc)
            )
        except (ValueError, TypeError, OverflowError):
            reasons.append("invalid_position")
            break
    if len(parsed) > 1 and parsed != sorted(parsed):
        reasons.append("unordered_positions")
    if range_start is not None and range_end is not None and parsed:
        start = range_start.astimezone(timezone.utc)
        end = range_end.astimezone(timezone.utc)
        if any(value < start or value >= end for value in parsed):
            reasons.append("position_outside_shard")
        expected_session_dates: list[str] = []
        observed_session_dates: list[str] = []
        if capability.calendar_name:
            local_zone = ZoneInfo({
                "XSHG": "Asia/Shanghai",
                "XHKG": "Asia/Hong_Kong",
            }.get(capability.calendar_name, "America/New_York"))
            first_date = start.astimezone(local_zone).date().isoformat()
            last_date = (
                end - timedelta(microseconds=1)
            ).astimezone(local_zone).date().isoformat()
            calendar = _exchange_calendar(capability.calendar_name)
            sessions = [
                value
                for value in calendar.sessions_in_range(first_date, last_date)
                if calendar.session_open(value).to_pydatetime() < end
                and calendar.session_close(value).to_pydatetime() >= start
            ]
            expected_session_dates = [
                value.date().isoformat() for value in sessions
            ]
            observed_session_dates = sorted({
                value.astimezone(local_zone).date().isoformat()
                for value in parsed
            })
            exempt = sorted(set(exempt_session_dates))
            missing = sorted(
                set(expected_session_dates) - set(observed_session_dates)
                - set(exempt)
            )
            if missing:
                reasons.append("missing_session_dates")
            observed_counts = {
                date: sum(
                    value.astimezone(local_zone).date().isoformat() == date
                    for value in parsed
                )
                for date in observed_session_dates
            }
            minimum_rows = max(
                1, int(capability.rows_per_trading_day * 0.9)
            )
            incomplete = sorted(
                value.date().isoformat()
                for value in sessions
                if calendar.session_open(value).to_pydatetime() >= start
                and calendar.session_close(value).to_pydatetime() < end
                and value.date().isoformat() not in set(exempt)
                and observed_counts.get(value.date().isoformat(), 0)
                < minimum_rows
            )
            if incomplete:
                reasons.append("incomplete_session_rows")
        else:
            missing = []
            exempt = []
            observed_counts = {}
            incomplete = []
    else:
        expected_session_dates = []
        observed_session_dates = []
        missing = []
        exempt = []
        observed_counts = {}
        incomplete = []
    if len(positions) >= capability.maximum_rows_per_request:
        reasons.append("provider_row_limit_reached")
    elif len(positions) >= capability.planning_row_budget:
        reasons.append("planning_row_budget_reached")
    return {
        "schema": "marketcow.history-coverage.v1",
        "provider": provider,
        "interval": interval,
        "observed_rows": len(positions),
        "unique_rows": len(unique),
        "planning_row_budget": capability.planning_row_budget,
        "maximum_rows_per_request": capability.maximum_rows_per_request,
        "limit_confidence": capability.limit_confidence,
        "capability_source": capability.capability_source,
        "status": "split_required" if reasons else "provisionally_complete",
        "reasons": reasons,
        "expected_session_dates": expected_session_dates,
        "observed_session_dates": observed_session_dates,
        "missing_session_dates": missing,
        "exempt_session_dates": exempt,
        "observed_session_row_counts": observed_counts,
        "incomplete_session_dates": incomplete,
    }


def split_history_shard(
    shard: Dict[str, Any], symbol: str, request: Dict[str, Any],
    first_index: int,
) -> list[Dict[str, Any]]:
    start = datetime.fromisoformat(
        str(shard["range_start"]).replace("Z", "+00:00")
    ).astimezone(timezone.utc)
    end = datetime.fromisoformat(
        str(shard["range_end"]).replace("Z", "+00:00")
    ).astimezone(timezone.utc)
    capability = provider_history_capability(
        str(request["provider"]), str(request["interval"]), symbol
    )
    if (end - start).total_seconds() <= capability.minimum_split_seconds:
        return []
    midpoint = start + (end - start) / 2
    if capability.minimum_split_seconds >= 86400:
        midpoint = midpoint.replace(hour=0, minute=0, second=0, microsecond=0)
        if midpoint <= start:
            midpoint = start + timedelta(days=1)
        if midpoint >= end:
            midpoint = end - timedelta(days=1)
    if midpoint <= start or midpoint >= end:
        return []
    children = []
    for offset, (left, right) in enumerate(((start, midpoint), (midpoint, end))):
        planned = {
            "shard_index": first_index + offset,
            "range_start": left.isoformat(timespec="seconds"),
            "range_end": right.isoformat(timespec="seconds"),
        }
        identity = history_ingestion_identity(symbol, request, planned)
        planned["shard_key"] = identity[:24]
        planned["ingestion_id"] = identity
        children.append(planned)
    return children
