from __future__ import annotations

import hashlib
import json
from functools import lru_cache
from datetime import datetime, timedelta, timezone
from typing import Any, Dict
from zoneinfo import ZoneInfo

import exchange_calendars

from .history_capabilities import provider_history_capability


@lru_cache(maxsize=8)
def _exchange_calendar(name: str):
    return exchange_calendars.get_calendar(name)


def _is_session(name: str, value: datetime) -> bool:
    local_zone = ZoneInfo({
        "XSHG": "Asia/Shanghai",
        "XHKG": "Asia/Hong_Kong",
    }.get(name, "America/New_York"))
    session_date = value.astimezone(local_zone).date().isoformat()
    return bool(_exchange_calendar(name).is_session(session_date))

RANGE_DAYS = {
    "1d": 1,
    "5d": 5,
    "1mo": 31,
    "3mo": 93,
    "6mo": 186,
    "1y": 366,
    "2y": 732,
    "5y": 1830,
    "10y": 3660,
    "max": 3660,
}

INTRADAY_INTERVALS = {
    "1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h"
}

INTERVAL_SECONDS = {
    "1m": 60,
    "2m": 120,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "60m": 3600,
    "90m": 5400,
    "1h": 3600,
    "1d": 86400,
    "5d": 432000,
    "1wk": 604800,
    "1mo": 2678400,
    "3mo": 8035200,
}


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("history range boundary must be timezone-aware")
    return value.astimezone(timezone.utc)


def freeze_history_range(
    request: Dict[str, Any], observed_at: datetime
) -> Dict[str, Any]:
    result = dict(request)
    if result.get("range_start") and result.get("range_end"):
        start = _utc(datetime.fromisoformat(
            str(result["range_start"]).replace("Z", "+00:00")
        ))
        end = _utc(datetime.fromisoformat(
            str(result["range_end"]).replace("Z", "+00:00")
        ))
    else:
        end = _utc(observed_at)
        range_name = str(result["range"])
        if range_name == "ytd":
            start = datetime(end.year, 1, 1, tzinfo=timezone.utc)
        elif range_name in RANGE_DAYS:
            start = end - timedelta(days=RANGE_DAYS[range_name])
        else:
            raise ValueError("unsupported range")
    if start >= end:
        raise ValueError("history range_start must precede range_end")
    result["range_start"] = start.isoformat(timespec="seconds")
    result["range_end"] = end.isoformat(timespec="seconds")
    return result


def shard_span(request: Dict[str, Any]) -> timedelta:
    provider = str(request["provider"])
    interval = str(request["interval"])
    if interval not in INTERVAL_SECONDS:
        raise ValueError("unsupported interval")
    if provider == "tushare":
        if interval not in INTRADAY_INTERVALS:
            raise ValueError("Tushare history shards require a minute interval")
        return timedelta(days=31)
    if provider == "yahoo":
        if interval == "1m":
            return timedelta(days=7)
        if interval in INTRADAY_INTERVALS:
            return timedelta(days=60)
        return timedelta(days=366)
    if provider == "hyperliquid":
        return timedelta(seconds=INTERVAL_SECONDS[interval] * 4000)
    raise ValueError("unsupported history provider")


def _budgeted_span(
    cursor: datetime, end: datetime, request: Dict[str, Any]
) -> tuple[datetime, int]:
    capability = provider_history_capability(
        str(request["provider"]), str(request["interval"])
    )
    if capability.provider == "hyperliquid":
        shard_end = min(end, cursor + shard_span(request))
        seconds = max(0.0, (shard_end - cursor).total_seconds())
        expected = int(
            seconds / max(1, INTERVAL_SECONDS[str(request["interval"])])
        )
        return shard_end, expected
    if capability.provider == "yahoo":
        shard_end = min(end, cursor + shard_span(request))
        days = max(1, int((shard_end - cursor).total_seconds() / 86400) + 1)
        return shard_end, days * capability.rows_per_trading_day

    # Tushare A-share minute data is planned using a conservative weekday
    # envelope. Exchange holidays only reduce the observed row count.
    budget_days = max(
        1, capability.planning_row_budget // capability.rows_per_trading_day
    )
    candidate = cursor
    weekdays = 0
    while candidate < end:
        candidate = min(end, candidate + timedelta(days=1))
        observed = candidate - timedelta(microseconds=1)
        if capability.calendar_name and _is_session(
            capability.calendar_name, observed
        ):
            weekdays += 1
        if weekdays >= budget_days:
            break
    return candidate, weekdays * capability.rows_per_trading_day


def plan_history_shards(request: Dict[str, Any]) -> list[Dict[str, Any]]:
    start = _utc(datetime.fromisoformat(
        str(request["range_start"]).replace("Z", "+00:00")
    ))
    end = _utc(datetime.fromisoformat(
        str(request["range_end"]).replace("Z", "+00:00")
    ))
    capability = provider_history_capability(
        str(request["provider"]), str(request["interval"])
    )
    shards = []
    cursor = start
    while cursor < end:
        shard_end, expected_max_rows = _budgeted_span(cursor, end, request)
        identity_payload = {
            "provider": request["provider"],
            "interval": request["interval"],
            "adjustment": request["adjustment"],
            "start": cursor.isoformat(timespec="seconds"),
            "end": shard_end.isoformat(timespec="seconds"),
        }
        identity = hashlib.sha256(json.dumps(
            identity_payload, sort_keys=True, separators=(",", ":")
        ).encode()).hexdigest()
        shards.append({
            "shard_index": len(shards),
            "shard_key": identity[:24],
            "range_start": identity_payload["start"],
            "range_end": identity_payload["end"],
            "expected_max_rows": expected_max_rows,
            "planning_row_budget": capability.planning_row_budget,
            "maximum_rows_per_request": capability.maximum_rows_per_request,
            "capability_schema_version": capability.schema_version,
            "limit_confidence": capability.limit_confidence,
            "capability_source": capability.capability_source,
        })
        cursor = shard_end
    return shards


def history_ingestion_identity(
    symbol: str, request: Dict[str, Any], shard: Dict[str, Any]
) -> str:
    payload = {
        "schema_version": 1,
        "symbol": str(symbol).strip().upper(),
        "provider": request["provider"],
        "interval": request["interval"],
        "adjustment": request["adjustment"],
        "range_start": shard["range_start"],
        "range_end": shard["range_end"],
    }
    return hashlib.sha256(json.dumps(
        payload, sort_keys=True, separators=(",", ":")
    ).encode()).hexdigest()
