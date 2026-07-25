from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict


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


def plan_history_shards(request: Dict[str, Any]) -> list[Dict[str, Any]]:
    start = _utc(datetime.fromisoformat(
        str(request["range_start"]).replace("Z", "+00:00")
    ))
    end = _utc(datetime.fromisoformat(
        str(request["range_end"]).replace("Z", "+00:00")
    ))
    span = shard_span(request)
    shards = []
    cursor = start
    while cursor < end:
        shard_end = min(end, cursor + span)
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
