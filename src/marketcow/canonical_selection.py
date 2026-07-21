from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Sequence


DEFAULT_SOURCE_PRIORITY = ("tushare", "sina", "eastmoney", "yahoo_chart", "baostock")


def utc_datetime(value: Any) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(
        str(value).replace("Z", "+00:00")
    )
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def canonical_selection_key(
    row: Dict[str, Any], source_priority: Sequence[str] = DEFAULT_SOURCE_PRIORITY,
) -> tuple[Any, ...]:
    priority = {source: index for index, source in enumerate(source_priority)}
    source = str(row["source"])
    return (
        priority.get(source, len(priority)),
        -utc_datetime(row["observed_at"]).timestamp(),
        -utc_datetime(row["ingested_at"]).timestamp(),
        source,
        str(row.get("raw_artifact_id") or ""),
        str(row.get("source_sequence") or ""),
    )


def canonical_page_payload(
    source: Any, observed_at: Any, raw_artifact_id: Any,
) -> Dict[str, Any]:
    return {
        "canonical": True,
        "selected_source": str(source),
        "observed_at": utc_datetime(observed_at).isoformat(),
        "raw_artifact_id": raw_artifact_id,
    }


def with_effective_time(row: Dict[str, Any], as_of: Any) -> Dict[str, Any]:
    """Attach explicit non-exact as-of semantics without changing bar identity."""
    result = dict(row)
    effective = utc_datetime(result["bar_at"])
    point = utc_datetime(as_of)
    staleness = max(0, int(point.timestamp()) - int(effective.timestamp()))
    result.update({
        "effective_bar_at": effective.isoformat(),
        "staleness_seconds": staleness,
        "effective_status": "exact" if staleness == 0 else "prior_within_lookback",
    })
    return result
