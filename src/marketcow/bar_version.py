from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict


RAW_CONTENT_FIELDS = (
    "open", "high", "low", "close", "raw_close", "adjustment_factor", "volume",
    "amount", "source_sequence", "observed_at", "raw_artifact_id",
)
TIME_FIELDS = {"bar_time", "observed_at"}
NUMBER_FIELDS = {
    "open", "high", "low", "close", "raw_close", "adjustment_factor",
    "volume", "amount",
}


def _utc_milliseconds(value: Any) -> str:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(
        str(value).replace("Z", "+00:00")
    )
    if parsed.tzinfo is None:
        raise ValueError("raw bar version timestamps must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat(timespec="milliseconds")


def _number(value: Any) -> str:
    decimal = Decimal(str(value))
    if not decimal.is_finite():
        raise ValueError("raw bar version numbers must be finite")
    if decimal == 0:
        return "0"
    return format(decimal.normalize(), "f")


def raw_content_rank(row: Dict[str, Any]) -> str:
    """Hash normalized logical content for a cross-backend deterministic tie-break."""
    normalized = {}
    for field in RAW_CONTENT_FIELDS:
        value = row.get(field)
        if value is None:
            normalized[field] = None
        elif field in TIME_FIELDS:
            normalized[field] = _utc_milliseconds(value)
        elif field in NUMBER_FIELDS:
            normalized[field] = _number(value)
        else:
            normalized[field] = str(value)
    encoded = json.dumps(
        normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:52]


def raw_content_version(ingested_at: Any, content_rank: str) -> int:
    parsed = ingested_at if isinstance(ingested_at, datetime) else datetime.fromisoformat(
        str(ingested_at).replace("Z", "+00:00")
    )
    if parsed.tzinfo is None:
        raise ValueError("ingested_at must include a timezone")
    epoch_millis = int(parsed.astimezone(timezone.utc).timestamp() * 1000)
    return (epoch_millis << 208) | int(content_rank, 16)
