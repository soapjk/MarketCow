from __future__ import annotations

import math
from datetime import date, datetime
from typing import Any, Dict, Optional

import pandas as pd


def json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    try:
        missing = pd.isna(value)
        if isinstance(missing, bool) and missing:
            return None
    except Exception:
        pass
    return value


def safe_record(record: Dict[str, Any]) -> Dict[str, Any]:
    return {str(key): json_safe(value) for key, value in record.items()}


def exchange_for_symbol(symbol: str) -> str:
    code = str(symbol).zfill(6)
    if code.startswith(("4", "8", "9")):
        return "XBSE"
    if code.startswith(("5", "6", "9")):
        return "XSHG"
    return "XSHE"


def instrument_id(symbol: str) -> str:
    code = str(symbol).zfill(6)
    return "CN.{0}.{1}".format(exchange_for_symbol(code), code)


def latest_broad_report_period(today: Optional[date] = None) -> str:
    """Choose the latest reporting period that should be broadly disclosed."""
    today = today or date.today()
    if today.month <= 4:
        return "{0}0930".format(today.year - 1)
    if today.month <= 8:
        return "{0}0331".format(today.year)
    if today.month <= 10:
        return "{0}0630".format(today.year)
    return "{0}0930".format(today.year)


def normalize_report_period(value: str) -> str:
    text = "".join(ch for ch in str(value) if ch.isdigit())
    if len(text) != 8 or text[4:] not in {"0331", "0630", "0930", "1231"}:
        raise ValueError("report_period must be YYYY0331, YYYY0630, YYYY0930 or YYYY1231")
    return text


def normalize_as_of(value: str) -> str:
    text = str(value or "").strip()
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError("as_of must be an ISO date in YYYY-MM-DD format") from exc
    return parsed.isoformat()
