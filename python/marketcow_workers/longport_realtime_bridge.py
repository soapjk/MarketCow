"""Credential-free typed raw frames for the Rust-owned LongPort realtime path.

This module deliberately does not own sequence, replay, subscriptions, WAL, databases or a
public listener. A supervised provider process may call these helpers on LongPort SDK callbacks
and send the canonical bytes over its owner-only UDS; Rust validates and normalizes every field.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Iterable


BRIDGE_VERSION = "marketcow.longport.raw-push.v1"
MAX_FRAME_BYTES = 1_048_576


def _decimal(value: Any) -> str:
    number = Decimal(str(value))
    if not number.is_finite() or number <= 0:
        raise ValueError("LongPort bridge financial values must be positive and finite")
    return format(number, "f")


def _iso(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("LongPort bridge timestamps must include a timezone")
    normalized = value.astimezone(timezone.utc)
    if normalized.microsecond:
        return normalized.isoformat(timespec="microseconds").replace("+00:00", "Z")
    return normalized.isoformat(timespec="seconds").replace("+00:00", "Z")


def _symbol(value: Any) -> str:
    symbol = str(value).strip().upper()
    if not symbol or "." not in symbol or len(symbol) > 64:
        raise ValueError("LongPort bridge symbol is invalid")
    if not all(character.isupper() or character.isdigit() or character in ".-" for character in symbol):
        raise ValueError("LongPort bridge symbol is invalid")
    return symbol


def depth_frame(symbol: Any, push: Any, *, observed_at: datetime) -> dict[str, Any]:
    def side(values: Iterable[Any]) -> list[dict[str, str]]:
        values = list(values or ())
        if len(values) > 1:
            # The frozen legacy contract is L1. Refuse hidden truncation in the bridge.
            raise ValueError("LongPort bridge depth must contain at most one level per side")
        return [
            {"price": _decimal(value.price), "size": _decimal(value.volume)}
            for value in values
        ]

    sequence = getattr(push, "sequence", None)
    if sequence is not None and (isinstance(sequence, bool) or int(sequence) < 0):
        raise ValueError("LongPort bridge provider sequence is invalid")
    return {
        "schema_version": BRIDGE_VERSION,
        "channel": "depth",
        "symbol": _symbol(symbol),
        "observed_at": _iso(observed_at),
        "provider_sequence": None if sequence is None else int(sequence),
        "bids": side(getattr(push, "bids", ())),
        "asks": side(getattr(push, "asks", ())),
    }


def market_state_frame(symbol: Any, push: Any) -> dict[str, Any]:
    timestamp = getattr(push, "timestamp", None)
    if not isinstance(timestamp, datetime):
        raise ValueError("LongPort bridge market-state timestamp is unavailable")
    sequence = getattr(push, "sequence", None)
    if sequence is not None and (isinstance(sequence, bool) or int(sequence) < 0):
        raise ValueError("LongPort bridge provider sequence is invalid")
    return {
        "schema_version": BRIDGE_VERSION,
        "channel": "market_state",
        "symbol": _symbol(symbol),
        "timestamp": _iso(timestamp),
        "provider_sequence": None if sequence is None else int(sequence),
        "trade_status": str(getattr(getattr(push, "trade_status", None), "name", getattr(push, "trade_status", ""))),
        "trade_session": str(getattr(getattr(push, "trade_session", None), "name", getattr(push, "trade_session", ""))),
    }


def trades_frame(symbol: Any, push: Any) -> dict[str, Any]:
    trades = []
    for trade in list(getattr(push, "trades", ()) or ()):
        timestamp = getattr(trade, "timestamp", None)
        if not isinstance(timestamp, datetime):
            raise ValueError("LongPort bridge trade timestamp is unavailable")
        trades.append(
            {
                "timestamp": _iso(timestamp),
                "price": _decimal(getattr(trade, "price", None)),
                "volume": _decimal(getattr(trade, "volume", None)),
                "direction": str(getattr(trade, "direction", "")),
                "trade_session": str(getattr(trade, "trade_session", "")),
            }
        )
    if not trades:
        raise ValueError("LongPort bridge trade batch is empty")
    return {
        "schema_version": BRIDGE_VERSION,
        "channel": "trades",
        "symbol": _symbol(symbol),
        "trades": trades,
    }


def canonical_frame_bytes(frame: dict[str, Any]) -> bytes:
    if frame.get("schema_version") != BRIDGE_VERSION:
        raise ValueError("LongPort bridge schema version is invalid")
    encoded = json.dumps(
        frame,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    if not encoded or len(encoded) > MAX_FRAME_BYTES:
        raise ValueError("LongPort bridge frame exceeds the configured bound")
    return encoded
