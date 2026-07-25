from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from .providers.hyperliquid import VERIFIED_XYZ_EQUITIES

CROSS_MARKET_SCHEMA_VERSION = "cross-market-snapshot-v1"
RELATIONSHIP_SCHEMA_VERSION = "instrument-relationship-v1"


def _instant(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("market timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def _text(value: Decimal) -> str:
    return format(value, "f")


def exact_relationship(
    derivative: dict[str, Any], underlying: dict[str, Any],
) -> dict[str, Any]:
    if derivative.get("instrument_type") != "equity_perpetual":
        raise ValueError("derivative must be an equity perpetual")
    expected = str(derivative["symbol"]).removesuffix("-PERP")
    if expected not in VERIFIED_XYZ_EQUITIES:
        raise ValueError("derivative underlying relationship is not verified")
    underlying_symbol = str(underlying["symbol"]).replace("-", ".")
    if expected.replace("-", ".") != underlying_symbol:
        raise ValueError("derivative and underlying symbols do not match")
    provider_symbol = derivative.get("provider_symbols", {}).get("hyperliquid")
    if not provider_symbol:
        raise ValueError("derivative lacks provider:hyperliquid mapping")
    return {
        "relationship_id": (
            f"{derivative['instrument_id']}~{underlying['instrument_id']}"
        ),
        "relationship_type": "perpetual_underlying",
        "derivative_instrument_id": derivative["instrument_id"],
        "underlying_instrument_id": underlying["instrument_id"],
        "quantity_multiplier": "1",
        "price_multiplier": "1",
        "currency": derivative["currency"],
        "hedge_quality": "exact_underlying",
        "status": "verified",
        "source": {
            "provider": "hyperliquid",
            "provider_symbol": provider_symbol,
        },
        "schema_version": RELATIONSHIP_SCHEMA_VERSION,
    }


def cross_market_snapshot(
    relationship: dict[str, Any],
    derivative_book: dict[str, Any],
    underlying_book: dict[str, Any],
    context: dict[str, Any],
    *,
    max_age_ms: int,
    max_skew_ms: int,
    now: datetime | None = None,
) -> dict[str, Any]:
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    derivative_at = _instant(derivative_book["observed_at"])
    underlying_book_at = _instant(underlying_book["observed_at"])
    quote_at = underlying_book.get("quote_at")
    underlying_state_at = (
        _instant(quote_at) if quote_at else underlying_book_at
    )
    skew_ms = int(
        abs((derivative_at - underlying_book_at).total_seconds()) * 1000
    )
    derivative_age_ms = int((now - derivative_at).total_seconds() * 1000)
    underlying_book_age_ms = int(
        (now - underlying_book_at).total_seconds() * 1000
    )
    underlying_state_age_ms = int(
        (now - underlying_state_at).total_seconds() * 1000
    )
    oldest_age_ms = max(
        derivative_age_ms, underlying_book_age_ms, underlying_state_age_ms
    )
    derivative_has_book = bool(
        derivative_book.get("bids") and derivative_book.get("asks")
    )
    underlying_has_book = bool(
        underlying_book.get("bids") and underlying_book.get("asks")
        and underlying_book.get("best_bid") is not None
        and underlying_book.get("best_ask") is not None
    )
    derivative_bid = (
        Decimal(str(derivative_book["bids"][0]["price"]))
        if derivative_has_book else None
    )
    derivative_ask = (
        Decimal(str(derivative_book["asks"][0]["price"]))
        if derivative_has_book else None
    )
    underlying_bid = (
        Decimal(str(underlying_book["best_bid"]))
        if underlying_has_book else None
    )
    underlying_ask = (
        Decimal(str(underlying_book["best_ask"]))
        if underlying_has_book else None
    )
    rich = (
        derivative_bid - underlying_ask
        if derivative_bid is not None and underlying_ask is not None else None
    )
    cheap = (
        underlying_bid - derivative_ask
        if underlying_bid is not None and derivative_ask is not None else None
    )
    blockers: list[str] = []
    if oldest_age_ms > max_age_ms:
        blockers.append("stale_market_data")
    if skew_ms > max_skew_ms:
        blockers.append("data_skew_exceeded")
    if context.get("market_status") != "active":
        blockers.append("derivative_not_active")
    if context.get("oracle_status") in {"stale", "unavailable"}:
        blockers.append("oracle_unavailable")
    if not derivative_has_book or not underlying_has_book:
        blockers.append("insufficient_depth")
    state_verified = bool(
        quote_at
        and underlying_book.get("session")
        and underlying_book.get("trade_status")
    )
    if not state_verified:
        blockers.append("underlying_session_unverified")
    elif not underlying_book.get("tradable", False):
        blockers.append("underlying_not_tradable")
    top_short_capacity = (
        min(
            Decimal(str(derivative_book["bids"][0]["size"])),
            Decimal(str(underlying_book["ask_volume"])),
        ) if derivative_has_book and underlying_has_book else None
    )
    top_long_capacity = (
        min(
            Decimal(str(derivative_book["asks"][0]["size"])),
            Decimal(str(underlying_book["bid_volume"])),
        ) if derivative_has_book and underlying_has_book else None
    )
    return {
        "relationship_id": relationship["relationship_id"],
        "relationship": relationship,
        "derivative": {
            "instrument_id": relationship["derivative_instrument_id"],
            "bid": None if derivative_bid is None else _text(derivative_bid),
            "ask": None if derivative_ask is None else _text(derivative_ask),
            "bid_size": (
                derivative_book["bids"][0]["size"] if derivative_has_book else None
            ),
            "ask_size": (
                derivative_book["asks"][0]["size"] if derivative_has_book else None
            ),
            "depth": derivative_book["depth"],
            "mark_price": context.get("mark_price"),
            "oracle_price": context.get("oracle_price"),
            "external_oracle_price": context.get("external_oracle_price"),
            "funding_rate": context.get("funding_rate"),
            "open_interest": context.get("open_interest"),
            "market_status": context.get("market_status"),
            "oracle_status": context.get("oracle_status"),
            "observed_at": derivative_book["observed_at"],
        },
        "underlying": {
            "instrument_id": relationship["underlying_instrument_id"],
            "bid": None if underlying_bid is None else _text(underlying_bid),
            "ask": None if underlying_ask is None else _text(underlying_ask),
            "bid_size": str(underlying_book["bid_volume"]),
            "ask_size": str(underlying_book["ask_volume"]),
            "depth": min(
                len(underlying_book.get("bids", [])),
                len(underlying_book.get("asks", [])),
            ),
            "trade_status": underlying_book.get("trade_status", "unknown"),
            "session": underlying_book.get("session", "unknown"),
            "session_status": "verified" if state_verified else "unverified",
            "tradable": bool(underlying_book.get("tradable", False)),
            "quote_event_at": quote_at,
            "book_received_at": underlying_book["observed_at"],
            "observed_at": underlying_book["observed_at"],
        },
        "gross_basis": {
            "short_derivative_long_underlying": {
                "absolute": None if rich is None else _text(rich),
                "bps": (
                    None if rich is None or underlying_ask is None
                    else _text(rich / underlying_ask * Decimal(10_000))
                ),
            },
            "long_derivative_short_underlying": {
                "absolute": None if cheap is None else _text(cheap),
                "bps": (
                    None if cheap is None or derivative_ask is None
                    else _text(cheap / derivative_ask * Decimal(10_000))
                ),
            },
        },
        "capacity": {
            "top_level_short_derivative_long_underlying": (
                None if top_short_capacity is None else _text(top_short_capacity)
            ),
            "top_level_long_derivative_short_underlying": (
                None if top_long_capacity is None else _text(top_long_capacity)
            ),
        },
        "timing": {
            "data_skew_ms": skew_ms,
            "oldest_leg_age_ms": max(0, oldest_age_ms),
            "derivative_age_ms": max(0, derivative_age_ms),
            "underlying_book_age_ms": max(0, underlying_book_age_ms),
            "underlying_status_age_ms": max(0, underlying_state_age_ms),
            "max_age_ms": max_age_ms,
            "max_skew_ms": max_skew_ms,
        },
        "quality": {
            "status": "usable" if not blockers else "limited",
            "market_overlap": (
                (
                    bool(underlying_book.get("tradable"))
                    and underlying_state_age_ms <= max_age_ms
                ) if state_verified else None
            ),
            "usable_for_immediate_hedge": not blockers,
            "blockers": blockers,
        },
        "as_of": now.isoformat(),
        "schema_version": CROSS_MARKET_SCHEMA_VERSION,
    }


def load_cross_market_inputs(
    hyperliquid_provider: Any, longport_provider: Any,
    derivative_symbol: str, underlying_symbol: str, depth: int,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    with ThreadPoolExecutor(max_workers=3) as executor:
        book_future = executor.submit(
            hyperliquid_provider.fetch_order_book, derivative_symbol, depth
        )
        context_future = executor.submit(
            hyperliquid_provider.fetch_asset_context, derivative_symbol
        )
        spread_method = getattr(
            longport_provider, "fetch_spread_state",
            longport_provider.fetch_spread,
        )
        underlying_future = executor.submit(spread_method, underlying_symbol)
        return (
            book_future.result(), underlying_future.result(),
            context_future.result(),
        )
