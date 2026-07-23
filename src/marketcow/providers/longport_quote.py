from __future__ import annotations

import threading
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Iterable
from zoneinfo import ZoneInfo

from ..normalize import exchange_for_symbol, instrument_id
from .eastmoney_realtime import normalize_a_symbol
from .yahoo_quote import normalize_yahoo_symbol


LONGPORT_QUOTE_URL = "wss://openapi-quote.longbridge.com/v2"
MAX_QUOTE_SYMBOLS = 500
_PROXY_VARIABLES = (
    "http_proxy", "https_proxy", "all_proxy",
    "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
)
_PROXY_ENVIRONMENT_LOCK = threading.RLock()
_LONGPORT_WALL_TIMEZONE = ZoneInfo("Asia/Shanghai")


class LongPortError(RuntimeError):
    """Stable error boundary for Longbridge OpenAPI quote failures."""


@contextmanager
def _direct_connection_environment():
    """Create the SDK socket directly without changing other providers permanently."""
    with _PROXY_ENVIRONMENT_LOCK:
        previous = {name: os.environ.get(name) for name in _PROXY_VARIABLES}
        try:
            for name in _PROXY_VARIABLES:
                os.environ.pop(name, None)
            yield
        finally:
            for name, value in previous.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value


def normalize_longport_symbol(value: str) -> tuple[str, str, str]:
    """Return MarketCow symbol, market and Longbridge ticker.region symbol."""

    try:
        normalized = normalize_a_symbol(value)
        return normalized, "CN", normalized
    except ValueError:
        normalized, market = normalize_yahoo_symbol(value)
    if market == "HK":
        ticker = str(int(normalized[:-3]))
        return normalized, market, ticker + ".HK"
    if market == "US":
        return normalized, market, normalized.replace("-", ".") + ".US"
    raise ValueError("LongPort quotes support CN, HK and US securities")


def _decimal(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = Decimal(str(value))
    except Exception as exc:
        raise LongPortError("LongPort returned an invalid numeric quote field") from exc
    return None if not number.is_finite() else float(number)


def _timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromtimestamp(int(value), timezone.utc)
        except (TypeError, ValueError, OSError) as exc:
            raise LongPortError("LongPort returned an invalid quote timestamp") from exc
    if parsed.tzinfo is None:
        # The Longbridge Python SDK exposes quote timestamps as naive UTC+8
        # wall-clock datetimes for every market, including US sessions.
        parsed = parsed.replace(tzinfo=_LONGPORT_WALL_TIMEZONE)
    return parsed.astimezone(timezone.utc)


class LongPortQuoteProvider:
    """Batch real-time quotes through the official Longbridge OpenAPI SDK."""

    name = "longbridge_openapi"

    def __init__(
        self,
        app_key: str,
        app_secret: str,
        access_token: str,
        *,
        enable_overnight: bool = False,
        context_factory: Callable[[], Any] | None = None,
    ):
        self._app_key = app_key
        self._app_secret = app_secret
        self._access_token = access_token
        self.enable_overnight = enable_overnight
        self._context_factory = context_factory
        self._context: Any = None
        self._lock = threading.Lock()

    @property
    def configured(self) -> bool:
        return all((self._app_key, self._app_secret, self._access_token))

    def _create_context(self) -> Any:
        if not self.configured:
            raise LongPortError("LongPort credentials are not configured")
        if self._context_factory is not None:
            return self._context_factory()
        try:
            from longbridge.openapi import Config, QuoteContext

            with _direct_connection_environment():
                config = Config.from_apikey(
                    self._app_key,
                    self._app_secret,
                    self._access_token,
                    enable_overnight=self.enable_overnight,
                    enable_print_quote_packages=False,
                )
                return QuoteContext(config)
        except Exception as exc:
            raise LongPortError("LongPort quote connection failed") from exc

    def _quote_context(self) -> Any:
        if self._context is None:
            self._context = self._create_context()
        return self._context

    def close(self) -> None:
        with self._lock:
            context, self._context = self._context, None
            close = getattr(context, "close", None)
            if callable(close):
                close()

    @staticmethod
    def _raw_payload(quote: Any) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for field in (
            "symbol", "last_done", "prev_close", "open", "high", "low",
            "timestamp", "volume", "turnover", "trade_status",
        ):
            value = getattr(quote, field, None)
            if isinstance(value, datetime):
                value = value.astimezone(timezone.utc).isoformat()
            elif isinstance(value, Decimal):
                value = str(value)
            elif value is not None and not isinstance(value, (str, int, float, bool)):
                value = str(value)
            result[field] = value
        return result

    def _normalize_quote(
        self, quote: Any, marketcow_symbol: str, market: str
    ) -> dict[str, Any]:
        sessions = [("regular", quote)]
        if market == "US":
            sessions.extend((
                ("pre_market", getattr(quote, "pre_market_quote", None)),
                ("post_market", getattr(quote, "post_market_quote", None)),
                ("overnight", getattr(quote, "overnight_quote", None)),
            ))
        candidates: list[tuple[datetime, str, float]] = []
        for session, item in sessions:
            if item is None:
                continue
            price = _decimal(getattr(item, "last_done", None))
            observed = _timestamp(getattr(item, "timestamp", None))
            if price is not None and price > 0 and observed is not None:
                candidates.append((observed, session, price))
        if not candidates:
            raise LongPortError("LongPort returned no usable price")
        observed, session, price = max(candidates, key=lambda item: item[0])
        previous_close = _decimal(getattr(quote, "prev_close", None))
        change = None if previous_close is None else price - previous_close
        exchange = (
            exchange_for_symbol(marketcow_symbol.split(".")[0]) if market == "CN"
            else "XHKG" if market == "HK" else "LONGPORT"
        )
        identifier = (
            instrument_id(marketcow_symbol.split(".")[0]) if market == "CN"
            else f"{market}.{exchange}.{marketcow_symbol.split('.')[0]}"
        )
        return {
            "instrument_id": identifier,
            "symbol": marketcow_symbol,
            "name": marketcow_symbol,
            "market": market,
            "exchange": exchange,
            "currency": "CNY" if market == "CN" else "HKD" if market == "HK" else "USD",
            "price": price,
            "previous_close": previous_close,
            "change": change,
            "change_pct": change / previous_close * 100 if change is not None and previous_close else None,
            "session": session,
            "quote_at": observed.isoformat(timespec="seconds"),
            "price_adjustment": "raw",
            "quality_status": "single_source_unverified",
            "source": self.name,
            "source_url": LONGPORT_QUOTE_URL,
            "raw_response_locator": "QuoteContext.quote SecurityQuote",
            "_raw_payload": self._raw_payload(quote),
        }

    def fetch_quotes(self, symbols: Iterable[str]) -> list[dict[str, Any]]:
        requested = tuple(symbols)
        if not requested:
            raise ValueError("at least one symbol is required")
        if len(requested) > MAX_QUOTE_SYMBOLS:
            raise ValueError("LongPort accepts at most 500 symbols per quote request")
        mappings = [normalize_longport_symbol(symbol) for symbol in requested]
        long_symbols = [item[2] for item in mappings]
        if len(set(long_symbols)) != len(long_symbols):
            raise ValueError("LongPort quote symbols must be unique")
        try:
            with self._lock:
                with _direct_connection_environment():
                    quotes = self._quote_context().quote(long_symbols)
        except LongPortError:
            raise
        except Exception as exc:
            raise LongPortError("LongPort quote request failed") from exc
        by_symbol = {str(getattr(item, "symbol", "")).upper(): item for item in quotes}
        missing = [symbol for symbol in long_symbols if symbol.upper() not in by_symbol]
        if missing:
            raise LongPortError("LongPort returned an incomplete quote batch")
        return [
            self._normalize_quote(by_symbol[long_symbol.upper()], marketcow_symbol, market)
            for marketcow_symbol, market, long_symbol in mappings
        ]

    def fetch_quote(self, symbol: str) -> dict[str, Any]:
        return self.fetch_quotes((symbol,))[0]

    @staticmethod
    def _normalize_depth_levels(levels: Any) -> list[dict[str, Any]]:
        result = []
        for level in levels or ():
            price = _decimal(getattr(level, "price", None))
            if price is None or price <= 0:
                continue
            result.append({
                "position": int(getattr(level, "position", 0)),
                "price": price,
                "volume": int(getattr(level, "volume", 0)),
                "order_num": int(getattr(level, "order_num", 0)),
            })
        return sorted(result, key=lambda item: item["position"])

    def fetch_spread(self, symbol: str) -> dict[str, Any]:
        """Pull the current order book and calculate the top-of-book spread."""

        marketcow_symbol, market, long_symbol = normalize_longport_symbol(symbol)
        try:
            with self._lock:
                with _direct_connection_environment():
                    depth = self._quote_context().depth(long_symbol)
        except LongPortError:
            raise
        except Exception as exc:
            raise LongPortError("LongPort depth request failed") from exc

        asks = self._normalize_depth_levels(getattr(depth, "asks", ()))
        bids = self._normalize_depth_levels(getattr(depth, "bids", ()))
        if not asks or not bids:
            raise LongPortError("LongPort returned no usable two-sided depth")
        best_ask = asks[0]["price"]
        best_bid = bids[0]["price"]
        spread = best_ask - best_bid
        midpoint = (best_ask + best_bid) / 2
        return {
            "symbol": marketcow_symbol,
            "market": market,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "spread": spread,
            "spread_bps": spread / midpoint * 10_000 if midpoint else None,
            "bid_volume": bids[0]["volume"],
            "ask_volume": asks[0]["volume"],
            "bids": bids,
            "asks": asks,
            "observed_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "source": self.name,
            "source_url": LONGPORT_QUOTE_URL,
            "quality_status": "single_source_unverified",
        }
