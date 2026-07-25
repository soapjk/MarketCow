from __future__ import annotations

import json
import subprocess
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import requests

from ..instruments import canonical_instrument

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
ALLOWED_RANGES = {"1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max"}
ALLOWED_INTERVALS = {"1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "5d", "1wk", "1mo", "3mo"}


def normalize_yahoo_symbol(value: str) -> Tuple[str, str]:
    instrument = canonical_instrument(value)
    if instrument.market not in {"CN", "HK", "US"}:
        raise ValueError("Yahoo quotes support CN, HK and US instruments")
    return instrument.provider_symbol("provider:yahoo"), instrument.market


def _float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _iso_utc(timestamp: Any) -> Optional[str]:
    try:
        return datetime.fromtimestamp(int(timestamp), timezone.utc).isoformat(timespec="seconds")
    except (TypeError, ValueError, OSError):
        return None


def _session(meta: Dict[str, Any], timestamp: Optional[int]) -> str:
    if timestamp is None:
        return "unknown"
    periods = meta.get("currentTradingPeriod") or {}
    for name in ("pre", "regular", "post"):
        period = periods.get(name) or {}
        start, end = period.get("start"), period.get("end")
        if start is not None and end is not None and int(start) <= timestamp <= int(end):
            return {"pre": "pre_market", "regular": "regular", "post": "post_market"}[name]
    return "closed"


class YahooQuoteProvider:
    name = "yahoo_chart"

    def __init__(self, timeout: float = 1.5, request_budget: float = 3.5):
        self.timeout = timeout
        self.request_budget = request_budget
        self.session = requests.Session()
        self.session.trust_env = True
        self.headers = {
            "User-Agent": "Mozilla/5.0 marketcow/0.1",
            "Referer": "https://finance.yahoo.com/",
            "Accept": "application/json,text/plain,*/*",
        }

    def _fetch_chart(self, symbol: str, params: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
        url = YAHOO_CHART_URL.format(symbol=symbol)
        source_url = url + "?" + urlencode(params)
        deadline = time.monotonic() + self.request_budget
        try:
            response = self.session.get(
                url, params=params, headers=self.headers,
                timeout=min(self.timeout, self.request_budget),
            )
            response.raise_for_status()
            return response.json(), source_url
        except (requests.RequestException, ValueError) as request_error:
            remaining = deadline - time.monotonic()
            if remaining <= 0.1:
                raise RuntimeError(f"Yahoo chart request budget exhausted for {symbol}") from request_error
            try:
                completed = subprocess.run(
                    [
                        "curl", "-fsSL", "--max-time", str(remaining),
                        "-A", self.headers["User-Agent"], "-e", self.headers["Referer"], source_url,
                    ],
                    capture_output=True,
                    text=True,
                    timeout=remaining + 0.2,
                    check=True,
                )
                return json.loads(completed.stdout), source_url
            except Exception as curl_error:
                raise RuntimeError(
                    "Yahoo chart fetch failed for {0}: requests={1}; curl={2}".format(
                        symbol, request_error, curl_error
                    )
                ) from curl_error

    @staticmethod
    def _result(payload: Dict[str, Any]) -> Dict[str, Any]:
        chart = payload.get("chart") or {}
        if chart.get("error"):
            raise RuntimeError(str(chart["error"]))
        results = chart.get("result") or []
        if not results:
            raise RuntimeError("Yahoo returned no chart result")
        return results[0]

    def fetch_quote(self, value: str) -> Dict[str, Any]:
        instrument = canonical_instrument(value)
        symbol, market = normalize_yahoo_symbol(value)
        params = {
            "range": "1d", "interval": "5m", "includePrePost": "true",
            "events": "div,splits", "includeAdjustedClose": "true",
        }
        payload, source_url = self._fetch_chart(symbol, params)
        result = self._result(payload)
        meta = result.get("meta") or {}
        timestamps = result.get("timestamp") or []
        quote_block = ((result.get("indicators") or {}).get("quote") or [{}])[0]
        closes = quote_block.get("close") or []
        price, quote_ts = None, None
        for timestamp, close in reversed(list(zip(timestamps, closes))):
            number = _float(close)
            if number is not None:
                price, quote_ts = number, int(timestamp)
                break
        if price is None:
            price = _float(meta.get("regularMarketPrice"))
            quote_ts = int(meta.get("regularMarketTime")) if meta.get("regularMarketTime") else None
        if price is None:
            raise RuntimeError("Yahoo returned no usable price")
        previous_close = _float(meta.get("previousClose"))
        if previous_close is None:
            previous_close = _float(meta.get("chartPreviousClose"))
        change = price - previous_close if previous_close is not None else None
        change_pct = change / previous_close * 100 if change is not None and previous_close else None
        exchange = str(meta.get("exchangeName") or meta.get("fullExchangeName") or "")
        return {
            "instrument_id": instrument.instrument_id,
            "symbol": instrument.instrument_id,
            "provider_symbol": symbol,
            "name": meta.get("shortName") or meta.get("longName") or symbol,
            "market": market,
            "exchange": exchange,
            "currency": meta.get("currency") or ("HKD" if market == "HK" else "USD"),
            "price": price,
            "previous_close": previous_close,
            "change": change,
            "change_pct": change_pct,
            "session": _session(meta, quote_ts),
            "quote_at": _iso_utc(quote_ts),
            "exchange_timezone": meta.get("exchangeTimezoneName"),
            "exchange_timezone_short": meta.get("timezone"),
            "data_delay_seconds": meta.get("exchangeDataDelayedBy"),
            "price_adjustment": "raw",
            "quality_status": "single_source_unverified",
            "source": self.name,
            "source_url": source_url,
            "raw_response_locator": "chart.result[0]",
            "_raw_payload": payload,
        }

    def fetch_history(
        self,
        value: str,
        range_: str = "1y",
        interval: str = "1d",
        adjustment: str = "qfq",
    ) -> Dict[str, Any]:
        if range_ not in ALLOWED_RANGES:
            raise ValueError("unsupported range")
        if interval not in ALLOWED_INTERVALS:
            raise ValueError("unsupported interval")
        if adjustment not in ("adjusted", "qfq", "raw"):
            raise ValueError("Yahoo adjustment must be raw or qfq")
        params = {
            "range": range_, "interval": interval, "includePrePost": "false",
            "events": "div,splits", "includeAdjustedClose": "true",
        }
        return self._fetch_history_params(
            value, params, range_, interval, adjustment
        )

    def fetch_history_window(
        self, value: str, start: datetime, end: datetime,
        interval: str, adjustment: str,
    ) -> Dict[str, Any]:
        if start.tzinfo is None or end.tzinfo is None or start >= end:
            raise ValueError("history window must be ordered and timezone-aware")
        if interval not in ALLOWED_INTERVALS:
            raise ValueError("unsupported interval")
        if adjustment not in ("adjusted", "qfq", "raw"):
            raise ValueError("Yahoo adjustment must be raw or qfq")
        params = {
            "period1": int(start.timestamp()), "period2": int(end.timestamp()),
            "interval": interval, "includePrePost": "false",
            "events": "div,splits", "includeAdjustedClose": "true",
        }
        return self._fetch_history_params(
            value, params,
            f"{start.astimezone(timezone.utc).isoformat()}/"
            f"{end.astimezone(timezone.utc).isoformat()}",
            interval, adjustment,
        )

    def _fetch_history_params(
        self, value: str, params: Dict[str, Any], range_label: str,
        interval: str, adjustment: str,
    ) -> Dict[str, Any]:
        instrument = canonical_instrument(value)
        symbol, market = normalize_yahoo_symbol(value)
        payload, source_url = self._fetch_chart(symbol, params)
        result = self._result(payload)
        meta = result.get("meta") or {}
        # Yahoo's adjusted close is a back-adjusted series. Keep accepting the
        # old provider-specific spelling at this boundary, but never emit it.
        adjustment = "qfq" if adjustment == "adjusted" else adjustment
        timestamps = result.get("timestamp") or []
        indicators = result.get("indicators") or {}
        quote = (indicators.get("quote") or [{}])[0]
        adjusted_close = (indicators.get("adjclose") or [{}])[0].get("adjclose") or []
        exchange_zone = ZoneInfo(meta.get("exchangeTimezoneName") or "UTC")
        reference_date = (
            datetime.fromtimestamp(int(timestamps[-1]), exchange_zone).date().isoformat()
            if timestamps else None
        )
        bars: List[Dict[str, Any]] = []
        for index, timestamp in enumerate(timestamps):
            raw_close = _float((quote.get("close") or [])[index]) if index < len(quote.get("close") or []) else None
            adj_close = _float(adjusted_close[index]) if index < len(adjusted_close) else None
            factor = adj_close / raw_close if adj_close is not None and raw_close else None
            if raw_close is not None and factor is None:
                raise ValueError(
                    "Yahoo history is missing an adjustment factor for a price bar"
                )
            applied_multiplier = factor if adjustment == "qfq" and factor else 1.0
            def value_for(field: str) -> Optional[float]:
                values = quote.get(field) or []
                number = _float(values[index]) if index < len(values) else None
                return (
                    number * applied_multiplier if number is not None else None
                )
            close = adj_close if adjustment == "qfq" and adj_close is not None else raw_close
            if close is None:
                continue
            volumes = quote.get("volume") or []
            bars.append({
                "timestamp": int(timestamp),
                "bar_at": _iso_utc(timestamp),
                "open": value_for("open"),
                "high": value_for("high"),
                "low": value_for("low"),
                "close": close,
                "raw_close": raw_close,
                "adjustment_factor": applied_multiplier,
                "factor_applicability": "applicable",
                "corporate_action_factor": (
                    None if factor is None else format(Decimal(str(factor)), "f")
                ),
                "applied_adjustment_multiplier": format(
                    Decimal(str(applied_multiplier)), "f"
                ),
                "adjustment_reference_date": (
                    reference_date if adjustment == "qfq" else None
                ),
                "reference_factor": "1" if adjustment == "qfq" else None,
                "factor_source": self.name,
                "volume": _float(volumes[index]) if index < len(volumes) else None,
            })
        return {
            "instrument_id": instrument.instrument_id,
            "symbol": instrument.instrument_id,
            "provider_symbol": symbol,
            "name": meta.get("shortName") or meta.get("longName") or symbol,
            "market": market,
            "exchange": meta.get("exchangeName") or meta.get("fullExchangeName"),
            "currency": meta.get("currency") or ("HKD" if market == "HK" else "USD"),
            "range": range_label,
            "interval": interval,
            "adjustment": adjustment,
            "quality_status": "single_source_unverified",
            "exchange_timezone": meta.get("exchangeTimezoneName"),
            "source": self.name,
            "source_url": source_url,
            "raw_response_locator": "chart.result[0].indicators",
            "bars": bars,
            "_raw_payload": payload,
        }
