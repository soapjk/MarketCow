from __future__ import annotations

import re
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List

import requests


HYPERLIQUID_MAINNET_URL = "https://api.hyperliquid.xyz"
INTERVALS = frozenset({
    "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "8h", "12h",
    "1d", "3d", "1w", "1M",
})
RANGES = {
    "1d": timedelta(days=1), "5d": timedelta(days=5),
    "1mo": timedelta(days=30), "3mo": timedelta(days=90),
    "6mo": timedelta(days=180), "1y": timedelta(days=365),
    "2y": timedelta(days=730), "5y": timedelta(days=1825),
    "10y": timedelta(days=3650), "max": timedelta(days=36500),
}
_PERP = re.compile(r"^([A-Z0-9]{1,20})-PERP(?:\.HYPL)?$")
_SPOT = re.compile(r"^([A-Z0-9]{1,20})[-/]([A-Z0-9]{1,20})(?:\.HYPL)?$")


def hyperliquid_instrument(value: str) -> tuple[str, str, str]:
    text = str(value).strip().upper()
    match = _PERP.fullmatch(text)
    if match:
        coin = match.group(1)
        return f"{coin}-PERP.HYPL", coin, "crypto_perpetual"
    match = _SPOT.fullmatch(text)
    if match:
        base, quote = match.groups()
        return f"{base}-{quote}.HYPL", f"{base}/{quote}", "crypto_spot"
    raise ValueError(
        "Hyperliquid symbols must use COIN-PERP.HYPL or BASE-QUOTE.HYPL"
    )


class HyperliquidProvider:
    name = "hyperliquid_mainnet"

    def __init__(
        self, base_url: str = HYPERLIQUID_MAINNET_URL, timeout: float = 3.0,
        request_budget: float = 10.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.request_budget = request_budget
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "MarketCow/0.2",
            "Accept": "application/json",
            "Content-Type": "application/json",
        })
        self._catalog: Dict[str, Dict[str, Any]] = {}
        self._catalog_at = 0.0

    @property
    def info_url(self) -> str:
        return self.base_url + "/info"

    def _post(self, payload: Dict[str, Any]) -> Any:
        started = time.monotonic()
        response = self.session.post(
            self.info_url, json=payload,
            timeout=min(self.timeout, self.request_budget),
        )
        response.raise_for_status()
        if time.monotonic() - started > self.request_budget:
            raise RuntimeError("Hyperliquid request budget exhausted")
        return response.json()

    def instruments(self, force: bool = False) -> List[Dict[str, Any]]:
        if self._catalog and not force and time.monotonic() - self._catalog_at < 300:
            return list(self._catalog.values())
        perp_meta, perp_contexts = self._post({"type": "metaAndAssetCtxs"})
        spot_meta, spot_contexts = self._post({"type": "spotMetaAndAssetCtxs"})
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        rows: List[Dict[str, Any]] = []
        for meta, context in zip(perp_meta["universe"], perp_contexts):
            coin = str(meta["name"]).upper()
            size_precision = int(meta["szDecimals"])
            price_precision = max(0, 6 - size_precision)
            rows.append({
                "instrument_id": f"{coin}-PERP.HYPL",
                "instrument_type": "crypto_perpetual", "asset_class": "crypto",
                "symbol": f"{coin}-PERP", "market": "CRYPTO", "mic": "HYPL",
                "currency": "USDC", "price_precision": price_precision,
                "size_precision": size_precision,
                "tick_size": format(Decimal(1).scaleb(-price_precision), "f"),
                "size_increment": format(Decimal(1).scaleb(-size_precision), "f"),
                "lot_size": format(Decimal(1).scaleb(-size_precision), "f"),
                "ts_event": now, "ts_init": now,
                "provider_symbols": {"hyperliquid": coin}, "broker_symbols": {},
                "venue_metadata": {"kind": "perpetual", "context": context},
            })
        tokens = {int(token["index"]): token for token in spot_meta["tokens"]}
        for meta, context in zip(spot_meta["universe"], spot_contexts):
            base = tokens[int(meta["tokens"][0])]
            quote = tokens[int(meta["tokens"][1])]
            base_name, quote_name = str(base["name"]).upper(), str(quote["name"]).upper()
            display = f"{base_name}-{quote_name}"
            provider_symbol = str(meta["name"])
            size_precision = int(base["szDecimals"])
            price_precision = max(0, 8 - size_precision)
            rows.append({
                "instrument_id": f"{display}.HYPL",
                "instrument_type": "crypto_spot", "asset_class": "crypto",
                "symbol": display, "market": "CRYPTO", "mic": "HYPL",
                "currency": quote_name, "price_precision": price_precision,
                "size_precision": size_precision,
                "tick_size": format(Decimal(1).scaleb(-price_precision), "f"),
                "size_increment": format(Decimal(1).scaleb(-size_precision), "f"),
                "lot_size": format(Decimal(1).scaleb(-size_precision), "f"),
                "ts_event": now, "ts_init": now,
                "provider_symbols": {"hyperliquid": provider_symbol},
                "broker_symbols": {},
                "venue_metadata": {
                    "kind": "spot", "spot_index": int(meta["index"]),
                    "token_ids": [base["tokenId"], quote["tokenId"]],
                    "is_canonical": bool(meta.get("isCanonical")), "context": context,
                },
            })
        self._catalog = {row["instrument_id"]: row for row in rows}
        self._catalog_at = time.monotonic()
        return list(rows)

    def _resolve(self, value: str) -> Dict[str, Any]:
        instrument_id, provider_symbol, kind = hyperliquid_instrument(value)
        catalog = {row["instrument_id"]: row for row in self.instruments()}
        row = catalog.get(instrument_id)
        if row is not None:
            return row
        if kind == "crypto_perpetual":
            return {
                "instrument_id": instrument_id, "symbol": instrument_id[:-5],
                "provider_symbols": {"hyperliquid": provider_symbol},
                "instrument_type": kind,
            }
        raise ValueError(f"unknown Hyperliquid instrument: {instrument_id}")

    def fetch_quote(self, value: str) -> Dict[str, Any]:
        instrument = self._resolve(value)
        provider_symbol = instrument["provider_symbols"]["hyperliquid"]
        mids = self._post({"type": "allMids"})
        mid = mids.get(provider_symbol)
        if mid is None:
            raise RuntimeError(f"Hyperliquid returned no mid for {provider_symbol}")
        context = (instrument.get("venue_metadata") or {}).get("context") or {}
        now = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        return {
            "instrument_id": instrument["instrument_id"],
            "symbol": instrument["instrument_id"], "name": instrument["symbol"],
            "market": "CRYPTO", "exchange": "HYPL",
            "currency": instrument.get("currency") or "USD",
            "price": float(mid), "previous_close": (
                float(context["prevDayPx"]) if context.get("prevDayPx") else None
            ),
            "change": None, "change_pct": None, "session": "regular",
            "quote_at": now, "price_adjustment": "raw",
            "quality_status": "single_source_unverified", "source": self.name,
            "source_url": self.info_url, "raw_response_locator": "allMids",
            "mark_price": context.get("markPx"), "oracle_price": context.get("oraclePx"),
            "funding_rate": context.get("funding"),
            "open_interest": context.get("openInterest"),
            "_raw_payload": {"mid": mid, "context": context},
        }

    def fetch_history(
        self, value: str, range_: str, interval: str, adjustment: str,
    ) -> Dict[str, Any]:
        if range_ not in RANGES:
            raise ValueError("unsupported Hyperliquid range")
        if interval not in INTERVALS:
            raise ValueError("unsupported Hyperliquid interval")
        if adjustment != "raw":
            raise ValueError("Hyperliquid history only supports raw adjustment")
        instrument = self._resolve(value)
        coin = instrument["provider_symbols"]["hyperliquid"]
        end = datetime.now(timezone.utc)
        start = end - RANGES[range_]
        payload = self._post({
            "type": "candleSnapshot",
            "req": {
                "coin": coin, "interval": interval,
                "startTime": int(start.timestamp() * 1000),
                "endTime": int(end.timestamp() * 1000),
            },
        })
        bars = [{
            "timestamp": int(candle["t"]) // 1000,
            "bar_at": datetime.fromtimestamp(
                int(candle["t"]) / 1000, timezone.utc
            ).isoformat(),
            "open": float(candle["o"]), "high": float(candle["h"]),
            "low": float(candle["l"]), "close": float(candle["c"]),
            "raw_close": float(candle["c"]), "adjustment_factor": 1.0,
            "volume": float(candle["v"]), "trade_count": int(candle["n"]),
            "window_end": int(candle["T"]),
        } for candle in payload]
        return {
            "instrument_id": instrument["instrument_id"],
            "symbol": instrument["instrument_id"], "name": instrument["symbol"],
            "market": "CRYPTO", "exchange": "HYPL",
            "currency": instrument.get("currency") or "USD",
            "range": range_, "interval": interval, "adjustment": adjustment,
            "quality_status": "single_source_unverified",
            "exchange_timezone": "UTC", "source": self.name,
            "source_url": self.info_url,
            "raw_response_locator": "candleSnapshot",
            "bars": bars, "_raw_payload": payload,
        }

    def fetch_funding_history(
        self, value: str, start: datetime, end: datetime,
    ) -> Dict[str, Any]:
        instrument = self._resolve(value)
        if instrument["instrument_type"] != "crypto_perpetual":
            raise ValueError("funding history is only available for perpetuals")
        if start.tzinfo is None or end.tzinfo is None or start > end:
            raise ValueError("funding range must be ordered and timezone-aware")
        coin = instrument["provider_symbols"]["hyperliquid"]
        payload = self._post({
            "type": "fundingHistory", "coin": coin,
            "startTime": int(start.timestamp() * 1000),
            "endTime": int(end.timestamp() * 1000),
        })
        rows = [{
            "instrument_id": instrument["instrument_id"],
            "funding_at": _iso_funding(row["time"]),
            "funding_rate": str(row["fundingRate"]),
            "premium": (
                str(row["premium"]) if row.get("premium") is not None else None
            ),
        } for row in payload]
        return {
            "instrument_id": instrument["instrument_id"],
            "source": self.name, "source_url": self.info_url,
            "count": len(rows), "items": rows, "_raw_payload": payload,
        }


def _iso_funding(value: Any) -> str:
    return datetime.fromtimestamp(
        int(value) / 1000, timezone.utc
    ).isoformat().replace("+00:00", "Z")
