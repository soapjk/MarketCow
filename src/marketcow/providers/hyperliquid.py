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
_HIP3 = re.compile(r"^([A-Z0-9.-]{1,20})-PERP\.([A-Z0-9]{4})$")
VERIFIED_XYZ_EQUITIES = frozenset({
    "AAPL", "MSFT", "META", "NVDA", "TSLA", "MU", "DRAM",
})
KNOWN_HIP3_INDICES = frozenset({"XYZ100", "SP500", "US500"})


def hip3_mic(dex: str) -> str:
    """Return a stable MarketCow venue code without claiming an ISO MIC."""
    name = re.sub(r"[^A-Z0-9]", "", dex.upper())
    if not name:
        raise ValueError("HIP-3 dex name is invalid")
    return (name[:3].ljust(3, "X") + "H")


def hyperliquid_instrument(value: str) -> tuple[str, str, str]:
    text = str(value).strip().upper()
    match = _HIP3.fullmatch(text)
    if match and match.group(2) != "HYPL":
        symbol, mic = match.groups()
        return text, f"{mic.lower()}:{symbol}", "hip3_perpetual"
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
        dex_rows = self._post({"type": "perpDexs"})
        for dex_entry in dex_rows:
            if not dex_entry:
                continue
            dex = str(
                dex_entry.get("name") if isinstance(dex_entry, dict) else dex_entry
            ).strip()
            if not dex:
                continue
            hip3_meta, hip3_contexts = self._post({
                "type": "metaAndAssetCtxs", "dex": dex,
            })
            mic = hip3_mic(dex)
            for meta, context in zip(hip3_meta.get("universe", []), hip3_contexts):
                provider_symbol = str(meta["name"])
                display_symbol = provider_symbol.split(":", 1)[-1].upper()
                size_precision = int(meta["szDecimals"])
                price_precision = max(0, 6 - size_precision)
                is_index = display_symbol in KNOWN_HIP3_INDICES
                is_verified_equity = (
                    dex.lower() == "xyz"
                    and display_symbol in VERIFIED_XYZ_EQUITIES
                )
                rows.append({
                    "instrument_id": f"{display_symbol}-PERP.{mic}",
                    "instrument_type": (
                        "index_perpetual" if is_index
                        else "equity_perpetual" if is_verified_equity
                        else "hip3_perpetual"
                    ),
                    "asset_class": (
                        "index_derivative" if is_index else "equity_derivative"
                        if is_verified_equity else "other_derivative"
                    ),
                    "symbol": f"{display_symbol}-PERP", "market": "US",
                    "mic": mic, "currency": "USD",
                    "price_precision": price_precision,
                    "size_precision": size_precision,
                    "tick_size": format(Decimal(1).scaleb(-price_precision), "f"),
                    "size_increment": format(
                        Decimal(1).scaleb(-size_precision), "f"
                    ),
                    "lot_size": format(Decimal(1).scaleb(-size_precision), "f"),
                    "ts_event": now, "ts_init": now,
                    "provider_symbols": {"hyperliquid": provider_symbol},
                    "broker_symbols": {},
                    "venue_metadata": {
                        "kind": "hip3_perpetual", "dex": dex,
                        "dex_mic": mic, "context": context,
                        "is_delisted": bool(meta.get("isDelisted", False)),
                        "classification_status": (
                            "verified" if is_index or is_verified_equity
                            else "unclassified"
                        ),
                        "max_leverage": meta.get("maxLeverage"),
                        "only_isolated": bool(meta.get("onlyIsolated", False)),
                    },
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
        venue = instrument.get("venue_metadata") or {}
        mids_payload: Dict[str, Any] = {"type": "allMids"}
        if venue.get("dex"):
            mids_payload["dex"] = venue["dex"]
        mids = self._post(mids_payload)
        mid = mids.get(provider_symbol)
        if mid is None:
            raise RuntimeError(f"Hyperliquid returned no mid for {provider_symbol}")
        context = (instrument.get("venue_metadata") or {}).get("context") or {}
        now = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        return {
            "instrument_id": instrument["instrument_id"],
            "symbol": instrument["instrument_id"], "name": instrument["symbol"],
            "market": instrument.get("market") or "CRYPTO",
            "exchange": instrument.get("mic") or "HYPL",
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

    def fetch_order_book(self, value: str, depth: int = 20) -> Dict[str, Any]:
        if depth not in {1, 5, 10, 20}:
            raise ValueError("Hyperliquid order book depth must be 1, 5, 10 or 20")
        instrument = self._resolve(value)
        coin = instrument["provider_symbols"]["hyperliquid"]
        payload = self._post({"type": "l2Book", "coin": coin})
        levels = payload.get("levels") or [[], []]
        normalize = lambda rows: [{
            "price": str(row["px"]), "size": str(row["sz"]),
            "order_count": int(row["n"]) if row.get("n") is not None else None,
        } for row in rows[:depth]]
        return {
            "instrument_id": instrument["instrument_id"],
            "source": self.name,
            "source_url": self.info_url,
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "depth": depth,
            "bids": normalize(levels[0]),
            "asks": normalize(levels[1]),
            "_raw_payload": payload,
        }

    def fetch_asset_context(self, value: str) -> Dict[str, Any]:
        instrument = self._resolve(value)
        venue = instrument.get("venue_metadata") or {}
        context = venue.get("context") or {}
        if venue.get("dex"):
            meta, contexts = self._post({
                "type": "metaAndAssetCtxs", "dex": venue["dex"],
            })
            names = [str(row["name"]) for row in meta.get("universe", [])]
            provider_symbol = instrument["provider_symbols"]["hyperliquid"]
            if provider_symbol in names:
                context = contexts[names.index(provider_symbol)]
        return {
            "instrument_id": instrument["instrument_id"],
            "mark_price": context.get("markPx"),
            "oracle_price": context.get("oraclePx"),
            "external_oracle_price": context.get("externalPerpPx"),
            "mid_price": context.get("midPx"),
            "funding_rate": context.get("funding"),
            "open_interest": context.get("openInterest"),
            "premium": context.get("premium"),
            "max_leverage": venue.get("max_leverage"),
            "market_status": "delisted" if venue.get("is_delisted") else "active",
            "oracle_status": (
                "external_live" if context.get("externalPerpPx")
                else "internal_only" if context.get("oraclePx") else "unavailable"
            ),
            "source": self.name,
            "observed_at": datetime.now(timezone.utc).isoformat(),
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
        end = datetime.now(timezone.utc)
        start = end - RANGES[range_]
        return self.fetch_history_window(
            value, start, end, interval, adjustment
        )

    def fetch_history_window(
        self, value: str, start: datetime, end: datetime,
        interval: str, adjustment: str,
    ) -> Dict[str, Any]:
        if start.tzinfo is None or end.tzinfo is None or start >= end:
            raise ValueError("history window must be ordered and timezone-aware")
        if interval not in INTERVALS:
            raise ValueError("unsupported Hyperliquid interval")
        if adjustment != "raw":
            raise ValueError("Hyperliquid history only supports raw adjustment")
        instrument = self._resolve(value)
        coin = instrument["provider_symbols"]["hyperliquid"]
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
            "market": instrument.get("market") or "CRYPTO",
            "exchange": instrument.get("mic") or "HYPL",
            "currency": instrument.get("currency") or "USD",
            "range": (
                f"{start.astimezone(timezone.utc).isoformat()}/"
                f"{end.astimezone(timezone.utc).isoformat()}"
            ),
            "interval": interval, "adjustment": adjustment,
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
        if instrument["instrument_type"] not in {
            "crypto_perpetual", "equity_perpetual", "index_perpetual",
            "hip3_perpetual",
        }:
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
