from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Iterable, Optional


def _decimal(value: Any) -> str:
    number = Decimal(str(value))
    if not number.is_finite():
        raise ValueError("Hyperliquid market value must be finite")
    return format(number, "f")


def _iso_millis(value: Any) -> str:
    return datetime.fromtimestamp(
        int(value) / 1000, timezone.utc
    ).isoformat().replace("+00:00", "Z")


class HyperliquidRealtimeProvider:
    """Reference-counted public Hyperliquid BBO/trade WebSocket adapter."""

    def __init__(
        self, base_url: str = "https://api.hyperliquid.xyz",
        app_factory: Optional[Callable[..., Any]] = None,
    ) -> None:
        self.ws_url = base_url.rstrip("/").replace("https://", "wss://").replace(
            "http://", "ws://"
        ) + "/ws"
        self.app_factory = app_factory
        self._app: Any = None
        self._thread: Optional[threading.Thread] = None
        self._connected = threading.Event()
        self._stop = threading.Event()
        self._sink: Callable[[dict[str, Any]], None] = lambda _event: None
        self._mapping: dict[str, str] = {}
        self._provider_symbol_by_instrument: dict[str, str] = {}
        self._references: dict[tuple[str, str], int] = {}
        self._lock = threading.RLock()

    def set_sink(self, sink: Callable[[dict[str, Any]], None]) -> None:
        self._sink = sink

    def _connect(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="hyperliquid-realtime", daemon=True,
        )
        self._thread.start()
        if not self._connected.wait(5):
            self.close()
            raise RuntimeError("Hyperliquid WebSocket connection timed out")

    def _run(self) -> None:
        delay = 0.5
        while not self._stop.is_set():
            factory = self.app_factory
            if factory is None:
                from websocket import WebSocketApp
                factory = WebSocketApp
            app = factory(
                self.ws_url, on_open=self._on_open,
                on_message=self._on_message, on_error=self._on_error,
                on_close=self._on_close,
            )
            self._app = app
            app.run_forever()
            self._connected.clear()
            if self._stop.wait(delay):
                break
            delay = min(10.0, delay * 2)

    def _on_open(self, _app: Any) -> None:
        self._connected.set()
        with self._lock:
            subscriptions = [
                (coin, kind)
                for (coin, kind), count in self._references.items()
                if count > 0
            ]
        for coin, kind in subscriptions:
            self._send("subscribe", kind, coin)

    def _send(self, method: str, kind: str, coin: str) -> None:
        if self._app is None:
            raise RuntimeError("Hyperliquid WebSocket is not connected")
        self._app.send(json.dumps({
            "method": method,
            "subscription": {"type": kind, "coin": coin},
        }, separators=(",", ":")))

    def subscribe(self, mappings: dict[str, str], data_types: set[str]) -> None:
        with self._lock:
            self._connect()
            wants_bbo = bool({"quote", "order_book"} & data_types)
            wants_trades = bool({"trade", "bar"} & data_types)
            for instrument, coin in mappings.items():
                self._mapping[coin.upper()] = instrument
                self._provider_symbol_by_instrument[instrument] = coin
                for kind, wanted in (("bbo", wants_bbo), ("trades", wants_trades)):
                    if not wanted:
                        continue
                    key = (coin, kind)
                    if self._references.get(key, 0) == 0:
                        self._send("subscribe", kind, coin)
                    self._references[key] = self._references.get(key, 0) + 1

    def unsubscribe(self, filters: Iterable[tuple[str, str]]) -> None:
        with self._lock:
            normalized = {
                (
                    instrument,
                    "trades" if kind in {"trade", "bar"} else "bbo",
                )
                for instrument, kind in filters
            }
            for instrument, kind in normalized:
                coin = self._provider_symbol_by_instrument.get(instrument)
                if coin is None:
                    continue
                key = (coin, kind)
                count = self._references.get(key, 0)
                if count <= 0:
                    continue
                if count == 1:
                    self._send("unsubscribe", kind, coin)
                    self._references.pop(key, None)
                else:
                    self._references[key] = count - 1

    def _on_message(self, _app: Any, raw: str) -> None:
        message = json.loads(raw)
        channel, data = message.get("channel"), message.get("data")
        if channel == "bbo" and isinstance(data, dict):
            self._on_bbo(data)
        elif channel == "trades" and isinstance(data, list):
            for trade in data:
                self._on_trade(trade)

    def _on_bbo(self, data: dict[str, Any]) -> None:
        instrument = self._mapping.get(str(data.get("coin")).upper())
        if instrument is None:
            return
        levels = data.get("bbo") or [None, None]
        bid, ask = levels[0], levels[1]
        bids = [] if not bid else [{
            "price": _decimal(bid["px"]), "size": _decimal(bid["sz"]),
            "order_id": "0",
        }]
        asks = [] if not ask else [{
            "price": _decimal(ask["px"]), "size": _decimal(ask["sz"]),
            "order_id": "0",
        }]
        ts_event = _iso_millis(data["time"])
        self._sink({
            "event_type": "order_book_snapshot", "instrument_id": instrument,
            "source": "hyperliquid", "ts_event": ts_event,
            "payload": {
                "book_type": "L1_MBP", "depth": 1, "baseline_sequence": 0,
                "bids": bids, "asks": asks,
            },
        })
        if bid and ask:
            self._sink({
                "event_type": "quote", "instrument_id": instrument,
                "source": "hyperliquid", "ts_event": ts_event,
                "payload": {
                    "bid_price": _decimal(bid["px"]),
                    "ask_price": _decimal(ask["px"]),
                    "bid_size": _decimal(bid["sz"]),
                    "ask_size": _decimal(ask["sz"]),
                    "ts_event_source": "provider",
                },
            })

    def _on_trade(self, trade: dict[str, Any]) -> None:
        instrument = self._mapping.get(str(trade.get("coin")).upper())
        if instrument is None:
            return
        trade_id = str(trade.get("tid") or trade.get("hash") or trade["time"])
        self._sink({
            "event_type": "trade", "instrument_id": instrument,
            "source": "hyperliquid", "ts_event": _iso_millis(trade["time"]),
            "payload": {
                "price": _decimal(trade["px"]), "size": _decimal(trade["sz"]),
                "trade_id": trade_id,
                "aggressor_side": (
                    "BUYER" if str(trade.get("side")).upper() == "B"
                    else "SELLER"
                ),
                "session": "regular",
            },
        })

    def _on_error(self, _app: Any, _error: Any) -> None:
        self._connected.clear()

    def _on_close(self, _app: Any, *_args: Any) -> None:
        self._connected.clear()

    def close(self) -> None:
        with self._lock:
            app, thread = self._app, self._thread
            self._stop.set()
            self._app = None
            self._thread = None
            self._connected.clear()
        if app is not None:
            app.close()
        if thread is not None:
            thread.join(timeout=5)


class RoutingRealtimeProvider:
    """Route one public stream contract across venue-specific adapters."""

    def __init__(self, longport: Any, hyperliquid: HyperliquidRealtimeProvider) -> None:
        self.longport = longport
        self.hyperliquid = hyperliquid

    def set_sink(self, sink: Callable[[dict[str, Any]], None]) -> None:
        self.longport.set_sink(sink)
        self.hyperliquid.set_sink(sink)

    @staticmethod
    def _is_hyperliquid(instrument: str) -> bool:
        return instrument.endswith(".HYPL")

    def subscribe(self, mappings: dict[str, str], data_types: set[str]) -> None:
        hyperliquid = {
            key: value for key, value in mappings.items()
            if self._is_hyperliquid(key)
        }
        longport = {
            key: value for key, value in mappings.items()
            if not self._is_hyperliquid(key)
        }
        if longport:
            self.longport.subscribe(longport, data_types)
        if hyperliquid:
            self.hyperliquid.subscribe(hyperliquid, data_types)

    def unsubscribe(self, filters: Iterable[tuple[str, str]]) -> None:
        values = set(filters)
        self.longport.unsubscribe({
            item for item in values if not self._is_hyperliquid(item[0])
        })
        self.hyperliquid.unsubscribe({
            item for item in values if self._is_hyperliquid(item[0])
        })

    def close(self) -> None:
        self.longport.close()
        self.hyperliquid.close()
