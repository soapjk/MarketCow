"""Bounded BTCUSDT spot 1m cold-start evidence and live-stream join.

Bootstrap observations become available only when MarketCow receives them. They
are never backdated to the exchange event time and never authorize historical PnL.
"""
from __future__ import annotations

import base64
import hashlib
import json
import threading
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable

import requests

from .btc_hourly_dataset import canonical


STEP_MS = 60_000
SOURCE = "binance_global_spot_rest"
DEFAULT_URL = "https://data-api.binance.vision/api/v3/klines"


class BootstrapError(RuntimeError):
    """A fail-closed bootstrap or live-join contract violation."""


@dataclass(frozen=True)
class BootstrapBudget:
    bars: int = 60
    maximum_bytes: int = 1_048_576
    timeout_seconds: float = 10.0

    def validate(self) -> None:
        if not 60 <= self.bars <= 1_000:
            raise ValueError("bootstrap bars must be between 60 and 1000")
        if not 1 <= self.maximum_bytes <= 8 * 1024 * 1024:
            raise ValueError("invalid bootstrap byte budget")
        if not 0.1 <= self.timeout_seconds <= 30:
            raise ValueError("invalid bootstrap timeout")


def _utc(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("timezone-aware timestamp required")
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _decimal(value: Any, field: str, *, positive: bool = False) -> str:
    if not isinstance(value, str):
        raise BootstrapError(f"{field}_must_be_decimal_string")
    try:
        number = Decimal(value)
    except InvalidOperation as exc:
        raise BootstrapError(f"invalid_{field}") from exc
    if not number.is_finite() or number < 0 or positive and number == 0:
        raise BootstrapError(f"invalid_{field}")
    return format(number, "f")


def _validate_row(row: Any, expected_open: int, completed_through: int) -> dict[str, Any]:
    if not isinstance(row, list) or len(row) != 12:
        raise BootstrapError("invalid_kline_shape")
    if type(row[0]) is not int or row[0] != expected_open:
        raise BootstrapError("kline_gap_or_order")
    if type(row[6]) is not int or row[6] != row[0] + STEP_MS - 1:
        raise BootstrapError("invalid_kline_window")
    if row[6] > completed_through:
        raise BootstrapError("unfinished_kline_in_bootstrap")
    if type(row[8]) is not int or row[8] < 0:
        raise BootstrapError("invalid_trade_count")
    o, high, low, close = (
        Decimal(_decimal(row[index], field, positive=True))
        for index, field in ((1, "open"), (2, "high"), (3, "low"), (4, "close"))
    )
    if not low <= min(o, close) <= max(o, close) <= high:
        raise BootstrapError("invalid_ohlc")
    return {
        "open_time_ms": row[0],
        "close_time_ms": row[6],
        "open": format(o, "f"),
        "high": format(high, "f"),
        "low": format(low, "f"),
        "close": format(close, "f"),
        "volume": _decimal(row[5], "volume"),
        "quote_volume": _decimal(row[7], "quote_volume"),
        "trade_count": row[8],
        "taker_buy_base_volume": _decimal(row[9], "taker_buy_base_volume"),
        "taker_buy_quote_volume": _decimal(row[10], "taker_buy_quote_volume"),
    }


def _live_bar(data: Any) -> dict[str, Any] | None:
    if not isinstance(data, dict) or data.get("e") != "kline":
        return None
    kline = data.get("k")
    if not isinstance(kline, dict) or kline.get("i") != "1m":
        return None
    if kline.get("s") != "BTCUSDT" or data.get("s") != "BTCUSDT":
        raise BootstrapError("live_instrument_mismatch")
    if kline.get("x") is not True:
        return None
    row = [
        kline.get("t"), kline.get("o"), kline.get("h"), kline.get("l"),
        kline.get("c"), kline.get("v"), kline.get("T"), kline.get("q"),
        kline.get("n"), kline.get("V"), kline.get("Q"), kline.get("B", "0"),
    ]
    close_time = kline.get("T")
    return _validate_row(row, kline.get("t"), close_time)


class BtcMinuteBootstrap:
    """One-request bootstrap with an explicit finalized-websocket join gate."""

    def __init__(
        self,
        *,
        budget: BootstrapBudget | None = None,
        url: str = DEFAULT_URL,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.budget = budget or BootstrapBudget()
        self.budget.validate()
        if url != DEFAULT_URL:
            raise ValueError("unsupported Binance bootstrap origin")
        self.url = url
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.lock = threading.Lock()
        self.phase = "not_started"
        self.reason = "bootstrap_not_started"
        self.bars: dict[int, dict[str, Any]] = {}
        self.evidence: dict[int, dict[str, Any]] = {}
        self.last_open_ms: int | None = None
        self.received_at: str | None = None
        self.raw_sha256: str | None = None
        self.raw_sequence: int | None = None
        self.validated_sequence: int | None = None
        self.readiness_sequence: int | None = None
        self.joined_at: str | None = None
        self.pending_checkpoints: deque[tuple[int, list[dict[str, Any]], str]] = deque()
        self.durable_checkpoint: tuple[int, list[dict[str, Any]], str] | None = None
        self.failure: str | None = None

    def request(self, now: datetime | None = None) -> tuple[dict[str, str], int]:
        current = now or self.clock()
        if current.tzinfo is None:
            raise ValueError("timezone-aware timestamp required")
        now_ms = int(current.astimezone(timezone.utc).timestamp() * 1000)
        last_open = now_ms // STEP_MS * STEP_MS - STEP_MS
        first_open = last_open - (self.budget.bars - 1) * STEP_MS
        return ({
            "symbol": "BTCUSDT",
            "interval": "1m",
            "startTime": str(first_open),
            "endTime": str(last_open + STEP_MS - 1),
            "limit": str(self.budget.bars),
        }, last_open)

    def capture(
        self,
        log: Any,
        *,
        session: Any | None = None,
        proxy_url: str | None = None,
    ) -> dict[str, Any]:
        with self.lock:
            if self.phase != "not_started":
                raise BootstrapError("bootstrap_already_attempted")
            self.phase, self.reason = "fetching", "bootstrap_fetching"
        params, last_open = self.request()
        client = session or requests.Session()
        if hasattr(client, "trust_env"):
            client.trust_env = True
        proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None
        try:
            response = client.get(
                self.url,
                params=params,
                timeout=self.budget.timeout_seconds,
                allow_redirects=False,
                stream=True,
                proxies=proxies,
                headers={"Accept": "application/json", "User-Agent": "MarketCow/btc-bootstrap-v1"},
            )
            try:
                if response.status_code != 200:
                    raise BootstrapError(f"bootstrap_http_status_{response.status_code}")
                raw = bytearray()
                for chunk in response.iter_content(chunk_size=65_536):
                    if not chunk:
                        continue
                    if len(raw) + len(chunk) > self.budget.maximum_bytes:
                        raise BootstrapError("bootstrap_response_size_exceeded")
                    raw.extend(chunk)
            finally:
                response.close()
            received_at = _utc(self.clock())
            raw_bytes = bytes(raw)
            raw_sha = hashlib.sha256(raw_bytes).hexdigest()
            raw_sequence = log.publish({
                "schema_version": "marketcow.binance-bootstrap-raw.v1",
                "event_type": "btc_1m_bootstrap_raw",
                "source": SOURCE,
                "instrument_id": "BTCUSDT.BINANCE",
                "request": {"url": self.url, "params": params},
                "http_status": response.status_code,
                "received_at": received_at,
                "first_received_at": received_at,
                "available_at": received_at,
                "historical_first_received_at": None,
                "raw_sha256": raw_sha,
                "raw_byte_size": len(raw_bytes),
                "raw_base64": base64.b64encode(raw_bytes).decode("ascii"),
                "decision_boundary": "current_and_future_after_received_at_only",
            })
            try:
                payload = json.loads(
                    raw_bytes,
                    parse_constant=lambda _value: (_ for _ in ()).throw(
                        BootstrapError("invalid_json_number")
                    ),
                )
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise BootstrapError("invalid_bootstrap_json") from exc
            if not isinstance(payload, list) or len(payload) != self.budget.bars:
                raise BootstrapError("incomplete_bootstrap")
            first_open = last_open - (self.budget.bars - 1) * STEP_MS
            completed_through = last_open + STEP_MS - 1
            bars = {
                first_open + index * STEP_MS: _validate_row(
                    row, first_open + index * STEP_MS, completed_through
                )
                for index, row in enumerate(payload)
            }
            manifest = {
                "schema_version": "marketcow.binance-bootstrap-manifest.v1",
                "event_type": "btc_1m_bootstrap_validated",
                "source": SOURCE,
                "instrument_id": "BTCUSDT.BINANCE",
                "interval": "1m",
                "count": len(bars),
                "first_open_ms": first_open,
                "last_open_ms": last_open,
                "completed_through_ms": completed_through,
                "received_at": received_at,
                "first_received_at": received_at,
                "available_at": received_at,
                "historical_first_received_at": None,
                "raw_sha256": raw_sha,
                "raw_fact_sequence": raw_sequence,
                "continuous": True,
                "unfinished_rows": 0,
                "gaps": [],
                "ready": False,
                "reason": "awaiting_live_finalized_join",
                "decision_boundary": "current_and_future_after_received_at_only",
                "bars": list(bars.values()),
            }
            validated_sequence = log.publish(manifest)
            with self.lock:
                self.phase = "awaiting_live_finalized"
                self.reason = "awaiting_live_finalized_join"
                self.bars = bars
                self.evidence = {
                    open_time: {
                        "available_at": received_at,
                        "first_received_at": received_at,
                        "source": SOURCE,
                        "source_raw_sha256": raw_sha,
                    }
                    for open_time in bars
                }
                self.last_open_ms = last_open
                self.received_at = received_at
                self.raw_sha256 = raw_sha
                self.raw_sequence = raw_sequence
                self.validated_sequence = validated_sequence
            return manifest
        except Exception as exc:
            with self.lock:
                self.phase = "failed"
                self.reason = "bootstrap_failed"
                self.failure = str(exc) or type(exc).__name__
            raise

    def observe_live_finalized(
        self,
        data: Any,
        *,
        first_received_at: str | None = None,
        source_raw_sha256: str | None = None,
    ) -> dict[str, Any] | None:
        bar = _live_bar(data)
        if bar is None:
            return None
        with self.lock:
            if self.phase == "failed":
                raise BootstrapError("bootstrap_is_failed")
            if self.last_open_ms is None:
                raise BootstrapError("live_before_bootstrap")
            position = bar["open_time_ms"]
            previous = self.bars.get(position)
            if previous is not None:
                if previous != bar:
                    self.phase, self.reason = "failed", "bootstrap_live_conflict"
                    self.failure = "BootstrapError"
                    raise BootstrapError("bootstrap_live_conflict")
                if self.phase == "joined":
                    return None
                relation = "exact_overlap"
                self.evidence[position] = {
                    **self.evidence[position],
                    "available_at": first_received_at,
                    "first_received_at": first_received_at,
                    "source": "binance_global_spot_rest+websocket_confirmed",
                    "source_raw_sha256": source_raw_sha256,
                    "bootstrap_raw_sha256": self.raw_sha256,
                }
            elif position == self.last_open_ms + STEP_MS:
                relation = "next_minute"
                self.bars[position] = bar
                self.evidence[position] = {
                    "available_at": first_received_at,
                    "first_received_at": first_received_at,
                    "source": "binance_global_spot_websocket",
                    "source_raw_sha256": source_raw_sha256,
                }
                # Retain a bounded comparison window.
                while len(self.bars) > self.budget.bars:
                    oldest = min(self.bars)
                    self.bars.pop(oldest)
                    self.evidence.pop(oldest)
                self.last_open_ms = position
            elif position < self.last_open_ms:
                raise BootstrapError("unverifiable_old_live_duplicate")
            else:
                self.phase, self.reason = "failed", "live_join_gap"
                self.failure = "BootstrapError"
                raise BootstrapError("live_join_gap")
            first_join = self.phase != "joined"
            self.phase = "joined"
            if first_join:
                self.reason = "awaiting_join_durability"
                self.joined_at = first_received_at
            return {
                "schema_version": "marketcow.binance-bootstrap-join.v1",
                "event_type": "btc_1m_bootstrap_live_join",
                "source": "binance_global_spot_websocket",
                "instrument_id": "BTCUSDT.BINANCE",
                "relation": relation,
                "open_time_ms": position,
                "bar": bar,
                "first_received_at": first_received_at,
                "available_at": first_received_at,
                "source_raw_sha256": source_raw_sha256,
                "bootstrap_raw_sha256": self.raw_sha256,
                "ready": True,
                "reason": "live_finalized_join_validated",
            }

    def acknowledge_join(self, sequence: int) -> None:
        with self.lock:
            if self.phase != "joined" or type(sequence) is not int or sequence <= 0:
                raise BootstrapError("invalid_join_acknowledgement")
            if self.readiness_sequence is None:
                self.readiness_sequence = sequence
            bars = [
                {**self.bars[open_time], **self.evidence[open_time]}
                for open_time in sorted(self.bars)
            ]
            if len(bars) != self.budget.bars:
                raise BootstrapError("invalid_checkpoint_window")
            digest = hashlib.sha256(canonical(bars)).hexdigest()
            self.pending_checkpoints.append((sequence, bars, digest))
            if len(self.pending_checkpoints) > self.budget.bars:
                self.phase, self.reason = "failed", "checkpoint_backlog_exceeded"
                self.failure = "checkpoint_backlog_exceeded"
                raise BootstrapError("checkpoint_backlog_exceeded")

    def _promote_durable(self, durable_sequence: int) -> None:
        while self.pending_checkpoints and self.pending_checkpoints[0][0] <= durable_sequence:
            self.durable_checkpoint = self.pending_checkpoints.popleft()

    def _expected_last_open(self) -> int:
        now_ms = int(self.clock().astimezone(timezone.utc).timestamp() * 1000)
        return now_ms // STEP_MS * STEP_MS - STEP_MS

    def checkpoint(self, durable_sequence: int) -> dict[str, Any]:
        with self.lock:
            self._promote_durable(durable_sequence)
            if self.durable_checkpoint is None or self.phase != "joined":
                raise BootstrapError("durable_finalized_window_unavailable")
            sequence, bars, digest = self.durable_checkpoint
            expected = self._expected_last_open()
            if bars[-1]["open_time_ms"] != expected:
                raise BootstrapError("durable_finalized_window_stale")
            return {
                "schema_version": "marketcow.binance-finalized-1m-window.v1",
                "instrument_id": "BTCUSDT.BINANCE",
                "interval": "1m",
                "count": len(bars),
                "first_open_ms": bars[0]["open_time_ms"],
                "last_open_ms": bars[-1]["open_time_ms"],
                "expected_last_open_ms": expected,
                "latest_available_at": bars[-1]["available_at"],
                "checkpoint_sequence": sequence,
                "durable_watermark": durable_sequence,
                "window_sha256": digest,
                "bootstrap_raw_sha256": self.raw_sha256,
                "bars": bars,
                "decision_boundary": "current_and_future_after_each_bar_available_at_only",
            }

    def status(self, durable_sequence: int) -> dict[str, Any]:
        with self.lock:
            self._promote_durable(durable_sequence)
            durable_window_current = (
                self.durable_checkpoint is not None
                and self.durable_checkpoint[1][-1]["open_time_ms"] == self._expected_last_open()
            )
            durable_ready = (
                self.readiness_sequence is not None
                and durable_sequence >= self.readiness_sequence
                and self.phase == "joined"
                and durable_window_current
            )
            reason = "ready" if durable_ready else self.reason
            return {
                "schema_version": "marketcow.binance-bootstrap-status.v1",
                "instrument_id": "BTCUSDT.BINANCE",
                "interval": "1m",
                "phase": self.phase,
                "ready": durable_ready,
                "reason": reason,
                "minimum_bars": self.budget.bars,
                "received_at": self.received_at,
                "available_at": self.received_at,
                "joined_at": self.joined_at,
                "historical_first_received_at": None,
                "raw_sha256": self.raw_sha256,
                "first_open_ms": min(self.bars) if self.bars else None,
                "last_open_ms": self.last_open_ms,
                "raw_sequence": self.raw_sequence,
                "validated_sequence": self.validated_sequence,
                "readiness_sequence": self.readiness_sequence,
                "durable_watermark": durable_sequence,
                "durable_checkpoint_sequence": (
                    self.durable_checkpoint[0] if self.durable_checkpoint else None
                ),
                "durable_window_sha256": (
                    self.durable_checkpoint[2] if self.durable_checkpoint else None
                ),
                "durable_window_current": durable_window_current,
                "failure": self.failure,
                "decision_boundary": "current_and_future_after_received_at_only",
            }
