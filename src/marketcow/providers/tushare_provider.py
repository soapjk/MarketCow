from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

import requests


class TushareError(RuntimeError):
    """Raised when the Tushare-compatible upstream rejects a request."""


class TushareProvider:
    """Generic adapter for every Tushare Pro API name."""

    name = "tushare_via_stockai888"

    def __init__(self, token: str, base_url: str, realtime_url: str, min_interval: float = 0.5,
                 timeout: float = 30.0, session: Optional[requests.Session] = None):
        self.token = token
        self.base_url = base_url.rstrip("/")
        self.realtime_url = realtime_url.rstrip("/")
        self.min_interval = max(0.0, min_interval)
        self.timeout = timeout
        self.session = session or requests.Session()
        self.session.headers.update({"Accept-Encoding": "gzip", "User-Agent": "MarketCow/0.1"})
        self._rate_lock = threading.Lock()
        self._sdk_lock = threading.Lock()
        self._last_request = 0.0

    @property
    def configured(self) -> bool:
        return bool(self.token)

    def _wait_for_rate_limit(self) -> None:
        with self._rate_lock:
            delay = self.min_interval - (time.monotonic() - self._last_request)
            if delay > 0:
                time.sleep(delay)
            self._last_request = time.monotonic()

    def call(self, api_name: str, params: Optional[Dict[str, Any]] = None, fields: str = "") -> Dict[str, Any]:
        api_name = api_name.strip()
        if not api_name:
            raise ValueError("api_name is required")
        if not self.token:
            raise TushareError("TUSHARE_TOKEN is not configured")
        payload: Dict[str, Any] = {
            "api_name": api_name, "token": self.token, "params": params or {}, "fields": fields,
        }
        self._wait_for_rate_limit()
        response = self.session.post(self.base_url + "/", json=payload, timeout=self.timeout)
        response.raise_for_status()
        result = response.json()
        if not isinstance(result, dict):
            raise TushareError("Tushare returned a non-object response")
        if result.get("code") not in (None, 0):
            raise TushareError(str(result.get("msg") or f"Tushare error {result.get('code')}"))
        return result

    @staticmethod
    def rows(result: Dict[str, Any]) -> list[Dict[str, Any]]:
        data = result.get("data") or {}
        fields = data.get("fields") or []
        return [dict(zip(fields, item)) for item in (data.get("items") or [])]

    @staticmethod
    def minute_bars(result: Dict[str, Any]) -> list[Dict[str, Any]]:
        bars = []
        for row in TushareProvider.rows(result):
            value = row.get("trade_time") or row.get("time")
            if not value:
                continue
            parsed = datetime.fromisoformat(str(value))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
            utc_value = parsed.astimezone(timezone.utc)
            close = row.get("close")
            if close is None:
                continue
            bars.append({
                "timestamp": int(utc_value.timestamp()),
                "bar_at": utc_value.isoformat(timespec="seconds"),
                "open": row.get("open"), "high": row.get("high"), "low": row.get("low"),
                "close": close, "raw_close": close, "adjustment_factor": 1.0,
                "volume": row.get("vol") if row.get("vol") is not None else row.get("volume"),
                "amount": row.get("amount"), "source_payload": row,
            })
        return sorted(bars, key=lambda bar: bar["timestamp"])

    @staticmethod
    def adjustment_factors(
        result: Dict[str, Any], expected_ts_code: str = ""
    ) -> list[Dict[str, Any]]:
        factors: Dict[str, Dict[str, Any]] = {}
        for row in TushareProvider.rows(result):
            ts_code = str(row.get("ts_code") or "").strip()
            if expected_ts_code and ts_code != expected_ts_code:
                raise TushareError(
                    f"adj_factor returned unexpected ts_code {ts_code or '<empty>'}"
                )
            raw_date = str(row.get("trade_date") or "").strip()
            if len(raw_date) != 8 or not raw_date.isdigit():
                raise TushareError("adj_factor returned an invalid trade_date")
            try:
                trade_date = datetime.strptime(raw_date, "%Y%m%d").date().isoformat()
                factor = Decimal(str(row.get("adj_factor")))
            except (ValueError, InvalidOperation) as error:
                raise TushareError("adj_factor returned an invalid factor row") from error
            if not factor.is_finite() or factor <= 0:
                raise TushareError("adj_factor must be finite and greater than zero")
            normalized = {
                "trade_date": trade_date,
                "adjustment_factor": format(factor, "f"),
                "source_payload": row,
            }
            existing = factors.get(trade_date)
            if existing and existing["adjustment_factor"] != normalized["adjustment_factor"]:
                raise TushareError(
                    f"adj_factor returned conflicting values for {trade_date}"
                )
            factors[trade_date] = normalized
        return [factors[key] for key in sorted(factors)]

    def realtime_quote(self, ts_code: str) -> list[Dict[str, Any]]:
        if not ts_code.strip():
            raise ValueError("ts_code is required")
        if not self.token:
            raise TushareError("TUSHARE_TOKEN is not configured")
        import tushare as ts
        from tushare.stock import cons as ct

        self._wait_for_rate_limit()
        # The SDK stores both values globally, so keep mutation and restoration
        # atomic when FastAPI serves multiple worker threads.
        with self._sdk_lock:
            previous_url = ct.verify_token_url
            sdk_globals = ts.realtime_quote.__globals__
            previous_get_token = sdk_globals["get_token"]
            try:
                ct.verify_token_url = self.realtime_url
                # Feed the decorator from memory. Calling ts.set_token() would
                # persist the credential in ~/.tushare.csv, contrary to the
                # service's .env-only credential policy.
                sdk_globals["get_token"] = lambda: self.token
                frame = ts.realtime_quote(ts_code=ts_code)
            finally:
                sdk_globals["get_token"] = previous_get_token
                ct.verify_token_url = previous_url
        return frame.where(frame.notna(), None).to_dict("records")
