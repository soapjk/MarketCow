from __future__ import annotations

import json
import re
import subprocess
import time
from datetime import datetime, timezone
from typing import Any, Dict
from urllib.parse import urlencode

import requests

from ..normalize import exchange_for_symbol, instrument_id


EASTMONEY_QUOTE_URL = "https://push2.eastmoney.com/api/qt/stock/get"


def normalize_a_symbol(value: str) -> str:
    text = str(value or "").strip().upper()
    match = re.fullmatch(r"(\d{6})(?:\.(SH|SS|SZ|BJ))?", text)
    if not match:
        raise ValueError("unsupported A-share or ETF symbol format")
    code, suffix = match.groups()
    inferred = "SH" if code.startswith(("5", "6", "9")) else "BJ" if code.startswith(("4", "8")) else "SZ"
    return code + "." + ("SH" if suffix == "SS" else suffix or inferred)


class EastmoneyRealtimeQuoteProvider:
    name = "eastmoney_quote_center"

    def __init__(self, timeout: float = 0.7, request_budget: float = 1.8):
        self.timeout = timeout
        self.request_budget = request_budget
        self.session = requests.Session()
        self.session.trust_env = True

    def fetch_quote(self, value: str) -> Dict[str, Any]:
        symbol = normalize_a_symbol(value)
        code, suffix = symbol.split(".")
        market_number = "1" if suffix == "SH" else "0"
        params = {"secid": market_number + "." + code, "fields": "f43,f57,f58,f59,f60,f86,f170"}
        headers={"User-Agent": "Mozilla/5.0 marketcow/0.1", "Referer": "https://quote.eastmoney.com/"}
        last_error = None
        payload = None
        deadline = time.monotonic() + self.request_budget
        for attempt in range(2):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                response = self.session.get(
                    EASTMONEY_QUOTE_URL, params=params, headers=headers,
                    timeout=max(0.05, min(self.timeout, remaining)),
                )
                response.raise_for_status()
                payload = response.json()
                break
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                if attempt == 0 and deadline - time.monotonic() > 0.1:
                    time.sleep(0.05)
        source_url = EASTMONEY_QUOTE_URL + "?" + urlencode(params)
        if payload is None:
            remaining = deadline - time.monotonic()
            if remaining <= 0.1:
                raise RuntimeError("Eastmoney realtime quote request budget exhausted") from last_error
            try:
                completed = subprocess.run(
                    ["curl", "-fsSL", "--max-time", str(remaining), "-A", headers["User-Agent"], "-e", headers["Referer"], source_url],
                    capture_output=True, text=True, timeout=remaining + 0.2, check=True,
                )
                payload = json.loads(completed.stdout)
            except Exception as curl_error:
                raise RuntimeError("Eastmoney realtime quote failed: requests={0}; curl={1}".format(last_error, curl_error)) from curl_error
        data = payload.get("data") or {}
        decimals = int(data.get("f59") or 2)
        scale = 10 ** decimals
        price = float(data["f43"]) / scale if data.get("f43") not in (None, "-") else None
        if price is None:
            raise RuntimeError("Eastmoney returned no usable price")
        previous_close = float(data["f60"]) / scale if data.get("f60") not in (None, "-") else None
        timestamp = int(data["f86"]) if data.get("f86") else None
        exchange = exchange_for_symbol(code)
        return {
            "instrument_id": instrument_id(code),
            "symbol": symbol,
            "name": data.get("f58") or code,
            "market": "CN",
            "exchange": exchange,
            "currency": "CNY",
            "price": price,
            "previous_close": previous_close,
            "change": price - previous_close if previous_close is not None else None,
            "change_pct": float(data["f170"]) / 100 if data.get("f170") not in (None, "-") else None,
            "session": "unknown",
            "quote_at": datetime.fromtimestamp(timestamp, timezone.utc).isoformat(timespec="seconds") if timestamp else None,
            "price_adjustment": "raw",
            "quality_status": "single_source_unverified",
            "source": self.name,
            "source_url": source_url,
            "raw_response_locator": "data",
            "_raw_payload": payload,
        }
