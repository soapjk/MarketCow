from __future__ import annotations

import re
import time
from datetime import datetime
from typing import Any, Dict
from zoneinfo import ZoneInfo

import requests

from ..normalize import exchange_for_symbol, instrument_id
from .eastmoney_realtime import normalize_a_symbol


SINA_QUOTE_URL = "https://hq.sinajs.cn/list={code}"
SINA_QUOTE_HTTP_URL = "http://hq.sinajs.cn/list={code}"
SINA_QUOTE_IP_URL = "http://123.125.107.29/list={code}"


class SinaRealtimeQuoteProvider:
    """A-share and ETF quotes with ordered Sina endpoint fallbacks."""

    name = "sina_finance_hq"

    def __init__(self, timeout: float = 0.4, request_budget: float = 1.8):
        self.timeout = timeout
        self.request_budget = request_budget
        self.session = requests.Session()
        self.direct_session = requests.Session()
        self.direct_session.trust_env = False

    @staticmethod
    def sina_code(value: str) -> tuple[str, str]:
        symbol = normalize_a_symbol(value)
        code, suffix = symbol.split(".")
        prefix = {"SH": "sh", "SZ": "sz", "BJ": "bj"}[suffix]
        return symbol, prefix + code

    def _fetch_raw(self, sina_code: str) -> tuple[str, str]:
        headers = {
            "User-Agent": "Mozilla/5.0 marketcow/0.1",
            "Referer": "https://finance.sina.com.cn/",
        }
        candidates = [
            (self.session, SINA_QUOTE_URL.format(code=sina_code), headers),
            (self.direct_session, SINA_QUOTE_URL.format(code=sina_code), headers),
            (self.direct_session, SINA_QUOTE_HTTP_URL.format(code=sina_code), headers),
            (
                self.direct_session,
                SINA_QUOTE_IP_URL.format(code=sina_code),
                {**headers, "Host": "hq.sinajs.cn"},
            ),
        ]
        errors: list[str] = []
        deadline = time.monotonic() + self.request_budget
        for session, url, request_headers in candidates:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                errors.append("request budget exhausted")
                break
            try:
                response = session.get(
                    url, headers=request_headers, timeout=max(0.05, min(self.timeout, remaining))
                )
                response.raise_for_status()
                text = response.content.decode("gbk", errors="replace").strip()
                if text:
                    return text, url
                errors.append(f"{url}: empty response")
            except requests.RequestException as exc:
                errors.append(f"{url}: {exc}")
        raise RuntimeError("Sina realtime quote failed: " + "; ".join(errors))

    @staticmethod
    def _parse_line(raw: str) -> list[str]:
        match = re.search(r'=\"(.*)\";?$', raw)
        if not match or not match.group(1):
            raise RuntimeError("Sina returned no usable quote")
        return match.group(1).split(",")

    def fetch_quote(self, value: str) -> Dict[str, Any]:
        symbol, sina_code = self.sina_code(value)
        raw, source_url = self._fetch_raw(sina_code)
        fields = self._parse_line(raw)
        if len(fields) < 32:
            raise RuntimeError("Sina returned an incomplete quote")
        try:
            price = float(fields[3])
            previous_close = float(fields[2])
        except (TypeError, ValueError) as exc:
            raise RuntimeError("Sina returned an invalid price") from exc
        if price <= 0:
            raise RuntimeError("Sina returned no usable price")

        code, _ = symbol.split(".")
        exchange = exchange_for_symbol(code)
        quote_at = None
        if fields[30] and fields[31]:
            try:
                quote_at = datetime.strptime(
                    fields[30] + " " + fields[31], "%Y-%m-%d %H:%M:%S"
                ).replace(tzinfo=ZoneInfo("Asia/Shanghai")).isoformat(timespec="seconds")
            except ValueError:
                quote_at = None
        return {
            "instrument_id": instrument_id(code),
            "symbol": symbol,
            "name": fields[0] or code,
            "market": "CN",
            "exchange": exchange,
            "currency": "CNY",
            "price": price,
            "previous_close": previous_close,
            "change": price - previous_close,
            "change_pct": (price / previous_close - 1) * 100 if previous_close else None,
            "session": "regular",
            "quote_at": quote_at,
            "price_adjustment": "raw",
            "quality_status": "single_source_unverified",
            "source": self.name,
            "source_url": source_url,
            "raw_response_locator": "hq_str payload fields",
            "is_cached": False,
            "_raw_payload": {"raw_line": raw, "encoding": "gbk"},
        }
