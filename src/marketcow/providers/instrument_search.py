from __future__ import annotations

from datetime import datetime, timezone
import os
import re
from typing import Any, Dict, List
from urllib.parse import urlencode

import requests

EASTMONEY_SEARCH_URL = "https://searchapi.eastmoney.com/api/suggest/get"
EASTMONEY_TOKEN = os.getenv("EASTMONEY_SEARCH_TOKEN", "")


class InstrumentSearchProvider:
    name = "eastmoney_suggest"

    def __init__(self, timeout: int = 8):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.trust_env = True

    def search(self, query: str, limit: int = 12) -> List[Dict[str, Any]]:
        text = str(query or "").strip()
        if not text:
            return []
        lookup = re.sub(r"^(\d{6})\.(?:HK|SH|SS|SZ|BJ)$", r"\1", text, flags=re.IGNORECASE)
        params = {"input": lookup, "type": "14", "count": str(max(limit * 3, 20))}
        if EASTMONEY_TOKEN:
            params["token"] = EASTMONEY_TOKEN
        response = self.session.get(
            EASTMONEY_SEARCH_URL,
            params=params,
            headers={"User-Agent": "Mozilla/5.0 marketcow/0.1", "Accept": "application/json"},
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        rows = ((payload.get("QuotationCodeTable") or {}).get("Data") or [])
        source_url = EASTMONEY_SEARCH_URL + "?" + urlencode(params)
        observed_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        results: List[Dict[str, Any]] = []
        seen = set()
        for row in rows:
            item = self._normalize(row, source_url, observed_at)
            if not item or item["symbol"] in seen:
                continue
            seen.add(item["symbol"])
            results.append(item)
            if len(results) >= limit:
                break
        return results

    def _normalize(self, row: Dict[str, Any], source_url: str, observed_at: str):
        code = str(row.get("Code") or "").strip().upper()
        classify = str(row.get("Classify") or "")
        name = str(row.get("Name") or code).strip()
        if classify in ("AStock", "Fund") and code.isdigit() and len(code) == 6:
            mic = "XSHG" if code.startswith(("5", "6", "9")) else "XBSE" if code.startswith(("4", "8")) else "XSHE"
            symbol, market, currency = f"{code}.{mic}", "CN", "CNY"
        elif classify == "HK" and code.isdigit() and int(code) <= 9999:
            symbol, market, currency = f"{int(code)}.XHKG", "HK", "HKD"
        elif classify == "UsStock" and str(row.get("TypeUS") or "") in ("1", "3"):
            # Search responses do not carry a reliable MIC. Do not guess a US venue.
            return None
        else:
            return None
        return {
            "symbol": symbol,
            "name": name,
            "market": market,
            "exchange": str(row.get("JYS") or ""),
            "currency": currency,
            "source": self.name,
            "source_url": source_url,
            "observed_at": observed_at,
            "raw_response_locator": "QuotationCodeTable.Data",
        }
