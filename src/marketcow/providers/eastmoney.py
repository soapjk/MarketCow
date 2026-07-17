from __future__ import annotations

import json
import subprocess
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import requests


EASTMONEY_SPOT_URLS = (
    "https://push2.eastmoney.com/api/qt/clist/get",
    "http://push2.eastmoney.com/api/qt/clist/get",
)

FIELDS = "f2,f3,f9,f12,f13,f14,f20,f21,f23"
MARKET_FILTER = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048"


class EastmoneySpotProvider:
    name = "eastmoney_quote_center"

    def __init__(self, timeout: int = 20, max_retries: int = 3, page_size: int = 100):
        self.timeout = timeout
        self.max_retries = max_retries
        self.page_size = min(max(20, page_size), 100)
        self.session = requests.Session()
        self.session.trust_env = True
        self.headers = {
            "User-Agent": "Mozilla/5.0 marketcow/0.1",
            "Referer": "https://quote.eastmoney.com/",
            "Accept": "application/json,text/plain,*/*",
        }

    def _fetch_page(self, page: int) -> Dict[str, Any]:
        params = {
            "pn": page,
            "pz": self.page_size,
            "po": 1,
            "np": 1,
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2,
            "invt": 2,
            "fid": "f12",
            "fs": MARKET_FILTER,
            "fields": FIELDS,
        }
        last_error: Optional[Exception] = None
        for url in EASTMONEY_SPOT_URLS:
            for attempt in range(self.max_retries):
                try:
                    response = self.session.get(
                        url, params=params, headers=self.headers, timeout=self.timeout
                    )
                    response.raise_for_status()
                    payload = response.json()
                    if payload.get("data") is None:
                        raise RuntimeError("Eastmoney returned an empty data object")
                    return payload
                except (requests.RequestException, ValueError, RuntimeError) as exc:
                    last_error = exc
                    if attempt + 1 < self.max_retries:
                        time.sleep(0.25 * (attempt + 1))
        # requests can receive intermittent proxy 502/connection resets while
        # the same endpoint still succeeds through the system curl stack.
        # Arguments are passed as a list; no provider value is evaluated by a shell.
        curl_url = EASTMONEY_SPOT_URLS[0] + "?" + urlencode(params, safe=":,+")
        try:
            completed = subprocess.run(
                [
                    "curl", "-fsSL", "--retry", "5", "--retry-delay", "1",
                    "--max-time", str(self.timeout * 2), "-A", self.headers["User-Agent"],
                    "-e", self.headers["Referer"], curl_url,
                ],
                capture_output=True,
                text=True,
                timeout=self.timeout * 3,
                check=True,
            )
            payload = json.loads(completed.stdout)
            if payload.get("data") is not None:
                return payload
        except Exception as exc:
            last_error = RuntimeError("requests={0}; curl={1}".format(last_error, exc))
        raise RuntimeError("Eastmoney spot page {0} failed: {1}".format(page, last_error))

    def fetch_all(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        total: Optional[int] = None
        page = 1
        while total is None or len(rows) < total:
            payload = self._fetch_page(page)
            data = payload.get("data") or {}
            total = int(data.get("total") or 0)
            diff = data.get("diff") or []
            if isinstance(diff, dict):
                diff = list(diff.values())
            if not diff:
                break
            rows.extend(diff)
            page += 1
        return [
            {
                "symbol": str(row.get("f12") or "").zfill(6),
                "name": row.get("f14"),
                "price": row.get("f2"),
                "change_pct": row.get("f3"),
                "pe_dynamic": row.get("f9"),
                "pb": row.get("f23"),
                "total_market_cap": row.get("f20"),
                "float_market_cap": row.get("f21"),
            }
            for row in rows
            if row.get("f12")
        ]
