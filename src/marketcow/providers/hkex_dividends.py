from __future__ import annotations

import hashlib
import io
import json
import re
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List

import requests
from pypdf import PdfReader

from ..instruments import canonical_instrument


def _pdf_text(content: bytes) -> str:
    return "\n".join(page.extract_text() or "" for page in PdfReader(io.BytesIO(content)).pages)


def parse_hkex_dividend_form(
    text: str, symbol: str, source_url: str, document_id: str
) -> List[Dict[str, Any]]:
    plain = re.sub(r"\s+", " ", text)
    amount = re.search(
        r"Dividend declared\s+(HKD|RMB|CNY|USD)\s*([\d.]+)\s+per\s+(?:(\d+)\s+)?share",
        plain, re.IGNORECASE,
    )
    payment = re.search(r"Payment date\s+(\d{1,2}\s+[A-Z][a-z]+\s+20\d{2})", plain)
    record = re.search(
        r"(?:Record date|Book close date)\s+(\d{1,2}\s+[A-Z][a-z]+\s+20\d{2})",
        plain,
    )
    ex_date = re.search(
        r"Ex-dividend date\s+(\d{1,2}\s+[A-Z][a-z]+\s+20\d{2})", plain
    )
    fiscal = re.search(
        r"(?:For the financial year end|Reporting period end for the dividend declared)"
        r"\s+(\d{1,2}\s+[A-Z][a-z]+\s+(20\d{2}))",
        plain,
    )
    announced = re.search(r"Announcement date\s+(\d{1,2}\s+[A-Z][a-z]+\s+20\d{2})", plain)
    dividend_type = re.search(r"Dividend type\s+([A-Za-z ]+?)(?:\s+Dividend nature)", plain)
    if not all((amount, payment, fiscal, announced)):
        return []
    currency = {"RMB": "CNY"}.get(amount.group(1).upper(), amount.group(1).upper())
    divisor = Decimal(amount.group(3) or "1")
    fiscal_year = int(fiscal.group(2))
    kind = (dividend_type.group(1).strip().lower() if dividend_type else "unspecified")
    event_key = f"{symbol}|{fiscal.group(1)}|{kind}"
    status_match = re.search(r"Status\s+(New announcement|Revised|Cancelled)", plain)
    event_status = (
        "cancelled" if status_match and status_match.group(1) == "Cancelled" else "active"
    )
    return [{
        "dividend_id": hashlib.sha256(event_key.encode()).hexdigest(),
        "symbol": symbol, "fiscal_year": fiscal_year,
        "amount_per_share": str(Decimal(amount.group(2)) / divisor),
        "currency": currency,
        "announcement_date": datetime.strptime(
            announced.group(1), "%d %B %Y"
        ).date().isoformat(),
        "record_date": (
            datetime.strptime(record.group(1), "%d %B %Y").date().isoformat()
            if record else None
        ),
        "ex_date": (
            datetime.strptime(ex_date.group(1), "%d %B %Y").date().isoformat()
            if ex_date else None
        ),
        "payment_date": datetime.strptime(
            payment.group(1), "%d %B %Y"
        ).date().isoformat(),
        "expected_payment_date": datetime.strptime(
            payment.group(1), "%d %B %Y"
        ).date().isoformat(),
        "confirmation_status": "confirmed",
        "event_status": event_status,
        "source_type": "exchange_announcement", "source_name": "HKEXnews",
        "source_url": source_url, "source_document_id": document_id,
        "payload": {"dividend_type": kind},
    }]


class HkexDividendProvider:
    def __init__(self, session: requests.Session | None = None) -> None:
        self.session = session or requests.Session()

    def fetch(self, symbol: str, fiscal_year: int) -> List[Dict[str, Any]]:
        instrument = canonical_instrument(symbol)
        if instrument.market != "HK":
            raise ValueError("HKEX provider only supports Hong Kong securities")
        code = instrument.symbol[:5]
        prefix = self.session.get(
            "https://www1.hkexnews.hk/search/prefix.do",
            params={"callback": "callback", "lang": "EN", "type": "A", "name": code},
            timeout=15,
        )
        prefix.raise_for_status()
        match = re.search(r"callback\((.*)\);", prefix.text, re.DOTALL)
        data = json.loads(match.group(1)) if match else {}
        stock = next((item for item in data.get("stockInfo", []) if item["code"] == code), None)
        if stock is None:
            raise ValueError("stock code is not present in HKEXnews")
        page = self.session.get(
            "https://www1.hkexnews.hk/search/titlesearch.xhtml",
            params={"category": 0, "lang": "EN", "market": "SEHK",
                    "stockId": stock["stockId"]},
            timeout=20,
        )
        page.raise_for_status()
        rows = []
        for block in re.findall(r"<tr\b.*?</tr>", page.text, re.DOTALL | re.IGNORECASE):
            if "Dividend or Distribution (Announcement Form)" not in block:
                continue
            link = re.search(r'href="([^"]+\.pdf)"', block, re.IGNORECASE)
            if link is None or f"/{fiscal_year}/" not in link.group(1) and f"/{fiscal_year + 1}/" not in link.group(1):
                continue
            url = "https://www1.hkexnews.hk" + link.group(1)
            document = self.session.get(url, timeout=20)
            document.raise_for_status()
            parsed = parse_hkex_dividend_form(
                _pdf_text(document.content), instrument.symbol, url, link.group(1)
            )
            for row in parsed:
                row["_raw_content"] = document.content
                row["_raw_extension"] = ".pdf"
            rows.extend(parsed)
        return [row for row in rows if row["fiscal_year"] == fiscal_year]
