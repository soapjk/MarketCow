from __future__ import annotations

import html
import re
from datetime import datetime
from decimal import Decimal
from typing import Any, Callable, Dict, List

import requests

from ..instruments import canonical_instrument


_AMOUNT = re.compile(
    r"(?:cash\s+dividend|dividend)[^.$]{0,100}\$\s*(\d+(?:\.\d+)?)\s+per\s+share",
    re.IGNORECASE,
)
_PAYABLE = re.compile(
    r"payable\s+(?:on\s+)?([A-Z][a-z]+\s+\d{1,2},\s+\d{4})", re.IGNORECASE
)
_RECORD = re.compile(
    r"(?:holders?|shareholders?)\s+of\s+record\s+(?:as\s+of\s+)?"
    r"(?:on\s+)?([A-Z][a-z]+\s+\d{1,2},\s+\d{4})",
    re.IGNORECASE,
)
_EX_DATE = re.compile(
    r"(?:ex-dividend|ex dividend)\s+date\s+(?:is\s+|of\s+)?"
    r"([A-Z][a-z]+\s+\d{1,2},\s+\d{4})",
    re.IGNORECASE,
)


def _matched_date(match: re.Match[str] | None) -> str | None:
    if match is None:
        return None
    try:
        return datetime.strptime(match.group(1), "%B %d, %Y").date().isoformat()
    except ValueError:
        return None


def parse_sec_dividend_filing(
    text: str, symbol: str, filed_at: str, source_url: str, accession: str
) -> List[Dict[str, Any]]:
    plain = re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", text)))
    payment = _PAYABLE.search(plain)
    if payment is None:
        return []
    payment_date = _matched_date(payment)
    if payment_date is None:
        return []
    record_date = _matched_date(_RECORD.search(plain))
    ex_date = _matched_date(_EX_DATE.search(plain))
    results = []
    for index, match in enumerate(_AMOUNT.finditer(plain)):
        amount = Decimal(match.group(1))
        if amount <= 0:
            continue
        results.append({
            "symbol": symbol, "fiscal_year": int(filed_at[:4]),
            "amount_per_share": str(amount), "currency": "USD",
            "announcement_date": filed_at[:10],
            "record_date": record_date,
            "ex_date": ex_date,
            "payment_date": payment_date,
            "expected_payment_date": payment_date,
            "confirmation_status": "confirmed",
            "source_type": "regulatory_filing", "source_name": "SEC EDGAR",
            "source_url": source_url,
            "source_document_id": f"{accession}#{index}",
        })
    return results


class SecDividendProvider:
    def __init__(
        self, user_agent: str,
        get_json: Callable[[str, Dict[str, str]], Dict[str, Any]] | None = None,
        get_text: Callable[[str, Dict[str, str]], str] | None = None,
    ) -> None:
        if "@" not in user_agent:
            raise ValueError("SEC user agent must include a contact email")
        self.headers = {"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"}
        self.get_json = get_json or self._get_json
        self.get_text = get_text or self._get_text

    @staticmethod
    def _get_json(url: str, headers: Dict[str, str]) -> Dict[str, Any]:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _get_text(url: str, headers: Dict[str, str]) -> str:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        return response.text

    def fetch(self, symbol: str, fiscal_year: int) -> List[Dict[str, Any]]:
        instrument = canonical_instrument(symbol)
        if instrument.market != "US":
            raise ValueError("SEC provider only supports US securities")
        tickers = self.get_json(
            "https://www.sec.gov/files/company_tickers.json", self.headers
        )
        match = next((
            row for row in tickers.values()
            if str(row.get("ticker", "")).upper().replace(".", "-") == instrument.symbol
        ), None)
        if match is None:
            raise ValueError("ticker is not present in SEC company tickers")
        cik = str(match["cik_str"]).zfill(10)
        recent = self.get_json(
            f"https://data.sec.gov/submissions/CIK{cik}.json", self.headers
        ).get("filings", {}).get("recent", {})
        rows = []
        for index, form in enumerate(recent.get("form", [])):
            filed_at = str(recent["filingDate"][index])
            if form not in {"8-K", "8-K/A", "6-K", "6-K/A"} or int(filed_at[:4]) != fiscal_year:
                continue
            accession = str(recent["accessionNumber"][index])
            archive = accession.replace("-", "")
            primary = str(recent["primaryDocument"][index])
            url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{archive}/{primary}"
            filing_text = self.get_text(url, self.headers)
            parsed = parse_sec_dividend_filing(
                filing_text, instrument.symbol,
                filed_at, url, accession,
            )
            for row in parsed:
                row["_raw_content"] = filing_text.encode("utf-8")
                row["_raw_extension"] = ".html"
            rows.extend(parsed)
        return rows
