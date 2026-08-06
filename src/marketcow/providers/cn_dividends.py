from __future__ import annotations

import io
import hashlib
import re
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Iterable, List

import requests
from pypdf import PdfReader

from ..instruments import canonical_instrument


_PER_TEN = re.compile(r"每\s*10\s*股[^。；]{0,50}?派(?:发)?(?:现金)?(?:红利|股利)?\s*([\d.]+)\s*元")
_PER_SHARE = re.compile(r"每\s*股[^。；]{0,40}?派(?:发)?(?:现金)?(?:红利|股利)?\s*([\d.]+)\s*元")
_PER_SHARE_CASH = re.compile(r"每\s*股\s*现金(?:红利|股利)\s*([\d.]+)\s*元")
_PER_TEN_UNITS = re.compile(
    r"每\s*10\s*份(?:基金)?份额[^。；]{0,60}?(?:分配|派发)[^。\d]{0,20}([\d.]+)\s*元"
)
_FUND_PLAN_PER_TEN = re.compile(
    r"本次分红方案\s*（?单位：?人民币元\s*/\s*10\s*份基金份额）?\s*([\d.]+)"
)
_PAY_DATE = re.compile(
    r"(?:现金红利发放日|红利发放日|派发日|现金红利将于)[：:\s]*"
    r"(\d{4})\s*[年/-]\s*(\d{1,2})\s*[月/-]\s*(\d{1,2})\s*日?"
)
_RECORD_DATE = re.compile(
    r"(?:股权登记日|权益登记日)[：:\s]*(\d{4})\s*[年/-]\s*"
    r"(\d{1,2})\s*[月/-]\s*(\d{1,2})\s*日?"
)
_EX_DATE = re.compile(
    r"(?:除权除息日|除息日)[：:\s]*(\d{4})\s*[年/-]\s*"
    r"(\d{1,2})\s*[月/-]\s*(\d{1,2})\s*日?"
)
_YEAR = re.compile(
    r"(20\d{2})\s*年?(?:年度|中期|半年度|前三季度)(?:权益分派|利润分配)"
)
_FUND_YEAR = re.compile(r"本次分红为\s*(20\d{2})\s*年度")
_PERIOD = re.compile(r"20\d{2}\s*年?(年度|中期|半年度|前三季度)")
_FUND_NAME = re.compile(r"基金名称\s*(.+?)\s*基金简称")


def pdf_text(content: bytes) -> str:
    return "\n".join(page.extract_text() or "" for page in PdfReader(io.BytesIO(content)).pages)


def parse_cn_implementation_announcement(
    text: str, symbol: str, announcement_date: str, source_url: str,
    document_id: str, source_name: str,
) -> List[Dict[str, Any]]:
    compact = re.sub(r"\s+", " ", text)
    semantic = re.sub(r"\s+", "", text)
    implemented = "实施公告" in semantic or (
        "基金" in semantic
        and ("利润分配公告" in semantic or "分红公告" in semantic)
    )
    if not implemented or not any(
        marker in semantic for marker in ("权益分派", "利润分配", "收益分配")
    ):
        return []
    implementation = compact
    marker = compact.find("本次实施的权益分派方案")
    if marker >= 0:
        implementation = compact[marker:marker + 1200]
    amount_match = _PER_TEN.search(implementation)
    divisor = Decimal("10")
    if amount_match is None:
        amount_match = _PER_SHARE.search(implementation)
        divisor = Decimal("1")
    if amount_match is None:
        amount_match = _PER_SHARE_CASH.search(implementation)
        divisor = Decimal("1")
    if amount_match is None:
        amount_match = _PER_TEN_UNITS.search(implementation)
        divisor = Decimal("10")
    if amount_match is None:
        amount_match = _FUND_PLAN_PER_TEN.search(implementation)
        divisor = Decimal("10")
    payment_match = _PAY_DATE.search(compact)
    if payment_match is None:
        table = re.search(r"现金红利发放日(.{0,120})", compact)
        dates = re.findall(
            r"(20\d{2})\s*[年/-]\s*(\d{1,2})\s*[月/-]\s*(\d{1,2})\s*日?",
            table.group(1),
        ) if table else []
        if dates:
            payment_match = dates[-1]
    year_match = _YEAR.search(compact) or _FUND_YEAR.search(compact)
    if amount_match is None or payment_match is None or year_match is None:
        return []
    parts = payment_match if isinstance(payment_match, tuple) else payment_match.groups()
    payment_date = datetime(
        int(parts[0]), int(parts[1]), int(parts[2])
    ).date().isoformat()
    def matched_iso(pattern: re.Pattern[str]) -> str | None:
        match = pattern.search(compact)
        if match is None:
            return None
        return datetime(*(int(part) for part in match.groups())).date().isoformat()
    period_match = _PERIOD.search(compact)
    period = period_match.group(1) if period_match else "annual_distribution"
    fund_name_match = _FUND_NAME.search(compact)
    fund_name = fund_name_match.group(1).strip() if fund_name_match else None
    if fund_name and re.search(r"[\u3400-\u9fff]", fund_name):
        fund_name = re.sub(r"\s+", "", fund_name)
    event_key = (
        f"{symbol}|{year_match.group(1)}|{period}|{payment_date}|"
        f"{Decimal(amount_match.group(1)) / divisor}|CNY"
    )
    return [{
        "dividend_id": hashlib.sha256(event_key.encode()).hexdigest(),
        "symbol": symbol, "fiscal_year": int(year_match.group(1)),
        "amount_per_share": str(Decimal(amount_match.group(1)) / divisor),
        "currency": "CNY", "announcement_date": announcement_date,
        "record_date": matched_iso(_RECORD_DATE),
        "ex_date": matched_iso(_EX_DATE),
        "payment_date": payment_date,
        "expected_payment_date": payment_date,
        "confirmation_status": "confirmed",
        "source_type": "exchange_announcement", "source_name": source_name,
        "source_url": source_url, "source_document_id": document_id,
        "payload": {
            "distribution_period": period,
            "fund_name": fund_name,
            "dividend_type": "cash",
            "declared_amount": amount_match.group(1),
            "declared_unit_count": "10" if divisor == Decimal("10") else "1",
            "declared_unit": "fund_unit" if fund_name else "share",
        },
    }]


class CnExchangeDividendProvider:
    def __init__(self, session: requests.Session | None = None) -> None:
        self.session = session or requests.Session()

    def _documents(self, symbol: str, year: int) -> Iterable[Dict[str, str]]:
        instrument = canonical_instrument(symbol)
        code = instrument.symbol[:6]
        begin, end = f"{year}-01-01", f"{year}-12-31"
        if instrument.mic == "XSHG":
            if code.startswith("5"):
                response = self.session.get(
                    "https://query.sse.com.cn/commonQuery.do",
                    params={
                        "isPagination": "true",
                        "pageHelp.pageSize": 100,
                        "pageHelp.pageNo": 1,
                        "pageHelp.beginPage": 1,
                        "pageHelp.cacheSize": 1,
                        "pageHelp.endPage": 1,
                        "type": "inParams",
                        "sqlId": "COMMON_PL_JJXX_JJGG_NEW_L",
                        "TITLE": "分红",
                        "SECURITY_CODE": code,
                        "BULLETIN_TYPE": "",
                        "START_DATE": begin,
                        "END_DATE": end,
                        "DATE_DESC": 1,
                    },
                    headers={
                        "Referer": "https://www.sse.com.cn/disclosure/fund/announcement/",
                        "User-Agent": "MarketCow/0.2",
                    },
                    timeout=15,
                )
                response.raise_for_status()
                payload = response.json()
                items = payload.get("result") or payload.get("pageHelp", {}).get(
                    "data", []
                )
                for row in items:
                    path = str(row.get("URL") or "")
                    title = str(row.get("TITLE") or "")
                    if not path or "分红" not in title:
                        continue
                    yield {
                        "url": "https://www.sse.com.cn" + path,
                        "date": str(row.get("SSEDATE") or "")[:10],
                        "id": path.rsplit("/", 1)[-1],
                        "source": "Shanghai Stock Exchange Fund Disclosure",
                    }
                return
            seen = set()
            for keyword in ("权益分派", "利润分配"):
                response = self.session.get(
                    "https://query.sse.com.cn/security/stock/queryCompanyBulletin.do",
                    params={
                    "isPagination": "true", "productId": code, "keyWord": keyword,
                    "securityType": "0101,120100,020100,020200,120200",
                    "reportType2": "", "reportType": "ALL",
                    "beginDate": begin, "endDate": end,
                    "pageHelp.pageSize": 100, "pageHelp.pageNo": 1,
                    },
                    headers={"Referer": "https://www.sse.com.cn/",
                             "User-Agent": "MarketCow/0.1"},
                    timeout=15,
                )
                response.raise_for_status()
                payload = response.json()
                items = payload.get("result") or payload.get("pageHelp", {}).get("data", [])
                for row in items:
                    if row["URL"] in seen:
                        continue
                    seen.add(row["URL"])
                    yield {
                    "url": (
                        "https://big5.sse.com.cn/site/cht/www.sse.com.cn"
                        + row["URL"]
                    ),
                    "date": str(row["SSEDATE"])[:10],
                    "id": str(row.get("BULLETIN_ID") or row["URL"]),
                    "source": "Shanghai Stock Exchange",
                    }
        elif instrument.mic == "XSHE":
            response = self.session.post(
                "https://www.szse.cn/api/disc/announcement/annList",
                json={
                    "seDate": [begin, end], "stock": [code],
                    "channelCode": ["listedNotice_disc"], "pageSize": 100, "pageNum": 1,
                },
                headers={"Referer": "https://www.szse.cn/", "User-Agent": "MarketCow/0.1"},
                timeout=15,
            )
            response.raise_for_status()
            for row in response.json().get("data", []):
                path = str(row.get("attachPath") or "")
                title = str(row.get("title", ""))
                if not any(term in title for term in ("权益分派", "利润分配")) or not path:
                    continue
                yield {
                    "url": "https://disc.static.szse.cn/download" + path,
                    "date": str(row.get("publishTime", ""))[:10],
                    "id": str(row.get("id") or path),
                    "source": "Shenzhen Stock Exchange",
                }

    def fetch(self, symbol: str, fiscal_year: int) -> List[Dict[str, Any]]:
        instrument = canonical_instrument(symbol)
        if instrument.market != "CN":
            raise ValueError("CN exchange provider only supports A shares")
        rows = []
        for document in self._documents(instrument.instrument_id, fiscal_year):
            response = self.session.get(document["url"], timeout=20)
            response.raise_for_status()
            parsed = parse_cn_implementation_announcement(
                pdf_text(response.content), instrument.instrument_id, document["date"],
                document["url"], document["id"], document["source"],
            )
            for row in parsed:
                row["_raw_content"] = response.content
                row["_raw_extension"] = ".pdf"
            rows.extend(parsed)
        return [row for row in rows if row["fiscal_year"] == fiscal_year]
