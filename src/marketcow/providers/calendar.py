from __future__ import annotations

import hashlib
import html
import re
from datetime import date, datetime, timedelta
from html.parser import HTMLParser
from typing import Any, Dict, List, Optional, Sequence
from zoneinfo import ZoneInfo

import requests


BEA_SCHEDULE_URL = "https://www.bea.gov/news/schedule"
CENSUS_SCHEDULE_URL = "https://www.census.gov/economic-indicators/calendar-listview.html"
BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
NASDAQ_EARNINGS_URL = "https://api.nasdaq.com/api/calendar/earnings"
SSE_PERIODIC_URL = "https://query.sse.com.cn/commonSoaQuery.do"
BNP_RESULTS_URL = (
    "https://www.bnppwarrant.com/en/market/result-announcement/action/ajax/"
    "type/table_l/year/{year}/month/{month}/code/{code}"
)

DEFAULT_BLS_INDICATORS = (
    {"indicator_id": "bls_cpi_all_items", "name": "CPI All Urban Consumers", "series_id": "CUSR0000SA0", "unit": "index"},
    {"indicator_id": "bls_unemployment_rate", "name": "Unemployment Rate", "series_id": "LNS14000000", "unit": "%"},
    {"indicator_id": "bls_nonfarm_payrolls", "name": "Nonfarm Payrolls", "series_id": "CES0000000001", "unit": "thousands"},
    {"indicator_id": "bls_avg_hourly_earnings", "name": "Average Hourly Earnings", "series_id": "CES0500000003", "unit": "USD/hour"},
    {"indicator_id": "bls_ppi_final_demand", "name": "PPI Final Demand", "series_id": "WPUFD4", "unit": "index"},
)

MARKET_TIMEZONES = {"US": "America/New_York", "CN": "Asia/Shanghai", "HK": "Asia/Hong_Kong"}


def _text(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", html.unescape(str(value))).strip()


def _number(value: Any) -> Optional[float]:
    raw = _text(value).replace(",", "")
    if not raw or raw in {"-", "--", "N/A"}:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _identifier(*parts: Any) -> str:
    value = "|".join(_text(part).lower() for part in parts)
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:24]


def _scheduled_at(day: str, clock: str, timezone_name: str) -> Optional[str]:
    if not day or not clock or not re.fullmatch(r"\d{2}:\d{2}:\d{2}", clock):
        return None
    try:
        value = datetime.fromisoformat(day + "T" + clock).replace(tzinfo=ZoneInfo(timezone_name))
    except (ValueError, KeyError):
        return None
    return value.isoformat(timespec="seconds")


def _iso_date(value: Any) -> str:
    raw = _text(value)
    match = re.search(r"(20\d{2})[-/]([01]?\d)[-/]([0-3]?\d)", raw)
    if not match:
        return ""
    try:
        return date(int(match.group(1)), int(match.group(2)), int(match.group(3))).isoformat()
    except ValueError:
        return ""


MONTHS = {
    name.lower(): index
    for index, name in enumerate(
        ("", "January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December")
    )
    if name
}
MONTHS.update({name[:3]: value for name, value in tuple(MONTHS.items())})


def _us_date(value: str, default_year: int) -> str:
    raw = _text(value).replace(",", "")
    match = re.search(r"\b([A-Za-z]{3,9})\s+(\d{1,2})(?:\s+(20\d{2}))?\b", raw)
    if not match:
        return _iso_date(raw)
    month = MONTHS.get(match.group(1).lower())
    if not month:
        return ""
    try:
        return date(int(match.group(3) or default_year), month, int(match.group(2))).isoformat()
    except ValueError:
        return ""


def _us_time(value: str) -> str:
    raw = _text(value).upper().replace(".", "")
    match = re.search(r"\b(\d{1,2})(?::(\d{2}))?\s*(AM|PM)\b", raw)
    if not match:
        return ""
    hour, minute = int(match.group(1)), int(match.group(2) or "0")
    if match.group(3) == "PM" and hour != 12:
        hour += 12
    if match.group(3) == "AM" and hour == 12:
        hour = 0
    return f"{hour:02d}:{minute:02d}:00"


class TableParser(HTMLParser):
    def __init__(self, table_id: str = ""):
        super().__init__(convert_charrefs=True)
        self.table_id = table_id
        self.active = not table_id
        self.depth = 0
        self.in_row = False
        self.in_cell = False
        self.cell: List[str] = []
        self.row: List[str] = []
        self.rows: List[List[str]] = []

    def handle_starttag(self, tag: str, attrs: List[tuple[str, Optional[str]]]) -> None:
        attributes = dict(attrs)
        if tag == "table" and (not self.table_id or attributes.get("id") == self.table_id):
            self.active, self.depth = True, 1
            return
        if self.active and tag == "table":
            self.depth += 1
        if not self.active:
            return
        if tag == "tr":
            self.in_row, self.row = True, []
        elif self.in_row and tag in {"td", "th"}:
            self.in_cell, self.cell = True, []

    def handle_endtag(self, tag: str) -> None:
        if not self.active:
            return
        if tag in {"td", "th"} and self.in_cell:
            self.row.append(_text(" ".join(self.cell)))
            self.in_cell = False
        elif tag == "tr" and self.in_row:
            if self.row:
                self.rows.append(self.row)
            self.in_row = False
        elif tag == "table":
            self.depth -= 1
            if self.depth <= 0 and self.table_id:
                self.active = False

    def handle_data(self, data: str) -> None:
        if self.in_cell:
            self.cell.append(data)


def normalize_economic_event(
    *, event_date: str, event_time: str, event_name: str, source: str,
    source_url: str, payload: Any, country: str = "US", impact: str = "Medium",
    actual: Any = "", estimate: Any = "", previous: Any = "", unit: str = "",
) -> Dict[str, Any]:
    timezone_name = MARKET_TIMEZONES.get(country, "UTC")
    return {
        "event_id": _identifier(country, event_date, event_time, event_name, source),
        "country": country,
        "event_date": event_date,
        "event_time": event_time,
        "timezone": timezone_name,
        "scheduled_at": _scheduled_at(event_date, event_time, timezone_name),
        "event_name": _text(event_name),
        "impact": _text(impact),
        "actual": _text(actual),
        "estimate": _text(estimate),
        "previous": _text(previous),
        "unit": _text(unit),
        "source": source,
        "source_url": source_url,
        "raw_response_locator": "calendar table row",
        "_raw_payload": payload,
    }


def normalize_earnings_event(
    *, market: str, symbol: Any, name: Any, report_date: str, report_time: Any,
    fiscal_period: Any, eps_forecast: Any, previous_eps: Any, source: str,
    source_url: str, payload: Any,
) -> Dict[str, Any]:
    market = _text(market).upper()
    symbol_text = _text(symbol).upper()
    if market == "CN":
        symbol_text = re.sub(r"\D", "", symbol_text)[-6:].zfill(6)
    elif market == "HK":
        digits = re.sub(r"\D", "", symbol_text)
        symbol_text = digits.zfill(5) if digits else symbol_text
    report_time_text = _text(report_time)
    timezone_name = MARKET_TIMEZONES.get(market, "UTC")
    return {
        "event_id": _identifier(market, symbol_text, report_date, report_time_text, fiscal_period, source),
        "market": market,
        "symbol": symbol_text,
        "name": _text(name) or symbol_text,
        "report_date": report_date,
        "report_time": report_time_text,
        "timezone": timezone_name,
        "scheduled_at": _scheduled_at(report_date, report_time_text, timezone_name),
        "fiscal_period": _text(fiscal_period),
        "eps_forecast": _text(eps_forecast),
        "previous_eps": _text(previous_eps),
        "source": source,
        "source_url": source_url,
        "raw_response_locator": "calendar response row",
        "_raw_payload": payload,
    }


class CalendarProvider:
    def __init__(self, timeout: int = 15):
        self.timeout = timeout
        self.user_agent = "Mozilla/5.0 MarketCow/0.1"

    def _get(self, url: str, **kwargs):
        headers = {"User-Agent": self.user_agent, **kwargs.pop("headers", {})}
        response = requests.get(url, headers=headers, timeout=self.timeout, **kwargs)
        response.raise_for_status()
        return response

    def fetch_economic_calendar(self, date_from: str, date_to: str, country: str = "US") -> List[Dict[str, Any]]:
        if country.upper() != "US":
            return []
        start, end = date.fromisoformat(date_from), date.fromisoformat(date_to)
        events: List[Dict[str, Any]] = []

        response = self._get(BEA_SCHEDULE_URL)
        parser = TableParser("release-schedule-table")
        parser.feed(response.text)
        for row in parser.rows:
            if len(row) < 3 or row[0].lower().startswith("year"):
                continue
            event_date = _us_date(row[0], start.year)
            if event_date and start <= date.fromisoformat(event_date) <= end:
                events.append(normalize_economic_event(
                    event_date=event_date, event_time=_us_time(row[0]), event_name=row[2],
                    source="bea_official", source_url=BEA_SCHEDULE_URL,
                    payload={"agency": "BEA", "row": row},
                ))

        response = self._get(CENSUS_SCHEDULE_URL)
        parser = TableParser("calendar")
        parser.feed(response.text)
        for row in parser.rows:
            if len(row) < 4 or row[0].lower() == "indicator":
                continue
            event_date = _us_date(row[1], start.year)
            if not event_date or not start <= date.fromisoformat(event_date) <= end:
                continue
            event_name = row[0] + (", " + row[3] if row[3] and row[3] not in row[0] else "")
            events.append(normalize_economic_event(
                event_date=event_date, event_time=_us_time(row[2]), event_name=event_name,
                source="census_official", source_url=CENSUS_SCHEDULE_URL,
                payload={"agency": "Census", "row": row},
            ))

        deduplicated = {(item["event_date"], item["event_time"], item["event_name"].lower()): item for item in events}
        return sorted(deduplicated.values(), key=lambda item: (item["event_date"], item["event_time"], item["event_name"]))

    def fetch_economic_indicators(self) -> List[Dict[str, Any]]:
        current_year = datetime.now().year
        indicator_by_series = {item["series_id"]: item for item in DEFAULT_BLS_INDICATORS}
        response = requests.post(
            BLS_API_URL,
            json={"seriesid": list(indicator_by_series), "startyear": str(current_year - 2), "endyear": str(current_year)},
            headers={"User-Agent": self.user_agent},
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        if _text(payload.get("status")).upper() != "REQUEST_SUCCEEDED":
            raise RuntimeError("BLS API did not return REQUEST_SUCCEEDED")
        items: List[Dict[str, Any]] = []
        for series in (payload.get("Results") or {}).get("series") or []:
            definition = indicator_by_series.get(_text(series.get("seriesID")))
            rows = series.get("data") or []
            if not definition or not rows:
                continue
            latest = rows[0] if isinstance(rows[0], dict) else {}
            previous = rows[1] if len(rows) > 1 and isinstance(rows[1], dict) else {}
            value, previous_value = _number(latest.get("value")), _number(previous.get("value"))
            change_value = value - previous_value if value is not None and previous_value is not None else None
            change_pct = change_value / previous_value * 100 if change_value is not None and previous_value else None
            period_code = _text(latest.get("period"))
            latest_date = ""
            if re.fullmatch(r"M\d{2}", period_code) and 1 <= int(period_code[1:]) <= 12:
                latest_date = f"{int(latest.get('year')):04d}-{int(period_code[1:]):02d}-01"
            items.append({
                **definition,
                "country": "US", "source": "bls", "source_series_id": definition["series_id"],
                "period": f"{_text(latest.get('year'))} {_text(latest.get('periodName'))}".strip(),
                "value": value, "previous_value": previous_value, "change_value": change_value,
                "change_pct": change_pct, "frequency": "monthly", "latest_date": latest_date,
                "source_url": BLS_API_URL, "raw_response_locator": f"Results.series[{definition['series_id']}]",
                "_raw_payload": {"latest": latest, "previous": previous},
            })
        return items

    def fetch_earnings_calendar(
        self, date_from: str, date_to: str, market: str = "", symbols: Optional[Sequence[str]] = None,
    ) -> List[Dict[str, Any]]:
        markets = [market.upper()] if market else ["US", "CN", "HK"]
        result: List[Dict[str, Any]] = []
        if "US" in markets:
            result.extend(self._fetch_nasdaq(date_from, date_to, symbols))
        if "CN" in markets:
            result.extend(self._fetch_sse(date_from, date_to, symbols))
        if "HK" in markets:
            result.extend(self._fetch_hk(date_from, date_to, symbols))
        return sorted(result, key=lambda item: (item["report_date"], item["market"], item["symbol"]))

    def _fetch_nasdaq(self, date_from: str, date_to: str, symbols: Optional[Sequence[str]]) -> List[Dict[str, Any]]:
        requested = {_text(item).upper() for item in symbols or [] if not re.fullmatch(r"\d+", _text(item))}
        start, end = date.fromisoformat(date_from), date.fromisoformat(date_to)
        events: List[Dict[str, Any]] = []
        current = start
        headers = {
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://www.nasdaq.com/market-activity/earnings",
            "Origin": "https://www.nasdaq.com",
        }
        while current <= end:
            if current.weekday() < 5:
                response = self._get(NASDAQ_EARNINGS_URL, params={"date": current.isoformat()}, headers=headers)
                rows = ((response.json().get("data") or {}).get("rows")) or []
                for row in rows if isinstance(rows, list) else []:
                    symbol = _text(row.get("symbol")).upper()
                    if requested and symbol not in requested:
                        continue
                    events.append(normalize_earnings_event(
                        market="US", symbol=symbol, name=row.get("name"), report_date=current.isoformat(),
                        report_time=row.get("time"), fiscal_period=row.get("fiscalQuarterEnding"),
                        eps_forecast=row.get("epsForecast"), previous_eps=row.get("lastYearEPS"),
                        source="nasdaq", source_url=NASDAQ_EARNINGS_URL, payload=row,
                    ))
            current += timedelta(days=1)
        return events

    def _fetch_sse(self, date_from: str, date_to: str, symbols: Optional[Sequence[str]]) -> List[Dict[str, Any]]:
        start, end = date.fromisoformat(date_from), date.fromisoformat(date_to)
        codes = [re.sub(r"\D", "", _text(item))[-6:] for item in symbols or []]
        codes = [code for code in codes if len(code) == 6 and code.startswith(("6", "9"))]
        headers = {"Referer": "https://www.sse.com.cn/disclosure/listedinfo/periodic/", "Accept": "application/json"}
        events: List[Dict[str, Any]] = []
        for code in codes:
            for year in sorted({start.year, end.year}):
                params = {
                    "sqlId": "SSE_SZSGG_DQBGYYQK_CAST_NEW", "isPagination": "true",
                    "pageHelp.pageSize": "20", "pageHelp.pageNo": "1", "companyCode": code,
                    "publishYear": str(year), "order": "companyCode|asc",
                }
                response = self._get(SSE_PERIODIC_URL, params=params, headers=headers)
                for row in response.json().get("result") or []:
                    report_date = _iso_date(row.get("publishDate0") or row.get("actualDate"))
                    if report_date and start <= date.fromisoformat(report_date) <= end:
                        events.append(normalize_earnings_event(
                            market="CN", symbol=row.get("companyCode"), name=row.get("companyAbbr"),
                            report_date=report_date, report_time="",
                            fiscal_period=f"{row.get('publishYear') or year} {_text(row.get('bulletinType'))}".strip(),
                            eps_forecast="", previous_eps="", source="sse_periodic",
                            source_url=SSE_PERIODIC_URL, payload=row,
                        ))
        return events

    def _fetch_hk(self, date_from: str, date_to: str, symbols: Optional[Sequence[str]]) -> List[Dict[str, Any]]:
        start, end = date.fromisoformat(date_from), date.fromisoformat(date_to)
        codes = [re.sub(r"\D", "", _text(item)) for item in symbols or []]
        codes = [code.zfill(5) for code in codes if code and len(code) <= 5]
        months: List[date] = []
        current = date(start.year, start.month, 1)
        while current <= end:
            months.append(current)
            current = date(current.year + (current.month == 12), 1 if current.month == 12 else current.month + 1, 1)
        events: List[Dict[str, Any]] = []
        for code in codes:
            for month in months:
                url = BNP_RESULTS_URL.format(year=month.year, month=month.month, code=int(code))
                response = self._get(url, headers={"Referer": "https://www.bnppwarrant.com/"})
                parser = TableParser()
                parser.feed(_text(response.json().get("table_content")))
                for row in parser.rows:
                    if len(row) < 4:
                        continue
                    report_date = _iso_date(row[0])
                    if report_date and start <= date.fromisoformat(report_date) <= end:
                        events.append(normalize_earnings_event(
                            market="HK", symbol=row[1], name=row[2], report_date=report_date,
                            report_time="", fiscal_period=row[3], eps_forecast="", previous_eps="",
                            source="bnp_result_announcement", source_url=url, payload=row,
                        ))
        return events
