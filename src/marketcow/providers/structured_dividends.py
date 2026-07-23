from __future__ import annotations

import re
import threading
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, List

from ..instruments import canonical_instrument
from .longport_quote import _direct_connection_environment, normalize_longport_symbol
from .tushare_provider import TushareProvider


_LONGPORT_CASH = re.compile(
    r"(?:Cash|Special)\s+dividend\s*:?\s*([\d.]+)\s*([A-Z]{3})",
    re.IGNORECASE,
)


def _compact_date(value: Any) -> str | None:
    text = str(value or "")
    if len(text) != 8 or not text.isdigit():
        return None
    return f"{text[:4]}-{text[4:6]}-{text[6:]}"


def _longport_date(value: Any) -> str | None:
    text = str(value or "").strip()
    for pattern in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, pattern).date().isoformat()
        except ValueError:
            continue
    return None


class TushareDividendProvider:
    """Fast structured A-share dividend source."""

    def __init__(self, provider: TushareProvider) -> None:
        self.provider = provider

    def fetch(self, symbol: str, fiscal_year: int) -> List[Dict[str, Any]]:
        instrument = canonical_instrument(symbol)
        if instrument.market != "CN":
            raise ValueError("Tushare dividend provider only supports A shares")
        if instrument.symbol[0] in {"1", "5"}:
            return self._fetch_fund(instrument.symbol, fiscal_year)
        result = self.provider.call(
            "dividend",
            {"ts_code": instrument.symbol},
            (
                "ts_code,end_date,ann_date,div_proc,cash_div_tax,"
                "cash_div,pay_date,record_date,ex_date"
            ),
        )
        announcements: List[Dict[str, Any]] = []
        for row in self.provider.rows(result):
            payment_date = _compact_date(row.get("pay_date"))
            if payment_date is None or int(payment_date[:4]) != fiscal_year:
                continue
            amount = row.get("cash_div_tax")
            if amount in (None, "", 0, "0"):
                amount = row.get("cash_div")
            announced = _compact_date(row.get("ann_date"))
            if amount in (None, "", 0, "0") or announced is None:
                continue
            try:
                if Decimal(str(amount)) <= 0:
                    continue
            except (InvalidOperation, ValueError):
                continue
            announcements.append({
                "symbol": instrument.symbol,
                "fiscal_year": fiscal_year,
                "amount_per_share": str(amount),
                "currency": "CNY",
                "announcement_date": announced,
                "record_date": _compact_date(row.get("record_date")),
                "ex_date": _compact_date(row.get("ex_date")),
                "payment_date": payment_date,
                "expected_payment_date": payment_date,
                "confirmation_status": "unverified",
                "source_type": "third_party",
                "source_name": self.provider.name,
                "source_url": self.provider.base_url + "/",
                "source_document_id": str(row.get("div_proc") or ""),
                "payload": row,
            })
        best: Dict[tuple[str, str, str], Dict[str, Any]] = {}
        for row in announcements:
            key = (
                row["amount_per_share"], row["currency"], row["payment_date"]
            )
            existing = best.get(key)
            if existing is None or sum(
                existing.get(field) is not None
                for field in ("record_date", "ex_date", "payment_date")
            ) < sum(
                row.get(field) is not None
                for field in ("record_date", "ex_date", "payment_date")
            ):
                best[key] = row
        return list(best.values())

    def _fetch_fund(self, symbol: str, payment_year: int) -> List[Dict[str, Any]]:
        result = self.provider.call(
            "fund_div",
            {"ts_code": symbol},
            (
                "ts_code,ann_date,imp_anndate,base_date,div_proc,record_date,"
                "ex_date,pay_date,div_cash,base_unit,base_year"
            ),
        )
        announcements: List[Dict[str, Any]] = []
        for row in self.provider.rows(result):
            payment_date = _compact_date(row.get("pay_date"))
            announced = _compact_date(
                row.get("imp_anndate") or row.get("ann_date")
            )
            if (
                payment_date is None
                or int(payment_date[:4]) != payment_year
                or announced is None
            ):
                continue
            try:
                amount = Decimal(str(row.get("div_cash")))
                base_unit = Decimal(str(row.get("base_unit") or "1"))
                per_share = amount / base_unit
            except (InvalidOperation, ValueError, TypeError, ZeroDivisionError):
                continue
            if per_share <= 0:
                continue
            announcements.append({
                "symbol": symbol,
                "fiscal_year": payment_year,
                "amount_per_share": str(per_share),
                "currency": "CNY",
                "announcement_date": announced,
                "record_date": _compact_date(row.get("record_date")),
                "ex_date": _compact_date(row.get("ex_date")),
                "payment_date": payment_date,
                "expected_payment_date": payment_date,
                "confirmation_status": "unverified",
                "source_type": "third_party",
                "source_name": self.provider.name,
                "source_url": self.provider.base_url + "/",
                "source_document_id": "|".join((
                    symbol,
                    str(row.get("base_date") or ""),
                    str(row.get("div_proc") or ""),
                )),
                "payload": {**row, "fiscal_year_basis": "payment_year"},
            })
        return announcements


class LongPortDividendProvider:
    """Fast structured CN/HK dividend source from Longbridge fundamentals."""

    def __init__(
        self,
        app_key: str,
        app_secret: str,
        access_token: str,
        context_factory: Callable[[], Any] | None = None,
        min_interval_seconds: float = 0.65,
        max_attempts: int = 3,
    ) -> None:
        self._app_key = app_key
        self._app_secret = app_secret
        self._access_token = access_token
        self._context_factory = context_factory
        self._context: Any = None
        self._lock = threading.Lock()
        self._request_lock = threading.Lock()
        self._last_request_at = 0.0
        self._min_interval_seconds = min_interval_seconds
        self._max_attempts = max_attempts

    @property
    def configured(self) -> bool:
        return all((self._app_key, self._app_secret, self._access_token))

    def _fundamental_context(self) -> Any:
        with self._lock:
            if self._context is not None:
                return self._context
            if self._context_factory is not None:
                self._context = self._context_factory()
                return self._context
            if not self.configured:
                raise RuntimeError("LongPort credentials are not configured")
            from longbridge.openapi import Config, FundamentalContext

            with _direct_connection_environment():
                config = Config.from_apikey(
                    self._app_key, self._app_secret, self._access_token
                )
                self._context = FundamentalContext(config)
            return self._context

    def close(self) -> None:
        with self._lock:
            context, self._context = self._context, None
        close = getattr(context, "close", None)
        if callable(close):
            close()

    def _call(self, method: Callable[[str], Any], symbol: str) -> Any:
        with self._request_lock:
            for attempt in range(self._max_attempts):
                delay = self._min_interval_seconds - (
                    time.monotonic() - self._last_request_at
                )
                if delay > 0:
                    time.sleep(delay)
                try:
                    result = method(symbol)
                    self._last_request_at = time.monotonic()
                    return result
                except Exception as exc:
                    self._last_request_at = time.monotonic()
                    text = str(exc).lower()
                    limited = "429" in text or "rate limit" in text
                    if not limited or attempt + 1 >= self._max_attempts:
                        raise
                    time.sleep(self._min_interval_seconds * (2 ** attempt))
        raise RuntimeError("LongPort dividend request failed")

    def fetch(self, symbol: str, fiscal_year: int) -> List[Dict[str, Any]]:
        instrument = canonical_instrument(symbol)
        if instrument.market not in {"CN", "HK", "US"}:
            raise ValueError("LongPort dividend provider only supports CN, HK and US")
        _, _, longport_symbol = normalize_longport_symbol(instrument.symbol)
        with _direct_connection_environment():
            context = self._fundamental_context()
            results = [self._call(context.dividend, longport_symbol)]
            try:
                results.append(self._call(context.dividend_detail, longport_symbol))
            except Exception:
                pass
        announcements: List[Dict[str, Any]] = []
        seen: set[tuple[str, str, str]] = set()
        requested_year_items = 0
        for item in (item for result in results for item in result.list):
            payment_date = _longport_date(getattr(item, "payment_date", None))
            ex_date = _longport_date(getattr(item, "ex_date", None))
            record_date = _longport_date(getattr(item, "record_date", None))
            effective_date = ex_date or record_date or payment_date
            if payment_date is None or int(payment_date[:4]) != fiscal_year:
                continue
            requested_year_items += 1
            match = _LONGPORT_CASH.search(str(getattr(item, "desc", "")))
            if match is None:
                continue
            event_key = (match.group(1), match.group(2).upper(), payment_date)
            if event_key in seen:
                continue
            seen.add(event_key)
            announcements.append({
                "symbol": instrument.symbol,
                # LongPort exposes payment-year events, not issuer report periods.
                "fiscal_year": fiscal_year,
                "amount_per_share": match.group(1),
                "currency": match.group(2).upper(),
                "announcement_date": effective_date,
                "record_date": record_date,
                "ex_date": ex_date,
                "payment_date": payment_date,
                "expected_payment_date": payment_date,
                "confirmation_status": "unverified",
                "source_type": "third_party",
                "source_name": "LongPort OpenAPI",
                "source_url": "https://open.longportapp.com/",
                "source_document_id": str(getattr(item, "id", "") or ""),
                "payload": {
                    "description": str(getattr(item, "desc", "")),
                    "ex_date": ex_date,
                    "record_date": record_date,
                    "payment_date": payment_date,
                    "fiscal_year_basis": "payment_year",
                    "announcement_date_basis": (
                        "ex_date" if ex_date else
                        "record_date" if record_date else "payment_date"
                    ),
                },
            })
        if requested_year_items and not announcements:
            raise ValueError(
                "LongPort dividend payload parse produced no usable events"
            )
        stable = [row for row in announcements if row["source_document_id"]]
        return [
            row for row in announcements
            if row["source_document_id"] or not any(
                other["currency"] == row["currency"]
                and other["payment_date"] == row["payment_date"]
                and abs(
                    Decimal(other["amount_per_share"])
                    - Decimal(row["amount_per_share"])
                ) / max(
                    abs(Decimal(other["amount_per_share"])),
                    abs(Decimal(row["amount_per_share"])),
                    Decimal("1"),
                ) <= Decimal("0.01")
                for other in stable
            )
        ]


class CnStructuredDividendProvider:
    """Prefer Tushare report-period data and fall back to LongPort."""

    name = "Tushare -> LongPort OpenAPI"

    def __init__(
        self,
        tushare: TushareDividendProvider,
        longport: LongPortDividendProvider,
    ) -> None:
        self.tushare = tushare
        self.longport = longport

    def fetch(self, symbol: str, fiscal_year: int) -> List[Dict[str, Any]]:
        try:
            rows = self.tushare.fetch(symbol, fiscal_year)
            if rows:
                return rows
        except Exception:
            pass
        return self.longport.fetch(symbol, fiscal_year)


class UsStructuredDividendProvider:
    """Prefer structured corporate actions; retain SEC as a deterministic fallback."""

    name = "LongPort OpenAPI -> SEC EDGAR"

    def __init__(self, longport: LongPortDividendProvider, sec: Any) -> None:
        self.longport = longport
        self.sec = sec

    def fetch(self, symbol: str, fiscal_year: int) -> List[Dict[str, Any]]:
        primary_error: Exception | None = None
        if self.longport.configured:
            try:
                rows = self.longport.fetch(symbol, fiscal_year)
                if rows:
                    return rows
            except Exception as exc:
                primary_error = exc
        rows = self.sec.fetch(symbol, fiscal_year)
        if rows:
            return rows
        if primary_error is not None:
            raise primary_error
        return []
