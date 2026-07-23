from __future__ import annotations

import hashlib
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable

from .instruments import canonical_instrument

OFFICIAL_SOURCE_TYPES = frozenset({
    "fund_manager",
    "issuer_announcement",
    "exchange_announcement",
    "ir_filing",
    "regulatory_filing",
})
SOURCE_TYPES = OFFICIAL_SOURCE_TYPES | {"third_party"}
SOURCE_PRIORITIES = {
    "fund_manager": 1,
    "issuer_announcement": 1,
    "exchange_announcement": 2,
    "ir_filing": 3,
    "regulatory_filing": 3,
    "third_party": 9,
}
CONFIRMATION_STATUSES = frozenset({"confirmed", "unverified"})
EVENT_STATUSES = frozenset({"active", "cancelled"})


def normalize_dividend_symbol(value: str) -> str:
    return canonical_instrument(value).symbol


def _iso_date(value: Any, field: str, required: bool = False) -> str | None:
    if value in (None, ""):
        if required:
            raise ValueError(f"{field} is required")
        return None
    try:
        return date.fromisoformat(str(value)).isoformat()
    except ValueError as exc:
        raise ValueError(f"{field} must use YYYY-MM-DD") from exc


def _amount_decimal(value: Any, field: str, allow_zero: bool = False) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a decimal number") from exc
    if not number.is_finite() or number < 0 or (number == 0 and not allow_zero):
        raise ValueError(f"{field} must be greater than zero")
    return number


def normalize_dividend_announcement(
    payload: Dict[str, Any], ingested_at: str
) -> Dict[str, Any]:
    instrument = canonical_instrument(payload.get("symbol", ""))
    symbol = instrument.symbol
    try:
        fiscal_year = int(payload.get("fiscal_year"))
    except (TypeError, ValueError) as exc:
        raise ValueError("fiscal_year must be an integer") from exc
    if not 1990 <= fiscal_year <= 2100:
        raise ValueError("fiscal_year must be between 1990 and 2100")

    event_status = str(payload.get("event_status", "active")).strip()
    if event_status not in EVENT_STATUSES:
        raise ValueError("event_status must be active or cancelled")
    amount = _amount_decimal(
        payload.get("amount_per_share"), "amount_per_share",
        allow_zero=event_status == "cancelled",
    )
    source_type = str(payload.get("source_type", "")).strip()
    if source_type not in SOURCE_TYPES:
        raise ValueError("source_type is unsupported")
    confirmation_status = str(payload.get("confirmation_status", "")).strip()
    if confirmation_status not in CONFIRMATION_STATUSES:
        raise ValueError("confirmation_status must be confirmed or unverified")
    if source_type == "third_party" and confirmation_status == "confirmed":
        raise ValueError("third-party dividend data cannot be marked confirmed")

    source_url = str(payload.get("source_url", "")).strip()
    source_document_id = str(payload.get("source_document_id", "")).strip()
    if confirmation_status == "confirmed" and (not source_url or not source_document_id):
        raise ValueError("confirmed dividend data requires source_url and source_document_id")
    announcement_date = _iso_date(
        payload.get("announcement_date"), "announcement_date", required=True
    )
    expected_payment_date = _iso_date(
        payload.get("expected_payment_date"), "expected_payment_date"
    )
    currency = str(payload.get("currency", "")).strip().upper()
    if len(currency) != 3 or not currency.isalpha():
        raise ValueError("currency must be a three-letter code")

    identity = "|".join((
        symbol, str(fiscal_year), announcement_date or "", str(amount), currency,
        expected_payment_date or "",
    ))
    return {
        "dividend_id": str(payload.get("dividend_id") or hashlib.sha256(
            identity.encode("utf-8")
        ).hexdigest()),
        "symbol": symbol,
        "instrument_id": instrument.instrument_id,
        "market": instrument.market,
        "exchange": instrument.exchange,
        "fiscal_year": fiscal_year,
        "amount_per_share": amount,
        "currency": currency,
        "announcement_date": announcement_date,
        "expected_payment_date": expected_payment_date,
        "confirmation_status": confirmation_status,
        "event_status": event_status,
        "source_type": source_type,
        "source_priority": SOURCE_PRIORITIES[source_type],
        "source_name": str(payload.get("source_name", "")).strip(),
        "source_url": source_url,
        "source_document_id": source_document_id,
        "observed_at": str(payload.get("observed_at") or ingested_at),
        "ingested_at": ingested_at,
        "raw_artifact_id": payload.get("raw_artifact_id") or None,
        "payload_json": payload.get("payload", {}),
    }


def dividend_summary(
    symbol: str, fiscal_year: int, rows: Iterable[Dict[str, Any]]
) -> Dict[str, Any]:
    normalized_symbol = normalize_dividend_symbol(symbol)
    records = list(rows)

    def confirmed_total(year: int) -> Decimal:
        return sum((
            Decimal(str(row["amount_per_share"]))
            for row in records
            if int(row["fiscal_year"]) == year
            and row["confirmation_status"] == "confirmed"
            and row.get("event_status", "active") == "active"
        ), Decimal("0"))

    current = [
        row for row in records
        if int(row["fiscal_year"]) == fiscal_year
        and row.get("event_status", "active") == "active"
    ]
    current.sort(key=lambda row: (
        str(row.get("announcement_date") or ""),
        str(row.get("dividend_id") or ""),
    ))
    currencies = sorted({
        str(row["currency"]) for row in current
        if row["confirmation_status"] == "confirmed"
    })
    previous_rows = [
        row for row in records
        if int(row["fiscal_year"]) == fiscal_year - 1
        and row["confirmation_status"] == "confirmed"
        and row.get("event_status", "active") == "active"
    ]
    previous_currencies = sorted({str(row["currency"]) for row in previous_rows})
    return {
        "symbol": normalized_symbol,
        "fiscal_year": fiscal_year,
        "announcements": current,
        "announced_count": len(current),
        "confirmed_amount_per_share_total": confirmed_total(fiscal_year),
        "confirmed_total_currencies": currencies,
        "previous_complete_year": {
            "fiscal_year": fiscal_year - 1,
            "confirmed_amount_per_share_total": confirmed_total(fiscal_year - 1),
            "currency": previous_currencies[0] if len(previous_currencies) == 1 else None,
            "is_estimate_basis": True,
            "basis": "confirmed_announcements",
        },
    }
