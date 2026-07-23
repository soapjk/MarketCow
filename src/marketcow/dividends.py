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
DIVIDEND_DATE_FIELDS = ("record_date", "ex_date", "payment_date")


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
    record_date = _iso_date(payload.get("record_date"), "record_date")
    ex_date = _iso_date(payload.get("ex_date"), "ex_date")
    payment_date = _iso_date(payload.get("payment_date"), "payment_date")
    if payment_date and expected_payment_date and payment_date != expected_payment_date:
        raise ValueError("expected_payment_date must equal payment_date when both are set")
    # Compatibility is one-way: a known actual payment date is also the expected
    # payment date. A legacy expected date is never promoted to an actual date.
    if payment_date and expected_payment_date is None:
        expected_payment_date = payment_date
    supplied_evidence = payload.get("date_evidence") or {}
    date_evidence: Dict[str, Dict[str, Any]] = {}
    for field in DIVIDEND_DATE_FIELDS:
        value = locals()[field]
        evidence = dict(supplied_evidence.get(field) or {})
        evidence["value"] = value
        evidence.setdefault("source_name", str(payload.get("source_name", "")).strip())
        evidence.setdefault("source_url", source_url or None)
        evidence.setdefault("source_document_id", source_document_id or None)
        evidence.setdefault("verification_status", confirmation_status)
        evidence.setdefault("source_priority", SOURCE_PRIORITIES[source_type])
        evidence.setdefault(
            "selection_policy", "lowest_source_priority_then_latest_observation"
        )
        if value is None:
            evidence.setdefault("missing_reason", f"{field}_not_provided_by_source")
        else:
            evidence["missing_reason"] = None
        date_evidence[field] = evidence
    currency = str(payload.get("currency", "")).strip().upper()
    if len(currency) != 3 or not currency.isalpha():
        raise ValueError("currency must be a three-letter code")

    identity = "|".join((
        symbol, str(fiscal_year), announcement_date or "", str(amount), currency,
        payment_date or expected_payment_date or "",
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
        "record_date": record_date,
        "ex_date": ex_date,
        "payment_date": payment_date,
        "date_evidence_json": date_evidence,
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
    records_by_event: Dict[tuple[Any, ...], Dict[str, Any]] = {}
    for raw_row in rows:
        row = {
            key: (
                value.decode("utf-8") if isinstance(value, bytes)
                else value.isoformat() if isinstance(value, date)
                else value
            )
            for key, value in raw_row.items()
        }
        date_evidence = row.pop("date_evidence_json", None)
        if date_evidence is not None:
            row["date_evidence"] = date_evidence
        for field in DIVIDEND_DATE_FIELDS:
            row.setdefault(field, None)
        row.setdefault("expected_payment_date", row.get("payment_date"))
        payment_date = str(row.get("expected_payment_date") or "")
        key = (
            Decimal(str(row["amount_per_share"])).normalize(),
            str(row["currency"]),
            payment_date or str(row.get("announcement_date") or ""),
        )
        existing = records_by_event.get(key)
        candidate_rank = (
            int(row.get("source_priority", 9)),
            -int(row.get("confirmation_status") == "confirmed"),
            sum(not row.get(field) for field in DIVIDEND_DATE_FIELDS),
            int(not bool(row.get("source_document_id"))),
        )
        existing_rank = (
            int(existing.get("source_priority", 9)),
            -int(existing.get("confirmation_status") == "confirmed"),
            sum(not existing.get(field) for field in DIVIDEND_DATE_FIELDS),
            int(not bool(existing.get("source_document_id"))),
        ) if existing else None
        if existing is None or candidate_rank < existing_rank:
            records_by_event[key] = row
    records = list(records_by_event.values())
    # LongPort history and detail can describe the same event at different
    # precision. Detail rows commonly have no stable id. Collapse only that
    # identifiable pair; preserve genuinely separate same-day distributions.
    collapsed: list[Dict[str, Any]] = []
    for candidate in records:
        duplicate_index = None
        duplicate_missing_date_revision = False
        candidate_amount = Decimal(str(candidate["amount_per_share"]))
        for index, existing in enumerate(collapsed):
            existing_payment = str(
                existing.get("payment_date")
                or existing.get("expected_payment_date") or ""
            )
            candidate_payment = str(
                candidate.get("payment_date")
                or candidate.get("expected_payment_date") or ""
            )
            if (
                int(existing["fiscal_year"]) != int(candidate["fiscal_year"])
                or str(existing["currency"]) != str(candidate["currency"])
            ):
                continue
            same_amount = (
                Decimal(str(existing["amount_per_share"])) == candidate_amount
            )
            missing_date_revision = (
                same_amount
                and bool(existing_payment) != bool(candidate_payment)
            )
            if existing_payment != candidate_payment and not missing_date_revision:
                continue
            existing_id = str(existing.get("source_document_id") or "")
            candidate_id = str(candidate.get("source_document_id") or "")
            if (
                str(existing.get("source_name") or "")
                != str(candidate.get("source_name") or "")
                and existing_id and candidate_id
            ):
                continue
            if not missing_date_revision and bool(existing_id) == bool(candidate_id):
                continue
            existing_amount = Decimal(str(existing["amount_per_share"]))
            denominator = max(abs(existing_amount), abs(candidate_amount), Decimal("1"))
            if abs(existing_amount - candidate_amount) / denominator <= Decimal("0.01"):
                duplicate_index = index
                duplicate_missing_date_revision = missing_date_revision
                break
        if duplicate_index is None:
            collapsed.append(candidate)
        elif (
            candidate.get("payment_date")
            or candidate.get("expected_payment_date")
        ) and not (
            collapsed[duplicate_index].get("payment_date")
            or collapsed[duplicate_index].get("expected_payment_date")
        ):
            collapsed[duplicate_index] = candidate
        elif (
            not duplicate_missing_date_revision
            and candidate.get("source_document_id")
        ):
            collapsed[duplicate_index] = candidate
    records = collapsed

    def event_year(row: Dict[str, Any]) -> int | None:
        effective = row.get("payment_date") or row.get("expected_payment_date")
        if effective:
            return int(str(effective)[:4])
        return None

    def confirmed_total(year: int) -> Decimal:
        return sum((
            Decimal(str(row["amount_per_share"]))
            for row in records
            if event_year(row) == year
            and row["confirmation_status"] == "confirmed"
            and row.get("event_status", "active") == "active"
        ), Decimal("0"))

    current = [
        row for row in records
        if event_year(row) == fiscal_year
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
    available_total = sum((
        Decimal(str(row["amount_per_share"])) for row in current
    ), Decimal("0"))
    available_currencies = sorted({str(row["currency"]) for row in current})
    previous_rows = [
        row for row in records
        if event_year(row) == fiscal_year - 1
        and row["confirmation_status"] == "confirmed"
        and row.get("event_status", "active") == "active"
    ]
    previous_currencies = sorted({str(row["currency"]) for row in previous_rows})
    excluded_incomplete = [
        row for row in records
        if int(row["fiscal_year"]) == fiscal_year and event_year(row) is None
    ]
    return {
        "symbol": normalized_symbol,
        "fiscal_year": fiscal_year,
        "announcements": current,
        "announced_count": len(current),
        "excluded_incomplete_event_count": len(excluded_incomplete),
        "amount_per_share_total": available_total,
        "total_currencies": available_currencies,
        "total_is_fully_confirmed": bool(current) and all(
            row["confirmation_status"] == "confirmed" for row in current
        ),
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
