from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, Mapping


DIVIDEND_ASSESSMENT_SCHEMA = "dividend-assessment-v1"
DIVIDEND_ASSESSMENT_STATUSES = frozenset({
    "confirmed_zero",
    "likely_zero",
    "not_announced",
    "pays_dividend",
    "unavailable",
})
FAILED_REFRESH_STATUSES = frozenset({
    "failed_source",
    "failed_rate_limited",
    "failed_timeout",
    "failed_parse",
})


def _iso_as_of(value: str | datetime | None) -> str:
    if value is None:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat(timespec="seconds")


def _evidence_for_state(year: int, state: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "kind": "completed_year_query",
        "fiscal_year": year,
        "source": state.get("query_source"),
        "status": state.get("status"),
        "completed_at": (
            str(state["completed_at"]) if state.get("completed_at") else None
        ),
        "result_count": state.get("result_count"),
        "cache_schema_version": state.get("cache_schema_version"),
        "parser_version": state.get("parser_version"),
    }


def assess_dividend(
    *,
    symbol: str,
    fiscal_year: int,
    announced_count: int,
    asset_type: str,
    refresh_state: Mapping[str, Any] | None,
    historical_states: Mapping[int, Mapping[str, Any]],
    previous_complete_year: Mapping[str, Any] | None,
    likely_zero_years: int,
    as_of: str | datetime | None = None,
    policy_evidence: Iterable[Mapping[str, Any]] = (),
) -> Dict[str, Any]:
    """Build an auditable assessment without turning an empty query into zero."""
    as_of_value = _iso_as_of(as_of)
    as_of_year = date.fromisoformat(as_of_value[:10]).year
    normalized_asset_type = asset_type if asset_type in {"equity", "etf"} else "unknown"
    current_state = dict(refresh_state or {})
    status = str(current_state.get("status") or "")
    evidence = [dict(item) for item in policy_evidence]
    coverage: Dict[str, Any] = {
        "start_year": None,
        "end_year": None,
        "complete_years": [],
        "required_complete_years": likely_zero_years,
    }
    result: Dict[str, Any] = {
        "schema": DIVIDEND_ASSESSMENT_SCHEMA,
        "version": 1,
        "status": "unavailable",
        "confidence": 0.0,
        "as_of": as_of_value,
        "symbol": symbol,
        "fiscal_year": fiscal_year,
        "asset_type": normalized_asset_type,
        "rule": "insufficient_evidence",
        "reason": "historical_coverage_missing",
        "coverage": coverage,
        "evidence": evidence,
        "previous_complete_year": previous_complete_year,
    }

    if announced_count > 0:
        result.update({
            "status": "pays_dividend",
            "confidence": 1.0,
            "rule": "current_year_has_dividend_events",
            "reason": None,
        })
        return result

    explicit_zero = [
        item for item in evidence
        if item.get("kind") in {"explicit_no_dividend_policy", "authoritative_zero_year"}
        and item.get("verification_status") == "confirmed"
        and item.get("source")
        and item.get("source_url")
        and item.get("source_document_id")
        and int(item.get("fiscal_year", fiscal_year)) == fiscal_year
    ]
    if explicit_zero and fiscal_year < as_of_year:
        result.update({
            "status": "confirmed_zero",
            "confidence": 1.0,
            "rule": "confirmed_policy_or_authoritative_zero_year",
            "reason": None,
            "evidence": explicit_zero,
        })
        return result

    if status in FAILED_REFRESH_STATUSES:
        result.update({
            "reason": {
                "failed_source": "current_announcement_source_failure",
                "failed_rate_limited": "current_announcement_source_rate_limited",
                "failed_timeout": "current_announcement_source_timeout",
                "failed_parse": "current_announcement_source_parse_failure",
            }[status],
            "rule": "current_query_failed",
            "evidence": [_evidence_for_state(fiscal_year, current_state)],
        })
        return result

    completed_empty_years = []
    for year in range(fiscal_year - 1, fiscal_year - likely_zero_years - 1, -1):
        state = historical_states.get(year)
        if not state or state.get("status") != "success_empty":
            break
        completed_empty_years.append(year)
        evidence.append(_evidence_for_state(year, state))
    if completed_empty_years:
        coverage.update({
            "start_year": min(completed_empty_years),
            "end_year": max(completed_empty_years),
            "complete_years": sorted(completed_empty_years),
        })

    previous_total = (
        (previous_complete_year or {}).get("confirmed_amount_per_share_total") or 0
    )
    if (
        fiscal_year >= as_of_year
        and status == "success_empty"
        and previous_total
    ):
        result.update({
            "status": "not_announced",
            "confidence": 0.9,
            "rule": "current_year_empty_with_prior_dividend_baseline",
            "reason": None,
            "evidence": evidence + [_evidence_for_state(fiscal_year, current_state)],
        })
        return result

    if normalized_asset_type == "etf" and not explicit_zero:
        result.update({
            "reason": "fund_distribution_coverage_or_policy_missing",
            "rule": "etf_empty_query_is_not_zero_evidence",
            "evidence": evidence,
        })
        return result

    if len(completed_empty_years) >= likely_zero_years:
        result.update({
            "status": "likely_zero",
            "confidence": 0.8,
            "rule": f"{likely_zero_years}_consecutive_complete_empty_years",
            "reason": None,
            "evidence": evidence,
        })
        return result

    if fiscal_year >= as_of_year and status == "success_empty":
        result.update({
            "status": "not_announced",
            "confidence": 0.5,
            "rule": "current_year_query_complete_without_announcement",
            "reason": None,
            "evidence": evidence + [_evidence_for_state(fiscal_year, current_state)],
        })
        return result

    reason = (
        "asset_type_unsupported"
        if normalized_asset_type == "unknown"
        else "historical_coverage_missing"
    )
    result.update({"reason": reason, "evidence": evidence})
    return result
