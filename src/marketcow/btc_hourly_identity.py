"""UTC-safe hour planning; no implicit subscriptions or settlement inference."""
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo


def local_hour_to_utc(local: datetime, zone: str, *, utc_evidence: datetime | None = None) -> datetime:
    if local.tzinfo is not None or local.minute or local.second or local.microsecond:
        raise ValueError("expected_naive_local_hour")
    timezone_info = ZoneInfo(zone)
    candidates = set()
    for fold in (0, 1):
        candidate = local.replace(tzinfo=timezone_info, fold=fold).astimezone(timezone.utc)
        if candidate.astimezone(timezone_info).replace(tzinfo=None) == local:
            candidates.add(candidate)
    if not candidates:
        raise ValueError("nonexistent_local_hour")
    if utc_evidence is not None:
        if utc_evidence.tzinfo is None or utc_evidence not in candidates:
            raise ValueError("utc_evidence_mismatch")
        return utc_evidence.astimezone(timezone.utc)
    if len(candidates) != 1:
        raise ValueError("ambiguous_local_hour_requires_evidence")
    return candidates.pop()


def plan_hours(markets: list[dict], now: datetime, *, maximum_markets: int, maximum_pending: int) -> dict:
    if now.tzinfo is None or min(maximum_markets, maximum_pending) <= 0:
        raise ValueError("invalid_clock_or_budget")
    now = now.astimezone(timezone.utc)
    seen = set()
    candidates = []
    pending = []
    for market in markets:
        identity = market["market_id"]
        if identity in seen:
            raise ValueError("duplicate_market")
        seen.add(identity)
        start, end = market["start"], market["end"]
        if start.tzinfo is None or end.tzinfo is None:
            raise ValueError("invalid_window")
        start, end = start.astimezone(timezone.utc), end.astimezone(timezone.utc)
        if end - start != timedelta(hours=1):
            raise ValueError("invalid_window")
        if market["up_token"] == market["down_token"] or not market["up_token"] or not market["down_token"]:
            raise ValueError("invalid_tokens")
        if end <= now:
            if market["settlement_status"] != "verified_final":
                pending.append(identity)
        elif start <= now + timedelta(hours=2) and market["rule_review_status"] == "approved":
            candidates.append((start, identity))
    if len(pending) > maximum_pending:
        raise ValueError("pending_settlement_capacity")
    candidates.sort()
    if len(candidates) > maximum_markets:
        raise ValueError("subscription_plan_capacity")
    return {"subscribe_proposal": [identity for _, identity in candidates],
            "settlement_pending": sorted(pending), "activation_performed": False}
