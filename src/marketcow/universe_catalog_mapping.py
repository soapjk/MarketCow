"""Source-backed phase-1 Gamma mapping, not a strategy eligibility filter."""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Callable

from marketcow.universe_control import wire_bytes


def optional_time(value):
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return None
        return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except ValueError:
        return None


def map_gamma_record(normalized: dict, raw: dict, *, observed_at: str,
                     metric_unit: str | None, contains: Callable[[str], bool]) -> dict:
    """Caller verifies raw/normalized provenance before mapping.

    metric_unit is an explicit source mapping setting, never inferred from a
    price. Without it metrics are unknown. Membership uses the final INCLUDED
    snapshot identities, not the 1000-market pool. Missing event identity fails
    mapping; the normalizer's market-id fallback is not a source event identity.
    """
    identity = normalized["identity"]
    mid = identity["market_id"]
    events = raw.get("events")
    event = events[0] if isinstance(events, list) and events and isinstance(events[0], dict) else {}
    event_id = raw.get("event_id") or event.get("id")
    condition = raw.get("conditionId") or raw.get("condition_id")
    if not event_id or not condition or str(raw.get("id", "")) != mid:
        raise ValueError("source_identity_missing")
    if str(event_id) != identity["event_id"] or str(condition) != identity["condition_id"]:
        raise ValueError("source_identity_mismatch")
    captured = optional_time(observed_at)
    if captured is None:
        raise ValueError("source_capture_time_missing")

    def metric(key):
        missing = {"value": None, "unit": None, "observed_at": None, "source": None}
        value = raw.get(key)
        if not metric_unit or value is None or type(value) is bool:
            return missing
        # Input parser must preserve decimal lexemes; no binary float roundtrip.
        if type(value) not in (str, int):
            return missing
        try:
            decimal = Decimal(value)
            if not decimal.is_finite() or decimal < 0:
                return missing
        except InvalidOperation:
            return missing
        return {"value": str(decimal), "unit": metric_unit,
                "observed_at": captured, "source": "polymarket_gamma"}

    relations = []
    for relation in normalized["relations"]:
        if relation["relation_type"] == "binary_complements":
            members = [mid]
        elif relation["relation_type"] == "standard_negative_risk":
            members = sorted({pair["market_id"] for pair in relation["outcome_pairs"]})
        else:
            raise ValueError("unsupported_source_relation")
        missing = [member for member in members if not contains(member)]
        relations.append({"relation_id": relation["relation_id"], "revision": relation["revision"],
                          "relation_type": relation["relation_type"], "member_market_ids": members,
                          "missing_market_ids": missing, "complete": bool(relation["complete"]) and not missing})

    def string(value):
        return value if isinstance(value, str) else None

    def flag(key):
        return raw.get(key) if type(raw.get(key)) is bool else None

    record = {"market_id": mid, "event_id": str(event_id), "condition_id": str(condition),
              "title": string(raw.get("title")) or string(event.get("title")),
              "question": string(raw.get("question")), "active": flag("active"),
              "closed": flag("closed"), "accepting_orders": flag("acceptingOrders"),
              "end_at": optional_time(raw.get("endDate")),
              "outcomes": [{"token_id": item["token_id"], "instrument_id": item["instrument_id"],
                            "outcome": item["outcome"]} for item in identity["outcomes"]],
              "relations": sorted(relations, key=lambda r: r["relation_id"]),
              "volume_24h": metric("volume24hr"), "liquidity": metric("liquidityNum"),
              "observed_at": captured, "source": "polymarket_gamma"}
    record["evidence_sha256"] = hashlib.sha256(wire_bytes({
        "normalized_record": normalized, "raw_evidence": raw, "record": record,
    })).hexdigest()
    return record
