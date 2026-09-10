"""Bind reviewed BTC-hour identities to original Gamma evidence and live scope.

No title-derived rule approval, subscriptions, payout inference or account writes.
"""
import base64
import hashlib
import re
from datetime import timedelta, timezone

from .btc_hourly_dataset import canonical, timestamp
from .universe_live_probe import strict_json


BINANCE_HOUR_RULE = (
    'This market will resolve to "Up" if the close price is greater than or equal to the open price '
    'for the BTC/USDT 1 hour candle that begins on the time and date specified in the title. '
    'Otherwise, this market will resolve to "Down".\n\n'
    'The resolution source for this market is information from Binance, specifically the BTC/USDT pair '
    '(https://www.binance.com/en/trade/BTC_USDT). The close « C » and open « O » displayed at the top of '
    'the graph for the relevant "1H" candle will be used once the data for that candle is finalized.\n\n'
    'Please note that this market is about the price according to Binance BTC/USDT, not according to '
    'other exchanges or trading pairs.'
)


def review_binance_hour(evidence, expected_start, *, maximum_raw_bytes=262144):
    """Approve only the exact source template and an independently proposed UTC hour."""
    if evidence.get("schema_version") != "marketcow.polymarket.market-evidence.v1":
        raise ValueError("evidence_schema")
    encoded = evidence.get("raw_base64")
    if not isinstance(encoded, str) or len(encoded) > 4*((maximum_raw_bytes+2)//3):
        raise ValueError("evidence_budget")
    raw = base64.b64decode(encoded, validate=True)
    digest = hashlib.sha256(raw).hexdigest()
    if (len(raw) > maximum_raw_bytes or evidence.get("raw_complete") is not True
            or evidence.get("raw_bytes") != len(raw) or evidence.get("raw_sha256") != digest):
        raise ValueError("evidence_integrity")
    source = strict_json(raw)
    start = timestamp(expected_start).astimezone(timezone.utc)
    observed_start = timestamp(source.get("eventStartTime")).astimezone(timezone.utc)
    end = timestamp(source.get("endDate")).astimezone(timezone.utc)
    if (start != observed_start or end-start != timedelta(hours=1) or start.minute or start.second or start.microsecond):
        raise ValueError("source_hour_mismatch")
    if (source.get("description") != BINANCE_HOUR_RULE
            or source.get("resolutionSource") != "https://www.binance.com/en/trade/BTC_USDT"):
        raise ValueError("unsupported_rule_template")
    if source.get("outcomes") != '["Up", "Down"]':
        raise ValueError("unsupported_outcomes")
    def utc(value):
        return value.isoformat(timespec="seconds").replace("+00:00", "Z")
    return {"schema_version": "marketcow.btc-hour.rule-review.v1",
            "market_id": evidence.get("market_id"), "raw_sha256": digest,
            "status": "approved", "source_instrument": "BINANCE_SPOT:BTCUSDT",
            "comparison": "final_close_gte_open", "start_utc": utc(start), "end_utc": utc(end),
            "review_method": "exact_source_template_v1"}


def bind_hour(evidence, review, *, maximum_raw_bytes=262144):
    if evidence.get("schema_version") != "marketcow.polymarket.market-evidence.v1":
        raise ValueError("evidence_schema")
    encoded = evidence.get("raw_base64")
    if not isinstance(encoded, str) or len(encoded) > 4*((maximum_raw_bytes+2)//3):
        raise ValueError("evidence_budget")
    raw = base64.b64decode(encoded, validate=True)
    digest = hashlib.sha256(raw).hexdigest()
    if len(raw) > maximum_raw_bytes or evidence.get("raw_complete") is not True or evidence.get("raw_bytes") != len(raw) or evidence.get("raw_sha256") != digest:
        raise ValueError("evidence_integrity")
    source = strict_json(raw)
    market, condition = evidence["market_id"], evidence["condition_id"]
    if source.get("id") != market or source.get("conditionId") != condition or not re.fullmatch(r"0x[0-9a-fA-F]{64}", condition):
        raise ValueError("evidence_identity")
    tokens = strict_json(source["clobTokenIds"])
    outcomes = strict_json(source["outcomes"])
    if outcomes != ["Up", "Down"] or len(tokens) != 2 or len(set(tokens)) != 2 or not all(isinstance(t, str) and t.isdecimal() for t in tokens):
        raise ValueError("not_bound_up_down")
    expected = [{"token_id": t, "outcome": o} for t, o in zip(tokens, outcomes)]
    if evidence.get("outcomes") != expected:
        raise ValueError("outcome_projection_mismatch")
    if review.get("schema_version") != "marketcow.btc-hour.rule-review.v1" or review.get("raw_sha256") != digest or review.get("market_id") != market:
        raise ValueError("rule_review_binding")
    if review.get("status") != "approved" or review.get("source_instrument") != "BINANCE_SPOT:BTCUSDT" or review.get("comparison") != "final_close_gte_open":
        raise ValueError("unsupported_or_unreviewed_rule")
    if not isinstance(source.get("description"), str) or not source["description"].strip():
        raise ValueError("full_rule_missing")
    start, end = timestamp(review["start_utc"]), timestamp(review["end_utc"])
    if end-start != timedelta(hours=1) or start.minute or start.second or start.microsecond:
        raise ValueError("invalid_hour")
    timestamp(evidence["observed_at"])
    return {"schema_version": "marketcow.btc-hour.binding.v1", "market_id": market,
            "condition_id": condition, "up_token": tokens[0], "down_token": tokens[1],
            "start_utc": review["start_utc"], "end_utc": review["end_utc"],
            "source_instrument": review["source_instrument"],
            "raw_sha256": digest, "review_sha256": hashlib.sha256(canonical(review)).hexdigest(),
            "source_observed_at": evidence["observed_at"],
            "settlement_finality": "unverified", "execution_eligible": False}


def scope_coverage(binding, baseline):
    """Identity coverage only; full book/confirmation validation stays in live decoder."""
    if baseline.get("schema_version") != "marketcow.polymarket.live-full-sync.v1":
        raise ValueError("full_sync_schema")
    instance, scope, cursor = (baseline.get(k) for k in ("stream_instance_id", "scope_id", "cursor"))
    if not isinstance(instance, str) or not instance or not isinstance(scope, str) or not scope or type(cursor) is not int or cursor < 0:
        raise ValueError("full_sync_boundary")
    matches = [m for m in baseline["bootstrap"]["markets"] if m["identity"]["market_id"] == binding["market_id"]]
    if len(matches) > 1:
        raise ValueError("duplicate_market")
    if not matches:
        return {"covered": False, "reason": "outside_current_scope", "activation_performed": False}
    identity = matches[0]["identity"]
    outcomes = identity["outcomes"]
    tokens = {(v["token_id"], v.get("outcome")) for v in outcomes}
    if (identity["condition_id"] != binding["condition_id"] or len(outcomes) != 2
            or tokens != {(binding["up_token"], "Up"), (binding["down_token"], "Down")}):
        raise ValueError("live_identity_mismatch")
    return {"covered": True, "scope_id": scope, "stream_instance_id": instance,
            "cursor": cursor, "book_ready": False, "activation_performed": False}
