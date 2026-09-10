import base64
import hashlib
import json

import pytest

from marketcow.btc_polymarket_binding import BINANCE_HOUR_RULE, bind_hour, review_binance_hour, scope_coverage


def sample():
    raw = json.dumps(dict(id="1", conditionId="0x"+"a"*64, description="Synthetic reviewed rule.",
                          clobTokenIds='["10","20"]', outcomes='["Up","Down"]')).encode()
    digest = hashlib.sha256(raw).hexdigest()
    evidence = dict(schema_version="marketcow.polymarket.market-evidence.v1", market_id="1",
                    condition_id="0x"+"a"*64, raw_base64=base64.b64encode(raw).decode(), raw_sha256=digest,
                    raw_bytes=len(raw), raw_complete=True, observed_at="2026-09-10T00:00:00Z",
                    outcomes=[dict(token_id="10", outcome="Up"), dict(token_id="20", outcome="Down")])
    review = dict(schema_version="marketcow.btc-hour.rule-review.v1", raw_sha256=digest, market_id="1",
                  status="approved", source_instrument="BINANCE_SPOT:BTCUSDT", comparison="final_close_gte_open",
                  start_utc="2026-09-09T01:00:00Z", end_utc="2026-09-09T02:00:00Z")
    return evidence, review


def test_binding_is_not_settlement_or_trading_permission():
    binding = bind_hour(*sample())
    assert binding["settlement_finality"] == "unverified"
    assert binding["execution_eligible"] is False
    baseline = dict(schema_version="marketcow.polymarket.live-full-sync.v1",
                    stream_instance_id="instance", scope_id="scope", cursor=1, bootstrap=dict(markets=[]))
    assert not scope_coverage(binding, baseline)["covered"]
    baseline["bootstrap"]["markets"] = [dict(identity=dict(market_id="1", condition_id=binding["condition_id"],
                                                           outcomes=[dict(token_id="10", outcome="Up"), dict(token_id="20", outcome="Down")]))]
    assert scope_coverage(binding, baseline)["covered"]
    assert not scope_coverage(binding, baseline)["book_ready"]
    outcomes = baseline["bootstrap"]["markets"][0]["identity"]["outcomes"]
    outcomes[0]["outcome"], outcomes[1]["outcome"] = "Down", "Up"
    with pytest.raises(ValueError, match="identity"):
        scope_coverage(binding, baseline)
    outcomes[0]["outcome"], outcomes[1]["outcome"] = "Up", "Down"
    baseline["bootstrap"]["markets"][0]["identity"]["condition_id"] = "wrong"
    with pytest.raises(ValueError, match="identity"):
        scope_coverage(binding, baseline)


@pytest.mark.parametrize("field,value", [("raw_sha256", "0"*64), ("status", "unreviewed"),
                                        ("source_instrument", "CHAINLINK:BTCUSD"),
                                        ("end_utc", "2026-09-09T03:00:00Z")])
def test_rule_changes_or_other_product_rejected(field, value):
    evidence, review = sample()
    review[field] = value
    with pytest.raises(ValueError):
        bind_hour(evidence, review)


def test_raw_tamper_and_projection_tamper():
    evidence, review = sample()
    evidence["outcomes"].reverse()
    with pytest.raises(ValueError, match="projection"):
        bind_hour(evidence, review)


def test_exact_source_template_generates_review_and_binding():
    raw = json.dumps(dict(id="1", conditionId="0x"+"a"*64,
        eventStartTime="2026-09-10T11:00:00Z", endDate="2026-09-10T12:00:00Z",
        description=BINANCE_HOUR_RULE, resolutionSource="https://www.binance.com/en/trade/BTC_USDT",
        clobTokenIds='["10", "20"]', outcomes='["Up", "Down"]')).encode()
    digest = hashlib.sha256(raw).hexdigest()
    evidence = dict(schema_version="marketcow.polymarket.market-evidence.v1", market_id="1",
        condition_id="0x"+"a"*64, raw_base64=base64.b64encode(raw).decode(), raw_sha256=digest,
        raw_bytes=len(raw), raw_complete=True, observed_at="2026-09-10T11:07:00Z",
        outcomes=[dict(token_id="10", outcome="Up"), dict(token_id="20", outcome="Down")])
    review = review_binance_hour(evidence, "2026-09-10T11:00:00Z")
    assert review["review_method"] == "exact_source_template_v1"
    assert bind_hour(evidence, review)["start_utc"] == "2026-09-10T11:00:00Z"
    source = json.loads(raw)
    source["description"] += " Unexpected clause."
    changed = json.dumps(source).encode()
    evidence.update(raw_base64=base64.b64encode(changed).decode(), raw_sha256=hashlib.sha256(changed).hexdigest(), raw_bytes=len(changed))
    with pytest.raises(ValueError, match="template"):
        review_binance_hour(evidence, "2026-09-10T11:00:00Z")
    evidence, review = sample()
    evidence["raw_sha256"] = "0"*64
    with pytest.raises(ValueError, match="integrity"):
        bind_hour(evidence, review)
