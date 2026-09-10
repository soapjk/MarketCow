import json
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta, timezone

import pytest

from marketcow.btc_hourly_identity import local_hour_to_utc, plan_hours
from marketcow.btc_nautilus_worker import decode_raw


def test_dst():
    with pytest.raises(ValueError, match="nonexistent"):
        local_hour_to_utc(datetime(2026, 3, 8, 2), "America/New_York")
    with pytest.raises(ValueError, match="ambiguous"):
        local_hour_to_utc(datetime(2026, 11, 1, 1), "America/New_York")
    evidence = datetime(2026, 11, 1, 6, tzinfo=timezone.utc)
    assert local_hour_to_utc(datetime(2026, 11, 1, 1), "America/New_York", utc_evidence=evidence) == evidence


def test_planner_dst_uses_elapsed_utc_not_wall_hour():
    zone = ZoneInfo("America/New_York")
    start = datetime(2026, 11, 1, 1, tzinfo=zone, fold=0)
    market = dict(market_id="1", start=start,
                  end=datetime(2026, 11, 1, 1, tzinfo=zone, fold=1),
                  up_token="1", down_token="2", rule_review_status="approved", settlement_status="pending")
    assert plan_hours([market], start, maximum_markets=1, maximum_pending=1)["subscribe_proposal"] == ["1"]
    market["end"] = datetime(2026, 11, 1, 2, tzinfo=zone)
    with pytest.raises(ValueError, match="invalid_window"):
        plan_hours([market], start, maximum_markets=1, maximum_pending=1)


def test_expired_still_tracks_settlement():
    now = datetime(2026, 9, 7, 12, tzinfo=timezone.utc)
    market = dict(market_id="1", start=now - timedelta(hours=1), end=now,
                  up_token="1", down_token="2", rule_review_status="approved", settlement_status="reported")
    result = plan_hours([market], now, maximum_markets=3, maximum_pending=3)
    assert result["settlement_pending"] == ["1"] and result["subscribe_proposal"] == []
    market["settlement_status"] = "verified_final"
    assert plan_hours([market], now, maximum_markets=3, maximum_pending=3)["settlement_pending"] == []


@pytest.mark.parametrize("final", [True, False])
def test_raw_keeps_incomplete_klines(final):
    raw = json.dumps({"stream": "btcusdt@kline_1m", "data": {"s": "BTCUSDT", "e": "kline", "E": 1,
                     "k": {"s": "BTCUSDT", "i": "1m", "x": final}}}).encode()
    fact = decode_raw(raw, processed_at="2026-09-10T00:00:00Z", monotonic_ns=1, maximum_bytes=1024)
    assert fact["final"] is final and fact["first_received_at"] == "2026-09-10T00:00:00Z"
    assert fact["received_monotonic_ns"] == 1
    assert fact["missing_reasons"] == ["socket_kernel_receive_time_unknown", "source_continuity_not_yet_verified"]
    assert fact["raw_utf8"].encode() == raw


def test_wrong_instrument_and_duplicate_keys():
    for raw in [b'{"data":{"s":"ETHUSDT"}}', b'{"data":{},"data":{}}']:
        with pytest.raises(ValueError):
            decode_raw(raw, processed_at="2026-09-10T00:00:00Z", monotonic_ns=1, maximum_bytes=1024)
