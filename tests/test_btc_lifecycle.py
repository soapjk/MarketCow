from datetime import timedelta

import pytest

from marketcow.btc_lifecycle import HourRegistry
from marketcow.btc_hourly_dataset import canonical
from marketcow.btc_polymarket_binding import bind_hour, timestamp
from tests.test_btc_polymarket_binding import sample


def binding():
    return bind_hour(*sample())


def test_expiration_and_restart_keep_settlement_pending(tmp_path):
    path = tmp_path / "hours.sqlite"
    b = binding()
    store = HourRegistry(path, maximum_markets=3, maximum_bytes=16384)
    store.register([b])
    assert store.plan(timestamp(b["start_utc"]), maximum_subscriptions=3)["subscribe_proposal"] == [b["market_id"]]
    store = HourRegistry(path, maximum_markets=3, maximum_bytes=16384)
    plan = store.plan(timestamp(b["end_utc"])+timedelta(days=3), maximum_subscriptions=3)
    assert plan["subscribe_proposal"] == []
    assert plan["settlement_pending"] == [b["market_id"]]
    assert not plan["activation_performed"]


def test_reported_resolution_does_not_remove_pending_and_preserves_raw(tmp_path):
    b = binding()
    store = HourRegistry(tmp_path / "hours.sqlite", maximum_markets=3, maximum_bytes=16384)
    store.register([b])
    observation = dict(schema_version="marketcow.polymarket.ctf-observation.v1",
                       condition_id=b["condition_id"], status="resolved_unverified",
                       observed_at=b["end_utc"], settlement_import_allowed=False)
    raw = canonical(observation)
    digest = store.observe_settlement(b["market_id"], raw)
    assert store.observe_settlement(b["market_id"], raw) == digest
    with store.db() as db:
        assert db.execute("SELECT body FROM observations").fetchall() == [(raw,)]
    assert store.plan(timestamp(b["end_utc"]), maximum_subscriptions=3)["settlement_pending"] == [b["market_id"]]
    observation["settlement_import_allowed"] = True
    with pytest.raises(ValueError, match="unsupported_finality"):
        store.observe_settlement(b["market_id"], canonical(observation))


def test_capacity_is_atomic_and_binding_change_is_not_silent(tmp_path):
    b = binding()
    store = HourRegistry(tmp_path / "hours.sqlite", maximum_markets=1, maximum_bytes=16384)
    other = dict(b, market_id="another")
    with pytest.raises(ValueError):
        store.register([b, other])
    assert not store.plan(timestamp(b["start_utc"]), maximum_subscriptions=1)["subscribe_proposal"]
    store.register([b])
    with pytest.raises(ValueError, match="binding_changed"):
        store.register([dict(b, up_token="999")])
    with pytest.raises(ValueError, match="capacity"):
        store.register([other])
    assert store.plan(timestamp(b["start_utc"]), maximum_subscriptions=1)["subscribe_proposal"] == [b["market_id"]]


def test_verified_quorum_removes_only_exact_hour_from_pending(tmp_path):
    b = binding()
    store = HourRegistry(tmp_path / "hours.sqlite", maximum_markets=3, maximum_bytes=32768)
    store.register([b])
    value = {"schema_version": "marketcow.polymarket.ctf-finality-quorum.v1",
             "condition_id": b["condition_id"], "token_ids": [b["up_token"], b["down_token"]],
             "payout_numerators": ["1", "0"], "payout_denominator": "1", "status": "verified_final",
             "finality_policy": "two_independent_rpc_finalized_receipts_v1",
             "provider_receipts": [{"provider_id": "a"}, {"provider_id": "b"}],
             "verified_at": b["end_utc"], "settlement_import_allowed": True}
    raw = canonical(value)
    assert store.observe_final_settlement(b["market_id"], raw) == store.observe_final_settlement(b["market_id"], raw)
    assert store.plan(timestamp(b["end_utc"]), maximum_subscriptions=3)["settlement_pending"] == []
    value["token_ids"] = [b["down_token"], b["up_token"]]
    with pytest.raises(ValueError, match="identity"):
        store.observe_final_settlement(b["market_id"], canonical(value))
