import json
from datetime import datetime, timedelta, timezone

import pytest

from marketcow.btc_polymarket_capture import CaptureBoundary, fact
from marketcow.polymarket_live import PolymarketLiveReadStore, LiveStateStore, GammaLiveNormalizer
from marketcow.polymarket_live_stream import PolymarketLiveProjection

NOW = datetime(2026, 8, 22, 11, tzinfo=timezone.utc)


def populated_store(root):
    store = LiveStateStore(root, now_provider=lambda: NOW)
    rows = [dict(id="m1", conditionId="0x"+"1"*64, slug="synthetic-hour",
                 question="Synthetic hourly fixture", title="Synthetic", active=True, closed=False,
                 acceptingOrders=True, startDate="2026-08-01T00:00:00Z", endDate="2026-09-01T00:00:00Z",
                 clobTokenIds='["10","20"]', outcomes='["Up","Down"]', orderPriceMinTickSize="0.01",
                 orderMinSize="1", feesEnabled=False, negRisk=False, updatedAt="2026-08-03T03:59:00Z",
                 events=[dict(id="e1")])]
    store.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
    for token in ("10", "20"):
        store.apply_snapshot(dict(event_type="book", asset_id=token, timestamp="1785739200000",
                                  hash="fixture-"+token, tick_size="0.01", min_order_size="1",
                                  bids=[dict(price="0.4", size="10")], asks=[dict(price="0.6", size="10")],
                                  last_trade_price="0.4"), received_at=NOW)
    return store


def boundary(root):
    store = populated_store(root)
    projection = PolymarketLiveProjection(replay_capacity=100)
    projection.install_state(dict(schema_version="marketcow.polymarket.live-stream.v1", type="state",
                                  catalog_revision=store.catalog_revision, catalog_source=store.catalog_source,
                                  latest_cursor=store.cursor, persisted_cursor=store.cursor, active_recovery_id=None,
                                  markets=[m.model_dump(mode="json") for m in store.catalog.values()],
                                  books=[b.model_dump(mode="json") for b in store.books.values()], gaps=[]))
    projection.mark_ready({"latest_cursor": store.cursor})
    reader = PolymarketLiveReadStore(root, now_provider=lambda: NOW+timedelta(seconds=1),
                                    stable_snapshot_max_book_age_seconds=5,
                                    consumer_maximum_book_age_seconds=5, minimum_delivery_headroom_seconds=1)
    body, _, _ = projection.full_sync_json(reader, ["m1"])
    baseline = json.loads(body)
    # Legacy in-process projection has no configured scope; add an explicit
    # synthetic scope consistently across the generated atomic components.
    for part in (baseline, baseline["health"], baseline["bootstrap"], baseline["snapshot"]):
        part["scope_id"] = "synthetic-configured-scope"
    identity = baseline["bootstrap"]["markets"][0]["identity"]
    binding = dict(market_id=identity["market_id"], condition_id=identity["condition_id"],
                   up_token=identity["outcomes"][0]["token_id"], down_token=identity["outcomes"][1]["token_id"])
    return CaptureBoundary(baseline, [binding])


def test_ready_identity_and_error_fail_closed(tmp_path):
    state = boundary(tmp_path)
    ready = dict(type="ready", cursor=state.cursor, stream_instance_id=state.instance,
                 confirmation_sequence=0, confirmation_books=[])
    assert state.consume(json.dumps(ready)) == "ready"
    with pytest.raises(ValueError, match="ready_identity"):
        state.consume(json.dumps(ready))
    with pytest.raises(ValueError, match="source_error"):
        state.consume('{"type":"error"}')


def test_duplicate_keys_and_regression(tmp_path):
    state = boundary(tmp_path)
    with pytest.raises(ValueError, match="duplicate"):
        state.consume('{"type":"ready","type":"event"}')
    with pytest.raises(ValueError, match="regression"):
        state.consume(json.dumps(dict(type="ready", cursor=state.cursor-1)))


def test_wire_fact_records_application_receive_boundary():
    raw = b'{"type":"ready"}'
    value = fact(raw, "ready", "instance", 7,
                 first_received_at="2026-09-10T00:00:00Z", received_monotonic_ns=42)
    assert value["first_received_at"] == "2026-09-10T00:00:00Z"
    assert value["received_monotonic_ns"] == 42
    assert value["missing_reasons"] == ["socket_kernel_receive_time_unknown"]
