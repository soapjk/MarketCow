import pytest

from marketcow.btc_continuity import Continuity


def trade(n):
    return dict(e="trade", s="BTCUSDT", E=100, T=99, t=n, p="60000", q="0.1", m=False)


def bar(t=0, final=False, event=1000):
    return dict(e="kline", s="BTCUSDT", E=event,
                k=dict(s="BTCUSDT", i="1m", t=t, T=t+59999, x=final,
                       o="10", h="12", l="9", c="11", v="1", q="10", V="0", Q="0", n=1))


def test_trade_gap_remains_unresolved_and_duplicate_does_not_advance():
    state = Continuity()
    state.observe(trade(1))
    assert not state.observe(trade(1))["apply_to_live"]
    assert state.observe(trade(3))["gap"] == {"start": 2, "end_exclusive": 3}
    assert state.observe(trade(4))["continuity"] == "gap_unresolved"
    assert not state.observe(trade(2))["apply_to_live"]
    assert state.states["trade"]["position"] == 4


def test_final_bar_cannot_be_reopened_and_missing_final_detected():
    state = Continuity()
    state.observe(bar())
    state.observe(bar(final=True, event=60000))
    assert not state.observe(bar(event=60001))["apply_to_live"]
    assert state.observe(bar(t=60000, event=61000))["gap"] is None
    assert state.observe(bar(t=120000, event=121000))["reason"] == "missing_final_bar"


@pytest.mark.parametrize("key,value", [("p", "NaN"), ("q", "0"), ("t", True), ("m", 1)])
def test_invalid_trade_does_not_mutate_state(key, value):
    state = Continuity()
    data = trade(1)
    data[key] = value
    with pytest.raises(ValueError):
        state.observe(data)
    assert not state.states


def test_entities_independent():
    state = Continuity()
    state.observe(trade(1))
    state.observe(trade(4))
    assert state.observe(bar())["continuity"] == "observed_segment_only"
    with pytest.raises(ValueError, match="premature"):
        state.observe(bar(final=True))
    assert state.observe(trade(5))["apply_to_live"]


def test_checkpoint_restart_preserves_gap_and_repair_eligibility():
    import json
    original = Continuity()
    original.observe(bar())
    original.observe(bar(t=120000, event=121000))
    restored = Continuity()
    restored.restore(json.loads(json.dumps(original.checkpoint())))
    quality = restored.observe(bar(t=120000, event=122000))
    assert quality["gap"] == {"start": 0, "end_exclusive": 120000}
    assert quality["continuity"] == "gap_unresolved"
    assert restored.repair_completed("1m", 0, 120000)
    assert original.states["1m"]["missing"]  # No shared mutable state.
    assert restored.observe(bar(t=120000, event=123000))["gap"] is None


def test_restore_rejects_invalid_state_atomically():
    state = Continuity()
    state.observe(trade(1))
    checkpoint = state.checkpoint()
    checkpoint["states"]["trade"]["missing"] = True
    with pytest.raises(ValueError, match="invalid_continuity_gap"):
        state.restore(checkpoint)
    assert not state.states["trade"]["missing"]


def test_skipped_window_includes_unfinished_previous_and_old_receipt_cannot_clear_new_gap():
    state = Continuity()
    state.observe(bar())
    gap = state.observe(bar(t=120000, event=121000))["gap"]
    assert gap == {"start": 0, "end_exclusive": 120000}
    latest = state.observe(bar(t=240000, event=241000))
    assert latest["gap"] == {"start": 0, "end_exclusive": 240000}
    assert state.repair_completed("1m", 0, 120000) is False
    assert state.states["1m"]["missing"]
    assert state.repair_completed("1m", 0, 240000) is True
    assert state.states["1m"]["position"] == 240000
    assert state.states["1m"]["missing"] is False
