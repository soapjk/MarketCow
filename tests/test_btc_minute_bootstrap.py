import base64
import hashlib
import json
from datetime import datetime, timedelta, timezone

import pytest

from marketcow.btc_minute_bootstrap import (
    BootstrapBudget,
    BootstrapError,
    BtcMinuteBootstrap,
    STEP_MS,
)


NOW = datetime(2026, 9, 23, 14, 0, 30, tzinfo=timezone.utc)


def row(open_time, close="101"):
    return [
        open_time, "100", "102", "99", close, "10",
        open_time + STEP_MS - 1, "1000", 12, "4", "400", "0",
    ]


def rows(count=60):
    last = int(NOW.timestamp() * 1000) // STEP_MS * STEP_MS - STEP_MS
    first = last - (count - 1) * STEP_MS
    return [row(first + index * STEP_MS) for index in range(count)]


class Response:
    status_code = 200

    def __init__(self, raw):
        self.raw = raw
        self.closed = False

    def iter_content(self, chunk_size):
        for offset in range(0, len(self.raw), 17):
            yield self.raw[offset:offset + 17]

    def close(self):
        self.closed = True


class Session:
    def __init__(self, raw, status=200):
        self.response = Response(raw)
        self.response.status_code = status
        self.calls = []
        self.trust_env = None

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.response


class Log:
    def __init__(self):
        self.facts = []
        self.durable = 0

    def publish(self, fact):
        self.facts.append(fact)
        return len(self.facts)


def final_event(item):
    return {
        "e": "kline", "E": item[6] + 1, "s": "BTCUSDT",
        "k": {
            "s": "BTCUSDT", "i": "1m", "t": item[0], "T": item[6],
            "x": True, "o": item[1], "h": item[2], "l": item[3],
            "c": item[4], "v": item[5], "q": item[7], "n": item[8],
            "V": item[9], "Q": item[10], "B": item[11],
        },
    }


def capture(payload=None):
    payload = rows() if payload is None else payload
    raw = json.dumps(payload, separators=(",", ":")).encode()
    state = BtcMinuteBootstrap(clock=lambda: NOW)
    log = Log()
    session = Session(raw)
    manifest = state.capture(log, session=session)
    return state, log, session, raw, manifest


def test_one_bounded_request_preserves_raw_and_time_semantics():
    state, log, session, raw, manifest = capture()
    assert len(session.calls) == 1
    url, options = session.calls[0]
    assert url == "https://data-api.binance.vision/api/v3/klines"
    assert options["allow_redirects"] is False
    assert options["params"]["symbol"] == "BTCUSDT"
    assert options["params"]["interval"] == "1m"
    assert options["params"]["limit"] == "60"
    assert log.facts[0]["raw_sha256"] == hashlib.sha256(raw).hexdigest()
    assert base64.b64decode(log.facts[0]["raw_base64"]) == raw
    assert log.facts[0]["received_at"] == log.facts[0]["available_at"]
    assert log.facts[0]["first_received_at"] == log.facts[0]["received_at"]
    assert log.facts[0]["historical_first_received_at"] is None
    assert manifest["continuous"] and manifest["gaps"] == []
    assert manifest["ready"] is False
    assert state.status(2)["reason"] == "awaiting_live_finalized_join"


@pytest.mark.parametrize("mutation,error", [
    (lambda value: value[:-1], "incomplete_bootstrap"),
    (lambda value: value[:10] + [row(value[10][0] + STEP_MS)] + value[11:], "kline_gap_or_order"),
    (lambda value: value[:-1] + [row(value[-1][0] + STEP_MS)], "kline_gap_or_order"),
])
def test_incomplete_gap_and_unfinished_fail_closed_after_raw_is_saved(mutation, error):
    payload = mutation(rows())
    raw = json.dumps(payload, separators=(",", ":")).encode()
    state = BtcMinuteBootstrap(clock=lambda: NOW)
    log = Log()
    with pytest.raises(BootstrapError, match=error):
        state.capture(log, session=Session(raw))
    assert len(log.facts) == 1
    assert log.facts[0]["event_type"] == "btc_1m_bootstrap_raw"
    assert state.status(1)["phase"] == "failed"


def test_exact_overlap_and_next_minute_join_are_durable_gated():
    state, log, _, _, _ = capture()
    overlap = state.observe_live_finalized(
        final_event(rows()[-1]), first_received_at="2026-09-23T14:00:31Z",
        source_raw_sha256="a" * 64,
    )
    assert overlap["relation"] == "exact_overlap"
    assert overlap["first_received_at"] == "2026-09-23T14:00:31Z"
    join_seq = log.publish(overlap)
    state.acknowledge_join(join_seq)
    assert not state.status(join_seq - 1)["ready"]
    assert state.status(join_seq)["ready"]
    assert state.status(join_seq)["joined_at"] == "2026-09-23T14:00:31Z"
    checkpoint = state.checkpoint(join_seq)
    assert checkpoint["count"] == 60
    assert checkpoint["checkpoint_sequence"] == join_seq
    assert checkpoint["bars"][-1]["available_at"] == "2026-09-23T14:00:31Z"
    assert checkpoint["bars"][-1]["source_raw_sha256"] == "a" * 64

    next_row = row(rows()[-1][0] + STEP_MS, close="101.5")
    continued = state.observe_live_finalized(
        final_event(next_row), first_received_at="2026-09-23T14:01:01Z",
        source_raw_sha256="b" * 64,
    )
    assert continued["relation"] == "next_minute"
    next_seq = log.publish(continued)
    state.acknowledge_join(next_seq)
    assert state.status(join_seq)["ready"]
    state.clock = lambda: NOW + timedelta(minutes=1)
    assert not state.status(join_seq)["ready"]
    assert state.status(next_seq)["ready"]
    assert state.checkpoint(next_seq)["bars"][-1]["source_raw_sha256"] == "b" * 64


def test_late_consumer_checkpoint_rejects_stale_or_not_durable_window():
    state, log, _, _, _ = capture()
    overlap = state.observe_live_finalized(
        final_event(rows()[-1]), first_received_at="2026-09-23T14:00:31Z",
        source_raw_sha256="c" * 64,
    )
    sequence = log.publish(overlap)
    state.acknowledge_join(sequence)
    with pytest.raises(BootstrapError, match="unavailable"):
        state.checkpoint(sequence - 1)
    state.clock = lambda: NOW + timedelta(minutes=1)
    with pytest.raises(BootstrapError, match="stale"):
        state.checkpoint(sequence)


def test_duplicate_conflict_and_gap_fail_closed():
    state, *_ = capture()
    conflict = rows()[-1]
    conflict = list(conflict)
    conflict[4] = "100.5"
    with pytest.raises(BootstrapError, match="conflict"):
        state.observe_live_finalized(final_event(conflict))
    assert state.status(999)["phase"] == "failed"

    state, *_ = capture()
    gap = row(rows()[-1][0] + 2 * STEP_MS)
    with pytest.raises(BootstrapError, match="live_join_gap"):
        state.observe_live_finalized(final_event(gap))


def test_nonfinal_update_does_not_join_and_bytes_are_bounded():
    state, *_ = capture()
    event = final_event(rows()[-1])
    event["k"]["x"] = False
    assert state.observe_live_finalized(event) is None
    assert state.status(999)["phase"] == "awaiting_live_finalized"

    raw = json.dumps(rows()).encode()
    state = BtcMinuteBootstrap(
        budget=BootstrapBudget(maximum_bytes=10), clock=lambda: NOW
    )
    session = Session(raw)
    with pytest.raises(BootstrapError, match="size"):
        state.capture(Log(), session=session)
    assert session.response.closed
