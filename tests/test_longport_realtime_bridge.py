from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from python.marketcow_workers.longport_realtime_bridge import (
    BRIDGE_VERSION,
    canonical_frame_bytes,
    depth_frame,
    market_state_frame,
    trades_frame,
)


FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "longport-realtime-bridge-v1.json").read_text(
        encoding="utf-8"
    )
)


def _time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def test_python_raw_bridge_matches_shared_rust_golden_inputs() -> None:
    depth = FIXTURE["cases"][0]["raw"]
    depth_push = SimpleNamespace(
        sequence=depth["provider_sequence"],
        bids=[SimpleNamespace(price=value["price"], volume=value["size"]) for value in depth["bids"]],
        asks=[SimpleNamespace(price=value["price"], volume=value["size"]) for value in depth["asks"]],
    )
    assert depth_frame(
        depth["symbol"], depth_push, observed_at=_time(depth["observed_at"])
    ) == depth

    state = FIXTURE["cases"][1]["raw"]
    state_push = SimpleNamespace(
        timestamp=_time(state["timestamp"]),
        sequence=state["provider_sequence"],
        trade_status=SimpleNamespace(name=state["trade_status"]),
        trade_session=SimpleNamespace(name=state["trade_session"]),
    )
    assert market_state_frame(state["symbol"], state_push) == state

    trades = FIXTURE["cases"][2]["raw"]
    trade_push = SimpleNamespace(
        trades=[
            SimpleNamespace(
                timestamp=_time(value["timestamp"]),
                price=value["price"],
                volume=value["volume"],
                direction=value["direction"],
                trade_session=value["trade_session"],
            )
            for value in trades["trades"]
        ]
    )
    assert trades_frame(trades["symbol"], trade_push) == trades


def test_bridge_bytes_are_canonical_bounded_and_credential_free() -> None:
    frame = FIXTURE["cases"][2]["raw"]
    encoded = canonical_frame_bytes(frame)
    assert encoded == json.dumps(
        frame,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    assert BRIDGE_VERSION.encode() in encoded
    assert b"app_key" not in encoded
    assert b"app_secret" not in encoded
    assert b"access_token" not in encoded


def test_bridge_rejects_hidden_depth_truncation_and_naive_time() -> None:
    levels = [SimpleNamespace(price="100", volume="1")] * 2
    with pytest.raises(ValueError, match="at most one"):
        depth_frame(
            "AAPL.US",
            SimpleNamespace(sequence=1, bids=levels, asks=[]),
            observed_at=_time("2026-07-23T01:00:00Z"),
        )
    with pytest.raises(ValueError, match="timezone"):
        depth_frame(
            "AAPL.US",
            SimpleNamespace(sequence=1, bids=[], asks=[]),
            observed_at=datetime(2026, 7, 23, 1, 0, 0),
        )
