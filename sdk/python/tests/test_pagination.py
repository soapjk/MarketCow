from __future__ import annotations

import httpx
import pytest

from marketcow_sdk import MarketCowClient, SnapshotChangedError


def page(snapshot: str, *, cursor: str | None) -> dict:
    start = "2026-01-01T00:00:00Z"
    end = "2026-01-03T00:00:00Z"
    window = "2026-01-01T00:00:00Z" if cursor is None else "2026-01-02T00:00:00Z"
    return {
        "manifest": {
            "dataset_id": "dataset",
            "snapshot_id": snapshot,
            "canonical_version": "v1",
            "instruments": ["AAPL.XNAS"],
            "interval": "1-DAY",
            "adjustment": "raw",
            "start": start,
            "end": end,
            "row_count": 2,
            "content_hash": "sha256:" + "a" * 64,
        },
        "count": 1,
        "bars": [
            {
                "instrument_id": "AAPL.XNAS",
                "interval": "1-DAY",
                "adjustment": "raw",
                "window_start": window,
                "window_end": "2026-01-02T00:00:00Z"
                if cursor is None
                else "2026-01-03T00:00:00Z",
                "ts_event": "2026-01-02T00:00:00Z"
                if cursor is None
                else "2026-01-03T00:00:00Z",
                "ts_init": "2026-01-03T01:00:00Z",
                "open": "1.1",
                "high": "1.2",
                "low": "1.0",
                "close": "1.15",
                "volume": "100",
                "selected_source": "longport",
                "quality_status": "single_source_unverified",
                "row_version": "1",
            }
        ],
        "page_size": 1,
        "next_cursor": "next" if cursor is None else None,
        "truncated": cursor is None,
        "provenance": {"layer": "canonical", "backend": "clickhouse"},
    }


def test_canonical_iterator_follows_cursor_and_preserves_manifest():
    observed = []

    def handler(request: httpx.Request) -> httpx.Response:
        cursor = request.url.params.get("cursor")
        observed.append(cursor)
        return httpx.Response(200, json=page("snapshot-one", cursor=cursor))

    raw = httpx.Client(base_url="https://example.test", transport=httpx.MockTransport(handler))
    client = MarketCowClient("https://example.test", http_client=raw)
    try:
        series = client.bars.iter_canonical(
            "AAPL.XNAS",
            start="2026-01-01T00:00:00Z",
            end="2026-01-03T00:00:00Z",
            interval="1-DAY",
            adjustment="raw",
            page_size=1,
        )
        bars = list(series)
        assert len(bars) == 2
        assert series.manifest.snapshot_id == "snapshot-one"
        assert observed == [None, "next"]
    finally:
        raw.close()


def test_canonical_iterator_rejects_cross_snapshot_concatenation():
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            200,
            json=page(
                "snapshot-one" if calls == 1 else "snapshot-two",
                cursor=request.url.params.get("cursor"),
            ),
        )

    raw = httpx.Client(base_url="https://example.test", transport=httpx.MockTransport(handler))
    client = MarketCowClient("https://example.test", http_client=raw)
    try:
        series = client.bars.iter_canonical(
            "AAPL.XNAS",
            start="2026-01-01T00:00:00Z",
            end="2026-01-03T00:00:00Z",
            interval="1-DAY",
            adjustment="raw",
            page_size=1,
        )
        with pytest.raises(SnapshotChangedError):
            list(series)
    finally:
        raw.close()
