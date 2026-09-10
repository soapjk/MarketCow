import asyncio
import base64
import hashlib
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from marketcow.btc_discovery import discover, hour_queries, monitor_discovery
from marketcow.btc_hourly_dataset import canonical
from marketcow.btc_polymarket_binding import BINANCE_HOUR_RULE


def test_utc_hours_and_dst_not_invented():
    queries = hour_queries(datetime(2026, 9, 10, 21, 20, tzinfo=timezone.utc))
    assert queries[0]["slug"] == "bitcoin-up-or-down-september-10-2026-5pm-et"
    assert queries[2]["proposed_start_utc"].startswith("2026-09-10T23:00")
    assert not queries[0]["hour_binding_verified"]
    with pytest.raises(ValueError, match="ambiguous_dst"):
        hour_queries(datetime(2026, 11, 1, 5, tzinfo=timezone.utc))


def test_three_bounded_queries_retain_identity_and_evidence():
    requests, facts = [], []
    def handle(request):
        requests.append(request)
        identity = str(len(requests))
        condition = "0x" + identity * 64
        start = datetime(2026, 9, 10, 20+len(requests), tzinfo=timezone.utc)
        raw = canonical(dict(slug=request.url.params["slug"], id=identity, conditionId=condition,
            eventStartTime=start.isoformat().replace("+00:00", "Z"),
            endDate=(start+timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
            description=BINANCE_HOUR_RULE, resolutionSource="https://www.binance.com/en/trade/BTC_USDT",
            clobTokenIds=f'["{identity}0", "{identity}1"]', outcomes='["Up", "Down"]'))
        return httpx.Response(200, json=dict(schema_version="marketcow.polymarket.market-evidence.v1",
            market_id=identity, condition_id=condition, raw_complete=True, raw_bytes=len(raw),
            raw_sha256=hashlib.sha256(raw).hexdigest(), raw_base64=base64.b64encode(raw).decode(),
            observed_at="2026-09-10T20:00:00Z",
            outcomes=[dict(token_id=identity+"0", outcome="Up"), dict(token_id=identity+"1", outcome="Down")]))
    values = asyncio.run(discover("http://127.0.0.1:8793", datetime(2026, 9, 10, 21, tzinfo=timezone.utc),
        facts.append, maximum_bytes=8192, seconds=5, transport=httpx.MockTransport(handle)))
    assert len(values) == len(requests) == 3 and len(facts) == 6
    assert all(not f["execution_eligible"] for f in facts)
    assert all(v["review"]["status"] == "approved" for v in values)
    assert [v["binding"]["market_id"] for v in values] == ["1", "2", "3"]


def test_http_failure_stops_without_next_lookup():
    calls = []
    def handle(request):
        calls.append(request)
        return httpx.Response(403)
    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(discover("http://127.0.0.1:8793", datetime.now(timezone.utc), lambda _: None,
            maximum_bytes=8192, seconds=5, transport=httpx.MockTransport(handle)))
    assert len(calls) == 1


def test_monitor_stops_after_inflight_round_without_next_round():
    async def run():
        stop, facts, calls = asyncio.Event(), [], []
        async def one(*args, **kwargs):
            calls.append(1)
            stop.set()
            return []
        await monitor_discovery("http://127.0.0.1", facts.append, maximum_bytes=8192, seconds=5,
                                interval_seconds=60, maximum_consecutive_failures=2,
                                stop=stop, discover_function=one)
        assert len(calls) == len(facts) == 1
        assert facts[0]["activation_performed"] is False
    asyncio.run(run())


def test_monitor_failure_budget_is_terminal_not_infinite_retry():
    async def run():
        calls, facts = [], []
        async def fail(*args, **kwargs):
            calls.append(1)
            raise ValueError("synthetic")
        with pytest.raises(RuntimeError, match="failure_budget"):
            await monitor_discovery("http://127.0.0.1", facts.append, maximum_bytes=8192, seconds=5,
                                    interval_seconds=60, maximum_consecutive_failures=1,
                                    stop=asyncio.Event(), discover_function=fail)
        assert len(calls) == 1 and facts[0]["consecutive_failures"] == 1
    asyncio.run(run())


def test_monitor_registers_only_after_complete_reviewed_round():
    class Registry:
        def __init__(self):
            self.rows = []

        def register(self, bindings):
            self.rows.append(bindings)

    async def run():
        stop = asyncio.Event()
        registry = Registry()

        async def complete(*args, **kwargs):
            stop.set()
            return [{"binding": {"market_id": "1"}}, {"binding": {"market_id": "2"}}]

        await monitor_discovery("http://127.0.0.1", lambda _: None,
                                maximum_bytes=8192, seconds=5, interval_seconds=60,
                                maximum_consecutive_failures=1, stop=stop,
                                discover_function=complete, registry=registry)
        assert registry.rows == [[{"market_id": "1"}, {"market_id": "2"}]]

    asyncio.run(run())
