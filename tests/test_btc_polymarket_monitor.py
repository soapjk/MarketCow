import asyncio
from datetime import datetime, timezone

import pytest

from marketcow.btc_polymarket_monitor import monitor


CONFIG = {
    "live_endpoint": "http://127.0.0.1:8793", "reconnect_seconds": 1,
    "maximum_consecutive_failures": 2,
    "capture": {"seconds": 1, "maximum_frames": 1, "maximum_total_bytes": 1,
                "maximum_frame_bytes": 1, "maximum_fullsync_bytes": 1},
}


class Registry:
    def plan(self, now, maximum_subscriptions):
        return {"subscribe_proposal": ["1"]}

    def binding(self, market):
        return {"market_id": market}


class Operations:
    def __init__(self):
        self.calls = 0

    def status(self, pool):
        self.calls += 1
        return {"schema_version": "marketcow.hot-scope-status.v1", "pool": pool,
                "actual": {"scope_id": "scope-" + str(self.calls)}}


def test_each_session_reads_scope_and_installs_fresh_baseline():
    async def run():
        operations, facts, stop = Operations(), [], asyncio.Event()

        async def capture(endpoint, scope, bindings, publish, **budgets):
            if operations.calls == 2:
                stop.set()
            return {"scope_id": scope, "ready_observed": True,
                    "stream_instance_id": "instance-" + str(operations.calls), "last_cursor": 10}

        await monitor(CONFIG, operations, Registry(), facts.append, stop,
                      capture_function=capture,
                      now_provider=lambda: datetime(2026, 9, 10, tzinfo=timezone.utc))
        assert operations.calls == 2
        assert [row["scope_id"] for row in facts] == ["scope-1", "scope-2"]
    asyncio.run(run())


def test_disconnects_are_bounded_and_never_reuse_status():
    async def run():
        operations, facts = Operations(), []

        async def fail(*args, **kwargs):
            raise ConnectionError("synthetic")

        with pytest.raises(RuntimeError, match="failure_budget"):
            await monitor(CONFIG, operations, Registry(), facts.append, asyncio.Event(),
                          capture_function=fail)
        assert operations.calls == 2
        assert [row["consecutive_failures"] for row in facts] == [1, 2]
    asyncio.run(run())


def test_stop_cancels_active_capture_without_counting_failure():
    async def run():
        operations, facts, stop = Operations(), [], asyncio.Event()
        cancelled = asyncio.Event()

        async def blocked(*args, **kwargs):
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        task = asyncio.create_task(monitor(CONFIG, operations, Registry(), facts.append, stop,
                                           capture_function=blocked))
        while operations.calls == 0:
            await asyncio.sleep(0)
        stop.set()
        await asyncio.wait_for(task, 1)
        assert cancelled.is_set() and facts == []
    asyncio.run(run())
