import asyncio

from marketcow.btc_backfill import KlineBackfill


def row(t):
    return [t, "10", "12", "9", "11", "1", t+59999, "10", 1, "0", "0", "0"]


def test_repair_is_supplement_not_live_rollback():
    async def run():
        output = []
        worker = KlineBackfill(output.append)
        async def fetch(interval, start, end, limit):
            assert (interval, start, end, limit) == ("1m", 0, 119999, 2)
            return [row(0), row(60000)]
        assert worker.submit("1m", dict(start=0, end_exclusive=120000), fetch)
        assert not worker.submit("1m", dict(start=0, end_exclusive=120000), fetch)
        await asyncio.gather(*worker.tasks.values())
        assert output[0]["event_type"] == "kline_backfill"
        assert output[0]["apply_to_latest"] is False
        assert output[0]["first_received_at"] is None
        assert output[0]["raw_http_available"] is False
    asyncio.run(run())


def test_incomplete_and_timeout_fail_no_retry():
    async def run():
        for timeout in (False, True):
            output, calls = [], []
            worker = KlineBackfill(output.append, timeout=.01)
            async def fetch(*args):
                calls.append(args)
                if timeout:
                    await asyncio.sleep(1)
                return []
            worker.submit("1m", dict(start=0, end_exclusive=60000), fetch)
            await asyncio.gather(*worker.tasks.values())
            assert len(calls) == 1 and output[0]["event_type"] == "backfill_failed"
            for _ in range(100):
                assert not worker.submit("1m", dict(start=0, end_exclusive=60000), fetch)
            assert len(calls) == 1
    asyncio.run(run())


def test_new_gap_during_inflight_is_bounded_and_processed():
    async def run():
        output, calls, completed = [], [], []
        gate = asyncio.Event()
        worker = KlineBackfill(output.append, completed=lambda *args: completed.append(args) or True)
        async def fetch(interval, start, end, limit):
            calls.append((start, end))
            await gate.wait()
            return [row(t) for t in range(start, end+1, 60000)]
        worker.submit("1m", dict(start=0, end_exclusive=60000), fetch)
        for _ in range(100):
            worker.submit("1m", dict(start=0, end_exclusive=120000), fetch)
        assert len(worker.tasks) == len(worker.pending) == 1
        gate.set()
        while worker.tasks:
            await asyncio.gather(*list(worker.tasks.values()))
            await asyncio.sleep(0)
        assert calls == [(0, 59999), (0, 119999)]
        assert len(completed) == 2
        await worker.close()
        assert not worker.submit("1m", dict(start=0, end_exclusive=60000), fetch)
    asyncio.run(run())
