"""One bounded REST Kline repair per interval; no trade reconstruction or retries."""
import asyncio
import hashlib
from datetime import datetime, timezone

from .btc_continuity import Continuity
from .btc_hourly_dataset import canonical


class KlineBackfill:
    def __init__(self, publish, *, maximum_bars=1000, timeout=15, maximum_bytes=1048576, completed=None):
        if not 1 <= maximum_bars <= 1000 or timeout <= 0 or maximum_bytes <= 0:
            raise ValueError("invalid_backfill_budget")
        self.publish = publish
        self.maximum_bars, self.timeout, self.maximum_bytes = maximum_bars, timeout, maximum_bytes
        self.tasks = {}
        self.ranges = {}
        self.pending = {}
        self.attempted = {}  # Fixed two-interval budget, not an unbounded retry ledger.
        self.completed = completed
        self.closed = False

    def submit(self, interval, gap, fetch):
        if self.closed or interval not in ("1m", "1h"):
            return False
        step = 60000 if interval == "1m" else 3600000
        start, end = gap["start"], gap["end_exclusive"]
        if self.attempted.get(interval) == (start, end):
            return False
        if start < 0 or start % step or end % step or not 0 < (end-start)//step <= self.maximum_bars:
            self.attempted[interval] = (start, end)
            self.publish({"event_type": "backfill_rejected", "interval": interval,
                          "gap": gap, "reason": "backfill_capacity"})
            return False
        if interval in self.tasks:
            active_start, active_end = self.ranges[interval]
            if active_start <= start and active_end >= end:
                return False
            previous = self.pending.get(interval)
            if previous:
                start, end = min(start, previous[0]), max(end, previous[1])
            self.pending[interval] = (start, end, fetch)
            return False
        self.attempted[interval] = (start, end)
        task = asyncio.create_task(self._run(interval, start, end, step, fetch))
        self.tasks[interval] = task
        self.ranges[interval] = (start, end)
        task.add_done_callback(lambda _: self._finished(interval))
        return True

    def _finished(self, interval):
        self.tasks.pop(interval, None)
        self.ranges.pop(interval, None)
        next_request = self.pending.pop(interval, None)
        if next_request and not self.closed:
            start, end, fetch = next_request
            self.submit(interval, {"start": start, "end_exclusive": end}, fetch)

    async def _run(self, interval, start, end, step, fetch):
        try:
            rows = await asyncio.wait_for(fetch(interval, start, end-1, (end-start)//step), self.timeout)
            # Adapter-decoded payload, deliberately not described as the original HTTP body.
            if len(rows) != (end-start)//step:
                raise ValueError("incomplete_backfill")
            encoded = canonical(rows)
            if len(encoded) > self.maximum_bytes:
                raise ValueError("backfill_response_budget")
            now = datetime.now(timezone.utc)
            event = int(now.timestamp()*1000)
            validator = Continuity()
            for index, row in enumerate(rows):
                if len(row) != 12 or row[0] != start+index*step:
                    raise ValueError("backfill_order_or_shape")
                k = dict(s="BTCUSDT", i=interval, t=row[0], T=row[6], x=True,
                         o=row[1], h=row[2], l=row[3], c=row[4], v=row[5],
                         q=row[7], n=row[8], V=row[9], Q=row[10])
                validator.observe(dict(e="kline", s="BTCUSDT", E=event, k=k))
            self.publish({"schema_version": "marketcow.btc-research.kline-repair.v1",
                          "event_type": "kline_backfill", "interval": interval,
                          "start_ms": start, "end_exclusive_ms": end, "rows": rows,
                          "payload_sha256": hashlib.sha256(encoded).hexdigest(),
                          "source": "binance_spot_rest_nautilus_decoded",
                          "observed_at": now.isoformat(), "first_received_at": None,
                          "raw_http_available": False, "apply_to_latest": False,
                          "continuity_restored": False})
            if self.completed is not None:
                repaired = self.completed(interval, start, end)
                self.publish({"event_type": "kline_gap_repair_status", "interval": interval,
                              "observed_gap_repaired": repaired,
                              "websocket_continuity_proven": False})
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.publish({"event_type": "backfill_failed", "interval": interval,
                          "start_ms": start, "end_exclusive_ms": end,
                          "reason": type(exc).__name__, "continuity_restored": False})

    async def close(self):
        self.closed = True
        self.pending.clear()
        tasks = list(self.tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
