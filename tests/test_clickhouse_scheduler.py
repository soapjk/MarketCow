import threading
import time
import tempfile
import unittest
from pathlib import Path

from marketcow.clickhouse_scheduler import BackgroundCanonicalScheduler
from marketcow.clickhouse_scheduler import create_canonical_scheduler
from marketcow.clickhouse_writer import ReliableClickHouseWriter
from marketcow.clickhouse_writer import LocalClickHouseSpool


ROWS = [{
    "symbol": "MU", "interval": "1m", "adjustment": "raw",
    "bar_time": "2026-07-20T01:00:00Z",
}]


class Clock:
    def __init__(self, value=1000.0):
        self.value = value

    def __call__(self):
        return self.value


class Builder:
    def __init__(self, failures=0, block=None):
        self.failures = failures
        self.block = block
        self.calls = []

    def rebuild(self, *args):
        self.calls.append(args)
        if self.block:
            self.block.wait(2)
        if self.failures:
            self.failures -= 1
            raise ConnectionError("clickhouse unavailable")
        return {"status": "ok", "written": 1, "spooled": 0}


class RawRepository:
    def __init__(self, fail_calls=(), short_calls=()):
        self.calls = []
        self.fail_calls = set(fail_calls)
        self.short_calls = set(short_calls)

    def insert_raw_bars(self, rows, batch_id=""):
        self.calls.append((rows, batch_id))
        if len(self.calls) in self.fail_calls:
            raise ConnectionError("clickhouse unavailable")
        if len(self.calls) in self.short_calls:
            return len(rows) - 1
        return len(rows)

    def insert_canonical_bars(self, rows, batch_id=""):
        return len(rows)


def raw_row(index: int) -> dict:
    minute = index % 60
    return {
        "symbol": "MU", "market": "CN", "interval": "1m",
        "adjustment": "raw", "bar_time": f"2026-07-20T01:{minute:02d}:00Z",
        "open": 10, "high": 11, "low": 9, "close": 10,
        "raw_close": None, "adjustment_factor": None, "volume": 100,
        "amount": None, "source": "fixture", "source_sequence": str(index),
        "observed_at": "2026-07-20T02:00:00Z",
        "ingested_at": "2026-07-20T02:00:01Z", "raw_artifact_id": None,
    }


def wait_until(predicate, seconds=2):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


class BackgroundCanonicalSchedulerTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.root = Path(self.folder.name)
        self.spool = LocalClickHouseSpool(self.root / "spool", self.root)
        self.schedulers = []

    def tearDown(self):
        for scheduler in reversed(self.schedulers):
            try:
                scheduler.close()
            except (RuntimeError, ValueError):
                pass
        self.folder.cleanup()

    def scheduler(self, builder=None, **kwargs):
        scheduler = BackgroundCanonicalScheduler(
            builder or Builder(), self.spool, poll_seconds=0.05, **kwargs
        )
        self.schedulers.append(scheduler)
        return scheduler

    def test_disabled_factory_has_no_thread_or_directory_side_effect(self):
        absent = self.root / "disabled-spool"
        before = {thread.name for thread in threading.enumerate()}
        self.assertIsNone(create_canonical_scheduler(False, invalid="never evaluated"))
        self.assertFalse(absent.exists())
        self.assertEqual(before, {thread.name for thread in threading.enumerate()})

    def test_sync_multichunk_commit_enqueues_one_exact_range_after_evidence(self):
        scheduler = self.scheduler(Builder(), start_paused=True)
        writer = ReliableClickHouseWriter(RawRepository(), self.spool, 1000)
        scheduler.bind_writer(writer)
        rows = [raw_row(index) for index in range(2501)]
        result = writer.write("raw", rows)
        self.assertEqual(result["status"], "success")
        self.assertTrue(result["acknowledged"])
        tasks = scheduler._files(scheduler.pending, 10)
        self.assertEqual(len(tasks), 1)
        task = self.spool.read(tasks[0], require_checksum=True)
        self.assertEqual(
            (task["symbol"], task["interval"], task["adjustment"]),
            ("MU", "1m", "raw"),
        )
        self.assertEqual(task["start"], "2026-07-20T01:00:00.000+00:00")
        self.assertEqual(task["end"], "2026-07-20T01:59:00.000+00:00")

    def test_partial_raw_never_enqueues_until_replay_evidence_is_complete(self):
        scheduler = self.scheduler(Builder(), start_paused=True)
        repository = RawRepository(fail_calls={2})
        writer = ReliableClickHouseWriter(repository, self.spool, 1000)
        scheduler.bind_writer(writer)
        rows = [raw_row(index) for index in range(2001)]
        result = writer.write("raw", rows)
        self.assertEqual(result["status"], "durable_pending")
        self.assertEqual(scheduler._files(scheduler.pending, 10), [])
        writer.replay(10)
        self.assertEqual(len(scheduler._files(scheduler.pending, 10)), 1)

    def test_short_acknowledgement_never_enqueues_canonical(self):
        scheduler = self.scheduler(Builder(), start_paused=True)
        writer = ReliableClickHouseWriter(
            RawRepository(short_calls={1}), self.spool, 1000
        )
        scheduler.bind_writer(writer)
        result = writer.write("raw", [raw_row(0)])
        self.assertEqual(result["status"], "durable_pending")
        self.assertEqual(scheduler._files(scheduler.pending, 10), [])

    def test_queue_failure_keeps_raw_intent_for_bounded_retry(self):
        scheduler = self.scheduler(Builder(), queue_cap=1, start_paused=True)
        scheduler.enqueue_rows([{**ROWS[0], "symbol": "FULL"}])
        writer = ReliableClickHouseWriter(RawRepository(), self.spool, 1000)
        scheduler.bind_writer(writer)
        result = writer.write("raw", [raw_row(0)])
        self.assertEqual(result["status"], "canonical_intent_pending")
        self.assertFalse(result["acknowledged"])
        self.assertEqual(len(list(self.spool.intents.glob("*.json"))), 1)
        scheduler._paused.clear()
        self.assertEqual(scheduler.run_once(), 1)
        writer.replay(1)
        self.assertEqual(len(list(self.spool.intents.glob("*.json"))), 0)
        self.assertEqual(len(scheduler._files(scheduler.pending, 10)), 1)
    def test_durable_enqueue_dedup_pause_resume_and_exact_range(self):
        builder = Builder()
        scheduler = self.scheduler(builder)
        scheduler.pause()
        rows = [ROWS[0], {**ROWS[0], "bar_time": "2026-07-20T01:05:00Z"}]
        first = scheduler.enqueue_rows(rows)
        second = scheduler.enqueue_rows(rows)
        self.assertEqual((first["accepted"], second["duplicate"]), (1, 1))
        time.sleep(0.08)
        self.assertEqual(builder.calls, [])
        scheduler.resume()
        self.assertTrue(wait_until(lambda: len(builder.calls) == 1))
        self.assertEqual(builder.calls[0][:5], (
            "MU", "1m", "raw", "2026-07-20T01:00:00Z",
            "2026-07-20T01:05:00Z",
        ))
        self.assertEqual(scheduler.diagnostics()["pending"], 0)

    def test_single_instance_lease_and_reverse_shutdown(self):
        scheduler = self.scheduler()
        with self.assertRaisesRegex(RuntimeError, "lease"):
            BackgroundCanonicalScheduler(Builder(), self.spool, poll_seconds=0.05)
        scheduler.close()
        self.schedulers.remove(scheduler)
        replacement = self.scheduler()
        self.assertTrue(replacement.diagnostics()["thread_alive"])
        replacement.close()
        self.schedulers.remove(replacement)
        self.assertFalse(replacement._thread.is_alive())

    def test_concurrent_duplicate_enqueue_stays_single_and_bounded(self):
        scheduler = self.scheduler(Builder(), queue_cap=1)
        scheduler.pause()
        barrier = threading.Barrier(9)
        outcomes = []

        def enqueue():
            barrier.wait()
            outcomes.append(scheduler.enqueue_rows(ROWS))

        threads = [threading.Thread(target=enqueue) for _ in range(8)]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join()
        self.assertEqual(sum(item["accepted"] for item in outcomes), 1)
        self.assertEqual(sum(item["duplicate"] for item in outcomes), 7)
        self.assertEqual(scheduler.diagnostics()["pending"], 1)

    def test_backoff_is_clock_controlled_bounded_and_recovers(self):
        clock, builder = Clock(), Builder(failures=1)
        scheduler = self.scheduler(
            builder, clock=clock, backoff_base_seconds=10,
            backoff_max_seconds=20, max_attempts=3, start_paused=True,
        )
        scheduler.enqueue_rows(ROWS)
        scheduler._paused.clear()
        self.assertEqual(scheduler.run_once(), 1)
        self.assertEqual(len(builder.calls), 1)
        self.assertEqual(scheduler.run_once(), 0)
        clock.value += 9
        self.assertEqual(scheduler.run_once(), 0)
        clock.value += 1
        self.assertEqual(scheduler.run_once(), 1)
        self.assertEqual(len(builder.calls), 2)
        self.assertEqual(scheduler.diagnostics()["pending"], 0)

    def test_backlog_cap_scan_window_and_fail_open_diagnostics(self):
        scheduler = self.scheduler(Builder(), queue_cap=2, scan_limit=1)
        scheduler.pause()
        outcomes = [scheduler.enqueue_rows([{
            **ROWS[0], "symbol": symbol,
        }]) for symbol in ("A", "B", "C")]
        self.assertEqual([item["accepted"] for item in outcomes], [1, 1, 0])
        self.assertEqual(outcomes[-1]["status"], "full")
        with self.assertRaisesRegex(RuntimeError, "queue is full"):
            scheduler.enqueue_replayed_rows([{
                **ROWS[0], "symbol": "REPLAY-MUST-STAY-DURABLE",
            }])
        self.assertEqual(scheduler.diagnostics()["pending"], 2)
        scheduler._paused.clear()
        self.assertEqual(scheduler.run_once(), 1)
        self.assertEqual(scheduler.run_once(), 1)

    def test_startup_recovers_claimed_crash_window_and_short_soak(self):
        scheduler_root = self.spool.root / "canonical-scheduler"
        processing = scheduler_root / "processing"
        processing.mkdir(parents=True)
        task = {"task_id": "crash", "symbol": "MU", "interval": "1m",
                "adjustment": "raw", "start": "2026-07-20T01:00:00Z",
                "end": "2026-07-20T01:00:00Z", "attempts": 0,
                "created_at_epoch": time.time(), "next_attempt_epoch": 0,
                "last_error": ""}
        self.spool._atomic_json(processing / "crash.json", task)
        builder = Builder()
        scheduler = self.scheduler(builder, queue_cap=200, scan_limit=25)
        self.assertTrue(wait_until(lambda: any(call[0] == "MU" for call in builder.calls)))
        for index in range(100):
            scheduler.enqueue_rows([{**ROWS[0], "symbol": f"SOAK-{index:03d}"}])
        self.assertTrue(wait_until(lambda: scheduler.diagnostics()["pending"] == 0, 5))
        self.assertEqual(len(builder.calls), 101)


if __name__ == "__main__":
    unittest.main()
