import tempfile
import threading
import time
import unittest
import json
from pathlib import Path
from unittest.mock import patch

from marketcow.clickhouse_scheduler import BackgroundCanonicalScheduler
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


def wait_until(predicate, seconds=2):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


class BackgroundCanonicalSchedulerTest(unittest.TestCase):
    def test_startup_migrates_legacy_scheduler_intent_before_recovery(self):
        builder = Builder()
        group = {"symbol": "MU", "interval": "1m", "adjustment": "raw",
                 "start": "2026-07-20T01:00:00Z", "end": "2026-07-20T01:00:00Z"}
        task_id = BackgroundCanonicalScheduler._task_id(group)
        processing = self.spool.root / "canonical-scheduler/processing"
        processing.mkdir(parents=True)
        legacy = processing / f"{task_id}.json"
        legacy.write_text(json.dumps({
            "task_id": task_id, **group, "attempts": 0,
            "created_at_epoch": 1000.0, "next_attempt_epoch": 1000.0,
            "last_error": "",
        }), encoding="utf-8")
        scheduler = self.scheduler(builder, clock=Clock(), start_paused=True)
        pending = scheduler.pending / legacy.name
        self.assertIn("_checksum", self.spool.read(pending, require_checksum=True))
        scheduler.resume()
        self.assertTrue(wait_until(lambda: len(builder.calls) == 1))

    def test_startup_preserves_legacy_intent_when_signing_fails_then_recovers(self):
        builder = Builder()
        group = {"symbol": "MU", "interval": "1m", "adjustment": "raw",
                 "start": "2026-07-20T01:00:00Z", "end": "2026-07-20T01:00:00Z"}
        task_id = BackgroundCanonicalScheduler._task_id(group)
        processing = self.spool.root / "canonical-scheduler/processing"
        processing.mkdir(parents=True)
        legacy = processing / f"{task_id}.json"
        original = json.dumps({
            "task_id": task_id, **group, "attempts": 0,
            "created_at_epoch": 1000.0, "next_attempt_epoch": 1000.0,
            "last_error": "",
        })
        legacy.write_text(original, encoding="utf-8")
        real_atomic = self.spool._atomic_json

        def deny_signing(target, payload):
            if target == legacy and "_checksum" not in payload:
                raise PermissionError("denied")
            return real_atomic(target, payload)

        with patch.object(self.spool, "_atomic_json", side_effect=deny_signing):
            scheduler = self.scheduler(builder, clock=Clock(), start_paused=True)
        self.assertTrue(legacy.exists())
        self.assertEqual(legacy.read_text(encoding="utf-8"), original)
        self.assertEqual(scheduler.diagnostics()["legacy_migration"]["remaining"], 1)
        self.assertEqual(builder.calls, [])

        scheduler._paused.clear()
        self.assertEqual(scheduler.run_once(), 1)
        self.assertEqual(len(builder.calls), 1)
        self.assertFalse(legacy.exists())

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
