import tempfile
import threading
import unittest
from pathlib import Path

from marketcow.clickhouse_writer import (
    LocalClickHouseSpool,
    ReliableClickHouseWriter,
    normalize_bar,
    stable_batch_id,
)


def raw_bar(index=0):
    return {
        "symbol": f"{index:06d}.SH", "market": "CN", "interval": "1m",
        "adjustment": "raw", "bar_time": "2026-07-20T01:31:00Z",
        "open": "100", "high": 102, "low": 99, "close": 101,
        "volume": 1000, "amount": None, "source": "fixture",
        "source_sequence": str(index), "observed_at": "2026-07-20T09:31:01+08:00",
        "ingested_at": "2026-07-20T01:31:02Z", "raw_artifact_id": None,
    }


class FakeRepository:
    def __init__(self, fail=True):
        self.fail = fail
        self.calls = []

    def insert_raw_bars(self, rows, batch_id=""):
        self.calls.append((rows, batch_id))
        if self.fail:
            raise ConnectionError("fixture unavailable")
        return len(rows)

    def insert_canonical_bars(self, rows, batch_id=""):
        return self.insert_raw_bars(rows, batch_id)


class ReliableClickHouseWriterTest(unittest.TestCase):
    def test_replay_quarantines_corrupt_wal_and_continues_healthy_items(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            spool = LocalClickHouseSpool(root / "spool", root)
            corrupt = spool.pending / "000-corrupt.json"
            corrupt.write_text("{", encoding="utf-8")
            spool.enqueue("raw", "healthy", [normalize_bar("raw", raw_bar())], "outage")
            writer = ReliableClickHouseWriter(FakeRepository(False), spool, 1000)
            result = writer.replay(10)
            self.assertEqual(result["quarantined"], 1)
            self.assertEqual(result["replayed"], 1)
            self.assertFalse(corrupt.exists())
            self.assertTrue(any(spool.quarantine.glob("*000-corrupt.json")))

    def test_replay_shared_budget_and_concurrent_claim_lock(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            spool = LocalClickHouseSpool(root / "spool", root)
            rows = [normalize_bar("raw", raw_bar())]
            spool.save_intent("ready", rows, [])
            spool.enqueue("canonical", "wal", [], "pending")
            writer = ReliableClickHouseWriter(FakeRepository(False), spool, 1000)
            callbacks = []
            writer.on_raw_replayed = lambda value: callbacks.append(value)
            first = writer.replay(limit=1)
            self.assertEqual(first["attempted"] + first["callback_attempted"], 1)
            self.assertEqual(callbacks, [])
            self.assertTrue(first["truncated"])
            second = writer.replay(limit=1)
            self.assertEqual(second["callback_attempted"], 1)
            self.assertEqual(len(callbacks), 1)

            spool.save_intent("blocked", rows, [])
            entered, release = threading.Event(), threading.Event()
            first_writer = ReliableClickHouseWriter(FakeRepository(False), spool, 1000)
            second_writer = ReliableClickHouseWriter(FakeRepository(False), spool, 1000)
            calls = []
            def blocking(value):
                calls.append(value)
                entered.set()
                release.wait(5)
            first_writer.on_raw_replayed = blocking
            second_writer.on_raw_replayed = lambda value: calls.append(value)
            thread = threading.Thread(target=first_writer.replay)
            thread.start()
            self.assertTrue(entered.wait(2))
            busy = second_writer.replay()
            self.assertTrue(busy["lock_busy"])
            self.assertEqual(len(calls), 1)
            release.set()
            thread.join(5)
            self.assertEqual(len(calls), 1)
            self.assertFalse((spool.intents / "blocked.json").exists())
    def test_partial_chunks_rebuild_only_after_complete_intent_and_report_callback_error(self):
        class Partial(FakeRepository):
            def insert_raw_bars(self, rows, batch_id=""):
                self.calls.append((rows, batch_id))
                if len(self.calls) in {2, 3}:
                    raise ConnectionError("partial")
                return len(rows)
        with tempfile.TemporaryDirectory() as folder:
            repository = Partial(False)
            spool = LocalClickHouseSpool(Path(folder) / "spool", Path(folder))
            writer = ReliableClickHouseWriter(repository, spool, 1000)
            callbacks = []
            writer.on_raw_replayed = lambda rows: callbacks.append(rows)
            result = writer.write("raw", [raw_bar(index) for index in range(2501)])
            self.assertEqual(result["spooled"], 1501)
            self.assertEqual(callbacks, [])
            writer.replay(limit=1)
            self.assertEqual(callbacks, [])
            writer.replay(limit=10)
            self.assertEqual(len(callbacks[0]), 2501)
            self.assertEqual(callbacks[0][0]["symbol"], "000000.SH")
            self.assertEqual(callbacks[0][-1]["symbol"], "002500.SH")
            repository.calls = []
            writer.write("raw", [raw_bar(index) for index in range(2501)])
            writer.on_raw_replayed = lambda rows: (_ for _ in ()).throw(RuntimeError("callback boom"))
            writer.replay(limit=10)
            self.assertEqual(writer.last_replay_callback["status"], "error")
            intents = list(spool.intents.glob("*.json"))
            self.assertTrue(any("callback boom" in spool.read(path).get("last_callback_error", "")
                                for path in intents))
            recovered = []
            restarted = ReliableClickHouseWriter(repository, spool, 1000)
            restarted.on_raw_replayed = lambda rows: recovered.append(rows)
            restarted.replay(limit=10)
            self.assertEqual(len(recovered[0]), 2501)
            self.assertEqual(list(spool.intents.glob("*.json")), [])
            restarted.replay(limit=10)
            self.assertEqual(len(recovered), 1)

    def test_intent_recovers_crash_after_wal_moved_before_completion(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            spool = LocalClickHouseSpool(root / "spool", root)
            writer = ReliableClickHouseWriter(FakeRepository(True), spool, 1000)
            writer.write("raw", [raw_bar()])
            wal = next(spool.pending.glob("*.json"))
            payload = spool.read(wal)
            spool.mark_replayed(wal, payload)  # simulate crash before complete_chunk
            recovered = []
            restarted = ReliableClickHouseWriter(FakeRepository(False), spool, 1000)
            restarted.on_raw_replayed = lambda rows: recovered.append(rows)
            restarted.replay()
            self.assertEqual(len(recovered), 1)
            self.assertEqual(list(spool.intents.glob("*.json")), [])
    def test_normalization_empty_batch_and_stable_identity(self):
        normalized = normalize_bar("raw", raw_bar())
        self.assertEqual(normalized["open"], 100.0)
        self.assertEqual(normalized["observed_at"], "2026-07-20T01:31:01.000+00:00")
        self.assertEqual(stable_batch_id("raw", [normalized]),
                         stable_batch_id("raw", [normalized]))
        with tempfile.TemporaryDirectory() as folder:
            writer = ReliableClickHouseWriter(
                FakeRepository(False),
                LocalClickHouseSpool(Path(folder) / "spool", Path(folder)), 1000,
            )
            self.assertEqual(writer.write("raw", []),
                             {"rows": 0, "written": 0, "spooled": 0, "batches": 0})
            canonical = {**raw_bar(), "selected_source": "fixture", "source_count": 1,
                         "quality_status": "single_source", "input_fingerprint": "abc",
                         "version": 1,
                         "updated_at": "2026-07-20T01:31:03Z"}
            self.assertEqual(writer.write("canonical", [canonical])["written"], 1)
        with self.assertRaisesRegex(ValueError, "requires close"):
            normalize_bar("raw", {**raw_bar(), "close": None})

    def test_micro_batches_spool_atomically_and_replay_after_recovery(self):
        with tempfile.TemporaryDirectory() as folder:
            repository = FakeRepository(True)
            spool = LocalClickHouseSpool(Path(folder) / "spool", Path(folder))
            writer = ReliableClickHouseWriter(repository, spool, 1000)
            result = writer.write("raw", [raw_bar(index) for index in range(2001)])
            self.assertEqual(result, {"rows": 2001, "written": 0,
                                      "spooled": 2001, "batches": 3})
            self.assertEqual(len(list(spool.pending.glob("*.json"))), 3)
            self.assertEqual(list(spool.pending.glob(".write-*")), [])
            diagnostics = spool.diagnostics(limit=2)
            self.assertEqual(diagnostics["pending"], 2)
            self.assertEqual(diagnostics["failed"], 2)
            self.assertTrue(diagnostics["truncated"])
            self.assertGreaterEqual(diagnostics["oldest_pending_lag_seconds"], 0)

            repository.fail = False
            replayed = writer.replay(limit=10)
            self.assertEqual({key: replayed[key] for key in ("attempted", "replayed", "failed")},
                             {"attempted": 3, "replayed": 3, "failed": 0})
            self.assertEqual(list(spool.pending.glob("*.json")), [])
            self.assertEqual(len(list(spool.replayed.glob("*.json"))), 3)
            replayed = writer.replay(limit=10)
            self.assertEqual({key: replayed[key] for key in ("attempted", "replayed", "failed")},
                             {"attempted": 0, "replayed": 0, "failed": 0})
            self.assertEqual(spool.diagnostics()["replayed"], 3)

    def test_failed_replay_increments_attempts_without_duplicate_wal(self):
        with tempfile.TemporaryDirectory() as folder:
            spool = LocalClickHouseSpool(Path(folder) / "spool", Path(folder))
            writer = ReliableClickHouseWriter(FakeRepository(True), spool, 1000)
            writer.write("raw", [raw_bar()])
            writer.replay()
            paths = list(spool.pending.glob("*.json"))
            self.assertEqual(len(paths), 1)
            self.assertEqual(spool.read(paths[0])["attempts"], 2)

    def test_allowed_root_rejects_formal_path_traversal_and_symlink_escape(self):
        formal = Path("/Volumes/T9/projects/market-data-service/data/spool/clickhouse")
        with tempfile.TemporaryDirectory() as folder:
            base = Path(folder)
            development = base / "data-development"
            outside = base / "outside"
            development.mkdir()
            outside.mkdir()
            with self.assertRaisesRegex(ValueError, "allowed development root"):
                LocalClickHouseSpool(formal, development)
            with self.assertRaisesRegex(ValueError, "allowed development root"):
                LocalClickHouseSpool(development / "../outside/spool", development)
            link = development / "linked-outside"
            link.symlink_to(outside, target_is_directory=True)
            with self.assertRaisesRegex(ValueError, "allowed development root"):
                LocalClickHouseSpool(link / "spool", development)
            allowed = LocalClickHouseSpool(development / "spool/clickhouse", development)
            self.assertEqual(allowed.allowed_root, development.resolve())


if __name__ == "__main__":
    unittest.main()
