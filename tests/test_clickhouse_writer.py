import tempfile
import threading
import unittest
from pathlib import Path

from marketcow.clickhouse_writer import (
    AuthoritativeWriteError,
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
    def test_telemetry_failure_does_not_change_writer_result(self):
        class BrokenTelemetry:
            def clock(self):
                raise RuntimeError("telemetry clock failed")

            def safe(self, *args, **kwargs):
                raise RuntimeError("telemetry update failed")

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            spool = LocalClickHouseSpool(root / "spool", root)
            spool.telemetry = BrokenTelemetry()
            writer = ReliableClickHouseWriter(FakeRepository(False), spool, 1000)
            self.assertEqual(writer.write("raw", [raw_bar()])["written"], 1)

    def test_replay_migrates_and_recovers_pre_checksum_wal(self):
        import json

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            spool = LocalClickHouseSpool(root / "spool", root)
            rows = [normalize_bar("raw", raw_bar())]
            batch_id = stable_batch_id("raw", rows)
            path = spool.pending / f"{batch_id}.json"
            path.write_text(json.dumps({
                "dataset": "raw", "batch_id": batch_id, "rows": rows, "attempts": 1,
                "created_at": "2026-07-20T00:00:00Z",
                "last_attempt_at": "2026-07-20T00:00:01Z", "last_error": "outage",
            }), encoding="utf-8")
            repository = FakeRepository(False)
            result = ReliableClickHouseWriter(repository, spool, 1000).replay(10)
            self.assertEqual(result["legacy_migrated"], 1)
            self.assertEqual(result["replayed"], 1)
            self.assertEqual(len(repository.calls), 1)
            self.assertTrue((spool.replayed / path.name).exists())

    def test_replay_preserves_legacy_wal_when_signing_fails_then_recovers(self):
        import json
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            spool = LocalClickHouseSpool(root / "spool", root)
            rows = [normalize_bar("raw", raw_bar())]
            batch_id = stable_batch_id("raw", rows)
            path = spool.pending / f"{batch_id}.json"
            original = json.dumps({
                "dataset": "raw", "batch_id": batch_id, "rows": rows, "attempts": 1,
                "created_at": "2026-07-20T00:00:00Z",
                "last_attempt_at": "2026-07-20T00:00:01Z", "last_error": "outage",
            })
            path.write_text(original, encoding="utf-8")
            repository = FakeRepository(False)
            writer = ReliableClickHouseWriter(repository, spool, 1000)
            real_atomic = spool._atomic_json

            def deny_signing(target, payload):
                if target == path and "_checksum" not in payload:
                    raise PermissionError("denied")
                return real_atomic(target, payload)

            with patch.object(spool, "_atomic_json", side_effect=deny_signing):
                blocked = writer.replay(10)
            self.assertEqual(blocked["legacy_errors"], 1)
            self.assertGreaterEqual(blocked["legacy_blocked"], 1)
            self.assertGreaterEqual(blocked["remaining"], 1)
            self.assertTrue(blocked["truncated"])
            self.assertEqual(path.read_text(encoding="utf-8"), original)
            self.assertEqual(repository.calls, [])
            self.assertEqual(list(spool.quarantine.glob("*" + path.name)), [])

            recovered = writer.replay(10)
            self.assertEqual(recovered["legacy_migrated"], 1)
            self.assertEqual(recovered["replayed"], 1)
            self.assertEqual(len(repository.calls), 1)
            self.assertFalse(path.exists())

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
            from marketcow.spool_operator import SpoolOperator
            self.assertEqual(SpoolOperator(spool).audit()["missing_wal_references"], [])
            recovered = []
            restarted = ReliableClickHouseWriter(FakeRepository(False), spool, 1000)
            restarted.on_raw_replayed = lambda rows: recovered.append(rows)
            restarted.replay()
            self.assertEqual(len(recovered), 1)
            self.assertEqual(list(spool.intents.glob("*.json")), [])

    def test_intent_publish_fsync_failure_cannot_create_false_ready_progress(self):
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            spool = LocalClickHouseSpool(root / "spool", root)
            repository = FakeRepository(False)
            writer = ReliableClickHouseWriter(repository, spool, 1000)
            real_atomic = spool._atomic_json

            def fail_after_intent_replace(path, payload):
                real_atomic(path, payload)
                if path.parent == spool.intents:
                    raise OSError("intent directory fsync failed")

            with patch.object(spool, "_atomic_json", side_effect=fail_after_intent_replace):
                with self.assertRaises(AuthoritativeWriteError) as raised:
                    writer.write("raw", [raw_bar()])
            self.assertEqual(raised.exception.outcome["status"], "terminal_failure")
            self.assertEqual(raised.exception.outcome["durability"], "not_durable")
            self.assertEqual(repository.calls, [])
            self.assertEqual(list(spool.pending.glob("*.json")), [])

            callbacks = []
            restarted_repository = FakeRepository(False)
            restarted = ReliableClickHouseWriter(restarted_repository, spool, 1000)
            restarted.on_raw_replayed = lambda rows: callbacks.append(rows)
            replay = restarted.replay()
            self.assertEqual(replay["attempted"], 0)
            self.assertEqual(replay["callback_attempted"], 0)
            self.assertEqual(restarted_repository.calls, [])
            self.assertEqual(callbacks, [])

    def test_missing_pending_without_replayed_evidence_stays_blocked(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            spool = LocalClickHouseSpool(root / "spool", root)
            rows = [normalize_bar("raw", raw_bar())]
            intent_id = stable_batch_id("raw", rows)
            missing_batch = "f" * 64
            spool.save_intent(intent_id, rows, [missing_batch])
            callbacks = []
            writer = ReliableClickHouseWriter(FakeRepository(False), spool, 1000)
            writer.on_raw_replayed = lambda value: callbacks.append(value)
            result = writer.replay()
            self.assertEqual(result["callback_attempted"], 0)
            self.assertTrue(result["truncated"])
            self.assertEqual(callbacks, [])
            intent = spool.read(spool.intents / f"{intent_id}.json")
            self.assertEqual(intent["pending"], [missing_batch])

    def test_preexisting_ready_intent_is_not_reset_or_deleted_by_retry(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            spool = LocalClickHouseSpool(root / "spool", root)
            rows = [normalize_bar("raw", raw_bar())]
            intent_id = stable_batch_id("raw", rows)
            spool.save_intent(intent_id, rows, [])
            path = spool.intents / f"{intent_id}.json"
            intent = spool.read(path)
            intent["callback_attempts"] = 3
            intent["last_callback_error"] = "bounded prior failure"
            spool._atomic_json(path, intent)

            writer = ReliableClickHouseWriter(FakeRepository(False), spool, 1000)
            result = writer.write("raw", [raw_bar()])
            self.assertEqual(result["status"], "success")
            preserved = spool.read(path)
            self.assertEqual(preserved["callback_attempts"], 3)
            self.assertEqual(preserved["last_callback_error"], "bounded prior failure")
            callbacks = []
            writer.on_raw_replayed = lambda value: callbacks.append(value)
            writer.replay()
            self.assertEqual(len(callbacks), 1)
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
            empty = writer.write("raw", [])
            self.assertEqual(
                {key: empty[key] for key in (
                    "status", "acknowledged", "verified", "rows", "written",
                    "spooled", "batches",
                )},
                {"status": "success", "acknowledged": True, "verified": True,
                 "rows": 0, "written": 0, "spooled": 0, "batches": 0},
            )
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
            self.assertEqual(result["status"], "durable_pending")
            self.assertFalse(result["acknowledged"])
            self.assertFalse(result["verified"])
            self.assertTrue(result["retryable"])
            self.assertEqual(
                {key: result[key] for key in ("rows", "written", "spooled", "batches")},
                {"rows": 2001, "written": 0, "spooled": 2001, "batches": 3},
            )
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

    def test_authoritative_ack_requires_complete_verified_batch(self):
        class ShortWrite(FakeRepository):
            def insert_raw_bars(self, rows, batch_id=""):
                self.calls.append((rows, batch_id))
                return len(rows) - 1

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            spool = LocalClickHouseSpool(root / "spool", root)
            successful = ReliableClickHouseWriter(FakeRepository(False), spool, 1000)
            result = successful.write("raw", [raw_bar()])
            self.assertEqual(result["status"], "success")
            self.assertTrue(result["acknowledged"])
            self.assertTrue(result["verified"])
            self.assertEqual(result["durability"], "committed")

            short = ReliableClickHouseWriter(ShortWrite(False), spool, 1000)
            failed = short.write("raw", [raw_bar(2)])
            self.assertEqual(failed["status"], "durable_pending")
            self.assertFalse(failed["acknowledged"])
            self.assertEqual(failed["durability"], "wal_fsynced")
            self.assertEqual(len(failed["pending_batches"]), 1)

    def test_wal_preflight_failure_is_terminal_bounded_and_does_not_write(self):
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            spool = LocalClickHouseSpool(root / "spool", root)
            repository = FakeRepository(False)
            writer = ReliableClickHouseWriter(repository, spool, 1000)
            with patch.object(
                spool, "enqueue",
                side_effect=PermissionError("password=hunter2 /Volumes/T9/private"),
            ):
                with self.assertRaises(AuthoritativeWriteError) as raised:
                    writer.write("raw", [raw_bar()])
            outcome = raised.exception.outcome
            self.assertEqual(outcome["status"], "terminal_failure")
            self.assertFalse(outcome["acknowledged"])
            self.assertEqual(outcome["durability"], "not_durable")
            self.assertEqual(repository.calls, [])
            self.assertNotIn("hunter2", outcome["error"])
            self.assertNotIn("/Volumes/T9", outcome["error"])

    def test_atomic_wal_rename_and_fsync_fail_before_target_mutation(self):
        from unittest.mock import patch

        for operation in ("replace", "fsync"):
            with self.subTest(operation=operation), tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                spool = LocalClickHouseSpool(root / "spool", root)
                repository = FakeRepository(False)
                writer = ReliableClickHouseWriter(repository, spool, 1000)
                with patch(
                    f"marketcow.clickhouse_writer.os.{operation}",
                    side_effect=OSError(f"{operation} crash"),
                ):
                    with self.assertRaises(AuthoritativeWriteError) as raised:
                        writer.write("raw", [raw_bar()])
                self.assertEqual(raised.exception.outcome["status"], "terminal_failure")
                self.assertFalse(raised.exception.outcome["acknowledged"])
                self.assertEqual(repository.calls, [])
                self.assertEqual(list(spool.pending.glob("*.json")), [])

    def test_post_ack_archive_crash_withholds_success_and_remains_replayable(self):
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            spool = LocalClickHouseSpool(root / "spool", root)
            repository = FakeRepository(False)
            writer = ReliableClickHouseWriter(repository, spool, 1000)
            with patch.object(
                spool, "mark_replayed", side_effect=OSError("archive fsync crash")
            ):
                result = writer.write("raw", [raw_bar()])
            self.assertEqual(result["status"], "durable_pending")
            self.assertFalse(result["acknowledged"])
            self.assertEqual(result["written"], 1)
            self.assertEqual(len(list(spool.pending.glob("*.json"))), 1)
            replay = writer.replay()
            self.assertEqual(replay["replayed"], 1)
            self.assertEqual(len(repository.calls), 2)

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
