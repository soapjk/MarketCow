import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from marketcow.offline_full_import import FULL_IMPORT_VERSION, FullImportTargets, _atomic, _digest
from marketcow.offline_incremental_catchup import CATCHUP_VERSION, OfflineIncrementalCatchup


class _Target:
    schema = "catchup_test"
    database = "catchup_test"


class OfflineIncrementalCatchupTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.root = Path(self.folder.name)
        self.targets = FullImportTargets(
            self.root / "run-test", self.root, _Target(), _Target(), Mock(), Mock(),
        )
        self.catchup = OfflineIncrementalCatchup(Mock(), self.targets)

    def tearDown(self):
        self.folder.cleanup()

    def _full_checkpoint(self):
        document = {
            "version": FULL_IMPORT_VERSION, "run_id": "full-run", "phase": "complete",
            "targets": self.catchup.full._target_ids(), "source_fingerprint": "old",
            "domains": {}, "errors": [],
        }
        document["checksum"] = _digest(document)
        self.catchup.full.state.mkdir(parents=True, exist_ok=True)
        _atomic(self.catchup.full.checkpoint_path, document)
        return document

    def test_requires_signed_complete_target_bound_full_checkpoint(self):
        with self.assertRaisesRegex(ValueError, "required"):
            self.catchup._full_checkpoint()
        document = self._full_checkpoint()
        document["targets"] = {"postgres_schema": "other", "clickhouse_database": "other"}
        document.pop("checksum")
        document["checksum"] = _digest(document)
        _atomic(self.catchup.full.checkpoint_path, document)
        with self.assertRaisesRegex(ValueError, "target-mismatched"):
            self.catchup._full_checkpoint()

    def test_checkpoint_is_signed_and_bound_to_full_evidence(self):
        full = self._full_checkpoint()
        self.catchup.state.mkdir(parents=True)
        self.catchup._load(full)
        loaded = json.loads(self.catchup.checkpoint_path.read_text())
        self.catchup._validate_signed(loaded, CATCHUP_VERSION)
        loaded["full_checkpoint_checksum"] = "0" * 64
        loaded.pop("checksum")
        loaded["checksum"] = _digest(loaded)
        _atomic(self.catchup.checkpoint_path, loaded)
        with self.assertRaisesRegex(ValueError, "binding mismatch"):
            self.catchup._load(full)

    def test_unstable_three_point_window_never_reports_zero_lag(self):
        self._full_checkpoint()
        self.catchup.source.inspect.side_effect = [
            {"source_fingerprint": "a"}, {"source_fingerprint": "b"},
        ]
        self.catchup._stage_snapshot = Mock()
        self.catchup._watermark = Mock(return_value={"source_fingerprint": "a", "tables": []})
        self.catchup._apply = Mock()
        self.catchup.full._reconcile = Mock()
        report = self.catchup.run(max_passes=1)
        self.assertEqual(report["status"], "incomplete")
        self.assertEqual(report["lag"], 1)
        self.catchup.full._reconcile.assert_not_called()

    def test_stable_window_requires_reconcile_and_three_equal_fingerprints(self):
        self._full_checkpoint()
        self.catchup.source.inspect.side_effect = [
            {"source_fingerprint": "stable"}, {"source_fingerprint": "stable"},
            {"source_fingerprint": "stable"},
        ]
        self.catchup._stage_snapshot = Mock()
        self.catchup._watermark = Mock(return_value={"source_fingerprint": "stable", "tables": []})
        self.catchup._apply = Mock()
        self.catchup.full._reconcile = Mock(return_value={"status": "ok", "domains": []})
        self.catchup._record_control_checkpoint = Mock()
        report = self.catchup.run(max_passes=1)
        self.assertEqual(report["lag"], 0)
        self.assertEqual(report["stability"], ["stable"] * 3)
        self.catchup.report_path.unlink()
        self.assertEqual(self.catchup.run(max_passes=1), report)

    def test_window_mutation_retries_and_only_second_stable_pass_completes(self):
        self._full_checkpoint()
        self.catchup.source.inspect.side_effect = [
            {"source_fingerprint": "a"}, {"source_fingerprint": "b"},
            {"source_fingerprint": "b"}, {"source_fingerprint": "b"},
            {"source_fingerprint": "b"},
        ]
        self.catchup._stage_snapshot = Mock()
        self.catchup._watermark = Mock(side_effect=lambda value: {
            "source_fingerprint": value, "tables": [],
        })
        self.catchup._apply = Mock()
        self.catchup.full._reconcile = Mock(return_value={"status": "ok", "domains": []})
        self.catchup._record_control_checkpoint = Mock()
        report = self.catchup.run(max_passes=2)
        self.assertEqual(report["lag"], 0)
        self.assertEqual(report["passes"], 2)
        self.assertEqual(report["stability"], ["b", "b", "b"])
        self.assertEqual(self.catchup._apply.call_count, 2)


if __name__ == "__main__":
    unittest.main()
