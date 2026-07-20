import json
import shutil
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from marketcow.final_acceptance import (
    FINAL_ACCEPTANCE_VERSION, FinalAcceptanceInputs, LocalFinalAcceptance,
)


class FinalAcceptanceTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.allowed = Path(self.folder.name)
        self.repository = Path(__file__).resolve().parents[1]
        source_readiness = (
            self.repository / "data-development/sv2-024-readiness-development/"
            "storage-v2-production-readiness"
        )
        self.readiness = self.allowed / "readiness-development/storage-v2-production-readiness"
        shutil.copytree(source_readiness, self.readiness)
        self.inputs = FinalAcceptanceInputs(
            self.allowed / "final-test", self.allowed, self.repository,
            self.readiness, {"fixture": ("true",)}, "test",
        )

    def tearDown(self):
        self.folder.cleanup()

    @staticmethod
    def runner(command, **_kwargs):
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    def test_preflight_and_report_bind_all_accepted_items(self):
        acceptance = LocalFinalAcceptance(self.inputs, self.runner)
        report = acceptance.run()
        self.assertEqual(report["version"], FINAL_ACCEPTANCE_VERSION)
        self.assertEqual(report["status"], "passed")
        self.assertEqual(len(report["accepted_artifacts"]), 18)
        self.assertFalse(report["production_connections_attempted"])
        self.assertFalse(report["remote_writes_executed"])
        self.assertEqual(json.loads(acceptance.report_path.read_text()), report)

    def test_failed_check_and_isolation_are_fail_closed(self):
        def failed(command, **_kwargs):
            return SimpleNamespace(returncode=1, stdout="", stderr="failure")
        with self.assertRaisesRegex(RuntimeError, "fixture"):
            LocalFinalAcceptance(self.inputs, failed).run()
        values = dict(self.inputs.__dict__)
        values["profile"] = "production"
        with self.assertRaisesRegex(ValueError, "development/test-only"):
            LocalFinalAcceptance(FinalAcceptanceInputs(**values), self.runner)


if __name__ == "__main__":
    unittest.main()
