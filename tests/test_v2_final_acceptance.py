import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from marketcow.v2_final_acceptance import (
    REQUIRED_COMMANDS, V2_FINAL_VERSION, V2FinalAcceptance, V2FinalAcceptanceInputs,
)


class V2FinalAcceptanceTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.allowed = Path(self.temp.name)
        self.repository = Path(__file__).resolve().parents[1]
        self.commands = {name: ("true",) for name in REQUIRED_COMMANDS}
        self.inputs = V2FinalAcceptanceInputs(
            self.allowed / "final-test", self.allowed, self.repository, self.commands)

    def tearDown(self):
        self.temp.cleanup()

    @staticmethod
    def passed(_command, **_kwargs):
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    def test_preflight_chain_and_content_addressed_report(self):
        gate = V2FinalAcceptance(self.inputs, self.passed)
        report = gate.run()
        self.assertEqual((report["version"], report["status"]), (V2_FINAL_VERSION, "passed"))
        self.assertEqual(len(report["accepted_artifacts"]), 19)
        self.assertEqual(len(report["checks"]), len(REQUIRED_COMMANDS))
        manifest = json.loads(gate.manifest_path.read_text())
        self.assertEqual(manifest["report_bytes"], gate.report_path.stat().st_size)

    def test_prior_passed_is_overwritten_by_failed_terminal(self):
        gate = V2FinalAcceptance(self.inputs, self.passed)
        gate.run()
        def failed(_command, **_kwargs):
            return SimpleNamespace(returncode=1, stdout="", stderr="token=secret")
        with self.assertRaises(RuntimeError):
            V2FinalAcceptance(self.inputs, failed).run()
        terminal = json.loads(gate.report_path.read_text())
        self.assertEqual(terminal["status"], "failed")
        self.assertNotIn("accepted_artifacts", terminal)
        self.assertNotIn("secret", json.dumps(terminal))

    def test_matrix_profile_and_dirty_worktree_fail_closed(self):
        with self.assertRaisesRegex(ValueError, "matrix"):
            V2FinalAcceptance(V2FinalAcceptanceInputs(
                self.allowed / "bad-test", self.allowed, self.repository,
                {name: ("true",) for name in REQUIRED_COMMANDS[:-1]}))
        with self.assertRaisesRegex(ValueError, "development/test-only"):
            V2FinalAcceptance(V2FinalAcceptanceInputs(
                self.allowed / "bad-test", self.allowed, self.repository, self.commands, "production"))
        def dirty(_command, **_kwargs):
            return SimpleNamespace(returncode=0, stdout=" M file\n", stderr="")
        with self.assertRaises(RuntimeError):
            V2FinalAcceptance(self.inputs, dirty).run()
        self.assertEqual(json.loads(
            (self.allowed / "final-test/storage-v2-pg-ch-final-acceptance.json").read_text()
        )["status"], "failed")


if __name__ == "__main__":
    unittest.main()
