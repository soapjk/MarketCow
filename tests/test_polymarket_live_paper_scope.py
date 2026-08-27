from __future__ import annotations

import importlib.util
import hashlib
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


SCRIPT = Path(__file__).parents[1] / "scripts" / "run_polymarket_live_paper_scope.py"
SPEC = importlib.util.spec_from_file_location("run_polymarket_live_paper_scope", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)

LIVE_SCRIPT = Path(__file__).parents[1] / "scripts" / "run_polymarket_live.py"
LIVE_SPEC = importlib.util.spec_from_file_location("run_polymarket_live", LIVE_SCRIPT)
assert LIVE_SPEC is not None and LIVE_SPEC.loader is not None
LIVE_MODULE = importlib.util.module_from_spec(LIVE_SPEC)
LIVE_SPEC.loader.exec_module(LIVE_MODULE)


class PolymarketLivePaperScopeTest(unittest.TestCase):
    @staticmethod
    def _bound_scope(temporary: str) -> tuple[Path, Path, Path]:
        root = Path(temporary)
        generated_at_ns = 1_000_000_000
        markets = []
        token_ids = []
        for index in range(100):
            yes = str(index * 2)
            no = str(index * 2 + 1)
            token_ids.extend((yes, no))
            markets.append(
                {
                    "market_id": str(index),
                    "accepting_orders": True,
                    "active": True,
                    "binary_yes_no": True,
                    "closed": False,
                    "rules_complete": True,
                    "end_at_ns": generated_at_ns + 2 * 3_600 * 1_000_000_000,
                    "yes_outcome_id": f"POLY:condition:{yes}",
                    "no_outcome_id": f"POLY:condition:{no}",
                }
            )
        manifest = {
            "schema": "tradude.prediction_market.scope_manifest.v1",
            "generated_at_ns": generated_at_ns,
            "candidate_snapshot_id": "candidate-1",
            "catalog_revision": "a" * 64,
            "market_ids": [str(index) for index in range(100)],
        }
        manifest["scope_id"] = hashlib.sha256(
            json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        report = {
            "schema": "tradude.prediction_market.clob_complete_scope.v2",
            "selection": {"manifest": manifest},
            "attempts": [
                {
                    "complete": True,
                    "checked_at_ns": generated_at_ns,
                    "requested_token_ids": token_ids,
                    "received_token_ids": token_ids,
                    "two_sided_token_ids": token_ids,
                    "missing_token_ids": [],
                    "non_two_sided_token_ids": [],
                }
            ],
        }
        candidate = {
            "schema": "tradude.prediction_market.scope_candidates.v1",
            "snapshot_id": "candidate-1",
            "catalog_revision": "a" * 64,
            "markets": markets,
        }
        paths = root / "manifest.json", root / "selection-report.json", root / "candidates.json"
        for path, document in zip(paths, (manifest, report, candidate), strict=True):
            path.write_text(json.dumps(document), encoding="utf-8")
        return paths

    def test_loads_exact_100_market_scope(self) -> None:
        with TemporaryDirectory() as temporary:
            path = Path(temporary) / "scope.yaml"
            items = "\n".join(f'    - "{index}"' for index in range(100))
            path.write_text(
                "schema: tradude.prediction_market.live_paper.v1\nmarketcow:\n  market_ids:\n" + items + "\n",
                encoding="utf-8",
            )
            self.assertEqual(MODULE.load_market_ids(path), [str(i) for i in range(100)])

    def test_rejects_duplicate_scope(self) -> None:
        with TemporaryDirectory() as temporary:
            path = Path(temporary) / "scope.yaml"
            path.write_text(
                "schema: tradude.prediction_market.live_paper.v1\nmarketcow:\n  market_ids:\n" + '    - "1"\n' * 100,
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "100 unique"):
                MODULE.load_market_ids(path)

    def test_loads_exact_100_market_scope_manifest(self) -> None:
        with TemporaryDirectory() as temporary:
            path = Path(temporary) / "manifest.json"
            document = {
                "schema": "tradude.prediction_market.scope_manifest.v1",
                "market_ids": [str(index) for index in range(100)],
            }
            document["scope_id"] = hashlib.sha256(
                json.dumps(
                    document,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8"),
            ).hexdigest()
            path.write_text(json.dumps(document), encoding="utf-8")

            self.assertEqual(
                MODULE.load_scope_manifest_market_ids(path),
                [str(index) for index in range(100)],
            )

    def test_rejects_modified_scope_manifest(self) -> None:
        with TemporaryDirectory() as temporary:
            path = Path(temporary) / "manifest.json"
            path.write_text(
                json.dumps(
                    {
                        "schema": "tradude.prediction_market.scope_manifest.v1",
                        "scope_id": "0" * 64,
                        "market_ids": [str(index) for index in range(100)],
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "does not match"):
                MODULE.load_scope_manifest_market_ids(path)

    def test_validates_manifest_report_candidate_and_thirty_day_lock(self) -> None:
        with TemporaryDirectory() as temporary:
            manifest, report, candidate = self._bound_scope(temporary)

            evidence = MODULE.validate_scope_selection(
                manifest,
                report,
                candidate,
                now_ns=1_000_000_000,
            )

            self.assertEqual(evidence["market_ids"], [str(i) for i in range(100)])
            self.assertEqual(evidence["token_count"], 200)

    def test_restart_accepts_scope_that_expired_after_valid_selection(self) -> None:
        with TemporaryDirectory() as temporary:
            manifest, report, candidate = self._bound_scope(temporary)

            evidence = MODULE.validate_scope_selection(
                manifest,
                report,
                candidate,
                now_ns=10 * 3_600 * 1_000_000_000,
            )

            self.assertEqual(
                evidence["required_valid_until_ns"],
                1_000_000_000 + MODULE.MINIMUM_RUNTIME_LIFETIME_SECONDS * 1_000_000_000,
            )
            self.assertEqual(evidence["validated_at_ns"], 10 * 3_600 * 1_000_000_000)

    def test_rejects_selection_report_not_bound_to_manifest(self) -> None:
        with TemporaryDirectory() as temporary:
            manifest, report, candidate = self._bound_scope(temporary)
            document = json.loads(report.read_text(encoding="utf-8"))
            document["selection"]["manifest"]["market_ids"][0] = "other"
            report.write_text(json.dumps(document), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "exactly bound"):
                MODULE.validate_scope_selection(
                    manifest,
                    report,
                    candidate,
                    now_ns=1_000_000_000,
                )

    def test_rejects_market_above_thirty_day_capital_lock(self) -> None:
        with TemporaryDirectory() as temporary:
            manifest, report, candidate = self._bound_scope(temporary)
            document = json.loads(candidate.read_text(encoding="utf-8"))
            document["markets"][0]["end_at_ns"] = 1_000_000_000 + MODULE.MAXIMUM_CAPITAL_LOCK_DURATION_NS + 1
            candidate.write_text(json.dumps(document), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "exceeds 30-day"):
                MODULE.validate_scope_selection(
                    manifest,
                    report,
                    candidate,
                    now_ns=1_000_000_000,
                )

    def test_rejects_incomplete_final_clob_coverage(self) -> None:
        with TemporaryDirectory() as temporary:
            manifest, report, candidate = self._bound_scope(temporary)
            document = json.loads(report.read_text(encoding="utf-8"))
            document["attempts"][-1]["complete"] = False
            report.write_text(json.dumps(document), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "incomplete"):
                MODULE.validate_scope_selection(
                    manifest,
                    report,
                    candidate,
                    now_ns=1_000_000_000,
                )


class PolymarketLiveBootstrapRetryTest(unittest.IsolatedAsyncioTestCase):
    async def test_incomplete_provider_snapshot_retries_in_process(self) -> None:
        class Collector:
            def __init__(self) -> None:
                self.reasons = []

            async def bootstrap_books(self, reason: str) -> str:
                self.reasons.append(reason)
                if len(self.reasons) < 3:
                    raise RuntimeError("provider omitted requested tokens")
                return "recovery-ready"

        collector = Collector()
        result = await LIVE_MODULE.bootstrap_books_until_ready(
            collector,
            retry_seconds=0,
        )

        self.assertEqual(result, "recovery-ready")
        self.assertEqual(
            collector.reasons,
            ["startup", "startup_retry:1", "startup_retry:2"],
        )


if __name__ == "__main__":
    unittest.main()
