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
    def test_loads_exact_100_market_scope(self) -> None:
        with TemporaryDirectory() as temporary:
            path = Path(temporary) / "scope.yaml"
            items = "\n".join(f'    - "{index}"' for index in range(100))
            path.write_text(
                "schema: tradude.prediction_market.live_paper.v1\n"
                "marketcow:\n  market_ids:\n" + items + "\n",
                encoding="utf-8",
            )
            self.assertEqual(MODULE.load_market_ids(path), [str(i) for i in range(100)])

    def test_rejects_duplicate_scope(self) -> None:
        with TemporaryDirectory() as temporary:
            path = Path(temporary) / "scope.yaml"
            path.write_text(
                "schema: tradude.prediction_market.live_paper.v1\n"
                "marketcow:\n  market_ids:\n" + "    - \"1\"\n" * 100,
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
                    document, sort_keys=True, separators=(",", ":"),
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
                json.dumps({
                    "schema": "tradude.prediction_market.scope_manifest.v1",
                    "scope_id": "0" * 64,
                    "market_ids": [str(index) for index in range(100)],
                }),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "does not match"):
                MODULE.load_scope_manifest_market_ids(path)


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
            collector, retry_seconds=0,
        )

        self.assertEqual(result, "recovery-ready")
        self.assertEqual(
            collector.reasons,
            ["startup", "startup_retry:1", "startup_retry:2"],
        )


if __name__ == "__main__":
    unittest.main()
