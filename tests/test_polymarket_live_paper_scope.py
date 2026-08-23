from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


SCRIPT = Path(__file__).parents[1] / "scripts" / "run_polymarket_live_paper_scope.py"
SPEC = importlib.util.spec_from_file_location("run_polymarket_live_paper_scope", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


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


if __name__ == "__main__":
    unittest.main()
