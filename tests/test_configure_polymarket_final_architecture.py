from __future__ import annotations

import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "configure_polymarket_final_architecture",
    ROOT / "scripts/configure_polymarket_final_architecture.py",
)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class ConfigurePolymarketFinalArchitectureTest(unittest.TestCase):
    def test_materializes_one_public_endpoint_and_preserves_admin_token(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            project = root / "project"
            support = root / "support"
            data = root / "data"
            tradude = root / "tradude"
            python = root / "python"
            rust = root / "marketcowd"
            for path in (
                project / "ops/polymarket",
                support / "polymarket-universe-refresh/old/work",
                data,
                tradude / "examples/polymarket",
            ):
                path.mkdir(parents=True)
            (project / "ops/polymarket/fee-semantics-v2.json").write_text("{}")
            (tradude / "examples/polymarket/run_opportunity_scope_controller.py").write_text("")
            python.write_text(
                "#!/bin/sh\n"
                "exit 0\n"
            )
            rust.write_text("binary")
            os.chmod(python, 0o700)
            os.chmod(rust, 0o700)
            scope_id = "a" * 64
            seed = support / "polymarket-universe-refresh/old/work/generation-9-candidate.json"
            seed.write_text(json.dumps({
                "schema_version": "marketcow.polymarket.rust-live-scope.v4",
                "scope_id": scope_id,
                "market_count": 1,
                "token_count": 2,
                "universe": {
                    "schema_version": "marketcow.polymarket.universe.v1",
                    "universe_id": scope_id,
                    "generation": 9,
                    "excluded_markets": [{"reason_code": "target_capacity"}],
                },
            }))
            env_file = support / "production.env"
            env_file.write_text(
                "# keep\n"
                "MARKETCOW_RUST_ADMIN_TOKEN=preserved-secret\n"
                f"MARKETCOW_POLYMARKET_TRADUDE_WORKTREE={tradude}\n"
                f"MARKETCOW_TRADUDE_PYTHON={python}\n"
            )

            result = MODULE.configure(
                project_dir=project,
                support_dir=support,
                data_root=data,
                env_file=env_file,
                rust_binary=rust,
            )

            self.assertEqual(result["public_port"], "8790")
            self.assertEqual(result["rust_internal_port"], "8796")
            env = env_file.read_text()
            self.assertIn("MARKETCOW_RUST_ADMIN_TOKEN=preserved-secret", env)
            self.assertNotIn("18872", env)
            controller = json.loads(Path(result["controller_config"]).read_text())
            self.assertEqual(
                controller["marketcow"]["discovery_base_url"],
                "http://127.0.0.1:8790",
            )
            self.assertEqual(
                controller["marketcow"]["live_base_url"],
                "http://127.0.0.1:8790",
            )
            refresh = json.loads(Path(result["refresh_config"]).read_text())
            self.assertEqual(refresh["service_url"], "http://127.0.0.1:8796")
            self.assertEqual(refresh["startup_scope"], result["scope_file"])
            self.assertEqual(refresh["target_market_count"], 100)
            self.assertEqual(refresh["minimum_market_count"], 1)
            active = json.loads(Path(result["scope_file"]).read_text())
            self.assertEqual(
                active["universe"]["schema_version"],
                "marketcow.polymarket.universe.v2",
            )
            self.assertEqual(active["universe"]["excluded_markets"], [])


if __name__ == "__main__":
    unittest.main()
