from __future__ import annotations

import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DASHBOARD = ROOT / "ops/grafana/dashboards/marketcow-data-inventory.json"


class GrafanaDashboardTest(unittest.TestCase):
    def setUp(self) -> None:
        self.dashboard = json.loads(DASHBOARD.read_text())

    def test_dashboard_has_stable_identity_and_unique_panels(self) -> None:
        self.assertEqual("marketcow-data-inventory", self.dashboard["uid"])
        panel_ids = [panel["id"] for panel in self.dashboard["panels"]]
        self.assertEqual(len(panel_ids), len(set(panel_ids)))
        self.assertGreaterEqual(len(panel_ids), 16)

    def test_every_panel_uses_a_provisioned_read_only_datasource(self) -> None:
        allowed = {"marketcow-clickhouse", "marketcow-postgres"}
        for panel in self.dashboard["panels"]:
            self.assertIn(panel["datasource"]["uid"], allowed)
            self.assertTrue(panel.get("targets"), panel["title"])

    def test_queries_are_read_only(self) -> None:
        forbidden = (
            " insert ",
            " update ",
            " delete ",
            " alter ",
            " drop ",
            " create ",
            " truncate ",
            " optimize ",
        )
        for panel in self.dashboard["panels"]:
            for target in panel["targets"]:
                query = f" {target.get('rawSql', '')} ".lower()
                for token in forbidden:
                    self.assertNotIn(token, query, panel["title"])

    def test_required_inventory_dimensions_exist(self) -> None:
        titles = {panel["title"] for panel in self.dashboard["panels"]}
        self.assertTrue(
            {
                "Rows by market",
                "Rows by interval",
                "Provider/source distribution",
                "Symbol coverage (top 200)",
                "Continuity: unexpected gaps",
                "Artifact inventory",
            }.issubset(titles)
        )
        self.assertEqual(
            {"market", "interval", "symbol"},
            {item["name"] for item in self.dashboard["templating"]["list"]},
        )


if __name__ == "__main__":
    unittest.main()

