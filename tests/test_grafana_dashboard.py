from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from ops.grafana.provision_local import _write_provisioning


ROOT = Path(__file__).resolve().parents[1]
DASHBOARD = ROOT / "ops/grafana/dashboards/marketcow-data-inventory.json"
API_DASHBOARD = ROOT / "ops/grafana/dashboards/marketcow-api-observability.json"
POLYMARKET_DASHBOARD = ROOT / "ops/grafana/dashboards/marketcow-polymarket-live.json"
PROVISIONING = ROOT / "ops/grafana/provisioning/dashboards/marketcow.yaml"


class GrafanaDashboardTest(unittest.TestCase):
    def setUp(self) -> None:
        self.dashboard = json.loads(DASHBOARD.read_text())
        self.polymarket = json.loads(POLYMARKET_DASHBOARD.read_text())

    def test_dashboard_has_stable_identity_and_unique_panels(self) -> None:
        self.assertEqual("marketcow-data-inventory", self.dashboard["uid"])
        panel_ids = [panel["id"] for panel in self.dashboard["panels"]]
        self.assertEqual(len(panel_ids), len(set(panel_ids)))
        self.assertGreaterEqual(len(panel_ids), 16)

    def test_every_panel_uses_a_provisioned_read_only_datasource(self) -> None:
        allowed = {"marketcow-clickhouse", "marketcow-postgres"}
        for panel in self.dashboard["panels"] + self.polymarket["panels"]:
            if panel["type"] == "text":
                continue
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
        for panel in self.dashboard["panels"] + self.polymarket["panels"]:
            for target in panel["targets"]:
                query = f" {target.get('rawSql', '')} ".lower()
                for token in forbidden:
                    self.assertNotIn(token, query, panel["title"])

    def test_required_trader_dimensions_exist(self) -> None:
        titles = {panel["title"] for panel in self.dashboard["panels"]}
        self.assertTrue(
            {
                "Registered instruments by market",
                "Tradable data coverage by market",
                "Latest quotes — selected market",
                "Historical coverage — selected market",
                "Rows by interval",
                "Quote sources",
                "Continuity exceptions",
                "Raw artifact inventory (global)",
            }.issubset(titles)
        )
        self.assertEqual(
            {"market", "interval", "symbol"},
            {item["name"] for item in self.dashboard["templating"]["list"]},
        )

    def test_market_scope_and_event_ingestion_freshness_are_explicit(self) -> None:
        panels = {panel["title"]: panel for panel in self.dashboard["panels"]}
        self.assertEqual(
            panels["Registered instruments by market"]["datasource"]["uid"],
            "marketcow-postgres",
        )
        for title in (
            "Latest quotes — selected market",
            "Historical coverage — selected market",
            "Rows by interval",
            "Quote sources",
            "Quality status",
            "Continuity exceptions",
        ):
            self.assertIn("$market", panels[title]["targets"][0]["rawSql"])
        quote_query = panels["Latest quotes — selected market"]["targets"][0]["rawSql"]
        self.assertIn("market_event_at", quote_query)
        self.assertIn("event_age_seconds", quote_query)
        self.assertIn("ingestion_age_seconds", quote_query)

    def test_polymarket_dashboard_is_fail_closed_and_provider_neutral(self) -> None:
        self.assertEqual("marketcow-polymarket-live", self.polymarket["uid"])
        titles = {panel["title"] for panel in self.polymarket["panels"]}
        self.assertTrue({
            "Catalog markets", "Outcome tokens", "Book-token coverage",
            "Complete binary books", "Unresolved gaps", "Fail-closed reasons",
            "Catalog identity",
        }.issubset(titles))
        for panel in self.polymarket["panels"]:
            for target in panel.get("targets", []):
                query = target.get("rawSql", "")
                self.assertIn("prediction_market_live_observation", query)
                self.assertNotIn("gamma", query.lower())
                self.assertNotIn("clob", query.lower())

    def test_api_dashboard_uses_bounded_prometheus_metrics(self) -> None:
        dashboard = json.loads(API_DASHBOARD.read_text())
        self.assertEqual(dashboard["uid"], "marketcow-api-observability")
        self.assertEqual(dashboard["refresh"], "5s")
        titles = {panel["title"] for panel in dashboard["panels"]}
        self.assertEqual(
            titles,
            {"Request rate", "Error ratio", "Request latency", "In-flight requests"},
        )
        for panel in dashboard["panels"]:
            self.assertEqual(panel["datasource"]["uid"], "marketcow-prometheus")
            for target in panel["targets"]:
                self.assertIn("marketcow_http_", target["expr"])
                self.assertNotIn("user_id", target["expr"])

    def test_dashboard_provider_uses_the_marketcow_permission_folder(self) -> None:
        checked_in = PROVISIONING.read_text(encoding="utf-8")
        self.assertIn("    folder: MarketCow\n", checked_in)
        self.assertNotIn('    folder: ""\n', checked_in)

        with TemporaryDirectory() as folder:
            root = Path(folder)
            sources = root / "sources"
            sources.mkdir()
            (sources / "dashboard.json").write_text(
                '{"uid":"example","title":"Example"}', encoding="utf-8"
            )
            _write_provisioning(
                root / "provisioning",
                sources,
                ("127.0.0.1", 5432, "marketcow"),
                "marketcow",
                "postgres-secret",
                ("127.0.0.1", 8123, "marketcow"),
                "clickhouse-secret",
            )
            generated = (
                root / "provisioning/dashboards/marketcow.yaml"
            ).read_text(encoding="utf-8")
            self.assertIn("    folder: MarketCow\n", generated)
            self.assertNotIn('    folder: ""\n', generated)


if __name__ == "__main__":
    unittest.main()
