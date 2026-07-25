from __future__ import annotations

import json
import unittest

from marketcow.dashboard_registry import (
    REGISTRY_SCHEMA,
    DashboardRegistration,
    load_dashboard_registry,
    registry_document,
)


class DashboardRegistryTest(unittest.TestCase):
    def test_default_dashboard_uses_safe_grafana_path(self):
        document = registry_document(load_dashboard_registry())
        self.assertEqual(document["schema"], REGISTRY_SCHEMA)
        self.assertEqual(document["items"][0]["dashboard_uid"], "marketcow-data-inventory")
        self.assertTrue(document["items"][0]["path"].startswith("/d/"))
        self.assertNotIn("http", document["items"][0]["path"])

    def test_additional_projects_are_ordered_and_panel_variables_are_encoded(self):
        raw = json.dumps([{
            "key": "api-errors",
            "project": "API Service",
            "name": "API errors",
            "dashboard_uid": "api-traffic",
            "slug": "api-traffic",
            "panel_id": 8,
            "theme": "dark",
            "variables": {"service": "public api"},
            "sort_order": 5,
        }])
        document = registry_document(load_dashboard_registry(raw))
        self.assertEqual([item["key"] for item in document["items"]],
                         ["api-errors", "marketcow-inventory", "marketcow-api-observability"])
        self.assertIn("/d-solo/api-traffic/api-traffic?", document["items"][0]["path"])
        self.assertIn("panelId=8", document["items"][0]["path"])
        self.assertIn("var-service=public+api", document["items"][0]["path"])

    def test_registry_rejects_duplicate_or_unsafe_values(self):
        duplicate = DashboardRegistration(
            key="marketcow-inventory", project="X", name="X",
            dashboard_uid="x", slug="x",
        )
        with self.assertRaisesRegex(ValueError, "unique"):
            registry_document((*load_dashboard_registry(), duplicate))
        with self.assertRaisesRegex(ValueError, "UID"):
            DashboardRegistration.from_mapping({
                "key": "x", "project": "x", "name": "x",
                "dashboard_uid": "javascript:alert(1)", "slug": "x",
            })


if __name__ == "__main__":
    unittest.main()
