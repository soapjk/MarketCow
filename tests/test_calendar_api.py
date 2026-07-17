import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.config import Settings
from marketcow.providers.calendar import normalize_earnings_event, normalize_economic_event
from marketcow.service import FundamentalService
from marketcow.storage import Warehouse


class FakeCalendarProvider:
    def fetch_economic_calendar(self, date_from, date_to, country="US"):
        return [
            normalize_economic_event(
                event_date=date_from,
                event_time="08:30:00",
                event_name="Consumer Price Index",
                source="bea_official",
                source_url="https://example.test/bea",
                payload={"row": ["CPI"]},
                country=country,
                estimate="3.1%",
                previous="3.0%",
            ),
            normalize_economic_event(
                event_date=date_to,
                event_time="10:00:00",
                event_name="Employment Situation",
                source="census_official",
                source_url="https://example.test/census",
                payload={"row": ["Employment"]},
                country=country,
            ),
        ]

    def fetch_economic_indicators(self):
        return [{
            "indicator_id": "bls_cpi_all_items",
            "country": "US",
            "name": "CPI All Urban Consumers",
            "source": "bls",
            "source_series_id": "CUSR0000SA0",
            "period": "2026 June",
            "value": 332.568,
            "previous_value": 333.979,
            "change_value": -1.411,
            "change_pct": -0.4225,
            "unit": "index",
            "frequency": "monthly",
            "latest_date": "2026-06-01",
            "source_url": "https://example.test/bls",
            "raw_response_locator": "Results.series[CUSR0000SA0]",
            "_raw_payload": {"value": "332.568"},
        }]

    def fetch_earnings_calendar(self, date_from, date_to, market="", symbols=None):
        return [normalize_earnings_event(
            market=market or "US",
            symbol=(symbols or ["PDD"])[0],
            name="PDD Holdings",
            report_date=date_to,
            report_time="time-after-hours",
            fiscal_period="2026-06-30",
            eps_forecast="2.95",
            previous_eps="2.82",
            source="nasdaq",
            source_url="https://example.test/nasdaq",
            payload={"symbol": "PDD"},
        )]


class CalendarApiTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.settings = Settings(root / "calendar.duckdb", root / "raw")
        self.warehouse = Warehouse(self.settings.database_path)
        self.service = FundamentalService(
            self.settings,
            warehouse=self.warehouse,
            calendar_provider=FakeCalendarProvider(),
        )
        self.client = TestClient(create_app(self.settings, self.service))
        self.today = datetime.now(ZoneInfo("Asia/Shanghai")).date()

    def tearDown(self):
        self.tmp.cleanup()

    def test_refresh_read_and_snapshot_contract(self):
        start = self.today.isoformat()
        end = (self.today + timedelta(days=7)).isoformat()

        economic = self.client.post(f"/v1/admin/economic-calendar/refresh?from={start}&to={end}")
        indicators = self.client.post("/v1/admin/economic-indicators/refresh")
        earnings = self.client.post(
            f"/v1/admin/earnings-calendar/refresh?market=US&symbols=PDD&from={start}&to={end}"
        )

        self.assertEqual(economic.status_code, 200)
        self.assertEqual(economic.json()["saved"], 2)
        self.assertEqual(indicators.json()["saved"], 1)
        self.assertEqual(earnings.json()["saved"], 1)

        response = self.client.get(f"/v1/economic-calendar?from={start}&to={end}&limit=1")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["count"], 1)
        event = response.json()["events"][0]
        self.assertEqual(event["timezone"], "America/New_York")
        self.assertTrue(event["scheduled_at"].endswith(("-04:00", "-05:00")))
        self.assertTrue(event["raw_path"])
        self.assertTrue(event["raw_artifact_id"])

        snapshot = self.client.get("/v1/snapshot?limit=50&days=30")
        self.assertEqual(snapshot.status_code, 200)
        body = snapshot.json()
        self.assertEqual(body["filter_timezone"], "Asia/Shanghai")
        self.assertIn("economic_calendar", body)
        self.assertEqual(body["economic_indicators"][0]["indicator_id"], "bls_cpi_all_items")
        self.assertEqual(body["earnings_calendar"][0]["symbol"], "PDD")

    def test_empty_database_returns_stable_empty_arrays(self):
        end = (self.today + timedelta(days=5)).isoformat()
        response = self.client.get(f"/v1/economic-calendar?to={end}")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["events"], [])

        snapshot = self.client.get("/v1/snapshot")
        self.assertEqual(snapshot.status_code, 200)
        self.assertEqual(snapshot.json()["economic_calendar"], [])
        self.assertEqual(snapshot.json()["economic_indicators"], [])
        self.assertEqual(snapshot.json()["earnings_calendar"], [])

    def test_parameter_boundaries_and_invalid_ranges(self):
        self.assertEqual(self.client.get("/v1/economic-calendar?limit=0").status_code, 422)
        self.assertEqual(self.client.get("/v1/economic-calendar?limit=501").status_code, 422)
        self.assertEqual(self.client.get("/v1/snapshot?days=121").status_code, 422)
        self.assertEqual(self.client.get("/v1/economic-calendar?from=bad-date").status_code, 400)
        self.assertEqual(
            self.client.get("/v1/economic-calendar?from=2026-08-02&to=2026-08-01").status_code,
            400,
        )

    def test_past_events_are_excluded_by_default_and_opt_in_is_explicit(self):
        past = (self.today - timedelta(days=3)).isoformat()
        future = (self.today + timedelta(days=3)).isoformat()
        self.client.post(f"/v1/admin/economic-calendar/refresh?from={past}&to={future}")

        default_response = self.client.get(f"/v1/economic-calendar?from={past}&to={future}")
        self.assertEqual(default_response.status_code, 200)
        self.assertTrue(default_response.json()["past_events_excluded"])
        self.assertEqual([item["event_date"] for item in default_response.json()["events"]], [future])

        history_response = self.client.get(
            f"/v1/economic-calendar?from={past}&to={future}&include_past=true"
        )
        self.assertEqual(history_response.status_code, 200)
        self.assertFalse(history_response.json()["past_events_excluded"])
        self.assertEqual(
            [item["event_date"] for item in history_response.json()["events"]],
            [past, future],
        )

    def test_duplicate_provider_events_are_saved_once(self):
        start = self.today.isoformat()
        end = (self.today + timedelta(days=7)).isoformat()
        duplicate = normalize_earnings_event(
            market="HK", symbol="00700", name="TENCENT", report_date=end,
            report_time="", fiscal_period="INT RES/DIV", eps_forecast="", previous_eps="",
            source="bnp_result_announcement", source_url="https://example.test/hk", payload=["first"],
        )
        self.service.calendar_provider.fetch_earnings_calendar = lambda *args: [duplicate, dict(duplicate)]

        response = self.client.post(
            f"/v1/admin/earnings-calendar/refresh?symbols=00700&from={start}&to={end}"
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["saved"], 1)
        self.assertEqual(len(response.json()["events"]), 1)


if __name__ == "__main__":
    unittest.main()
