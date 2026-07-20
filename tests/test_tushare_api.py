import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock

from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.config import Settings
from marketcow.service import FundamentalService
from marketcow.providers.tushare_provider import TushareProvider


class TushareApiTest(unittest.TestCase):
    def test_generic_tushare_route(self):
        with TemporaryDirectory() as folder:
            settings = Settings(Path(folder) / "db.duckdb", Path(folder) / "raw")
            provider = Mock()
            provider.name = "tushare_via_test"
            provider.base_url = "https://proxy.test"
            provider.call.return_value = {
                "code": 0, "data": {"fields": ["ts_code"], "items": [["000001.SZ"]]},
            }
            provider.rows.side_effect = lambda result: [
                dict(zip(result["data"]["fields"], row)) for row in result["data"]["items"]
            ]
            service = FundamentalService(settings, tushare_provider=provider)
            client = TestClient(create_app(settings, service))
            response = client.post(
                "/v1/tushare/daily",
                json={"params": {"trade_date": "20260717"}, "fields": "ts_code"},
            )
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["data"]["items"], [["000001.SZ"]])
            provider.call.assert_called_once_with("daily", {"trade_date": "20260717"}, "ts_code")
            with service.warehouse.connect() as con:
                request = con.execute("SELECT api_name, source, row_count FROM tushare_request").fetchone()
                row = con.execute("SELECT api_name, symbol, source, payload_json FROM tushare_data_row").fetchone()
            self.assertEqual(request, ("daily", "tushare_via_test", 1))
            self.assertEqual(row[:3], ("daily", "000001.SZ", "tushare_via_test"))
            self.assertIn('"ts_code": "000001.SZ"', row[3])

    def test_a_share_minute_history_uses_tushare_and_persists_unified_bars(self):
        with TemporaryDirectory() as folder:
            settings = Settings(Path(folder) / "db.duckdb", Path(folder) / "raw")
            provider = Mock()
            provider.name = "tushare_via_test"
            provider.base_url = "https://proxy.test"
            provider.call.return_value = {
                "code": 0,
                "data": {
                    "fields": ["ts_code", "trade_time", "open", "high", "low", "close", "vol", "amount"],
                    "items": [["600000.SH", "2026-07-17 09:35:00", 10.0, 10.2, 9.9, 10.1, 1200, 12120]],
                },
            }
            provider.rows.side_effect = TushareProvider.rows
            provider.minute_bars.side_effect = TushareProvider.minute_bars
            service = FundamentalService(settings, tushare_provider=provider)
            client = TestClient(create_app(settings, service))

            response = client.get("/v1/quotes/600000.SH/history?range=1d&interval=5m&adjustment=raw")

            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["source"], "tushare_via_test")
            self.assertEqual(response.json()["bars"][0]["amount"], 12120)
            cached = client.get("/v1/quotes/600000.SH/history?interval=5m&adjustment=raw&refresh=false")
            self.assertEqual(cached.status_code, 200)
            self.assertEqual(cached.json()["bars"][0]["source"], "tushare_via_test")
            self.assertEqual(cached.json()["bars"][0]["source_payload"]["ts_code"], "600000.SH")


if __name__ == "__main__":
    unittest.main()
