import tempfile
import unittest
from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.config import Settings
from marketcow.service import FundamentalService
from marketcow.storage import Warehouse


class FakeSpotProvider:
    def fetch_all(self):
        return [
            {
                "symbol": "600298", "name": "安琪酵母", "price": 33.0,
                "change_pct": 1.0, "pe_dynamic": 20.0, "pb": 2.5,
                "total_market_cap": 28_000_000_000, "float_market_cap": 27_000_000_000,
            }
        ]


class FakeFinancialProvider:
    def fetch_market_summaries(self, report_period):
        return {
            "performance": pd.DataFrame([
                {
                    "股票代码": "600298", "股票简称": "安琪酵母", "每股收益": 0.5,
                    "营业总收入-营业总收入": 4_000_000_000, "营业总收入-同比增长": 10.0,
                    "净利润-净利润": 400_000_000, "净利润-同比增长": 12.0,
                    "每股净资产": 12.0, "净资产收益率": 8.5,
                    "每股经营现金流量": 0.4, "销售毛利率": 24.0,
                    "所处行业": "食品饮料", "最新公告日期": "2026-04-30",
                }
            ]),
            "balance": pd.DataFrame([
                {
                    "股票代码": "600298", "股票简称": "安琪酵母", "资产-货币资金": 1_000_000_000,
                    "资产-总资产": 20_000_000_000, "负债-总负债": 9_000_000_000,
                    "资产负债率": 45.0, "股东权益合计": 11_000_000_000, "公告日期": "2026-04-30",
                }
            ]),
            "income": pd.DataFrame([
                {
                    "股票代码": "600298", "股票简称": "安琪酵母", "净利润": 400_000_000,
                    "营业总收入": 4_000_000_000, "营业利润": 500_000_000,
                    "利润总额": 490_000_000, "公告日期": "2026-04-30",
                }
            ]),
            "cashflow": pd.DataFrame([
                {
                    "股票代码": "600298", "股票简称": "安琪酵母",
                    "经营性现金流-现金流量净额": 350_000_000,
                    "投资性现金流-现金流量净额": -200_000_000,
                    "融资性现金流-现金流量净额": -50_000_000,
                    "公告日期": "2026-04-30",
                }
            ]),
        }

    def fetch_company_statements(self, eastmoney_symbol):
        row = {"REPORT_DATE": "2026-03-31", "NOTICE_DATE": "2026-04-30", "TOTAL_OPERATE_INCOME": 4_000_000_000}
        return {
            "income": pd.DataFrame([row]),
            "balance": pd.DataFrame([{**row, "TOTAL_ASSETS": 20_000_000_000}]),
            "cashflow": pd.DataFrame([{**row, "NETCASH_OPERATE": 350_000_000}]),
        }


class FailingSpotProvider:
    def fetch_all(self):
        raise RuntimeError("temporary upstream failure")


class FakeBaoStockProvider:
    def fetch_valuation(self, symbol):
        return {
            "date": "2026-07-14", "close": "37.46", "tradestatus": "1",
            "peTTM": "20.315688", "pbMRQ": "2.608233", "psTTM": "1.861054",
            "pcfNcfTTM": "43.383880", "isST": "0",
        }

    def fetch_financials(self, symbol, report_period):
        return {
            "profit": {"pubDate": "2026-04-28", "roeAvg": "0.034787", "npMargin": "0.096925", "gpMargin": "0.264679", "netProfit": "439432474.96", "epsTTM": "1.843725", "totalShare": "867928671"},
            "operation": {"AssetTurnRatio": "0.177397", "INVTurnRatio": "0.646195"},
            "growth": {"YOYNI": "0.135599", "YOYEquity": "0.111795", "YOYAsset": "0.145239"},
            "balance": {"pubDate": "2026-04-28", "currentRatio": "1.427293", "quickRatio": "0.780629", "liabilityToAsset": "0.479289"},
            "cashflow": {"CFOToOR": "-0.081716", "CFOToNP": "-0.843086"},
            "dupont": {"dupontROE": "0.034787"},
        }


class FakeQuoteProvider:
    def fetch_quote(self, symbol):
        market = "HK" if symbol.endswith(".HK") else "US"
        return {
            "instrument_id": "TEST." + symbol, "symbol": symbol, "name": "Test " + symbol,
            "market": market, "exchange": "TEST", "currency": "HKD" if market == "HK" else "USD",
            "price": 100.0, "previous_close": 98.0, "change": 2.0,
            "change_pct": 2.040816, "session": "regular", "quote_at": "2026-07-15T07:00:00+00:00",
            "exchange_timezone": "UTC", "source": "yahoo_chart",
            "source_url": "https://example/" + symbol, "raw_response_locator": "chart.result[0]",
            "_raw_payload": {"chart": {"symbol": symbol}},
        }

    def fetch_history(self, symbol, range_, interval, adjustment):
        return {
            "instrument_id": "TEST." + symbol, "symbol": symbol, "name": "Test " + symbol,
            "market": "HK" if symbol.endswith(".HK") else "US", "exchange": "TEST", "currency": "USD",
            "range": range_, "interval": interval, "adjustment": adjustment,
            "source": "yahoo_chart", "source_url": "https://example/" + symbol,
            "raw_response_locator": "chart.result[0].indicators",
            "bars": [{"timestamp": 1, "bar_at": "1970-01-01T00:00:01+00:00", "open": 9.0, "high": 11.0, "low": 8.0, "close": 10.0, "raw_close": 20.0, "adjustment_factor": 0.5, "volume": 100.0}],
            "_raw_payload": {"chart": {"symbol": symbol}},
        }

class ServiceApiTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.settings = Settings(root / "test.duckdb", root / "raw")
        warehouse = Warehouse(self.settings.database_path)
        self.service = FundamentalService(
            self.settings,
            warehouse=warehouse,
            spot_provider=FakeSpotProvider(),
            financial_provider=FakeFinancialProvider(),
            quote_provider=FakeQuoteProvider(),
        )
        self.client = TestClient(create_app(self.settings, self.service))

    def tearDown(self):
        self.tmp.cleanup()

    def test_refresh_and_query_fundamentals(self):
        response = self.client.post("/v1/admin/fundamentals/refresh?report_period=20260331")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["row_count"], 1)
        self.assertEqual(response.json()["active_count"], 1)

        response = self.client.get("/v1/fundamentals/600298")
        self.assertEqual(response.status_code, 200)
        item = response.json()
        self.assertEqual(item["instrument_id"], "CN.XSHG.600298")
        self.assertTrue(item["is_active"])
        self.assertEqual(item["pe_dynamic"], 20.0)
        self.assertEqual(item["roe_weighted"], 8.5)
        self.assertEqual(item["operating_cashflow"], 350_000_000)
        self.assertEqual(item["quality_status"], "single_independent_source_unverified")

        response = self.client.get("/v1/fundamentals?min_roe=8&max_pe=25")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["count"], 1)

        strict_before_ingestion = self.client.get("/v1/fundamentals/600298?as_of=2026-04-30")
        self.assertEqual(strict_before_ingestion.status_code, 404)
        strict_available = self.client.get("/v1/fundamentals/600298?as_of=2099-12-31")
        self.assertEqual(strict_available.status_code, 200)
        self.assertEqual(strict_available.json()["symbol"], "600298")
        invalid_as_of = self.client.get("/v1/fundamentals/600298?as_of=not-a-date")
        self.assertEqual(invalid_as_of.status_code, 400)

        with self.service.warehouse.connect() as con:
            manifest_count = con.execute("SELECT COUNT(*) FROM raw_artifact_manifest").fetchone()[0]
            history_count = con.execute("SELECT COUNT(*) FROM fundamental_snapshot_history").fetchone()[0]
        self.assertEqual(manifest_count, 5)
        self.assertEqual(history_count, 1)

    def test_company_statement_refresh_and_query(self):
        response = self.client.post("/v1/admin/financials/600298/refresh")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["counts"], {"income": 1, "balance": 1, "cashflow": 1})

        response = self.client.get("/v1/financials/600298/statements?statement=income")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["count"], 1)
        self.assertEqual(response.json()["items"][0]["payload"]["TOTAL_OPERATE_INCOME"], 4_000_000_000)

    def test_valuation_falls_back_to_last_complete_raw_snapshot(self):
        first = self.service.refresh_market_fundamentals("20260331")
        self.assertEqual(first["valuation_status"], "fresh")
        self.service.spot_provider = FailingSpotProvider()

        second = self.service.refresh_market_fundamentals("20260331")
        self.assertEqual(second["status"], "success")
        self.assertEqual(second["valuation_status"], "cached_after_provider_error")
        self.assertIn("temporary upstream failure", second["warnings"][0])
        row = self.service.warehouse.query_fundamentals(symbol="600298", limit=1)[0]
        self.assertTrue(row["is_active"])
        self.assertIn("valuation_cached", row["quality_status"])

    def test_baostock_refresh_and_three_source_validation(self):
        self.service.refresh_market_fundamentals("20260331")
        self.service.baostock_provider = FakeBaoStockProvider()
        response = self.client.post("/v1/admin/baostock/600298/refresh?report_period=20260331")
        self.assertEqual(response.status_code, 200)
        self.assertAlmostEqual(response.json()["roe_avg"], 3.4787)
        self.assertAlmostEqual(response.json()["pe_ttm"], 20.315688)

        self.service.warehouse.replace_tdx_period(
            "20260331",
            [{
                "symbol": "600298", "report_period": "20260331", "published_at": "2026-04-28",
                "roe_weighted": 8.5, "revenue": 4_000_000_000, "net_profit_parent": 400_000_000,
                "total_assets": 20_000_000_000, "total_liabilities": 9_000_000_000,
                "operating_cashflow": 350_000_000, "source_file": "fixture.zip",
            }],
        )
        response = self.client.get("/v1/validation/600298?report_period=20260331")
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["comparisons"]["revenue_eastmoney_vs_tdx"]["status"], "consistent")
        self.assertIsNotNone(body["sources"]["baostock"])
        self.assertEqual(body["persisted_count"], 7)

        persisted = self.client.get("/v1/validation/600298/results?report_period=20260331")
        self.assertEqual(persisted.status_code, 200)
        self.assertEqual(persisted.json()["count"], 7)
        rebuilt = self.client.post("/v1/admin/validation/rebuild?report_period=20260331")
        self.assertEqual(rebuilt.status_code, 200)
        self.assertEqual(rebuilt.json()["row_count"], 7)
        funnel = self.client.get("/v1/funnel/metrics?active_only=false")
        self.assertEqual(funnel.json()["items"][0]["quality_status"], "cross_source_difference_over_1pct")

    def test_builds_and_filters_funnel_metrics_from_history(self):
        self.service.refresh_market_fundamentals("20260331")
        for period, revenue, profit, roe in [
            ("20231231", 100.0, 10.0, 15.0),
            ("20241231", 110.0, 11.0, 16.0),
            ("20251231", 121.0, 12.1, 17.0),
            ("20260331", 125.0, 12.5, 4.0),
        ]:
            self.service.warehouse.replace_tdx_period(
                period,
                [{
                    "symbol": "600298", "report_period": period, "published_at": "2026-04-30",
                    "roe_weighted": roe, "revenue_ttm": revenue,
                    "net_profit_parent_ttm": profit, "total_assets": 200.0,
                    "total_liabilities": 80.0, "cash": 20.0,
                    "operating_cashflow": 20.0, "capex": 5.0,
                    "source_file": "fixture-{0}.zip".format(period),
                    "source": "tdx_financial_via_mootdx", "source_url": "https://example/tdx",
                    "observed_at": "2026-07-15T00:00:00+00:00",
                    "ingested_at": "2026-07-15T00:00:00+00:00",
                    "fetched_at": "2026-07-15T00:00:00+00:00",
                }],
            )
        response = self.client.post("/v1/admin/funnel/metrics/rebuild")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["row_count"], 1)

        response = self.client.get(
            "/v1/funnel/metrics?min_roe_median=15&min_revenue_cagr=9&min_annual_periods=3&max_pe=25"
        )
        self.assertEqual(response.status_code, 200)
        item = response.json()["items"][0]
        self.assertEqual(item["annual_period_count"], 3)
        self.assertAlmostEqual(item["roe_annual_median"], 16.0)
        self.assertAlmostEqual(item["revenue_cagr_pct"], 10.0)
        self.assertEqual(item["fcf_latest_period"], 15.0)
        self.assertEqual(item["latest_annual_period"], "20251231")
        self.assertEqual(item["operating_cashflow_latest_annual"], 20.0)
        self.assertEqual(item["fcf_latest_annual"], 15.0)
        self.assertAlmostEqual(item["ocf_to_net_profit_latest_annual_pct"], 20.0 / 12.1 * 100)
        self.assertAlmostEqual(item["ocf_to_net_profit_annual_sum_pct"], 60.0 / 33.1 * 100)

        unavailable = self.client.get("/v1/funnel/metrics?as_of=2026-04-30&active_only=false")
        self.assertEqual(unavailable.status_code, 200)
        self.assertEqual(unavailable.json()["count"], 0)
        available = self.client.get("/v1/funnel/metrics?as_of=2099-12-31&active_only=false")
        self.assertEqual(available.status_code, 200)
        self.assertEqual(available.json()["count"], 1)

        history = self.client.get("/v1/fundamentals/600298/history?as_of=2099-12-31")
        self.assertEqual(history.status_code, 200)
        self.assertEqual(history.json()["count"], 4)

    def test_health(self):
        response = self.client.get("/v1/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["profile"], "production")
        self.assertEqual(response.json()["status"], "ok")
        sources = self.client.get("/v1/sources/health")
        self.assertEqual(sources.status_code, 200)

    def test_hk_us_quote_batch_cache_and_adjusted_history(self):
        response = self.client.get("/v1/quotes?symbols=0700.HK,AAPL&refresh=true")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["count"], 2)
        self.assertEqual({item["symbol"] for item in response.json()["items"]}, {"0700.HK", "AAPL"})

        cached = self.client.get("/v1/quotes/0700.HK?refresh=false")
        self.assertEqual(cached.status_code, 200)
        self.assertEqual(cached.json()["price"], 100.0)
        self.assertTrue(Path(cached.json()["raw_path"]).exists())

        history = self.client.get("/v1/quotes/AAPL/history?range=1y&interval=1d&adjustment=adjusted")
        self.assertEqual(history.status_code, 200)
        self.assertEqual(history.json()["bars"][0]["close"], 10.0)
        cached_history = self.client.get("/v1/quotes/AAPL/history?interval=1d&adjustment=adjusted&refresh=false")
        self.assertEqual(cached_history.json()["count"], 1)


if __name__ == "__main__":
    unittest.main()
