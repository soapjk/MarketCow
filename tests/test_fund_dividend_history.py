from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock, patch

from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.config import Settings
from marketcow.dividends import fund_dividend_history
from marketcow.providers.cn_dividends import (
    CnExchangeDividendProvider,
    parse_cn_implementation_announcement,
)
from marketcow.service import FundamentalService


FUND_ANNOUNCEMENT_TEXT = """
易方达中证红利低波动交易型开放式指数证券投资基金分红公
告
基金名称 易方达中证红利低波动交易型开放式指
数证券投资基金
基金简称 易方达中证红利低波动ETF
收益分配基准日 2026年5月29日
本次分红方案（单位：人民币元/10份基金份额） 0.120
有关年度分红次数的说明 本次分红为2026年度的第2次分红
权益登记日 2026年6月9日
除息日 2026年6月10日
现金红利发放日 2026年6月15日
本基金收益分配方式采用现金分红。
"""


def event(payment_date: str, amount: str = "0.012") -> dict:
    year = int(payment_date[:4])
    return {
        "dividend_id": payment_date,
        "symbol": "563020.XSHG",
        "fiscal_year": year,
        "amount_per_share": amount,
        "currency": "CNY",
        "announcement_date": payment_date,
        "record_date": payment_date,
        "ex_date": payment_date,
        "payment_date": payment_date,
        "confirmation_status": "confirmed",
        "event_status": "active",
        "source_type": "exchange_announcement",
        "source_name": "Shanghai Stock Exchange Fund Disclosure",
        "source_url": "https://www.sse.com.cn/example.pdf",
        "source_document_id": payment_date + ".pdf",
        "observed_at": "2026-08-06T00:00:00+00:00",
        "ingested_at": "2026-08-06T00:00:01+00:00",
        "raw_artifact_id": "a" * 64,
        "payload_json": {
            "fund_name": "易方达中证红利低波动交易型开放式指数证券投资基金",
            "dividend_type": "cash",
            "declared_amount": "0.120",
            "declared_unit_count": "10",
            "declared_unit": "fund_unit",
        },
    }


class FundDividendParserTest(unittest.TestCase):
    def test_sse_fund_announcement_preserves_unit_and_dates(self) -> None:
        rows = parse_cn_implementation_announcement(
            FUND_ANNOUNCEMENT_TEXT,
            "563020.XSHG",
            "2026-06-05",
            "https://www.sse.com.cn/example.pdf",
            "example.pdf",
            "Shanghai Stock Exchange Fund Disclosure",
        )
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["amount_per_share"], "0.012")
        self.assertEqual(row["record_date"], "2026-06-09")
        self.assertEqual(row["ex_date"], "2026-06-10")
        self.assertEqual(row["payment_date"], "2026-06-15")
        self.assertEqual(row["payload"]["declared_unit_count"], "10")
        self.assertEqual(row["payload"]["dividend_type"], "cash")
        self.assertNotIn(" ", row["payload"]["fund_name"])

    def test_sse_fund_catalog_uses_official_common_query(self) -> None:
        listing = Mock()
        listing.raise_for_status.return_value = None
        listing.json.return_value = {"result": [{
            "SSEDATE": "2026-06-05",
            "TITLE": "某基金分红公告",
            "URL": "/disclosure/fund/announcement/c/new/sample.pdf",
        }]}
        pdf = Mock()
        pdf.raise_for_status.return_value = None
        pdf.content = b"pdf"
        session = Mock()
        session.get.side_effect = [listing, pdf]
        provider = CnExchangeDividendProvider(session)
        with patch("marketcow.providers.cn_dividends.pdf_text", return_value=FUND_ANNOUNCEMENT_TEXT):
            rows = provider.fetch("563020.XSHG", 2026)
        self.assertEqual(len(rows), 1)
        query = session.get.call_args_list[0]
        self.assertEqual(query.args[0], "https://query.sse.com.cn/commonQuery.do")
        self.assertEqual(query.kwargs["params"]["sqlId"], "COMMON_PL_JJXX_JJGG_NEW_L")
        self.assertEqual(query.kwargs["params"]["SECURITY_CODE"], "563020")
        self.assertEqual(rows[0]["source_type"], "exchange_announcement")

    def test_repeated_quarterly_distributions_have_distinct_ids(self) -> None:
        march = FUND_ANNOUNCEMENT_TEXT.replace("6月9日", "3月10日").replace(
            "6月10日", "3月11日"
        ).replace("6月15日", "3月16日")
        june = parse_cn_implementation_announcement(
            FUND_ANNOUNCEMENT_TEXT, "563020.XSHG", "2026-06-05",
            "https://www.sse.com.cn/june.pdf", "june.pdf", "SSE",
        )[0]
        march_row = parse_cn_implementation_announcement(
            march, "563020.XSHG", "2026-03-06",
            "https://www.sse.com.cn/march.pdf", "march.pdf", "SSE",
        )[0]
        self.assertNotEqual(june["dividend_id"], march_row["dividend_id"])


class FundDividendHistoryContractTest(unittest.TestCase):
    def test_inclusive_window_returns_expected_four_event_ttm(self) -> None:
        results = [{
            "fiscal_year": 2025,
            "announcements": [event("2025-09-15"), event("2025-12-15")],
            "data_status": "fresh",
            "refresh_status": "success_data",
            "last_refreshed_at": "2026-08-06T00:00:00+00:00",
            "query_source": "Shanghai Stock Exchange Fund Disclosure",
        }, {
            "fiscal_year": 2026,
            "announcements": [event("2026-03-16"), event("2026-06-15")],
            "data_status": "fresh",
            "refresh_status": "success_data",
            "last_refreshed_at": "2026-08-06T00:00:00+00:00",
            "query_source": "Shanghai Stock Exchange Fund Disclosure",
        }]
        result = fund_dividend_history(
            "563020.XSHG", "2025-08-06", "2026-08-06", results
        )
        self.assertEqual(result["event_count"], 4)
        self.assertEqual(result["aggregate"]["amount_per_unit_total"], "0.048")
        self.assertEqual(result["aggregate"]["currency"], "CNY")
        self.assertEqual(result["coverage"]["status"], "complete")
        self.assertIsNone(result["aggregate"]["yield"])
        self.assertIn("Index", result["aggregate"]["yield_note"])

    def test_date_boundary_and_third_party_warning_are_explicit(self) -> None:
        row = event("2026-06-15")
        row.update({
            "confirmation_status": "unverified",
            "source_type": "third_party",
            "source_url": "https://third-party.invalid/",
        })
        result = fund_dividend_history(
            "563020.XSHG", "2026-06-15", "2026-06-15", [{
                "fiscal_year": 2026,
                "announcements": [row],
            }]
        )
        self.assertEqual(result["event_count"], 1)
        self.assertEqual(result["status"], "incomplete")
        self.assertEqual(
            result["coverage"]["warnings"][0]["code"],
            "third_party_evidence_present",
        )

    def test_completed_empty_range_is_not_a_source_error(self) -> None:
        result = fund_dividend_history(
            "563020.XSHG", "2024-01-01", "2024-01-31", [{
                "fiscal_year": 2024,
                "announcements": [],
                "data_status": "fresh",
                "refresh_status": "success_empty",
            }]
        )
        self.assertEqual(result["status"], "no_dividends")
        self.assertEqual(result["event_count"], 0)
        self.assertEqual(
            result["coverage"]["warnings"][0]["code"], "no_dividend_events"
        )


class FundDividendHistoryApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.folder = TemporaryDirectory()
        root = Path(self.folder.name)
        self.settings = Settings(
            raw_path=root / "raw",
            storage_root=root / "storage",
            allowed_root=root,
            postgres_dsn="postgresql://u:p@127.0.0.1/test",
            clickhouse_password="secret",
            profile="test",
            postgres_schema="test",
            clickhouse_database="test",
            clickhouse_spool_path=root / "spool",
        )

    def tearDown(self) -> None:
        self.folder.cleanup()

    def test_api_is_in_openapi_and_preserves_query(self) -> None:
        expected = fund_dividend_history(
            "563020.XSHG", "2025-08-06", "2026-08-06", [{
                "fiscal_year": 2026,
                "announcements": [event("2026-06-15")],
            }]
        )
        service = SimpleNamespace(
            market_bar_repository=SimpleNamespace(),
            get_fund_dividend_history=Mock(return_value=expected),
            close=lambda: None,
        )
        with TestClient(create_app(self.settings, service)) as client:
            response = client.get(
                "/v1/funds/563020.XSHG/dividends",
                params={"from": "2025-08-06", "to": "2026-08-06", "refresh": "false"},
            )
            schema = client.get("/openapi.json").json()
        self.assertEqual(response.status_code, 200)
        self.assertIn("/v1/funds/{symbol}/dividends", schema["paths"])
        service.get_fund_dividend_history.assert_called_once_with(
            "563020.XSHG", "2025-08-06", "2026-08-06", refresh=False
        )

    def test_service_selects_official_provider_for_cn_etf(self) -> None:
        service = FundamentalService.__new__(FundamentalService)
        service.cn_dividend_provider = Mock(name="official")
        service.cn_structured_dividend_provider = Mock(name="fallback")
        service.fundamental_repository = Mock()
        service._dividend_asset_type = Mock(return_value="etf")
        service._dividend_state = Mock(return_value=None)
        service._fetch_and_ingest_dividends = Mock(return_value={
            "status": "success",
            "count": 1,
            "ingested_at": "2026-08-06T00:00:00+00:00",
            "query_source": "Shanghai Stock Exchange Fund Disclosure",
        })
        service._read_dividends = Mock(return_value={
            "symbol": "563020.XSHG",
            "fiscal_year": 2026,
            "announcements": [event("2026-06-15")],
            "announced_count": 1,
        })
        service._with_dividend_cache_metadata = Mock(side_effect=lambda data, *_: data)
        service._refresh_dividends_now("563020.XSHG", 2026)
        service._fetch_and_ingest_dividends.assert_called_once_with(
            service.cn_dividend_provider, "563020.XSHG", 2026
        )

    def test_api_errors_are_machine_readable(self) -> None:
        service = SimpleNamespace(
            market_bar_repository=SimpleNamespace(),
            get_fund_dividend_history=Mock(
                side_effect=ValueError("instrument is not recognized as a fund or ETF")
            ),
            close=lambda: None,
        )
        with TestClient(create_app(self.settings, service)) as client:
            response = client.get(
                "/v1/funds/600519.XSHG/dividends",
                params={"from": "2025-01-01", "to": "2026-01-01"},
            )
        self.assertEqual(response.status_code, 422)
        self.assertEqual(response.json()["detail"]["code"], "unsupported_asset_type")


if __name__ == "__main__":
    unittest.main()
