from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.config import Settings
from marketcow.dividend_assessment import (
    DIVIDEND_ASSESSMENT_SCHEMA,
    assess_dividend,
)


def state(status: str, year: int, source: str = "LongPort OpenAPI") -> dict:
    return {
        "status": status,
        "query_source": source,
        "completed_at": f"{year}-12-31T12:00:00+00:00",
        "result_count": 0,
        "cache_schema_version": "dividend-cache-v2",
        "parser_version": "structured-v6-payment-year",
    }


def assessment(**overrides) -> dict:
    values = {
        "symbol": "BRK-B",
        "fiscal_year": 2025,
        "announced_count": 0,
        "asset_type": "equity",
        "refresh_state": state("success_empty", 2025),
        "historical_states": {},
        "previous_complete_year": None,
        "likely_zero_years": 3,
        "as_of": "2026-07-23T00:00:00+00:00",
    }
    values.update(overrides)
    return assess_dividend(**values)


class DividendAssessmentTest(unittest.TestCase):
    def test_current_events_mean_pays_dividend(self):
        result = assessment(announced_count=1)

        self.assertEqual(result["status"], "pays_dividend")
        self.assertEqual(result["schema"], DIVIDEND_ASSESSMENT_SCHEMA)
        self.assertEqual(result["confidence"], 1.0)

    def test_confirmed_zero_requires_authoritative_evidence_and_closed_year(self):
        evidence = [{
            "kind": "explicit_no_dividend_policy",
            "fiscal_year": 2025,
            "verification_status": "confirmed",
            "source": "issuer annual report",
            "source_url": "https://issuer.example/annual-report",
            "source_document_id": "annual-report-2025",
        }]

        closed = assessment(policy_evidence=evidence)
        current = assessment(fiscal_year=2026, policy_evidence=[{
            **evidence[0], "fiscal_year": 2026,
        }])

        self.assertEqual(closed["status"], "confirmed_zero")
        self.assertNotEqual(current["status"], "confirmed_zero")

        incomplete = assessment(policy_evidence=[{
            **evidence[0], "source_document_id": None,
        }])
        self.assertNotEqual(incomplete["status"], "confirmed_zero")

    def test_multiple_complete_empty_years_mean_likely_zero(self):
        result = assessment(
            historical_states={
                2024: state("success_empty", 2024),
                2023: state("success_empty", 2023),
                2022: state("success_empty", 2022),
            },
        )

        self.assertEqual(result["status"], "likely_zero")
        self.assertEqual(result["coverage"]["start_year"], 2022)
        self.assertEqual(result["coverage"]["end_year"], 2024)
        self.assertEqual(result["coverage"]["complete_years"], [2022, 2023, 2024])

    def test_current_empty_with_last_year_dividend_is_not_announced(self):
        result = assessment(
            fiscal_year=2026,
            previous_complete_year={
                "fiscal_year": 2025,
                "confirmed_amount_per_share_total": 1.25,
                "currency": "USD",
            },
        )

        self.assertEqual(result["status"], "not_announced")
        self.assertEqual(
            result["previous_complete_year"]["confirmed_amount_per_share_total"],
            1.25,
        )

    def test_etf_empty_source_is_unavailable_not_zero(self):
        result = assessment(
            symbol="563020.SH",
            asset_type="etf",
            historical_states={
                2024: state("success_empty", 2024, "Tushare fund_div"),
                2023: state("success_empty", 2023, "Tushare fund_div"),
                2022: state("success_empty", 2022, "Tushare fund_div"),
            },
        )

        self.assertEqual(result["status"], "unavailable")
        self.assertEqual(
            result["reason"], "fund_distribution_coverage_or_policy_missing"
        )

    def test_upstream_failures_are_auditable_unavailable(self):
        expected = {
            "failed_rate_limited": "current_announcement_source_rate_limited",
            "failed_timeout": "current_announcement_source_timeout",
            "failed_parse": "current_announcement_source_parse_failure",
            "failed_source": "current_announcement_source_failure",
        }
        for failure, reason in expected.items():
            with self.subTest(failure=failure):
                result = assessment(refresh_state=state(failure, 2025))
                self.assertEqual(result["status"], "unavailable")
                self.assertEqual(result["reason"], reason)

    def test_get_and_batch_return_identical_assessment(self):
        item_assessment = assessment(announced_count=1)

        class Service:
            market_bar_repository = SimpleNamespace()

            def close(self):
                pass

            def get_dividends(self, symbol, fiscal_year):
                return {
                    "symbol": symbol,
                    "fiscal_year": fiscal_year,
                    "announcements": [{"dividend_id": "one"}],
                    "announced_count": 1,
                    "amount_per_share_total": 1,
                    "data_status": "fresh",
                    "assessment": item_assessment,
                }

        with TemporaryDirectory() as folder:
            root = Path(folder)
            settings = Settings(
                raw_path=root / "raw",
                storage_root=root,
                allowed_root=root,
                postgres_dsn="postgresql://u:p@localhost/test",
                clickhouse_password="test",
                profile="test",
                postgres_schema="marketcow_test",
                clickhouse_database="marketcow_test",
                clickhouse_spool_path=root / "spool",
            )
            client = TestClient(create_app(settings, Service()))
            single = client.get("/v1/dividends/AAPL?fiscal_year=2025").json()
            batch = client.post("/v1/dividends/query", json={
                "symbols": ["AAPL"], "fiscal_year": 2025,
            }).json()["items"][0]["data"]

        self.assertEqual(single["assessment"], batch["assessment"])


if __name__ == "__main__":
    unittest.main()
