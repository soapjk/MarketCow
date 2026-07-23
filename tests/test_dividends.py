from __future__ import annotations

import unittest
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock

from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.config import Settings
from marketcow.dividends import dividend_summary, normalize_dividend_announcement
from marketcow.service import FundamentalService


class DividendNormalizationTest(unittest.TestCase):
    def fixture(self, **updates):
        row = {
            "symbol": "510300.SH",
            "fiscal_year": 2025,
            "amount_per_share": "0.086",
            "currency": "CNY",
            "announcement_date": "2025-11-01",
            "expected_payment_date": "2025-11-08",
            "confirmation_status": "confirmed",
            "source_type": "fund_manager",
            "source_name": "Example Fund Manager",
            "source_url": "https://example.invalid/announcement/1",
            "source_document_id": "ANN-1",
        }
        row.update(updates)
        return row

    def test_official_announcement_can_be_confirmed(self):
        row = normalize_dividend_announcement(
            self.fixture(), "2026-07-23T00:00:00+00:00"
        )
        self.assertEqual(row["amount_per_share"], Decimal("0.086"))
        self.assertEqual(row["confirmation_status"], "confirmed")
        self.assertEqual(row["source_priority"], 1)
        self.assertEqual(len(row["dividend_id"]), 64)

    def test_same_distribution_has_same_identity_across_sources(self):
        official = normalize_dividend_announcement(
            self.fixture(), "2026-07-23T00:00:00+00:00"
        )
        exchange = normalize_dividend_announcement(
            self.fixture(
                source_type="exchange_announcement",
                source_url="https://example.invalid/exchange/1",
                source_document_id="EX-1",
            ),
            "2026-07-23T00:00:00+00:00",
        )
        self.assertEqual(official["dividend_id"], exchange["dividend_id"])
        self.assertLess(official["source_priority"], exchange["source_priority"])

    def test_third_party_cannot_be_promoted_to_confirmed(self):
        with self.assertRaisesRegex(ValueError, "third-party"):
            normalize_dividend_announcement(
                self.fixture(source_type="third_party"),
                "2026-07-23T00:00:00+00:00",
            )

    def test_confirmed_record_requires_traceable_evidence(self):
        with self.assertRaisesRegex(ValueError, "requires source_url"):
            normalize_dividend_announcement(
                self.fixture(source_url=""), "2026-07-23T00:00:00+00:00"
            )


class DividendSummaryTest(unittest.TestCase):
    def test_summary_separates_announced_total_and_prior_year_estimate_basis(self):
        rows = [
            {
                "dividend_id": "prior", "symbol": "510300.SH", "fiscal_year": 2024,
                "amount_per_share": Decimal("0.12"), "currency": "CNY",
                "announcement_date": "2024-11-01", "confirmation_status": "confirmed",
            },
            {
                "dividend_id": "official", "symbol": "510300.SH", "fiscal_year": 2025,
                "amount_per_share": Decimal("0.08"), "currency": "CNY",
                "announcement_date": "2025-06-01", "confirmation_status": "confirmed",
            },
            {
                "dividend_id": "discovery", "symbol": "510300.SH", "fiscal_year": 2025,
                "amount_per_share": Decimal("0.02"), "currency": "CNY",
                "announcement_date": "2025-09-01", "confirmation_status": "unverified",
            },
        ]
        result = dividend_summary("510300.sh", 2025, rows)
        self.assertEqual(result["confirmed_amount_per_share_total"], Decimal("0.08"))
        self.assertEqual(
            result["previous_complete_year"]["confirmed_amount_per_share_total"],
            Decimal("0.12"),
        )
        self.assertTrue(result["previous_complete_year"]["is_estimate_basis"])
        self.assertEqual(result["announced_count"], 2)

    def test_cancelled_revision_is_excluded(self):
        rows = [{
            "dividend_id": "cancelled", "symbol": "00700.HK", "fiscal_year": 2025,
            "amount_per_share": Decimal("5.3"), "currency": "HKD",
            "announcement_date": "2026-03-18", "confirmation_status": "confirmed",
            "event_status": "cancelled",
        }]
        result = dividend_summary("00700.HK", 2025, rows)
        self.assertEqual(result["announcements"], [])
        self.assertEqual(result["confirmed_amount_per_share_total"], Decimal("0"))


class DividendServiceTest(unittest.TestCase):
    def test_service_reads_requested_and_previous_year(self):
        repository = SimpleNamespace()
        repository.get_dividend_announcements = Mock(return_value=[])
        service = FundamentalService.__new__(FundamentalService)
        service.fundamental_repository = repository

        result = service.get_dividends("AAPL", 2026)

        repository.get_dividend_announcements.assert_called_once_with("AAPL", 2025, 2026)
        self.assertEqual(result["fiscal_year"], 2026)


class DividendApiTest(unittest.TestCase):
    def test_public_query_exposes_confirmed_and_estimate_basis_separately(self):
        class Service:
            market_bar_repository = SimpleNamespace()

            def close(self):
                pass

            def get_dividends(self, symbol, fiscal_year):
                return {
                    "symbol": symbol, "fiscal_year": fiscal_year,
                    "announcements": [], "announced_count": 0,
                    "confirmed_amount_per_share_total": Decimal("0.08"),
                    "confirmed_total_currencies": ["CNY"],
                    "previous_complete_year": {
                        "fiscal_year": fiscal_year - 1,
                        "confirmed_amount_per_share_total": Decimal("0.12"),
                        "currency": "CNY", "is_estimate_basis": True,
                        "basis": "confirmed_announcements",
                    },
                }

        with TemporaryDirectory() as folder:
            root = Path(folder)
            settings = Settings(
                raw_path=root / "raw", storage_root=root, allowed_root=root.parent,
                postgres_dsn="postgresql://u:p@127.0.0.1/test",
                postgres_schema="test", clickhouse_database="test",
                clickhouse_password="secret", clickhouse_spool_path=root / "spool",
                profile="test", port=8793,
            )
            response = TestClient(create_app(settings, Service())).get(
                "/v1/dividends/510300.SH?fiscal_year=2025"
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["confirmed_amount_per_share_total"], 0.08)
        self.assertEqual(
            payload["previous_complete_year"]["confirmed_amount_per_share_total"], 0.12
        )
        self.assertTrue(payload["previous_complete_year"]["is_estimate_basis"])


if __name__ == "__main__":
    unittest.main()
