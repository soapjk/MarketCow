from __future__ import annotations

import unittest
import threading
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock

from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.config import Settings
from marketcow.dividends import dividend_summary, normalize_dividend_announcement
from marketcow.service import (
    DIVIDEND_CACHE_SCHEMA,
    DIVIDEND_REFRESH_STRATEGY,
    FundamentalService,
)


class DividendNormalizationTest(unittest.TestCase):
    def fixture(self, **updates):
        row = {
            "symbol": "510300.SH",
            "fiscal_year": 2025,
            "amount_per_share": "0.086",
            "currency": "CNY",
            "announcement_date": "2025-11-01",
            "expected_payment_date": "2025-11-08",
            "record_date": "2025-11-06",
            "ex_date": "2025-11-07",
            "payment_date": "2025-11-08",
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
        self.assertEqual(row["record_date"], "2025-11-06")
        self.assertEqual(row["ex_date"], "2025-11-07")
        self.assertEqual(row["payment_date"], "2025-11-08")
        self.assertEqual(
            row["date_evidence_json"]["record_date"]["source_document_id"], "ANN-1"
        )
        self.assertEqual(
            row["date_evidence_json"]["record_date"]["verification_status"],
            "confirmed",
        )

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

    def test_missing_dates_remain_null_with_auditable_reason(self):
        row = normalize_dividend_announcement(
            self.fixture(
                record_date=None,
                ex_date=None,
                payment_date=None,
                expected_payment_date="2025-11-08",
            ),
            "2026-07-23T00:00:00+00:00",
        )
        self.assertIsNone(row["record_date"])
        self.assertIsNone(row["ex_date"])
        self.assertIsNone(row["payment_date"])
        self.assertEqual(row["expected_payment_date"], "2025-11-08")
        self.assertEqual(
            row["date_evidence_json"]["record_date"]["missing_reason"],
            "record_date_not_provided_by_source",
        )

    def test_expected_and_actual_payment_date_must_not_conflict(self):
        with self.assertRaisesRegex(ValueError, "must equal payment_date"):
            normalize_dividend_announcement(
                self.fixture(payment_date="2025-11-09"),
                "2026-07-23T00:00:00+00:00",
            )


class DividendSummaryTest(unittest.TestCase):
    def test_summary_separates_announced_total_and_prior_year_estimate_basis(self):
        rows = [
            {
                "dividend_id": "prior", "symbol": "510300.SH", "fiscal_year": 2024,
                "amount_per_share": Decimal("0.12"), "currency": "CNY",
                "announcement_date": "2024-11-01", "confirmation_status": "confirmed",
                "expected_payment_date": "2024-11-08",
            },
            {
                "dividend_id": "official", "symbol": "510300.SH", "fiscal_year": 2025,
                "amount_per_share": Decimal("0.08"), "currency": "CNY",
                "announcement_date": "2025-06-01", "confirmation_status": "confirmed",
                "expected_payment_date": "2025-06-08",
            },
            {
                "dividend_id": "discovery", "symbol": "510300.SH", "fiscal_year": 2025,
                "amount_per_share": Decimal("0.02"), "currency": "CNY",
                "announcement_date": "2025-09-01", "confirmation_status": "unverified",
                "expected_payment_date": "2025-09-08",
            },
        ]
        result = dividend_summary("510300.sh", 2025, rows)
        self.assertEqual(result["amount_per_share_total"], Decimal("0.10"))
        self.assertFalse(result["total_is_fully_confirmed"])
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

    def test_postgres_byte_text_fields_are_normalized(self):
        result = dividend_summary("00700.HK", 2026, [{
            "dividend_id": b"event",
            "symbol": b"00700.HK",
            "fiscal_year": 2026,
            "amount_per_share": Decimal("5.3"),
            "currency": b"HKD",
            "announcement_date": date(2026, 5, 15),
            "expected_payment_date": date(2026, 6, 1),
            "record_date": date(2026, 5, 18),
            "ex_date": date(2026, 5, 15),
            "payment_date": date(2026, 6, 1),
            "date_evidence_json": {
                "record_date": {"value": "2026-05-18"}
            },
            "confirmation_status": b"unverified",
            "event_status": b"active",
            "source_priority": 9,
        }])

        self.assertEqual(result["announced_count"], 1)
        self.assertEqual(result["amount_per_share_total"], Decimal("5.3"))
        self.assertEqual(result["total_currencies"], ["HKD"])
        self.assertEqual(
            result["announcements"][0]["record_date"], "2026-05-18"
        )
        self.assertEqual(
            result["announcements"][0]["date_evidence"]["record_date"]["value"],
            "2026-05-18",
        )

    def test_longport_history_and_detail_precision_duplicates_collapse(self):
        common = {
            "symbol": "SOXX", "fiscal_year": 2025, "currency": "USD",
            "announcement_date": "2025-03-18",
            "expected_payment_date": "2025-03-21",
            "payment_date": "2025-03-21",
            "confirmation_status": "unverified",
            "event_status": "active", "source_priority": 9,
            "source_name": "LongPort OpenAPI",
        }
        result = dividend_summary("SOXX", 2025, [
            {**common, "dividend_id": "history", "amount_per_share": "0.2611",
             "source_document_id": "561692"},
            {**common, "dividend_id": "detail", "amount_per_share": "0.261115",
             "source_document_id": ""},
        ])

        self.assertEqual(result["announced_count"], 1)
        self.assertEqual(result["amount_per_share_total"], Decimal("0.2611"))
        self.assertEqual(
            result["announcements"][0]["source_document_id"], "561692"
        )

    def test_payment_year_controls_summary_even_for_legacy_report_year_row(self):
        row = {
            "dividend_id": "legacy", "symbol": "600036.SH",
            "fiscal_year": 2025, "amount_per_share": "1.003",
            "currency": "CNY", "announcement_date": "2026-07-01",
            "expected_payment_date": "2026-07-10",
            "payment_date": None, "confirmation_status": "unverified",
            "event_status": "active", "source_priority": 9,
            "source_name": "tushare",
        }

        self.assertEqual(
            dividend_summary("600036.SH", 2025, [row])["announced_count"], 0
        )
        self.assertEqual(
            dividend_summary("600036.SH", 2026, [row])["announced_count"], 1
        )


class DividendServiceTest(unittest.TestCase):
    def test_legacy_refresh_state_is_invalidated(self):
        service = FundamentalService.__new__(FundamentalService)
        service.fundamental_repository = SimpleNamespace(
            get_dividend_refresh_state=Mock(return_value={
                "status": "success",
                "last_success_at": "2026-07-23T00:00:00+00:00",
                "strategy_version": b"official-pdf-v1",
            })
        )

        self.assertIsNone(service._dividend_state("AAPL", 2026))

    def test_current_byte_encoded_refresh_state_is_accepted(self):
        service = FundamentalService.__new__(FundamentalService)
        service.fundamental_repository = SimpleNamespace(
            get_dividend_refresh_state=Mock(return_value={
                "status": b"success_data",
                "strategy_version": DIVIDEND_REFRESH_STRATEGY.encode(),
                "cache_schema_version": DIVIDEND_CACHE_SCHEMA.encode(),
                "parser_version": DIVIDEND_REFRESH_STRATEGY.encode(),
            })
        )

        state = service._dividend_state("AAPL", 2026)

        self.assertEqual(state["status"], "success_data")

    def test_service_reads_requested_and_previous_year(self):
        repository = SimpleNamespace()
        repository.get_dividend_announcements = Mock(return_value=[])
        service = FundamentalService.__new__(FundamentalService)
        service.fundamental_repository = repository

        result = service._read_dividends("AAPL", 2026)

        repository.get_dividend_announcements.assert_called_once_with("AAPL", 2025, 2026)
        self.assertEqual(result["fiscal_year"], 2026)

    def test_cache_miss_refreshes_transparently(self):
        service = FundamentalService.__new__(FundamentalService)
        service.settings = SimpleNamespace(
            dividend_cache_ttl_seconds=3600,
            dividend_refresh_retry_seconds=60,
        )
        service._read_dividends = Mock(return_value={
            "symbol": "AAPL", "fiscal_year": 2026, "announcements": [],
        })
        service._dividend_state = Mock(return_value=None)
        service._refresh_dividends_locked = Mock(return_value={"data": {
            "symbol": "AAPL", "fiscal_year": 2026, "announcements": [],
            "data_status": "fresh", "last_refreshed_at": "2026-07-23T00:00:00+00:00",
        }})

        result = service.get_dividends("AAPL", 2026)

        service._refresh_dividends_locked.assert_called_once_with("AAPL", 2026)
        self.assertEqual(result["data_status"], "fresh")

    def test_stale_cache_returns_immediately_and_schedules_refresh(self):
        service = FundamentalService.__new__(FundamentalService)
        service.settings = SimpleNamespace(
            dividend_cache_ttl_seconds=60,
            dividend_refresh_retry_seconds=30,
        )
        service._read_dividends = Mock(return_value={
            "symbol": "AAPL", "fiscal_year": 2026,
            "announcements": [{"dividend_id": "cached"}],
        })
        service._dividend_state = Mock(return_value={
            "status": "success",
            "last_success_at": "2020-01-01T00:00:00+00:00",
            "last_attempt_at": "2020-01-01T00:00:00+00:00",
        })
        service._schedule_dividend_refresh = Mock(return_value=True)

        result = service.get_dividends("AAPL", 2026)

        service._schedule_dividend_refresh.assert_called_once_with("AAPL", 2026)
        self.assertEqual(result["data_status"], "refreshing")
        self.assertEqual(result["announcements"][0]["dividend_id"], "cached")

    def test_recent_failed_refresh_serves_stale_cache_without_retry_storm(self):
        now = datetime.now(timezone.utc).isoformat()
        service = FundamentalService.__new__(FundamentalService)
        service.settings = SimpleNamespace(
            dividend_cache_ttl_seconds=60,
            dividend_refresh_retry_seconds=300,
        )
        service._read_dividends = Mock(return_value={
            "symbol": "AAPL", "fiscal_year": 2026,
            "announcements": [{"dividend_id": "cached"}],
        })
        service._dividend_state = Mock(return_value={
            "status": "failed",
            "last_success_at": "2020-01-01T00:00:00+00:00",
            "last_attempt_at": now,
        })
        service._schedule_dividend_refresh = Mock()

        result = service.get_dividends("AAPL", 2026)

        service._schedule_dividend_refresh.assert_not_called()
        self.assertEqual(result["data_status"], "stale")

    def test_successful_empty_result_is_recorded_as_fresh_cache(self):
        class Repository:
            state = None

            def get_dividend_announcements(self, symbol, year_from, year_to):
                return []

            def get_dividend_refresh_state(self, symbol, fiscal_year):
                return self.state

            def upsert_dividend_refresh_state(self, row):
                self.state = dict(row)

        repository = Repository()
        service = FundamentalService.__new__(FundamentalService)
        service.settings = SimpleNamespace(dividend_cache_ttl_seconds=3600)
        service.fundamental_repository = repository
        service.sec_dividend_provider = SimpleNamespace(fetch=Mock(return_value=[]))
        service._dividend_refresh_guard = threading.Lock()
        service._dividend_refresh_locks = {}

        result = service.get_dividends("AAPL", 2026)

        self.assertEqual(result["data_status"], "fresh")
        self.assertIsNotNone(result["last_refreshed_at"])
        self.assertEqual(repository.state["status"], "success_empty")
        self.assertEqual(repository.state["result_count"], 0)
        service.sec_dividend_provider.fetch.assert_called_once_with("AAPL", 2026)

    def test_old_empty_cache_version_is_refetched_transparently(self):
        service = FundamentalService.__new__(FundamentalService)
        service.settings = SimpleNamespace(
            dividend_cache_ttl_seconds=3600,
            dividend_empty_cache_ttl_seconds=60,
            dividend_refresh_retry_seconds=30,
        )
        service.fundamental_repository = SimpleNamespace(
            get_dividend_refresh_state=Mock(return_value={
                "status": "success_empty",
                "last_success_at": datetime.now(timezone.utc).isoformat(),
                "strategy_version": "structured-v3",
                "cache_schema_version": "dividend-cache-v1",
                "parser_version": "structured-v3",
            })
        )
        service._read_dividends = Mock(return_value={
            "symbol": "600036.SH", "fiscal_year": 2026,
            "announcements": [], "announced_count": 0,
        })
        refreshed = {
            "symbol": "600036.SH", "fiscal_year": 2026,
            "announcements": [{"amount_per_share": Decimal("1.013")}],
            "announced_count": 1, "data_status": "fresh",
        }
        service._refresh_dividends_locked = Mock(return_value={"data": refreshed})

        result = service.get_dividends("600036.SH", 2026)

        self.assertEqual(result["announced_count"], 1)
        service._refresh_dividends_locked.assert_called_once_with("600036.SH", 2026)

    def test_success_empty_uses_short_ttl_and_schedules_refresh(self):
        old = (datetime.now(timezone.utc) - timedelta(seconds=120)).isoformat()
        service = FundamentalService.__new__(FundamentalService)
        service.settings = SimpleNamespace(
            dividend_cache_ttl_seconds=3600,
            dividend_empty_cache_ttl_seconds=60,
            dividend_refresh_retry_seconds=30,
        )
        service._read_dividends = Mock(return_value={
            "symbol": "AAPL", "fiscal_year": 2026,
            "announcements": [], "announced_count": 0,
        })
        service._dividend_state = Mock(return_value={
            "status": "success_empty", "last_success_at": old,
            "last_attempt_at": old, "query_source": "fixture",
        })
        service._schedule_dividend_refresh = Mock(return_value=True)

        result = service.get_dividends("AAPL", 2026)

        self.assertEqual(result["data_status"], "refreshing")
        self.assertEqual(result["query_source"], "fixture")
        service._schedule_dividend_refresh.assert_called_once_with("AAPL", 2026)

    def test_rate_limit_failure_does_not_write_success_empty(self):
        states = []
        service = FundamentalService.__new__(FundamentalService)
        service.sec_dividend_provider = SimpleNamespace(
            fetch=Mock(side_effect=RuntimeError("429002 api request is limited"))
        )
        service.fundamental_repository = SimpleNamespace(
            upsert_dividend_refresh_state=lambda row: states.append(dict(row))
        )
        service._dividend_state = Mock(return_value=None)
        service._fetch_and_ingest_dividends = Mock(
            side_effect=RuntimeError("429002 api request is limited")
        )

        with self.assertRaisesRegex(RuntimeError, "429002"):
            service._refresh_dividends_now("AAPL", 2026)

        self.assertEqual(states[-1]["status"], "failed_rate_limited")
        self.assertIsNone(states[-1]["last_success_at"])
        self.assertIsNone(states[-1]["result_count"])

    def test_failure_classification_distinguishes_timeout_parse_and_source(self):
        self.assertEqual(
            FundamentalService._dividend_failure_status(TimeoutError("upstream")),
            "failed_timeout",
        )
        self.assertEqual(
            FundamentalService._dividend_failure_status(
                ValueError("payment date parse failed")
            ),
            "failed_parse",
        )
        self.assertEqual(
            FundamentalService._dividend_failure_status(
                RuntimeError("connection reset")
            ),
            "failed_source",
        )

    def test_timeout_failure_is_persisted_without_success_timestamp(self):
        states = []
        service = FundamentalService.__new__(FundamentalService)
        service.sec_dividend_provider = SimpleNamespace(fetch=Mock())
        service.fundamental_repository = SimpleNamespace(
            upsert_dividend_refresh_state=lambda row: states.append(dict(row))
        )
        service._dividend_state = Mock(return_value=None)
        service._fetch_and_ingest_dividends = Mock(
            side_effect=TimeoutError("upstream timed out")
        )

        with self.assertRaises(TimeoutError):
            service._refresh_dividends_now("AAPL", 2026)

        self.assertEqual(states[-1]["status"], "failed_timeout")
        self.assertIsNone(states[-1]["last_success_at"])
        self.assertNotEqual(states[-1]["status"], "success_empty")

    def test_force_refresh_bypasses_fresh_empty_cache(self):
        service = FundamentalService.__new__(FundamentalService)
        service.settings = SimpleNamespace(
            dividend_cache_ttl_seconds=3600,
            dividend_empty_cache_ttl_seconds=900,
            dividend_refresh_retry_seconds=300,
        )
        service._dividend_refresh_guard = threading.Lock()
        service._dividend_refresh_locks = {}
        service._dividend_state = Mock(return_value={
            "status": "success_empty",
            "last_success_at": datetime.now(timezone.utc).isoformat(),
            "last_attempt_at": datetime.now(timezone.utc).isoformat(),
        })
        service._refresh_dividends_now = Mock(return_value={
            "data": {"announced_count": 1}
        })

        result = service.refresh_dividends("600036.SH", 2026)

        self.assertEqual(result["data"]["announced_count"], 1)
        service._refresh_dividends_now.assert_called_once_with("600036.SH", 2026)

    def test_cn_refresh_uses_only_structured_source(self):
        service = FundamentalService.__new__(FundamentalService)
        service.cn_structured_dividend_provider = Mock()
        service._dividend_state = Mock(return_value=None)
        service.fundamental_repository = SimpleNamespace(
            upsert_dividend_refresh_state=Mock()
        )
        service._fetch_and_ingest_dividends = Mock(return_value={
            "status": "success", "count": 1, "ingested_at": "now",
        })
        service._read_dividends = Mock(return_value={
            "symbol": "600519.SH", "fiscal_year": 2026,
            "announcements": [{"confirmation_status": "unverified"}],
        })

        result = service._refresh_dividends_now("600519.SH", 2026)

        service._fetch_and_ingest_dividends.assert_called_once_with(
            service.cn_structured_dividend_provider, "600519.SH", 2026
        )
        self.assertEqual(result["data"]["data_status"], "fresh")


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
