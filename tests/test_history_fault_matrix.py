from __future__ import annotations

import unittest

import requests

from marketcow.clickhouse_repositories import ClickHouseRepositoryError
from marketcow.history_jobs import HistoryJobManager
from tests.history_fault_harness import FaultSequence
from tests.test_history_jobs import JobService, request, terminal


def http_error(status):
    response = requests.Response()
    response.status_code = status
    if status == 429:
        response.headers["Retry-After"] = "0"
    return requests.HTTPError(f"status {status}", response=response)


SUCCESS = {"source": "yahoo", "bars": [{"close": 1}], "count": 1}


class HistoryFaultMatrixTest(unittest.TestCase):
    def test_transient_fault_matrix_recovers(self):
        cases = (
            ("disconnect", requests.ConnectionError("reset")),
            ("timeout", requests.Timeout("timeout")),
            ("rate_limit", http_error(429)),
            ("upstream_5xx", http_error(503)),
            ("clickhouse_unavailable", ClickHouseRepositoryError("unavailable")),
        )
        for name, fault in cases:
            with self.subTest(fault=name):
                service = JobService()
                sequence = FaultSequence(fault, SUCCESS)
                service.refresh_quote_history_window = sequence
                manager = HistoryJobManager(
                    service, service.metadata_repository, max_workers=1
                )
                job, _ = manager.create(request(
                    symbols=["AAPL.XNAS"], max_attempts=2,
                    retry_backoff_seconds=0,
                ))
                detail = terminal(manager, job["job_id"])

                self.assertEqual(detail["status"], "succeeded")
                self.assertEqual(sequence.calls, 2)
                manager.close()

    def test_permanent_fault_matrix_fails_once(self):
        cases = (
            ("authentication", http_error(401), "provider_authentication_failed"),
            ("invalid_request", ValueError("bad interval"), "invalid_history_request"),
            ("upstream_4xx", http_error(400), "upstream_request_rejected"),
        )
        for name, fault, code in cases:
            with self.subTest(fault=name):
                service = JobService()
                sequence = FaultSequence(fault, SUCCESS)
                service.refresh_quote_history_window = sequence
                manager = HistoryJobManager(
                    service, service.metadata_repository, max_workers=1
                )
                job, _ = manager.create(request(
                    symbols=["AAPL.XNAS"], max_attempts=3
                ))
                detail = terminal(manager, job["job_id"])

                self.assertEqual(detail["status"], "failed")
                self.assertEqual(detail["items"][0]["error_code"], code)
                self.assertEqual(sequence.calls, 1)
                manager.close()

