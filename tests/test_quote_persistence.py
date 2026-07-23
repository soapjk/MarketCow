import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

from marketcow.quote_persistence import AsyncQuotePersistence
from marketcow.service import FundamentalService


class AsyncQuotePersistenceTest(unittest.TestCase):
    def test_submit_returns_before_operation_and_close_drains(self):
        release = threading.Event()
        completed = []
        worker = AsyncQuotePersistence(capacity=2)
        started = time.perf_counter()
        self.assertTrue(worker.submit(lambda: (release.wait(), completed.append("done"))))
        self.assertLess(time.perf_counter() - started, 0.05)
        self.assertEqual(completed, [])
        release.set()
        self.assertTrue(worker.close(1.0))
        self.assertEqual(completed, ["done"])
        self.assertEqual(worker.snapshot().completed, 1)

    def test_queue_is_bounded_and_rejection_is_explicit(self):
        started = threading.Event()
        release = threading.Event()
        worker = AsyncQuotePersistence(capacity=1)
        self.assertTrue(worker.submit(lambda: (started.set(), release.wait())))
        self.assertTrue(started.wait(1.0))
        self.assertTrue(worker.submit(lambda: None))
        self.assertFalse(worker.submit(lambda: None))
        self.assertEqual(worker.snapshot().rejected, 1)
        release.set()
        self.assertTrue(worker.close(1.0))

    def test_failure_is_isolated_and_counted(self):
        worker = AsyncQuotePersistence(capacity=2)

        def fail():
            raise RuntimeError("secret must not escape worker")

        self.assertTrue(worker.submit(fail))
        self.assertTrue(worker.submit(lambda: None))
        self.assertTrue(worker.close(1.0))
        snapshot = worker.snapshot()
        self.assertEqual(snapshot.failed, 1)
        self.assertEqual(snapshot.completed, 1)

    def test_service_returns_pending_rows_before_persistence_runs(self):
        submitted = []
        service = FundamentalService.__new__(FundamentalService)
        service.quote_persistence = SimpleNamespace(
            submit=lambda operation: submitted.append(operation) or True
        )
        service.metadata_repository = MagicMock()
        service._persist_quote = MagicMock()
        row = {
            "symbol": "AAPL", "price": 200.0,
            "quote_at": "2026-07-22T01:00:00+00:00",
            "_raw_payload": {"secret": "raw"},
        }

        result = service._persist_quotes_async([row], "longport")

        self.assertEqual(result[0]["persistence_status"], "queued")
        self.assertIsNone(result[0]["raw_artifact_id"])
        self.assertNotIn("_raw_payload", result[0])
        service._persist_quote.assert_not_called()
        submitted[0]()
        service._persist_quote.assert_called_once()
        service.metadata_repository.record_provider_health.assert_called_once()


if __name__ == "__main__":
    unittest.main()
