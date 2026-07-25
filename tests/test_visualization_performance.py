from __future__ import annotations

import time
import unittest

from marketcow.admin_events import AdminEventHub
from marketcow.http_metrics import RequestMetrics


class VisualizationPerformanceTest(unittest.IsolatedAsyncioTestCase):
    def test_request_metric_hot_path_is_bounded(self):
        metrics = RequestMetrics()
        started_at = time.perf_counter()
        for index in range(50_000):
            started = metrics.start("GET")
            metrics.finish("GET", f"/v1/fixed/{index % 10}", 200, started)
        elapsed = time.perf_counter() - started_at
        self.assertLess(elapsed, 5.0)
        self.assertLess(len(metrics.render()), 100_000)

    async def test_event_publish_and_replay_remain_bounded(self):
        hub = AdminEventHub(
            replay_capacity=1_000, subscriber_capacity=64,
            heartbeat_seconds=1, max_subscribers=10,
        )
        started_at = time.perf_counter()
        for index in range(10_000):
            await hub.publish("request.summary", {
                "method": "GET", "route": "/v1/health",
                "duration_ms": index % 100,
            })
        elapsed = time.perf_counter() - started_at
        self.assertLess(elapsed, 5.0)
        self.assertEqual(len(hub._replay), 1_000)


if __name__ == "__main__":
    unittest.main()
