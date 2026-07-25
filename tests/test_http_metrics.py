from __future__ import annotations

import unittest

from marketcow.http_metrics import MAX_ROUTE_SERIES, RequestMetrics


class Clock:
    def __init__(self):
        self.value = 0.0

    def __call__(self):
        return self.value


class RequestMetricsTest(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.metrics = RequestMetrics(self.clock)

    def test_counter_histogram_and_in_flight_are_prometheus_compatible(self):
        started = self.metrics.start("GET")
        self.clock.value = 0.075
        self.metrics.finish("GET", "/v1/quotes/{symbol}", 200, started)
        rendered = self.metrics.render()
        self.assertIn('route="/v1/quotes/{symbol}"', rendered)
        self.assertIn('status_family="2xx"} 1', rendered)
        self.assertIn('le="0.1"} 1', rendered)
        self.assertIn('marketcow_http_requests_in_flight{method="GET"} 0', rendered)
        self.assertEqual(self.metrics.in_flight_total(), 0)

    def test_unbounded_methods_routes_and_statuses_are_normalized(self):
        for index in range(MAX_ROUTE_SERIES + 2):
            started = self.metrics.start("CUSTOM")
            self.metrics.finish("CUSTOM", f"/route/{index}", 999, started)
        rendered = self.metrics.render()
        self.assertIn('method="OTHER"', rendered)
        self.assertIn('status_family="5xx"', rendered)
        self.assertIn('route="overflow"', rendered)
        self.assertIn("marketcow_http_metric_routes_dropped_total 2", rendered)

    def test_invalid_raw_path_is_not_used_as_a_label(self):
        started = self.metrics.start("GET")
        self.metrics.finish("GET", "https://example.com/user/123", 404, started)
        self.assertIn('route="unmatched"', self.metrics.render())
        self.assertNotIn("user/123", self.metrics.render())


if __name__ == "__main__":
    unittest.main()
