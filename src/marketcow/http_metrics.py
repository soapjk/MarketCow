from __future__ import annotations

import threading
import time
from collections import defaultdict
from typing import Any, Callable


REQUEST_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
ALLOWED_METHODS = frozenset({"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"})
MAX_ROUTE_SERIES = 256


def _method(value: str) -> str:
    value = value.upper()
    return value if value in ALLOWED_METHODS else "OTHER"


def _status_family(value: int) -> str:
    return f"{min(max(value // 100, 1), 5)}xx"


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


class RequestMetrics:
    """Bounded, process-local Prometheus request metrics."""

    def __init__(self, clock: Callable[[], float] | None = None) -> None:
        self.clock = clock or time.perf_counter
        self._lock = threading.RLock()
        self._requests: dict[tuple[str, str, str], int] = defaultdict(int)
        self._duration: dict[tuple[str, str], dict[str, Any]] = {}
        self._in_flight: dict[str, int] = defaultdict(int)
        self._exceptions: dict[tuple[str, str], int] = defaultdict(int)
        self._routes: set[str] = set()
        self._dropped_routes = 0

    def start(self, method: str) -> float:
        method = _method(method)
        with self._lock:
            self._in_flight[method] += 1
        return self.clock()

    def finish(
        self, method: str, route: str, status: int, started: float,
        exception: str = "",
    ) -> None:
        method = _method(method)
        route = self._bounded_route(route)
        elapsed = max(0.0, self.clock() - started)
        with self._lock:
            self._in_flight[method] = max(0, self._in_flight[method] - 1)
            self._requests[(method, route, _status_family(status))] += 1
            state = self._duration.setdefault(
                (method, route),
                {"count": 0, "sum": 0.0, "buckets": [0] * len(REQUEST_BUCKETS)},
            )
            state["count"] += 1
            state["sum"] += elapsed
            for index, boundary in enumerate(REQUEST_BUCKETS):
                if elapsed <= boundary:
                    state["buckets"][index] += 1
            if exception:
                self._exceptions[(route, exception[:80])] += 1

    def _bounded_route(self, route: str) -> str:
        route = route if route.startswith("/") and len(route) <= 200 else "unmatched"
        with self._lock:
            if route in self._routes:
                return route
            if len(self._routes) >= MAX_ROUTE_SERIES:
                self._dropped_routes += 1
                return "overflow"
            self._routes.add(route)
            return route

    def render(self) -> str:
        with self._lock:
            requests = dict(self._requests)
            duration = {
                key: {"count": value["count"], "sum": value["sum"],
                      "buckets": list(value["buckets"])}
                for key, value in self._duration.items()
            }
            in_flight = dict(self._in_flight)
            exceptions = dict(self._exceptions)
            dropped_routes = self._dropped_routes
        lines = [
            "# HELP marketcow_http_requests_total Completed HTTP requests.",
            "# TYPE marketcow_http_requests_total counter",
        ]
        for (method, route, status), value in sorted(requests.items()):
            lines.append(
                f'marketcow_http_requests_total{{method="{method}",route="{_escape(route)}",'
                f'status_family="{status}"}} {value}'
            )
        lines.extend([
            "# HELP marketcow_http_request_duration_seconds HTTP request duration.",
            "# TYPE marketcow_http_request_duration_seconds histogram",
        ])
        for (method, route), value in sorted(duration.items()):
            labels = f'method="{method}",route="{_escape(route)}"'
            for boundary, count in zip(REQUEST_BUCKETS, value["buckets"]):
                lines.append(
                    f'marketcow_http_request_duration_seconds_bucket{{{labels},le="{boundary:g}"}} {count}'
                )
            lines.append(
                f'marketcow_http_request_duration_seconds_bucket{{{labels},le="+Inf"}} {value["count"]}'
            )
            lines.append(
                f'marketcow_http_request_duration_seconds_sum{{{labels}}} {value["sum"]:.9f}'
            )
            lines.append(
                f'marketcow_http_request_duration_seconds_count{{{labels}}} {value["count"]}'
            )
        lines.extend([
            "# HELP marketcow_http_requests_in_flight Current HTTP requests.",
            "# TYPE marketcow_http_requests_in_flight gauge",
        ])
        for method, value in sorted(in_flight.items()):
            lines.append(f'marketcow_http_requests_in_flight{{method="{method}"}} {value}')
        lines.extend([
            "# HELP marketcow_http_exceptions_total Unhandled HTTP exceptions.",
            "# TYPE marketcow_http_exceptions_total counter",
        ])
        for (route, exception), value in sorted(exceptions.items()):
            lines.append(
                f'marketcow_http_exceptions_total{{route="{_escape(route)}",'
                f'exception="{_escape(exception)}"}} {value}'
            )
        lines.extend([
            "# HELP marketcow_http_metric_routes_dropped_total Routes mapped to overflow.",
            "# TYPE marketcow_http_metric_routes_dropped_total counter",
            f"marketcow_http_metric_routes_dropped_total {dropped_routes}",
        ])
        return "\n".join(lines) + "\n"

    def in_flight_total(self) -> int:
        with self._lock:
            return sum(self._in_flight.values())


class RequestMetricsMiddleware:
    def __init__(
        self, app: Any, metrics: RequestMetrics, event_sink: Any = None
    ) -> None:
        self.app = app
        self.metrics = metrics
        self.event_sink = event_sink

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        method = scope.get("method", "OTHER")
        started = self.metrics.start(method)
        status = 500
        exception = ""

        async def capture(message: dict[str, Any]) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = int(message["status"])
            await send(message)

        try:
            await self.app(scope, receive, capture)
        except Exception as exc:
            exception = type(exc).__name__
            raise
        finally:
            route_object = scope.get("route")
            route = getattr(route_object, "path", "unmatched")
            self.metrics.finish(method, route, status, started, exception)
            if self.event_sink is not None and route != "/v1/admin/events":
                try:
                    await self.event_sink({
                        "method": _method(method),
                        "route": route,
                        "status_family": _status_family(status),
                        "duration_ms": round(
                            max(0.0, self.metrics.clock() - started) * 1000, 3
                        ),
                        "in_flight": self.metrics.in_flight_total(),
                        "exception": exception,
                    })
                except Exception:
                    pass
