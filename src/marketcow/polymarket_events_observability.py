from __future__ import annotations

import json
import logging
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qs


# The shared API is served by Uvicorn. Use its configured error stream so the
# bounded structured request record is reliably persisted in production; a
# standalone child logger can be disabled or left without a handler by
# Uvicorn's logging configuration.
LOGGER = logging.getLogger("uvicorn.error")
EVENTS_PATH = "/v1/prediction-markets/polymarket/live/events"
PHASES = (
    "executor_queue",
    "scope_bootstrap",
    "stable_boundary_wait",
    "sqlite_query",
    "model_construction",
    "json_serialization",
    "response_write",
    "total",
)
BUCKETS_SECONDS = (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 3, 10)


class PolymarketEventMetrics:
    """Bounded process-local metrics for the fixed /events phase set."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._phase: dict[str, dict[str, Any]] = {}
        self._requests: dict[str, int] = defaultdict(int)
        self._exceptions: dict[str, int] = defaultdict(int)
        self._timeouts = 0
        self._response_bytes = 0

    def observe(self, trace: dict[str, Any]) -> None:
        status = int(trace.get("status", 500))
        family = f"{min(max(status // 100, 1), 5)}xx"
        with self._lock:
            self._requests[family] += 1
            error_code = str(trace.get("error_code") or "")[:80]
            if error_code:
                self._exceptions[error_code] += 1
            if trace.get("timed_out"):
                self._timeouts += 1
            self._response_bytes += int(trace.get("response_bytes") or 0)
            for phase in PHASES:
                value = float(trace.get(f"{phase}_ms") or 0) / 1000
                state = self._phase.setdefault(
                    phase,
                    {"count": 0, "sum": 0.0, "max": 0.0,
                     "buckets": [0] * len(BUCKETS_SECONDS)},
                )
                state["count"] += 1
                state["sum"] += value
                state["max"] = max(state["max"], value)
                for index, boundary in enumerate(BUCKETS_SECONDS):
                    if value <= boundary:
                        state["buckets"][index] += 1

    def render(self) -> str:
        with self._lock:
            phase_states = {
                key: {
                    "count": value["count"], "sum": value["sum"],
                    "max": value["max"], "buckets": list(value["buckets"]),
                }
                for key, value in self._phase.items()
            }
            requests = dict(self._requests)
            exceptions = dict(self._exceptions)
            timeouts = self._timeouts
            response_bytes = self._response_bytes
        lines = [
            "# HELP marketcow_polymarket_events_requests_total Completed /events requests.",
            "# TYPE marketcow_polymarket_events_requests_total counter",
        ]
        for family, count in sorted(requests.items()):
            lines.append(
                "marketcow_polymarket_events_requests_total"
                f'{{status_family="{family}"}} {count}'
            )
        lines.extend([
            "# HELP marketcow_polymarket_events_phase_seconds /events phase duration.",
            "# TYPE marketcow_polymarket_events_phase_seconds histogram",
        ])
        for phase, state in sorted(phase_states.items()):
            labels = f'phase="{phase}"'
            for boundary, count in zip(BUCKETS_SECONDS, state["buckets"]):
                lines.append(
                    "marketcow_polymarket_events_phase_seconds_bucket"
                    f'{{{labels},le="{boundary:g}"}} {count}'
                )
            lines.append(
                "marketcow_polymarket_events_phase_seconds_bucket"
                f'{{{labels},le="+Inf"}} {state["count"]}'
            )
            lines.append(
                "marketcow_polymarket_events_phase_seconds_sum"
                f'{{{labels}}} {state["sum"]:.9f}'
            )
            lines.append(
                "marketcow_polymarket_events_phase_seconds_count"
                f'{{{labels}}} {state["count"]}'
            )
            lines.append(
                "marketcow_polymarket_events_phase_seconds_max"
                f'{{{labels}}} {state["max"]:.9f}'
            )
        lines.extend([
            "# HELP marketcow_polymarket_events_timeouts_total Server-observed /events timeouts.",
            "# TYPE marketcow_polymarket_events_timeouts_total counter",
            f"marketcow_polymarket_events_timeouts_total {timeouts}",
            "# HELP marketcow_polymarket_events_response_bytes_total /events response bytes.",
            "# TYPE marketcow_polymarket_events_response_bytes_total counter",
            f"marketcow_polymarket_events_response_bytes_total {response_bytes}",
            "# HELP marketcow_polymarket_events_exceptions_total /events failures by bounded code.",
            "# TYPE marketcow_polymarket_events_exceptions_total counter",
        ])
        for code, count in sorted(exceptions.items()):
            safe_code = code.replace("\\", "_").replace('"', "_")
            lines.append(
                "marketcow_polymarket_events_exceptions_total"
                f'{{code="{safe_code}"}} {count}'
            )
        return "\n".join(lines) + "\n"


class PolymarketEventsTraceMiddleware:
    """Measure ASGI send backpressure and emit one structured record per page."""

    def __init__(self, app: Any, metrics: PolymarketEventMetrics) -> None:
        self.app = app
        self.metrics = metrics

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") != "http" or scope.get("path") != EVENTS_PATH:
            await self.app(scope, receive, send)
            return
        started = time.perf_counter()
        query = parse_qs(
            bytes(scope.get("query_string", b"")).decode("utf-8", "replace"),
            keep_blank_values=True,
        )
        trace: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "after_cursor": _query_int(query, "after_cursor", 0),
            "limit": _query_int(query, "limit", 1000),
            "market_count": len(query.get("market_id", [])),
            "status": 500,
            "response_bytes": 0,
            "timed_out": False,
        }
        scope["polymarket_events_trace"] = trace
        write_seconds = 0.0

        async def capture(message: dict[str, Any]) -> None:
            nonlocal write_seconds
            if message["type"] == "http.response.start":
                trace["status"] = int(message["status"])
            elif message["type"] == "http.response.body":
                trace["response_bytes"] = (
                    int(trace.get("response_bytes") or 0)
                    + len(message.get("body", b""))
                )
            write_started = time.perf_counter()
            await send(message)
            write_seconds += time.perf_counter() - write_started

        try:
            await self.app(scope, receive, capture)
        except BaseException as exc:
            trace.setdefault("error_code", type(exc).__name__)
            trace["timed_out"] = isinstance(exc, TimeoutError)
            raise
        finally:
            trace["response_write_ms"] = write_seconds * 1000
            trace["total_ms"] = (time.perf_counter() - started) * 1000
            self.metrics.observe(trace)
            LOGGER.info(
                "polymarket_events_request %s",
                json.dumps(trace, sort_keys=True, separators=(",", ":")),
            )


def server_timing(phases: dict[str, float], queue_ms: float) -> str:
    values = {"executor_queue_ms": queue_ms, **phases}
    return ", ".join(
        f'{name.removesuffix("_ms")};dur={float(value):.3f}'
        for name, value in values.items()
        if name.endswith("_ms")
    )


def _query_int(query: dict[str, list[str]], name: str, default: int) -> int:
    try:
        return int(query.get(name, [str(default)])[-1])
    except (TypeError, ValueError):
        return default
