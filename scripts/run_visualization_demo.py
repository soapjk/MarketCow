"""Run the administration API with deterministic local demo data."""

from __future__ import annotations

import asyncio
import random
import sys
from pathlib import Path

import uvicorn

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from marketcow.api import create_app  # noqa: E402
from marketcow.config import Settings  # noqa: E402
from tests.test_history_jobs import JobService, MemoryJobs  # noqa: E402
from tests.test_market_data_api import Bars  # noqa: E402


class DemoRepository(MemoryJobs):
    def provider_health(self):
        return [
            {
                "provider": "longport",
                "status": "ok",
                "last_attempt_at": "2026-07-25T07:30:10Z",
                "last_success_at": "2026-07-25T07:30:10Z",
                "last_error": "",
                "consecutive_failures": 0,
            },
            {
                "provider": "eastmoney",
                "status": "ok",
                "last_attempt_at": "2026-07-25T07:29:48Z",
                "last_success_at": "2026-07-25T07:29:48Z",
                "last_error": "",
                "consecutive_failures": 0,
            },
            {
                "provider": "yahoo",
                "status": "error",
                "last_attempt_at": "2026-07-25T07:29:31Z",
                "last_success_at": "2026-07-25T07:21:02Z",
                "last_error": "demo upstream timeout",
                "consecutive_failures": 2,
            },
        ]

    def get_instrument(self, _instrument_id):
        return None


class DemoService(JobService):
    def __init__(self):
        super().__init__(DemoRepository())
        self.market_bar_repository = Bars()
        self.search_results = {
            "苹果": [
                {
                    "symbol": "AAPL",
                    "name": "Apple Inc.",
                    "market": "US",
                    "exchange": "NASDAQ",
                    "currency": "USD",
                    "source": "demo",
                }
            ],
            "AAPL": [
                {
                    "symbol": "AAPL",
                    "name": "Apple Inc.",
                    "market": "US",
                    "exchange": "NASDAQ",
                    "currency": "USD",
                    "source": "demo",
                }
            ],
        }

    def search_instruments(self, query, limit):
        if query in self.search_results:
            return self.search_results[query][:limit]
        values = [
            {
                "symbol": symbol,
                "name": name,
                "market": market,
                "exchange": exchange,
                "currency": currency,
                "source": "demo",
            }
            for symbol, name, market, exchange, currency in (
                ("AAPL", "Apple Inc.", "US", "NASDAQ", "USD"),
                ("MSFT", "Microsoft Corp.", "US", "NASDAQ", "USD"),
                ("NVDA", "NVIDIA Corp.", "US", "NASDAQ", "USD"),
                ("600519.SH", "贵州茅台", "CN", "SSE", "CNY"),
                ("BTC-PERP.HYPL", "Bitcoin Perpetual", "CRYPTO", "HYPL", "USDC"),
            )
            if query.lower() in f"{symbol} {name}".lower()
        ]
        return values[:limit]


async def generate_requests(app):
    routes = (
        ("/v1/quotes", "2xx"),
        ("/v1/canonical-bars/{symbol}", "2xx"),
        ("/v1/instruments/search", "2xx"),
        ("/v1/admin/history-jobs", "2xx"),
        ("/v1/quotes", "5xx"),
    )
    while True:
        route, family = random.choices(routes, weights=(32, 24, 18, 15, 2), k=1)[0]
        await app.state.admin_events.publish(
            "request.summary",
            {
                "method": "GET",
                "route": route,
                "status_family": family,
                "duration_ms": round(random.uniform(4, 180), 2),
                "in_flight": random.randint(1, 8),
                "exception": "demo upstream timeout" if family == "5xx" else "",
            },
            source="marketcow-demo",
        )
        await asyncio.sleep(random.uniform(0.12, 0.45))


def main():
    root = Path("/tmp/marketcow-visualization-development-demo")
    settings = Settings(
        raw_path=root / "raw",
        storage_root=root,
        allowed_root=root.parent,
        postgres_dsn="postgresql://demo:demo@127.0.0.1/marketcow_test",
        clickhouse_password="demo",
        profile="development",
        port=8795,
        postgres_schema="marketcow_development",
        clickhouse_database="marketcow_development",
        clickhouse_spool_path=root / "spool",
        admin_auth_required=False,
        dashboard_registry_json="",
    )
    service = DemoService()
    app = create_app(settings, service)

    @app.on_event("startup")
    async def seed_demo():
        manager = app.state.history_job_manager
        for index, symbols in enumerate(
            (["AAPL", "MSFT", "NVDA"], ["600519.SH", "000001.SZ"])
        ):
            manager.create(
                {
                    "symbols": symbols,
                    "provider": "demo",
                    "range": "1mo",
                    "interval": "1d",
                    "adjustment": "raw",
                    "allow_fallback": False,
                    "max_concurrency": 2,
                    "max_attempts": 1,
                    "retry_backoff_seconds": 0,
                    "canonical_wait_seconds": 0,
                    "idempotency_key": f"development-demo-{index}",
                }
            )
        app.state.demo_event_task = asyncio.create_task(generate_requests(app))

    @app.on_event("shutdown")
    async def stop_demo():
        task = getattr(app.state, "demo_event_task", None)
        if task is not None:
            task.cancel()

    uvicorn.run(app, host="127.0.0.1", port=8795, log_level="warning")


if __name__ == "__main__":
    main()
