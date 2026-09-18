from __future__ import annotations

import json
from decimal import Decimal

import httpx
import pytest

from marketcow_sdk import (
    AsyncMarketCowClient,
    DualJwtAuth,
    InstrumentNotFoundError,
    MarketCowClient,
    ProviderUnavailableError,
    RateLimitError,
    RetryConfig,
)


def sync_client(handler, **kwargs):
    raw = httpx.Client(
        base_url="https://marketcow.test",
        transport=httpx.MockTransport(handler),
    )
    return MarketCowClient(
        "https://marketcow.test", http_client=raw, **kwargs
    ), raw


def test_health_and_quote_preserve_headers_decimal_and_provenance():
    observed = []

    def handler(request: httpx.Request) -> httpx.Response:
        observed.append(request)
        if request.url.path == "/v1/health":
            return httpx.Response(200, json={"status": "ok", "version": "0.2.0"})
        assert request.url.path == "/v1/quotes/query"
        assert request.method == "POST"
        return httpx.Response(
            200,
            json={
                "count": 1,
                "items": [
                    {
                        "instrument_id": "AAPL.XNAS",
                        "symbol": "AAPL",
                        "market": "US",
                        "price": "237.125",
                        "source": "longbridge_openapi",
                        "observed_at": "2026-09-17T01:02:03Z",
                        "quality_status": "single_source_unverified",
                        "cached": True,
                        "stale": False,
                    }
                ],
                "errors": [],
                "routing": {"provider_requested": None, "allow_fallback": False},
            },
        )

    client, raw = sync_client(handler)
    try:
        assert client.health.check().status == "ok"
        batch = client.quotes.get(["AAPL.XNAS"])
        assert batch.items[0].price == Decimal("237.125")
        assert batch.items[0].source == "longbridge_openapi"
        assert batch.items[0].observed_at.tzinfo is not None
        assert observed[0].headers["x-request-id"]
        assert observed[0].headers["user-agent"].startswith("marketcow-client/")
        body = json.loads(observed[1].content)
        assert body["refresh"] is False
        assert body["allow_fallback"] is False
    finally:
        client.close()
        raw.close()


def test_structured_not_found_error_keeps_request_context():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            404,
            headers={"X-Request-ID": "server-request-123"},
            json={
                "detail": {
                    "code": "instrument_not_found",
                    "instrument_id": "NONE.XNAS",
                }
            },
        )

    client, raw = sync_client(handler)
    try:
        with pytest.raises(InstrumentNotFoundError) as captured:
            client.instruments.get("NONE.XNAS")
        assert captured.value.code == "instrument_not_found"
        assert captured.value.request_id == "server-request-123"
        assert captured.value.instrument_id == "NONE.XNAS"
    finally:
        client.close()
        raw.close()


def test_rate_limit_retries_safe_read_then_raises_typed_error():
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(
            429,
            headers={"Retry-After": "0"},
            json={"detail": {"code": "rate_limit_exceeded", "message": "slow down"}},
        )

    client, raw = sync_client(
        handler,
        retry=RetryConfig(
            maximum_attempts=2,
            base_delay_seconds=0,
            maximum_delay_seconds=0,
            jitter_seconds=0,
        ),
    )
    try:
        with pytest.raises(RateLimitError) as captured:
            client.health.check()
        assert attempts == 2
        assert captured.value.retry_after == 0
    finally:
        client.close()
        raw.close()


def test_market_data_read_that_can_acquire_upstream_is_not_retried():
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(
            503,
            json={
                "detail": {
                    "code": "provider_unavailable",
                    "provider": "longport",
                    "message": "temporarily unavailable",
                }
            },
        )

    client, raw = sync_client(
        handler,
        retry=RetryConfig(
            maximum_attempts=3,
            base_delay_seconds=0,
            maximum_delay_seconds=0,
            jitter_seconds=0,
        ),
    )
    try:
        with pytest.raises(ProviderUnavailableError) as captured:
            client.quotes.get(["AAPL.XNAS"], refresh=False)
        assert attempts == 1
        assert captured.value.provider == "longport"
    finally:
        client.close()
        raw.close()


@pytest.mark.asyncio
async def test_async_dual_jwt_auth_and_instrument_search():
    async def access():
        return "access-token"

    async def secondary():
        return "secondary-token"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == "Bearer access-token"
        assert request.headers["x-marketcow-jwt"] == "secondary-token"
        assert request.url.params["q"] == "AAPL"
        return httpx.Response(
            200,
            json={
                "count": 1,
                "items": [{"instrument_id": "AAPL.XNAS", "symbol": "AAPL"}],
            },
        )

    raw = httpx.AsyncClient(
        base_url="https://marketcow.test",
        transport=httpx.MockTransport(handler),
    )
    client = AsyncMarketCowClient(
        "https://marketcow.test",
        auth=DualJwtAuth(access, secondary),
        http_client=raw,
    )
    try:
        result = await client.instruments.search("AAPL")
        assert result.items[0].instrument_id == "AAPL.XNAS"
    finally:
        await client.close()
        await raw.aclose()
