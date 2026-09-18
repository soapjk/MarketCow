from __future__ import annotations

from typing import Any

import httpx

from .auth import AuthProvider
from .config import ClientConfig, RetryConfig, TimeoutConfig
from .resources import (
    AsyncBarsResource,
    AsyncDividendsResource,
    AsyncExposureResource,
    AsyncFxResource,
    AsyncHealthResource,
    AsyncInstrumentsResource,
    AsyncQuotesResource,
)
from .transport import AsyncTransport


class AsyncMarketCowClient:
    def __init__(
        self,
        base_url: str,
        *,
        auth: AuthProvider | Any = None,
        timeout: TimeoutConfig | float | None = None,
        retry: RetryConfig | None = None,
        maximum_response_bytes: int = 16 * 1024 * 1024,
        trust_env: bool = False,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        timeout_config = (
            timeout
            if isinstance(timeout, TimeoutConfig)
            else TimeoutConfig(
                connect=float(timeout),
                read=float(timeout),
                write=float(timeout),
                pool=float(timeout),
            )
            if timeout is not None
            else TimeoutConfig()
        )
        config = ClientConfig(
            base_url=base_url,
            timeout=timeout_config,
            retry=retry or RetryConfig(),
            maximum_response_bytes=maximum_response_bytes,
            trust_env=trust_env,
        )
        self._transport = AsyncTransport(config, auth, client=http_client)
        self.health = AsyncHealthResource(self._transport)
        self.instruments = AsyncInstrumentsResource(self._transport)
        self.quotes = AsyncQuotesResource(self._transport)
        self.bars = AsyncBarsResource(self._transport)
        self.fx = AsyncFxResource(self._transport)
        self.dividends = AsyncDividendsResource(self._transport)
        self.exposure = AsyncExposureResource(self._transport)

    async def close(self) -> None:
        await self._transport.close()

    async def __aenter__(self) -> AsyncMarketCowClient:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()
