from __future__ import annotations

from typing import Any

import httpx

from .auth import AuthProvider
from .config import ClientConfig, RetryConfig, TimeoutConfig
from .resources import (
    BarsResource,
    DividendsResource,
    ExposureResource,
    FxResource,
    HealthResource,
    InstrumentsResource,
    QuotesResource,
)
from .transport import SyncTransport


class MarketCowClient:
    def __init__(
        self,
        base_url: str,
        *,
        auth: AuthProvider | Any = None,
        timeout: TimeoutConfig | float | None = None,
        retry: RetryConfig | None = None,
        maximum_response_bytes: int = 16 * 1024 * 1024,
        trust_env: bool = False,
        http_client: httpx.Client | None = None,
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
        self._transport = SyncTransport(config, auth, client=http_client)
        self.health = HealthResource(self._transport)
        self.instruments = InstrumentsResource(self._transport)
        self.quotes = QuotesResource(self._transport)
        self.bars = BarsResource(self._transport)
        self.fx = FxResource(self._transport)
        self.dividends = DividendsResource(self._transport)
        self.exposure = ExposureResource(self._transport)

    def close(self) -> None:
        self._transport.close()

    def __enter__(self) -> MarketCowClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
