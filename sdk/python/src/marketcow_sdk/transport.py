from __future__ import annotations

import asyncio
import json
import random
import time
import uuid
from email.utils import parsedate_to_datetime
from typing import Any, Mapping

import httpx

from .auth import AuthProvider, NoAuth
from .config import ClientConfig
from .errors import MarketCowTimeoutError, TransportError, error_from_response


def _retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            return max(0.0, parsedate_to_datetime(value).timestamp() - time.time())
        except (TypeError, ValueError, OverflowError):
            return None


class _TransportBase:
    def __init__(self, config: ClientConfig, auth: AuthProvider | Any = None) -> None:
        self.config = config
        self.auth = auth or NoAuth()

    def _headers(self, request_id: str, auth_headers: Mapping[str, str]) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "User-Agent": self.config.user_agent,
            "X-Request-ID": request_id,
            **dict(auth_headers),
        }

    def _delay(self, attempt: int, retry_after: float | None = None) -> float:
        if retry_after is not None:
            return min(retry_after, self.config.retry.maximum_delay_seconds)
        base = min(
            self.config.retry.maximum_delay_seconds,
            self.config.retry.base_delay_seconds * (2**attempt),
        )
        return base + random.uniform(0, self.config.retry.jitter_seconds)

    def _decode(self, response: httpx.Response, request_id: str) -> Any:
        content = response.content
        if len(content) > self.config.maximum_response_bytes:
            raise TransportError(
                "response exceeds configured byte limit",
                code="response_too_large",
                status_code=response.status_code,
                request_id=request_id,
            )
        try:
            payload = response.json() if content else None
        except json.JSONDecodeError as exc:
            raise TransportError(
                "MarketCow returned invalid JSON",
                code="invalid_json_response",
                status_code=response.status_code,
                request_id=request_id,
            ) from exc
        if response.is_error:
            raise error_from_response(
                response.status_code,
                payload,
                request_id=response.headers.get("X-Request-ID", request_id),
                retry_after=_retry_after(response.headers.get("Retry-After")),
            )
        return payload


class SyncTransport(_TransportBase):
    def __init__(
        self,
        config: ClientConfig,
        auth: AuthProvider | Any = None,
        *,
        client: httpx.Client | None = None,
    ) -> None:
        super().__init__(config, auth)
        timeout = httpx.Timeout(
            connect=config.timeout.connect,
            read=config.timeout.read,
            write=config.timeout.write,
            pool=config.timeout.pool,
        )
        self.client = client or httpx.Client(
            base_url=config.base_url, timeout=timeout, trust_env=config.trust_env
        )
        self._owned = client is None

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        json_body: Any = None,
        retryable: bool = False,
    ) -> Any:
        request_id = uuid.uuid4().hex
        headers = self._headers(request_id, self.auth.headers(request_id))
        attempts = self.config.retry.maximum_attempts if retryable else 1
        for attempt in range(attempts):
            try:
                response = self.client.request(
                    method, path, params=params, json=json_body, headers=headers
                )
                if response.status_code in {429, 502, 503, 504} and attempt + 1 < attempts:
                    delay = self._delay(
                        attempt, _retry_after(response.headers.get("Retry-After"))
                    )
                    time.sleep(delay)
                    continue
                return self._decode(response, request_id)
            except httpx.TimeoutException as exc:
                if attempt + 1 >= attempts:
                    raise MarketCowTimeoutError(
                        "MarketCow request timed out", code="timeout", request_id=request_id
                    ) from exc
                time.sleep(self._delay(attempt))
            except httpx.TransportError as exc:
                if attempt + 1 >= attempts:
                    raise TransportError(
                        "MarketCow transport failed",
                        code="transport_error",
                        request_id=request_id,
                    ) from exc
                time.sleep(self._delay(attempt))
        raise AssertionError("unreachable")

    def close(self) -> None:
        if self._owned:
            self.client.close()


class AsyncTransport(_TransportBase):
    def __init__(
        self,
        config: ClientConfig,
        auth: AuthProvider | Any = None,
        *,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        super().__init__(config, auth)
        timeout = httpx.Timeout(
            connect=config.timeout.connect,
            read=config.timeout.read,
            write=config.timeout.write,
            pool=config.timeout.pool,
        )
        self.client = client or httpx.AsyncClient(
            base_url=config.base_url, timeout=timeout, trust_env=config.trust_env
        )
        self._owned = client is None

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        json_body: Any = None,
        retryable: bool = False,
    ) -> Any:
        request_id = uuid.uuid4().hex
        if hasattr(self.auth, "async_headers"):
            auth_headers = await self.auth.async_headers(request_id)
        else:
            auth_headers = self.auth.headers(request_id)
        headers = self._headers(request_id, auth_headers)
        attempts = self.config.retry.maximum_attempts if retryable else 1
        for attempt in range(attempts):
            try:
                response = await self.client.request(
                    method, path, params=params, json=json_body, headers=headers
                )
                if response.status_code in {429, 502, 503, 504} and attempt + 1 < attempts:
                    await asyncio.sleep(
                        self._delay(attempt, _retry_after(response.headers.get("Retry-After")))
                    )
                    continue
                return self._decode(response, request_id)
            except httpx.TimeoutException as exc:
                if attempt + 1 >= attempts:
                    raise MarketCowTimeoutError(
                        "MarketCow request timed out", code="timeout", request_id=request_id
                    ) from exc
                await asyncio.sleep(self._delay(attempt))
            except httpx.TransportError as exc:
                if attempt + 1 >= attempts:
                    raise TransportError(
                        "MarketCow transport failed",
                        code="transport_error",
                        request_id=request_id,
                    ) from exc
                await asyncio.sleep(self._delay(attempt))
        raise AssertionError("unreachable")

    async def close(self) -> None:
        if self._owned:
            await self.client.aclose()
