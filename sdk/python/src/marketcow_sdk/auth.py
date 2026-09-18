from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Awaitable, Callable, Mapping, Protocol, TypeAlias

HeaderValue: TypeAlias = str
TokenProvider: TypeAlias = Callable[[], str | Awaitable[str]]
HeaderProvider: TypeAlias = Callable[[str], Mapping[str, str] | Awaitable[Mapping[str, str]]]


class AuthProvider(Protocol):
    def headers(self, request_id: str) -> Mapping[str, HeaderValue]: ...


class AsyncAuthProvider(Protocol):
    async def async_headers(self, request_id: str) -> Mapping[str, HeaderValue]: ...


@dataclass(frozen=True)
class NoAuth:
    def headers(self, request_id: str) -> Mapping[str, str]:
        return {}

    async def async_headers(self, request_id: str) -> Mapping[str, str]:
        return {}


@dataclass(frozen=True)
class BearerTokenAuth:
    token: str

    def __post_init__(self) -> None:
        if not self.token.strip():
            raise ValueError("bearer token must not be empty")

    def headers(self, request_id: str) -> Mapping[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    async def async_headers(self, request_id: str) -> Mapping[str, str]:
        return self.headers(request_id)

    def __repr__(self) -> str:
        return "BearerTokenAuth(token='[REDACTED]')"


@dataclass(frozen=True)
class DualJwtAuth:
    access_token_provider: TokenProvider
    marketcow_token_provider: TokenProvider

    @staticmethod
    def _sync_token(provider: TokenProvider) -> str:
        value = provider()
        if inspect.isawaitable(value):
            raise TypeError("async token providers require AsyncMarketCowClient")
        if not value.strip():
            raise ValueError("token provider returned an empty token")
        return value

    @staticmethod
    async def _async_token(provider: TokenProvider) -> str:
        value = provider()
        value = await value if inspect.isawaitable(value) else value
        if not value.strip():
            raise ValueError("token provider returned an empty token")
        return value

    def headers(self, request_id: str) -> Mapping[str, str]:
        return {
            "Authorization": f"Bearer {self._sync_token(self.access_token_provider)}",
            "X-MarketCow-JWT": self._sync_token(self.marketcow_token_provider),
        }

    async def async_headers(self, request_id: str) -> Mapping[str, str]:
        access = await self._async_token(self.access_token_provider)
        marketcow = await self._async_token(self.marketcow_token_provider)
        return {
            "Authorization": f"Bearer {access}",
            "X-MarketCow-JWT": marketcow,
        }


@dataclass(frozen=True)
class CallbackAuth:
    provider: HeaderProvider

    def headers(self, request_id: str) -> Mapping[str, str]:
        value = self.provider(request_id)
        if inspect.isawaitable(value):
            raise TypeError("async header providers require AsyncMarketCowClient")
        return dict(value)

    async def async_headers(self, request_id: str) -> Mapping[str, str]:
        value = self.provider(request_id)
        return dict(await value if inspect.isawaitable(value) else value)
