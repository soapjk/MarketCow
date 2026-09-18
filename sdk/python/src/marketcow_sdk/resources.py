from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from urllib.parse import quote

from .models import (
    CanonicalBarPage,
    DocumentResponse,
    FxResponse,
    HealthResponse,
    Instrument,
    InstrumentResolveBatch,
    InstrumentSearchResult,
    MarketBarResult,
    QuoteBatch,
)
from .pagination import AsyncCanonicalSeries, CanonicalSeries
from .transport import AsyncTransport, SyncTransport


def _iso(value: datetime | str) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("timestamps must be timezone-aware")
        return value.isoformat()
    return value


def _path_value(value: str) -> str:
    return quote(value, safe="")


class HealthResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def check(self) -> HealthResponse:
        return HealthResponse.model_validate(
            self._transport.request("GET", "/v1/health", retryable=True)
        )


class InstrumentsResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def search(self, query: str, *, limit: int = 12) -> InstrumentSearchResult:
        return InstrumentSearchResult.model_validate(
            self._transport.request(
                "GET", "/v1/instruments/search", params={"q": query, "limit": limit}, retryable=True
            )
        )

    def get(self, instrument_id: str) -> Instrument:
        return Instrument.model_validate(
            self._transport.request(
                "GET", f"/v1/instruments/{_path_value(instrument_id)}", retryable=True
            )
        )

    def resolve(self, namespace: str, external_symbol: str) -> Instrument:
        return Instrument.model_validate(
            self._transport.request(
                "GET",
                "/v1/instruments:resolve",
                params={"namespace": namespace, "external_symbol": external_symbol},
                retryable=True,
            )
        )

    def resolve_many(self, namespace: str, symbols: list[str]) -> InstrumentResolveBatch:
        return InstrumentResolveBatch.model_validate(
            self._transport.request(
                "POST",
                "/v1/instruments:resolve/query",
                json_body={"namespace": namespace, "symbols": symbols},
            )
        )


class QuotesResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def get(
        self,
        symbols: list[str],
        *,
        refresh: bool = False,
        provider: str | None = None,
        allow_fallback: bool = False,
    ) -> QuoteBatch:
        return QuoteBatch.model_validate(
            self._transport.request(
                "POST",
                "/v1/quotes/query",
                json_body={
                    "symbols": symbols,
                    "refresh": refresh,
                    "provider": provider,
                    "allow_fallback": allow_fallback,
                },
            )
        )


class BarsResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def get(
        self,
        symbols: list[str],
        *,
        range: str = "1y",
        interval: str = "1d",
        adjustment: Literal["raw", "qfq", "hfq"] = "qfq",
        refresh: bool = False,
        provider: str | None = None,
        allow_fallback: bool = False,
        limit: int = 500,
    ) -> MarketBarResult:
        return MarketBarResult.model_validate(
            self._transport.request(
                "POST",
                "/v1/market-bars/query",
                json_body={
                    "symbols": symbols,
                    "range": range,
                    "interval": interval,
                    "adjustment": adjustment,
                    "refresh": refresh,
                    "provider": provider,
                    "allow_fallback": allow_fallback,
                    "limit": limit,
                },
            )
        )

    def canonical_page(
        self,
        instrument_id: str,
        *,
        start: datetime | str,
        end: datetime | str,
        interval: str,
        adjustment: Literal["raw", "qfq", "hfq"],
        page_size: int = 1000,
        cursor: str | None = None,
    ) -> CanonicalBarPage:
        params = {
            "start": _iso(start),
            "end": _iso(end),
            "interval": interval,
            "adjustment": adjustment,
            "page_size": page_size,
        }
        if cursor is not None:
            params["cursor"] = cursor
        return CanonicalBarPage.model_validate(
            self._transport.request(
                "GET",
                f"/v1/canonical-bars/{_path_value(instrument_id)}",
                params=params,
                retryable=True,
            )
        )

    def iter_canonical(self, instrument_id: str, **kwargs: Any) -> CanonicalSeries:
        first = self.canonical_page(instrument_id, **kwargs)
        return CanonicalSeries(
            first,
            lambda cursor: self.canonical_page(instrument_id, **kwargs, cursor=cursor),
        )


class FxResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def get(self, base: str, symbols: list[str], *, refresh: bool = False) -> FxResponse:
        return FxResponse.model_validate(
            self._transport.request(
                "GET",
                "/v1/fx",
                params={"base": base, "symbols": ",".join(symbols), "refresh": refresh},
            )
        )


class DividendsResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def get(self, symbol: str, fiscal_year: int) -> DocumentResponse:
        return DocumentResponse.model_validate(
            self._transport.request(
                "GET",
                f"/v1/dividends/{_path_value(symbol)}",
                params={"fiscal_year": fiscal_year},
            )
        )

    def get_fund_history(
        self,
        symbol: str,
        *,
        start: str,
        end: str,
        refresh: bool = False,
    ) -> DocumentResponse:
        return DocumentResponse.model_validate(
            self._transport.request(
                "GET",
                f"/v1/funds/{_path_value(symbol)}/dividends",
                params={"from": start, "to": end, "refresh": refresh},
            )
        )


class ExposureResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._transport = transport

    def get(self, symbol: str, *, refresh: bool = False) -> DocumentResponse:
        return DocumentResponse.model_validate(
            self._transport.request(
                "GET",
                f"/v1/exposure-facts/{_path_value(symbol)}",
                params={"refresh": refresh},
            )
        )


class AsyncHealthResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def check(self) -> HealthResponse:
        return HealthResponse.model_validate(
            await self._transport.request("GET", "/v1/health", retryable=True)
        )


class AsyncInstrumentsResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def search(self, query: str, *, limit: int = 12) -> InstrumentSearchResult:
        return InstrumentSearchResult.model_validate(
            await self._transport.request(
                "GET", "/v1/instruments/search", params={"q": query, "limit": limit}, retryable=True
            )
        )

    async def get(self, instrument_id: str) -> Instrument:
        return Instrument.model_validate(
            await self._transport.request(
                "GET", f"/v1/instruments/{_path_value(instrument_id)}", retryable=True
            )
        )

    async def resolve(self, namespace: str, external_symbol: str) -> Instrument:
        return Instrument.model_validate(
            await self._transport.request(
                "GET",
                "/v1/instruments:resolve",
                params={"namespace": namespace, "external_symbol": external_symbol},
                retryable=True,
            )
        )

    async def resolve_many(self, namespace: str, symbols: list[str]) -> InstrumentResolveBatch:
        return InstrumentResolveBatch.model_validate(
            await self._transport.request(
                "POST",
                "/v1/instruments:resolve/query",
                json_body={"namespace": namespace, "symbols": symbols},
            )
        )


class AsyncQuotesResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def get(
        self,
        symbols: list[str],
        *,
        refresh: bool = False,
        provider: str | None = None,
        allow_fallback: bool = False,
    ) -> QuoteBatch:
        return QuoteBatch.model_validate(
            await self._transport.request(
                "POST",
                "/v1/quotes/query",
                json_body={
                    "symbols": symbols,
                    "refresh": refresh,
                    "provider": provider,
                    "allow_fallback": allow_fallback,
                },
            )
        )


class AsyncBarsResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def get(self, symbols: list[str], **kwargs: Any) -> MarketBarResult:
        refresh = bool(kwargs.get("refresh", False))
        body = {
            "symbols": symbols,
            "range": kwargs.get("range", "1y"),
            "interval": kwargs.get("interval", "1d"),
            "adjustment": kwargs.get("adjustment", "qfq"),
            "refresh": refresh,
            "provider": kwargs.get("provider"),
            "allow_fallback": kwargs.get("allow_fallback", False),
            "limit": kwargs.get("limit", 500),
        }
        return MarketBarResult.model_validate(
            await self._transport.request("POST", "/v1/market-bars/query", json_body=body)
        )

    async def canonical_page(
        self,
        instrument_id: str,
        *,
        start: datetime | str,
        end: datetime | str,
        interval: str,
        adjustment: Literal["raw", "qfq", "hfq"],
        page_size: int = 1000,
        cursor: str | None = None,
    ) -> CanonicalBarPage:
        params = {
            "start": _iso(start),
            "end": _iso(end),
            "interval": interval,
            "adjustment": adjustment,
            "page_size": page_size,
        }
        if cursor is not None:
            params["cursor"] = cursor
        return CanonicalBarPage.model_validate(
            await self._transport.request(
                "GET",
                f"/v1/canonical-bars/{_path_value(instrument_id)}",
                params=params,
                retryable=True,
            )
        )

    async def iter_canonical(self, instrument_id: str, **kwargs: Any) -> AsyncCanonicalSeries:
        first = await self.canonical_page(instrument_id, **kwargs)

        async def fetch(cursor: str) -> CanonicalBarPage:
            return await self.canonical_page(instrument_id, **kwargs, cursor=cursor)

        return AsyncCanonicalSeries(first, fetch)


class AsyncFxResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def get(self, base: str, symbols: list[str], *, refresh: bool = False) -> FxResponse:
        return FxResponse.model_validate(
            await self._transport.request(
                "GET",
                "/v1/fx",
                params={"base": base, "symbols": ",".join(symbols), "refresh": refresh},
            )
        )


class AsyncDividendsResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def get(self, symbol: str, fiscal_year: int) -> DocumentResponse:
        return DocumentResponse.model_validate(
            await self._transport.request(
                "GET",
                f"/v1/dividends/{_path_value(symbol)}",
                params={"fiscal_year": fiscal_year},
            )
        )

    async def get_fund_history(
        self, symbol: str, *, start: str, end: str, refresh: bool = False
    ) -> DocumentResponse:
        return DocumentResponse.model_validate(
            await self._transport.request(
                "GET",
                f"/v1/funds/{_path_value(symbol)}/dividends",
                params={"from": start, "to": end, "refresh": refresh},
            )
        )


class AsyncExposureResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._transport = transport

    async def get(self, symbol: str, *, refresh: bool = False) -> DocumentResponse:
        return DocumentResponse.model_validate(
            await self._transport.request(
                "GET",
                f"/v1/exposure-facts/{_path_value(symbol)}",
                params={"refresh": refresh},
            )
        )
