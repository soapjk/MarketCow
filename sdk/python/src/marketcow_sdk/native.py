"""Thin public facade over the shared Rust client; no Python state machine."""

import asyncio
from enum import IntEnum
from typing import Any


class Source(IntEnum):
    BINANCE_BTCUSDT = 1
    CHAINLINK_BTC_USD = 2
    CHAINLINK_TWAP60 = 3


def _native_client(endpoint: str, api_key: str | None) -> Any:
    try:
        from ._native import NativeClient
    except ImportError as exc:
        raise ImportError(
            "MarketCow native extension is missing. Install the platform wheel or build "
            "sdk/python with maturin; the legacy REST clients remain separately available."
        ) from exc
    return NativeClient(endpoint, api_key)


class _Iterator:
    def __init__(self, factory: Any) -> None:
        self._factory = factory
        self._inner: Any = None
        self._closed = False

    def __enter__(self) -> "_Iterator":
        self._open()
        return self

    def _open(self) -> None:
        if self._inner is None and not self._closed:
            self._inner = self._factory()

    def __iter__(self) -> "_Iterator":
        return self

    def snapshot(self, depth: int | None = None) -> Any:
        """Stable book copy, only for market subscriptions. No network request."""
        if self._closed:
            raise RuntimeError("subscription is closed")
        self._open()
        return self._inner.snapshot(depth)

    def __next__(self) -> Any:
        if self._closed:
            raise StopIteration
        self._open()
        value = self._inner.next()
        if value is None:
            raise StopIteration
        return value

    def close(self) -> None:
        self._closed = True
        if self._inner is not None:
            self._inner.close()

    def __exit__(self, _kind: Any, exc: BaseException | None, _tb: Any) -> None:
        try:
            self.close()
        except Exception as cleanup:
            if exc is None:
                raise
            exc.add_note(f"MarketCow cleanup also failed: {cleanup}")

    def save_checkpoint(self, path: str) -> None:
        """Fixed-ID history only; save after application consumption."""
        if self._closed:
            raise RuntimeError("iterator is closed")
        self._open()
        self._inner.save_checkpoint(path)

    def lifecycle(self) -> Any:
        """Fixed-ID job receipt; usable after close. No application install claim."""
        self._open()
        if self._inner is None:
            raise RuntimeError("history was closed before allocation")
        return self._inner.lifecycle()


class _AsyncIterator:
    def __init__(self, factory: Any) -> None:
        self._factory = factory
        self._inner: Any = None
        self._closed = False
        self._open_lock = asyncio.Lock()

    async def __aenter__(self) -> "_AsyncIterator":
        await self._open()
        return self

    async def _open(self) -> None:
        async with self._open_lock:
            if self._inner is None and not self._closed:
                inner = await self._factory()
                if self._closed:
                    await inner.close_async()
                else:
                    self._inner = inner

    def __aiter__(self) -> "_AsyncIterator":
        return self

    async def snapshot(self, depth: int | None = None) -> Any:
        if self._closed:
            raise RuntimeError("subscription is closed")
        await self._open()
        return await self._inner.snapshot_async(depth)

    async def __anext__(self) -> Any:
        if self._closed:
            raise StopAsyncIteration
        await self._open()
        if self._closed:
            raise StopAsyncIteration
        value = await self._inner.next_async()
        if value is None:
            raise StopAsyncIteration
        return value

    async def save_checkpoint(self, path: str) -> None:
        if self._closed:
            raise RuntimeError("iterator is closed")
        await self._open()
        await self._inner.save_checkpoint_async(path)

    async def lifecycle(self) -> Any:
        await self._open()
        if self._inner is None:
            raise RuntimeError("history was closed before allocation")
        return await self._inner.lifecycle_async()

    async def close(self) -> None:
        self._closed = True
        if self._inner is not None:
            await self._inner.close_async()

    async def __aexit__(self, _kind: Any, exc: BaseException | None, _tb: Any) -> None:
        try:
            await self.close()
        except Exception as cleanup:
            if exc is None:
                raise
            exc.add_note(f"MarketCow cleanup also failed: {cleanup}")


class _Prices:
    def __init__(self, client: Any, asynchronous: bool) -> None:
        self._client = client
        self._asynchronous = asynchronous

    def latest(self, source: Source = Source.BINANCE_BTCUSDT) -> Any:
        if self._asynchronous:
            return self._client.latest_price_async(int(source))
        return self._client.latest_price(int(source))

    def subscribe(self, source: Source = Source.BINANCE_BTCUSDT) -> Any:
        source = Source(source)
        if self._asynchronous:
            return _AsyncIterator(lambda: self._client.subscribe_async(int(source)))
        return _Iterator(lambda: self._client.subscribe(int(source)))


class _History:
    def __init__(self, client: Any, asynchronous: bool) -> None:
        self._client = client
        self._asynchronous = asynchronous

    def resume(self, path: str) -> Any:
        """Resume exact fixed-ID session; expired sessions fail, never refreeze."""
        if self._asynchronous:
            return _AsyncIterator(lambda: self._client.resume_history_async(path))
        return _Iterator(lambda: self._client.resume_history(path))

    def trades(
        self, *, start_time_ms: int, end_time_ms: int | None = None, limit: int = 1000
    ) -> Any:
        """Indexed DB history, [start,end); no end means persisted DB highwater."""
        if self._asynchronous:
            return _AsyncIterator(
                lambda: self._client.time_history_async(start_time_ms, end_time_ms, limit)
            )
        return _Iterator(lambda: self._client.time_history(start_time_ms, end_time_ms, limit))

    def observations(
        self, source: Source, *, start_time_ms: int, end_time_ms: int, limit: int = 1000
    ) -> Any:
        """Chainlink indexed DB records, automatic bounded time segments.

        Ordinary observations and TWAP60 remain separate sources. This does not
        start collection, fill gaps or silently fall back to archived JSONL.
        """

        def create() -> Any:
            return self._client.observation_range(int(source), start_time_ms, end_time_ms, limit)

        if self._asynchronous:
            return _AsyncIterator(
                lambda: self._client.observation_range_async(
                    int(source), start_time_ms, end_time_ms, limit
                )
            )
        return _Iterator(create)

    def trades_through(
        self,
        *,
        start_time_ms: int,
        through_trade_id: int,
        limit: int = 1000,
        release_on_complete: bool = True,
    ) -> Any:
        """Fixed-ID freeze. False retains until close, e.g. join/install first.

        Complete means validated retrieval, NEVER application installation.
        Errors, cancellation, Client close and TTL still release/expire resources.
        """

        def create() -> Any:
            owner = self._client.trade_history(start_time_ms, through_trade_id, limit)
            if not release_on_complete:
                owner.defer_release()
            return owner

        if self._asynchronous:

            async def create_async() -> Any:
                return create()  # Allocates a bounded handle, performs no I/O.

            return _AsyncIterator(create_async)
        return _Iterator(create)


class _Markets:
    def __init__(self, client: Any, asynchronous: bool) -> None:
        self._client = client
        self._asynchronous = asynchronous

    def directory(self) -> Any:
        """Complete source directory and budget; failures are never an empty list.

        Uses the same bounded discovery cache as list/current subscriptions.
        An empty successful directory or an absent budget is not a health claim.
        """
        if self._asynchronous:
            return self._client.market_directory_async()
        return self._client.market_directory()

    def list(self) -> Any:
        if self._asynchronous:
            return self._client.markets_async()
        return self._client.markets()

    def subscribe(self, market: Any) -> Any:
        if self._asynchronous:
            return _AsyncIterator(lambda: self._client.subscribe_market_async(market))
        return _Iterator(lambda: self._client.subscribe_market(market))

    def subscribe_current(self) -> Any:
        if self._asynchronous:

            async def create() -> Any:
                return self._client.follow_market()

            return _AsyncIterator(create)
        return _Iterator(self._client.follow_market)


class _PublicRest:
    """Explicit upstream public reads; not automatic trading preparation."""

    def __init__(self, client: Any, asynchronous: bool) -> None:
        self._client, self._asynchronous = client, asynchronous

    def btc_trade(self, source: Source = Source.BINANCE_BTCUSDT) -> Any:
        if self._asynchronous:
            return self._client.official_btc_trade_async(int(source))
        return self._client.official_btc_trade(int(source))

    def book(self, market: Any, *, token_id: str) -> Any:
        if self._asynchronous:
            return self._client.official_book_async(market, token_id)
        return self._client.official_book(market, token_id)


class _Bars:
    def __init__(self, client: Any, asynchronous: bool) -> None:
        self._client, self._asynchronous = client, asynchronous

    def latest(self, *, interval_ms: int = 1000) -> Any:
        """Existing finalized 300x1s or 60x1m window, not arbitrary bar history."""
        if self._asynchronous:
            return self._client.finalized_bars_async(interval_ms)
        return self._client.finalized_bars(interval_ms)

    def subscribe(self, *, interval_ms: int = 1000) -> Any:
        """Initial window, then new cached finalized bars; no upstream feed."""

        def create() -> Any:
            return self._client.subscribe_bars(interval_ms)

        if self._asynchronous:

            async def create_async() -> Any:
                return create()

            return _AsyncIterator(create_async)
        return _Iterator(create)


class Client:
    """Native synchronous client. Existing MarketCowClient remains the legacy API."""

    def __init__(self, endpoint: str, *, api_key: str | None = None) -> None:
        self._inner = _native_client(endpoint, api_key)
        self.prices = _Prices(self._inner, False)
        self.history = _History(self._inner, False)
        self.markets = _Markets(self._inner, False)
        self.bars = _Bars(self._inner, False)
        self.public_rest = _PublicRest(self._inner, False)

    def btc_capabilities(self) -> Any:
        """Implemented BTC features, not history coverage or entitlement."""
        return self._inner.btc_capabilities()

    def btc_health(self) -> Any:
        """Live source/writer diagnostics; not a trading freshness decision."""
        return self._inner.btc_health()

    def usage(self) -> Any:
        """Local payload counters, not supplier bytes or a provider bill."""
        return self._inner.usage()

    def server_usage(self) -> Any:
        """Configured access-layer payload quota; not a provider billing receipt."""
        return self._inner.server_usage()

    def rules(self, rules_id: str, revision: int) -> Any:
        return self._inner.rules(rules_id, revision)

    def terms(self, market: Any) -> Any:
        """Public facts; absent supplier fields and operator policies stay separate."""
        return self._inner.terms(market)

    def reference(self, market: Any) -> Any:
        return self._inner.reference(market)

    def finality(self, market: Any) -> Any:
        """Public payout fact, not an account redemption or cash receipt."""
        return self._inner.finality(market)

    def evidence(self, evidence_id: str) -> Any:
        return self._inner.evidence(evidence_id)

    def close(self) -> None:
        self._inner.close()

    def __enter__(self) -> "Client":
        self._inner.connect()
        return self

    def __exit__(self, _kind: Any, exc: BaseException | None, _tb: Any) -> None:
        try:
            self.close()
        except Exception as cleanup:
            if exc is None:
                raise
            exc.add_note(f"MarketCow cleanup also failed: {cleanup}")


class AsyncClient:
    """Async facade; network futures execute on the common Rust runtime."""

    def __init__(self, endpoint: str, *, api_key: str | None = None) -> None:
        self._inner = _native_client(endpoint, api_key)
        self.prices = _Prices(self._inner, True)
        self.history = _History(self._inner, True)
        self.markets = _Markets(self._inner, True)
        self.bars = _Bars(self._inner, True)
        self.public_rest = _PublicRest(self._inner, True)

    async def btc_capabilities(self) -> Any:
        return await self._inner.btc_capabilities_async()

    async def btc_health(self) -> Any:
        return await self._inner.btc_health_async()

    def usage(self) -> Any:
        return self._inner.usage()

    async def server_usage(self) -> Any:
        return await self._inner.server_usage_async()

    async def rules(self, rules_id: str, revision: int) -> Any:
        return await self._inner.rules_async(rules_id, revision)

    async def terms(self, market: Any) -> Any:
        return await self._inner.terms_async(market)

    async def reference(self, market: Any) -> Any:
        return await self._inner.reference_async(market)

    async def finality(self, market: Any) -> Any:
        return await self._inner.finality_async(market)

    async def evidence(self, evidence_id: str) -> Any:
        return await self._inner.evidence_async(evidence_id)

    async def close(self) -> None:
        await self._inner.close_async()

    async def __aenter__(self) -> "AsyncClient":
        await self._inner.connect_async()
        return self

    async def __aexit__(self, _kind: Any, exc: BaseException | None, _tb: Any) -> None:
        try:
            await self.close()
        except Exception as cleanup:
            if exc is None:
                raise
            exc.add_note(f"MarketCow cleanup also failed: {cleanup}")
