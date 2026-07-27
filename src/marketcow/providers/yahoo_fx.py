from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from .yahoo_quote import YahooQuoteProvider, _float, _iso_utc


SUPPORTED_FX_CURRENCIES = frozenset({"USD", "CNY", "HKD"})


class FxRateError(RuntimeError):
    def __init__(self, code: str, message: str, *, currency: str | None = None):
        super().__init__(message)
        self.code = code
        self.currency = currency


@dataclass(frozen=True)
class _CachedRate:
    rate_per_usd: float
    as_of: str
    fetched_at: datetime
    source_url: str


class YahooFxProvider:
    """Auditable USD cross rates backed by MarketCow's Yahoo chart provider."""

    source = "yahoo_chart"

    def __init__(
        self,
        quote_provider: YahooQuoteProvider | None = None,
        *,
        cache_ttl_seconds: float = 900,
        stale_max_seconds: float = 86400,
        now_provider: Callable[[], datetime] | None = None,
    ):
        self.quote_provider = quote_provider or YahooQuoteProvider()
        self.cache_ttl_seconds = max(0.0, float(cache_ttl_seconds))
        self.stale_max_seconds = max(
            self.cache_ttl_seconds, float(stale_max_seconds)
        )
        self.now_provider = now_provider or (lambda: datetime.now(timezone.utc))
        self._cache: dict[str, _CachedRate] = {}
        self._lock = threading.RLock()

    @staticmethod
    def _normalize_currency(value: str) -> str:
        currency = str(value or "").strip().upper()
        if currency not in SUPPORTED_FX_CURRENCIES:
            raise ValueError(
                "currency must be one of USD, CNY or HKD"
            )
        return currency

    @staticmethod
    def _quote_value(result: dict[str, Any]) -> tuple[float, str]:
        meta = result.get("meta") or {}
        value = _float(meta.get("regularMarketPrice"))
        timestamp = meta.get("regularMarketTime")
        timestamps = result.get("timestamp") or []
        closes = (
            ((result.get("indicators") or {}).get("quote") or [{}])[0]
            .get("close") or []
        )
        for bar_at, close in reversed(list(zip(timestamps, closes))):
            candidate = _float(close)
            if candidate is not None:
                value = candidate
                timestamp = bar_at
                break
        if value is None or value <= 0:
            raise FxRateError("no_data", "Yahoo returned no usable FX rate")
        as_of = _iso_utc(timestamp)
        if not as_of:
            raise FxRateError("no_data", "Yahoo FX rate has no market timestamp")
        return value, as_of

    def _fetch_rate(self, currency: str, fetched_at: datetime) -> _CachedRate:
        provider_symbol = f"{currency}=X"
        params = {
            "range": "1d",
            "interval": "5m",
            "includePrePost": "true",
            "events": "",
            "includeAdjustedClose": "false",
        }
        try:
            payload, source_url = self.quote_provider._fetch_chart(
                provider_symbol, params
            )
        except Exception as exc:
            raise FxRateError(
                "provider_unavailable",
                f"Yahoo FX request failed for {currency}: {exc}",
                currency=currency,
            ) from exc
        try:
            result = self.quote_provider._result(payload)
        except Exception as exc:
            raise FxRateError(
                "no_data",
                f"Yahoo returned no FX data for {currency}: {exc}",
                currency=currency,
            ) from exc
        try:
            value, as_of = self._quote_value(result)
        except FxRateError as exc:
            raise FxRateError(
                exc.code, str(exc), currency=currency
            ) from exc
        return _CachedRate(value, as_of, fetched_at, source_url)

    @staticmethod
    def _age_seconds(now: datetime, item: _CachedRate) -> float:
        return max(0.0, (now - item.fetched_at).total_seconds())

    def get_rates(
        self, base: str, symbols: list[str], *, refresh: bool = False
    ) -> dict[str, Any]:
        base = self._normalize_currency(base)
        targets = [self._normalize_currency(symbol) for symbol in symbols]
        if not targets:
            raise ValueError("at least one target currency is required")
        if len(targets) != len(set(targets)):
            raise ValueError("target currencies must be unique")
        now = self.now_provider().astimezone(timezone.utc)
        required = sorted(({base, *targets} - {"USD"}))
        selected: dict[str, _CachedRate] = {}
        errors: list[dict[str, str]] = []
        cached = True
        stale = False

        with self._lock:
            for currency in required:
                existing = self._cache.get(currency)
                age = (
                    self._age_seconds(now, existing)
                    if existing is not None else None
                )
                if (
                    not refresh
                    and existing is not None
                    and age is not None
                    and age <= self.cache_ttl_seconds
                ):
                    selected[currency] = existing
                    continue
                try:
                    item = self._fetch_rate(currency, now)
                    self._cache[currency] = item
                    selected[currency] = item
                    cached = False
                except FxRateError as exc:
                    if (
                        existing is not None
                        and age is not None
                        and age <= self.stale_max_seconds
                    ):
                        selected[currency] = existing
                        stale = True
                        errors.append({
                            "currency": currency,
                            "code": exc.code,
                            "message": str(exc),
                        })
                        continue
                    code = (
                        "stale_data"
                        if existing is not None else exc.code
                    )
                    message = (
                        f"cached FX rate for {currency} is older than "
                        f"{self.stale_max_seconds:g} seconds"
                        if code == "stale_data" else str(exc)
                    )
                    raise FxRateError(
                        code, message, currency=currency
                    ) from exc

        per_usd = {"USD": 1.0}
        per_usd.update({
            currency: selected[currency].rate_per_usd
            for currency in required
        })
        base_per_usd = per_usd[base]
        rates = {base: 1.0}
        rates.update({
            currency: per_usd[currency] / base_per_usd
            for currency in targets
        })
        points = [selected[currency] for currency in required]
        as_of = min(
            (item.as_of for item in points),
            default=now.isoformat(timespec="seconds"),
        )
        fetched_at = max(
            (item.fetched_at for item in points),
            default=now,
        ).isoformat(timespec="seconds")
        return {
            "base": base,
            "rates": rates,
            "source": self.source,
            "source_urls": {
                currency: selected[currency].source_url
                for currency in required
            },
            "as_of": as_of,
            "fetched_at": fetched_at,
            "ingested_at": now.isoformat(timespec="seconds"),
            "cached": cached,
            "stale": stale,
            "cache_status": (
                "stale_if_error" if stale else "hit" if cached else "refreshed"
            ),
            "cache_ttl_seconds": self.cache_ttl_seconds,
            "stale_max_seconds": self.stale_max_seconds,
            "errors": errors,
        }
