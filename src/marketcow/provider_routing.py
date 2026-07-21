from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


REALTIME_QUOTE = "realtime_quote"
MARKET_BAR_HISTORY = "market_bar_history"


class ProviderRoutingError(ValueError):
    code = "provider_routing_error"

    def __init__(self, message: str, *, provider: str = "", capability: str = "", market: str = ""):
        super().__init__(message)
        self.provider = provider
        self.capability = capability
        self.market = market

    def detail(self) -> dict[str, str]:
        return {
            "code": self.code,
            "message": str(self),
            "provider": self.provider,
            "capability": self.capability,
            "market": self.market,
        }


class ProviderNotSupported(ProviderRoutingError):
    code = "provider_not_supported"


class ProviderUnavailable(ProviderRoutingError):
    code = "provider_unavailable"


@dataclass(frozen=True)
class ProviderCapability:
    provider: str
    capability: str
    markets: frozenset[str]

    def supports(self, capability: str, market: str) -> bool:
        return self.capability == capability and market in self.markets


CAPABILITIES = (
    ProviderCapability("tushare", REALTIME_QUOTE, frozenset({"CN"})),
    ProviderCapability("sina", REALTIME_QUOTE, frozenset({"CN"})),
    ProviderCapability("eastmoney", REALTIME_QUOTE, frozenset({"CN"})),
    ProviderCapability("yahoo", REALTIME_QUOTE, frozenset({"US", "HK", "FX"})),
    ProviderCapability("tushare", MARKET_BAR_HISTORY, frozenset({"CN"})),
    ProviderCapability("yahoo", MARKET_BAR_HISTORY, frozenset({"US", "HK", "FX", "CN"})),
)


def supported_providers(capability: str, market: str) -> tuple[str, ...]:
    return tuple(item.provider for item in CAPABILITIES if item.supports(capability, market))


def select_providers(
    capability: str,
    market: str,
    requested: str | None,
    priority: Iterable[str],
    *,
    allow_fallback: bool,
) -> tuple[str, ...]:
    supported = supported_providers(capability, market)
    normalized = (requested or "").strip().lower()
    if normalized:
        if normalized not in supported:
            raise ProviderNotSupported(
                f"provider {normalized!r} does not support {capability} for market {market}",
                provider=normalized,
                capability=capability,
                market=market,
            )
        if not allow_fallback:
            return (normalized,)
        return (normalized,) + tuple(item for item in priority if item in supported and item != normalized)
    ordered = tuple(item for item in priority if item in supported)
    return ordered + tuple(item for item in supported if item not in ordered)
