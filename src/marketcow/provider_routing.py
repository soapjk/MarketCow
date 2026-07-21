from __future__ import annotations

from typing import Iterable

from .providers import contracts
from .providers.contracts import DEFAULT_PROVIDER_MANIFESTS, ProviderRegistry


REALTIME_QUOTE = contracts.REALTIME_QUOTE
MARKET_BAR_HISTORY = contracts.MARKET_BAR_HISTORY



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

    def detail(self) -> dict[str, str]:
        return {**super().detail(), "status": "unavailable"}


_CATALOG = ProviderRegistry(DEFAULT_PROVIDER_MANIFESTS)


def supported_providers(capability: str, market: str) -> tuple[str, ...]:
    return _CATALOG.supported(capability, market)


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
