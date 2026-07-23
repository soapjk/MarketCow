from __future__ import annotations

import re
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Iterable, Mapping


REALTIME_QUOTE = "realtime_quote"
MARKET_BAR_HISTORY = "market_bar_history"

_PROVIDER_ID = re.compile(r"^[a-z][a-z0-9_]{1,31}$")
_MARKETS = frozenset({"CN", "US", "HK", "FX"})
_QUOTE_REQUIRED_FIELDS = frozenset({
    "instrument_id",
    "symbol",
    "market",
    "price",
    "source",
    "source_url",
    "raw_response_locator",
    "_raw_payload",
})


@dataclass(frozen=True)
class CapabilityDeclaration:
    """One externally routed capability implemented by a provider adapter."""

    name: str
    markets: frozenset[str]
    operation: str

    def __post_init__(self) -> None:
        if not self.name or not self.operation.isidentifier():
            raise ValueError("capability name and operation are required")
        if not self.markets or not self.markets <= _MARKETS:
            raise ValueError("capability markets must be a non-empty supported subset")


@dataclass(frozen=True)
class ProviderManifest:
    """Static, reviewable provider metadata. It contains no credentials."""

    provider_id: str
    source_name: str
    capabilities: tuple[CapabilityDeclaration, ...]

    def __post_init__(self) -> None:
        if not _PROVIDER_ID.fullmatch(self.provider_id):
            raise ValueError("provider_id must be lowercase snake_case")
        if not self.source_name.strip():
            raise ValueError("source_name is required")
        names = [item.name for item in self.capabilities]
        if not names or len(names) != len(set(names)):
            raise ValueError("provider capability names must be non-empty and unique")

    def capability(self, name: str) -> CapabilityDeclaration | None:
        return next((item for item in self.capabilities if item.name == name), None)


DEFAULT_PROVIDER_MANIFESTS = (
    ProviderManifest("tushare", "tushare_via_stockai888", (
        CapabilityDeclaration(REALTIME_QUOTE, frozenset({"CN"}), "realtime_quote"),
        CapabilityDeclaration(MARKET_BAR_HISTORY, frozenset({"CN"}), "call"),
    )),
    ProviderManifest("sina", "sina_finance_hq", (
        CapabilityDeclaration(REALTIME_QUOTE, frozenset({"CN"}), "fetch_quote"),
    )),
    ProviderManifest("eastmoney", "eastmoney_quote_center", (
        CapabilityDeclaration(REALTIME_QUOTE, frozenset({"CN"}), "fetch_quote"),
    )),
    ProviderManifest("yahoo", "yahoo_chart", (
        CapabilityDeclaration(REALTIME_QUOTE, frozenset({"US", "HK", "FX"}), "fetch_quote"),
        CapabilityDeclaration(
            MARKET_BAR_HISTORY, frozenset({"US", "HK", "FX", "CN"}), "fetch_history"
        ),
    )),
    ProviderManifest("longport", "longbridge_openapi", (
        CapabilityDeclaration(REALTIME_QUOTE, frozenset({"CN", "US", "HK"}), "fetch_quote"),
    )),
)


class ProviderRegistry:
    """Binds reviewed manifests to runtime adapters without owning their lifecycle."""

    def __init__(self, manifests: Iterable[ProviderManifest]):
        entries = tuple(manifests)
        mapping = {item.provider_id: item for item in entries}
        if len(mapping) != len(entries):
            raise ValueError("duplicate provider manifest")
        self._manifests: Mapping[str, ProviderManifest] = MappingProxyType(mapping)
        self._adapters: dict[str, Any] = {}

    @property
    def manifests(self) -> tuple[ProviderManifest, ...]:
        return tuple(self._manifests.values())

    def bind(
        self,
        provider_id: str,
        adapter: Any,
        capabilities: Iterable[str] | None = None,
    ) -> None:
        manifest = self._manifests.get(provider_id)
        if manifest is None:
            raise KeyError(f"unknown provider {provider_id!r}")
        if provider_id in self._adapters:
            raise ValueError(f"provider {provider_id!r} is already bound")
        selected = frozenset(capabilities) if capabilities is not None else frozenset(
            item.name for item in manifest.capabilities
        )
        declared = frozenset(item.name for item in manifest.capabilities)
        unknown = sorted(selected - declared)
        if unknown:
            raise ValueError(
                f"provider {provider_id!r} does not declare capabilities: {', '.join(unknown)}"
            )
        missing = [
            item.operation
            for item in manifest.capabilities
            if item.name in selected and not callable(getattr(adapter, item.operation, None))
        ]
        if missing:
            raise TypeError(
                f"provider {provider_id!r} is missing operations: {', '.join(sorted(missing))}"
            )
        self._adapters[provider_id] = adapter

    def get(self, provider_id: str) -> Any:
        try:
            return self._adapters[provider_id]
        except KeyError as exc:
            raise KeyError(f"provider {provider_id!r} is not bound") from exc

    def supported(self, capability: str, market: str) -> tuple[str, ...]:
        return tuple(
            manifest.provider_id
            for manifest in self._manifests.values()
            if (item := manifest.capability(capability)) is not None and market in item.markets
        )


def validate_realtime_quote(payload: Mapping[str, Any]) -> None:
    """Reusable contract check for normalized realtime quote adapter output."""

    missing = sorted(_QUOTE_REQUIRED_FIELDS - payload.keys())
    if missing:
        raise ValueError("realtime quote is missing fields: " + ", ".join(missing))
    if payload["market"] not in _MARKETS:
        raise ValueError("realtime quote market is unsupported")
    if not isinstance(payload["price"], (int, float)) or isinstance(payload["price"], bool):
        raise ValueError("realtime quote price must be numeric")
    for field in ("instrument_id", "symbol", "source", "source_url", "raw_response_locator"):
        if not isinstance(payload[field], str) or not payload[field].strip():
            raise ValueError(f"realtime quote {field} must be a non-empty string")
