from __future__ import annotations

import re
from dataclasses import dataclass


_INSTRUMENT_ID = re.compile(
    r"^([A-Z0-9][A-Z0-9.-]{0,31})\.([A-Z0-9]{4})$"
)
_A_SHARE_EXTERNAL = re.compile(r"^(\d{6})\.(SH|SS|SZ|BJ)$")
_HK_EXTERNAL = re.compile(r"^(\d{1,5})\.HK$")
_US_EXTERNAL = re.compile(r"^([A-Z][A-Z0-9-]{0,31})\.US$")

_MIC_MARKET = {
    "XSHG": "CN",
    "XSHE": "CN",
    "XBSE": "CN",
    "XHKG": "HK",
    "XNAS": "US",
    "XNYS": "US",
    "HYPL": "CRYPTO",
}
_CN_MIC_SUFFIX = {
    "XSHG": "SH",
    "XSHE": "SZ",
    "XBSE": "BJ",
}


@dataclass(frozen=True)
class CanonicalInstrument:
    instrument_id: str
    symbol: str
    market: str
    mic: str

    def provider_symbol(self, namespace: str) -> str:
        """Return one explicit external namespace mapping for this identity."""
        normalized = _namespace(namespace)
        if self.market == "CN":
            suffix = _CN_MIC_SUFFIX.get(self.mic)
            if suffix is None:
                raise ValueError(f"{self.mic} has no A-share provider mapping")
            if normalized in {
                "provider:tushare", "provider:longport", "provider:yahoo",
                "provider:eastmoney", "provider:sina", "broker:longport",
            }:
                if normalized == "provider:yahoo" and suffix == "SH":
                    suffix = "SS"
                return f"{self.symbol}.{suffix}"
        elif self.market == "HK":
            if normalized == "provider:yahoo":
                return f"{self.symbol.zfill(4)}.HK"
            if normalized in {"provider:longport", "broker:longport"}:
                return f"{self.symbol}.HK"
        elif self.market == "US":
            if normalized in {"provider:longport", "broker:longport"}:
                return f"{self.symbol.replace('-', '.')}.US"
            if normalized in {"provider:yahoo", "provider:sec"}:
                return self.symbol
        elif self.mic == "HYPL" and normalized == "provider:hyperliquid":
            return self.symbol.removesuffix("-PERP")
        raise ValueError(
            f"instrument {self.instrument_id} has no mapping for {normalized}"
        )


def _namespace(value: str) -> str:
    namespace = str(value or "").strip().lower()
    if not re.fullmatch(r"(?:provider|broker):[a-z0-9_-]+", namespace):
        raise ValueError("namespace must use provider:<name> or broker:<name>")
    return namespace


def canonical_instrument(value: str) -> CanonicalInstrument:
    """Parse MarketCow's sole internal identity: provider-neutral SYMBOL.MIC."""
    raw = str(value or "").strip().upper().replace(" ", "")
    match = _INSTRUMENT_ID.fullmatch(raw)
    if match is None:
        raise ValueError("instrument_id must use provider-neutral SYMBOL.MIC")
    symbol, mic = match.groups()
    market = _MIC_MARKET.get(mic)
    if market is None:
        if mic.endswith("H") and symbol.endswith("-PERP"):
            market = "US"
        else:
            raise ValueError(f"unsupported or unmapped MIC {mic}")
    if market == "CN" and not (len(symbol) == 6 and symbol.isdigit()):
        raise ValueError("A-share canonical symbol must contain six digits")
    if market == "HK":
        if not (1 <= len(symbol) <= 5 and symbol.isdigit()):
            raise ValueError("Hong Kong canonical symbol must contain one to five digits")
        symbol = symbol.lstrip("0") or "0"
    instrument_id = f"{symbol}.{mic}"
    return CanonicalInstrument(instrument_id, symbol, market, mic)


def external_instrument(
    namespace: str, external_symbol: str, *, mic: str | None = None
) -> CanonicalInstrument:
    """Resolve an external symbol only through an explicit namespace boundary."""
    normalized_namespace = _namespace(namespace)
    symbol = str(external_symbol or "").strip().upper().replace(" ", "")
    a_share = _A_SHARE_EXTERNAL.fullmatch(symbol)
    if a_share and normalized_namespace in {
        "provider:tushare", "provider:longport", "provider:yahoo",
        "provider:eastmoney", "provider:sina", "broker:longport",
    }:
        code, suffix = a_share.groups()
        resolved_mic = {"SH": "XSHG", "SS": "XSHG", "SZ": "XSHE", "BJ": "XBSE"}[
            suffix
        ]
        if mic is not None and mic.upper() != resolved_mic:
            raise ValueError("external symbol conflicts with explicit MIC")
        return canonical_instrument(f"{code}.{resolved_mic}")
    hk = _HK_EXTERNAL.fullmatch(symbol)
    if hk and normalized_namespace in {
        "provider:longport", "provider:yahoo", "broker:longport",
    }:
        if mic is not None and mic.upper() != "XHKG":
            raise ValueError("external symbol conflicts with explicit MIC")
        return canonical_instrument(f"{hk.group(1)}.XHKG")
    us = _US_EXTERNAL.fullmatch(symbol)
    if us and normalized_namespace in {
        "provider:longport", "broker:longport",
    }:
        if mic is None:
            raise ValueError("US external symbols require an explicit MIC mapping")
        return canonical_instrument(f"{us.group(1)}.{mic.upper()}")
    if normalized_namespace in {"provider:yahoo", "provider:sec"}:
        if mic is None:
            raise ValueError("US external symbols require an explicit MIC mapping")
        return canonical_instrument(f"{symbol}.{mic.upper()}")
    raise ValueError(
        f"unsupported external symbol for namespace {normalized_namespace}"
    )
