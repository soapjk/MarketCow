from __future__ import annotations

import re
from dataclasses import dataclass


_US = re.compile(r"^[A-Z][A-Z0-9]*(?:[.-][A-Z0-9]+)?$")


@dataclass(frozen=True)
class CanonicalInstrument:
    instrument_id: str
    symbol: str
    market: str
    exchange: str


def canonical_instrument(value: str) -> CanonicalInstrument:
    raw = str(value).strip().upper()
    if not raw:
        raise ValueError("symbol is required")
    compact = raw.replace(" ", "")
    suffixes = {
        ".SH": ("CN", "SSE"), ".SS": ("CN", "SSE"),
        ".SZ": ("CN", "SZSE"), ".BJ": ("CN", "BSE"),
    }
    for suffix, (market, exchange) in suffixes.items():
        if compact.endswith(suffix):
            code = compact[:-len(suffix)]
            if not (len(code) == 6 and code.isdigit()):
                raise ValueError("A-share symbol must contain six digits")
            canonical = code + (".SH" if exchange == "SSE" else suffix)
            return CanonicalInstrument(f"{market}:{exchange}:{code}", canonical, market, exchange)
    if len(compact) == 6 and compact.isdigit():
        exchange = "SSE" if compact[0] in "569" else "SZSE"
        suffix = ".SH" if exchange == "SSE" else ".SZ"
        return CanonicalInstrument(
            f"CN:{exchange}:{compact}", compact + suffix, "CN", exchange
        )
    if compact.isdigit() or compact.endswith(".HK"):
        code = compact.removesuffix(".HK")
        if not (1 <= len(code) <= 5 and code.isdigit()):
            raise ValueError("Hong Kong symbol must contain one to five digits")
        code = code.zfill(5)
        return CanonicalInstrument(f"HK:HKEX:{code}", f"{code}.HK", "HK", "HKEX")
    if _US.fullmatch(compact):
        ticker = compact.replace(".", "-")
        return CanonicalInstrument(f"US:US:{ticker}", ticker, "US", "US")
    raise ValueError("unsupported security symbol")
