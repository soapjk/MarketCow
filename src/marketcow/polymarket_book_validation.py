"""Dependency-light book validation shared by live reads and offline history."""

from decimal import Decimal
from typing import Any


def _validate_book(state: dict[str, Any]) -> None:
    tick = Decimal(state["tick_size"])
    if tick <= 0:
        raise ValueError("tick_size must be positive")
    for side in ("bids", "asks"):
        for price_text, size_text in state[side].items():
            price, size = Decimal(price_text), Decimal(size_text)
            if price < 0 or price > 1 or price % tick != 0:
                raise ValueError("book price must be within [0,1] and tick aligned")
            if size < 0:
                raise ValueError("book size must be nonnegative")
    if state["bids"] and state["asks"]:
        if max(map(Decimal, state["bids"])) >= min(map(Decimal, state["asks"])):
            raise ValueError("order book must not be crossed or locked")
