"""Typed clients for MarketCow's stable market-data surface."""

from typing import TYPE_CHECKING

from .async_client import AsyncMarketCowClient
from .auth import BearerTokenAuth, CallbackAuth, DualJwtAuth, NoAuth
from .client import MarketCowClient
from .config import ClientConfig, RetryConfig, TimeoutConfig
from .errors import *  # noqa: F403
from .native import AsyncClient, Client, Source

if TYPE_CHECKING:
    from ._native import Finality, Market, MarketDirectory, NativeError, Reference, Rules

__all__ = [
    "AsyncMarketCowClient",
    "AsyncClient",
    "Client",
    "Source",
    "NativeError",
    "Market",
    "MarketDirectory",
    "Reference",
    "Finality",
    "Rules",
    "BearerTokenAuth",
    "CallbackAuth",
    "ClientConfig",
    "DualJwtAuth",
    "MarketCowClient",
    "NoAuth",
    "RetryConfig",
    "TimeoutConfig",
]

__version__ = "0.1.2"


def __getattr__(name: str):
    if name in {"NativeError", "Market", "MarketDirectory", "Reference", "Finality", "Rules"}:
        from . import _native

        return getattr(_native, name)
    raise AttributeError(name)
