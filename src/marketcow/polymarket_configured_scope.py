"""Lightweight shared read-only Shadow scope contract (no API/storage imports)."""
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .polymarket_contracts import content_sha256


class PolymarketConfiguredMarket(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    market_id: str = Field(min_length=1)
    condition_id: str = Field(min_length=1)
    token_ids: tuple[str, ...] = Field(min_length=2, max_length=2)
    end_at: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_identity(self) -> "PolymarketConfiguredMarket":
        if len(set(self.token_ids)) != 2 or any(not token for token in self.token_ids):
            raise ValueError("configured market token IDs must be distinct")
        datetime.fromisoformat(self.end_at.replace("Z", "+00:00"))
        return self


class PolymarketConfiguredScope(BaseModel):
    """Explicit read-only incumbent supplied to an isolated Shadow API."""
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["marketcow.polymarket.scope-discovery.v1"]
    mode: Literal["shadow"]
    active_scope_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    catalog_revision: str = Field(pattern=r"^[0-9a-f]{64}$")
    configured_market_count: int = Field(ge=0, le=250)
    configured_markets: tuple[PolymarketConfiguredMarket, ...] = Field(
        max_length=250,
    )

    @model_validator(mode="after")
    def validate_content_identity(self) -> "PolymarketConfiguredScope":
        if self.configured_market_count != len(self.configured_markets):
            raise ValueError("configured market count is inconsistent")
        market_ids = [market.market_id for market in self.configured_markets]
        if len(set(market_ids)) != len(market_ids):
            raise ValueError("configured market IDs must be unique")
        expected_scope_id = content_sha256({
            "catalog_revision": self.catalog_revision,
            "configured_markets": [market.model_dump(mode="json") for market in self.configured_markets],
            "mode": self.mode,
        })
        if self.active_scope_id != expected_scope_id:
            raise ValueError("configured scope ID is not content-addressed")
        return self
