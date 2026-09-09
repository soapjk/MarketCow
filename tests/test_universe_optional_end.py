import pytest

from marketcow.polymarket_configured_scope import PolymarketConfiguredMarket, PolymarketConfiguredScope
from marketcow.polymarket_contracts import content_sha256


def test_explicit_unknown_end_is_content_bound_not_filtered():
    market = dict(market_id="1", condition_id="condition", token_ids=["yes", "no"], end_at=None)
    identity = dict(catalog_revision="a"*64, mode="shadow", configured_markets=[market])
    raw = dict(**identity, schema_version="marketcow.polymarket.scope-discovery.v1",
               configured_market_count=1, active_scope_id=content_sha256(identity))
    result = PolymarketConfiguredScope.model_validate(raw)
    assert result.configured_markets[0].end_at is None
    raw["configured_markets"][0]["end_at"] = "2026-09-07T00:00:00Z"
    with pytest.raises(ValueError, match="content-addressed"):
        PolymarketConfiguredScope.model_validate(raw)


def test_missing_or_naive_end_is_not_explicit_unknown():
    market = dict(market_id="1", condition_id="condition", token_ids=["yes", "no"])
    with pytest.raises(ValueError):
        PolymarketConfiguredMarket.model_validate(market)
    with pytest.raises(ValueError, match="timezone"):
        PolymarketConfiguredMarket.model_validate(dict(**market, end_at="2026-09-07T00:00:00"))
