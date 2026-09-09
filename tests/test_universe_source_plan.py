import pytest

from marketcow.universe_source_plan import build_source_plan


class Source:
    revision = "a"*64
    def get(self, mid):
        if mid == "3":
            return None
        return {"market_id": mid, "condition_id": "condition"+mid,
                "outcomes": [{"token_id": mid+"yes"}, {"token_id": mid+"no"}],
                "relations": [{"relation_id": "r", "member_market_ids": ["1", "2", "3"],
                               "complete": False, "missing_market_ids": ["3"]}]}


def build(**kw):
    return build_source_plan(Source(), catalog_revision=Source.revision, pool="live", market_ids=("1",),
                             maximum_dependency_markets=kw.get("dependencies", 2),
                             maximum_total_tokens=kw.get("tokens", 4))


def test_transitive_closure_does_not_replace_requested_identities():
    result = build()
    assert [r["market_id"] for r in result["plan"]["markets"]] == ["1"]
    assert [r["market_id"] for r in result["dependency_markets"]] == ["2"]
    assert result["missing_dependency_market_ids"] == ["3"]
    assert result["total_token_count"] == 4
    assert result["relations"][0]["complete"] is False


@pytest.mark.parametrize("limits,reason", [({"dependencies": 1}, "dependency capacity"),
                                          ({"tokens": 2}, "token capacity")])
def test_closure_budgets_include_missing_members(limits, reason):
    with pytest.raises(ValueError, match=reason):
        build(**limits)
