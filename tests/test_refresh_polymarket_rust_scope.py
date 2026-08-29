from __future__ import annotations

from scripts.migration.refresh_polymarket_rust_scope import (
    build_refreshed_manifest,
    scope_content_id,
)


def _candidate(market_id: str, end_at_ns: int, liquidity: str = "1") -> dict:
    return {
        "market_id": market_id,
        "active": True,
        "accepting_orders": True,
        "closed": False,
        "binary_yes_no": True,
        "rules_complete": True,
        "end_at_ns": end_at_ns,
        "liquidity": liquidity,
        "yes_outcome_id": f"POLY:condition:{market_id}1",
        "no_outcome_id": f"POLY:condition:{market_id}2",
    }


def test_refresh_replaces_expired_members_and_preserves_complete_relation() -> None:
    current = {
        "schema": "tradude.prediction_market.scope_manifest.v1",
        "catalog_revision": "catalog-v1",
        "candidate_snapshot_id": "old-candidates",
        "generated_at_ns": 1,
        "market_ids": ["1", "2", "3", "4"],
        "market_limit": 4,
        "negative_risk_relation_ids": ["relation-1"],
        "logical_relation_ids": [],
        "registry_id": "registry-v1",
        "selection_config_id": "selection-v1",
    }
    current["scope_id"] = scope_content_id(current)
    candidates = {
        "schema": "tradude.prediction_market.scope_candidates.v1",
        "catalog_revision": "catalog-v1",
        "snapshot_id": "new-candidates",
        "markets": [
            _candidate("1", 5_000_000_000),
            _candidate("2", 5_000_000_000),
            _candidate("3", 1_500_000_000),
            _candidate("4", 5_000_000_000),
            _candidate("5", 6_000_000_000, "10"),
        ],
    }
    registry = {
        "catalog_revision": "catalog-v1",
        "registry_id": "registry-v1",
        "negative_risk_groups": [
            {
                "relation_id": "relation-1",
                "complete": True,
                "members": [{"market_id": "1"}, {"market_id": "2"}],
            }
        ],
    }

    manifest, evidence = build_refreshed_manifest(
        current,
        candidates,
        registry,
        generated_at_ns=1_000_000_000,
        minimum_scope_lifetime_seconds=1,
        expected_market_count=4,
    )

    assert manifest["market_ids"] == ["1", "2", "4", "5"]
    assert manifest["negative_risk_relation_ids"] == ["relation-1"]
    assert manifest["scope_id"] == scope_content_id(manifest)
    assert evidence["removed_market_ids"] == ["3"]
    assert evidence["replacement_market_ids"] == ["5"]


def test_refresh_drops_entire_relation_when_one_member_expires() -> None:
    current = {
        "schema": "tradude.prediction_market.scope_manifest.v1",
        "catalog_revision": "catalog-v1",
        "market_ids": ["1", "2"],
        "negative_risk_relation_ids": ["relation-1"],
    }
    current["scope_id"] = scope_content_id(current)
    candidates = {
        "schema": "tradude.prediction_market.scope_candidates.v1",
        "catalog_revision": "catalog-v1",
        "snapshot_id": "candidates",
        "markets": [
            _candidate("1", 1_500_000_000),
            _candidate("2", 5_000_000_000),
            _candidate("3", 6_000_000_000, "10"),
            _candidate("4", 6_000_000_000, "9"),
        ],
    }
    registry = {
        "catalog_revision": "catalog-v1",
        "negative_risk_groups": [
            {
                "relation_id": "relation-1",
                "complete": True,
                "members": [{"market_id": "1"}, {"market_id": "2"}],
            }
        ],
    }
    manifest, evidence = build_refreshed_manifest(
        current,
        candidates,
        registry,
        generated_at_ns=1_000_000_000,
        minimum_scope_lifetime_seconds=1,
        expected_market_count=2,
    )
    assert manifest["market_ids"] == ["3", "4"]
    assert manifest["negative_risk_relation_ids"] == []
    assert evidence["dropped_negative_risk_relation_ids"] == ["relation-1"]
