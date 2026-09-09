"""Identity-only bridge for an existing legacy Discovery universe.

Never selects, collects or installs a pool. The operator pins the returned
binding; startup/new admission compare it against the manifest read again.
"""
from __future__ import annotations

import re

from marketcow.polymarket_contracts import content_sha256
from marketcow.universe_phase1 import selection_sha256


def legacy_binding(manifest: dict) -> dict:
    universe = manifest["realtime_universe"]
    payload = dict(universe)
    claimed = payload.pop("universe_id")
    revision = manifest["catalog_revision"]
    if (not isinstance(revision, str) or not re.fullmatch("[0-9a-f]{64}", revision)
            or universe.get("catalog_revision") != revision or content_sha256(payload) != claimed):
        raise ValueError("legacy universe binding invalid")
    ids = universe.get("market_ids")
    count = universe.get("market_count")
    if (not isinstance(ids, list) or not ids or type(count) is not int
            or count != len(ids) or any(type(mid) is not str or not mid or not mid.isascii() for mid in ids)
            or ids != sorted(set(ids))):
        raise ValueError("legacy universe identities invalid")
    return {
        "schema_version": "marketcow.polymarket.legacy-incumbent-binding.v1",
        "origin": "legacy_discovery", "catalog_revision": revision,
        "universe_revision": claimed, "market_count": count,
        "market_ids_sha256": selection_sha256(ids),
    }


def legacy_incumbent_id(binding: dict) -> str:
    if (set(binding) != {"schema_version", "origin", "catalog_revision", "universe_revision",
                         "market_count", "market_ids_sha256"}
            or binding["schema_version"] != "marketcow.polymarket.legacy-incumbent-binding.v1"
            or binding["origin"] != "legacy_discovery"):
        raise ValueError("invalid legacy binding schema")
    return "legacy-discovery:" + selection_sha256(binding)


def verify_legacy_binding(manifest: dict, expected: dict, expected_id: str) -> str:
    actual = legacy_binding(manifest)
    if actual != expected or legacy_incumbent_id(actual) != expected_id:
        raise ValueError("legacy incumbent changed")
    return expected_id
