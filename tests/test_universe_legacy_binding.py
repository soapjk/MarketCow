import json

import pytest

from marketcow.polymarket_contracts import content_sha256
from marketcow.universe_legacy_binding import legacy_binding, legacy_incumbent_id, verify_legacy_binding


def manifest(ids=None):
    ids = ["1", "2"] if ids is None else ids
    universe = {"catalog_revision": "a"*64, "market_ids": ids, "market_count": len(ids)}
    universe["universe_id"] = content_sha256(universe)
    return {"catalog_revision": "a"*64, "realtime_universe": universe}


def test_binding_roundtrip_and_reread_detects_change(tmp_path):
    path = tmp_path/"manifest.json"
    original = manifest()
    path.write_text(json.dumps(original))
    binding = legacy_binding(json.loads(path.read_bytes()))
    incumbent = legacy_incumbent_id(binding)
    assert incumbent.startswith("legacy-discovery:")
    assert verify_legacy_binding(json.loads(path.read_bytes()), binding, incumbent) == incumbent
    path.write_text(json.dumps(manifest(["1", "3"])))
    with pytest.raises(ValueError, match="legacy incumbent changed"):
        verify_legacy_binding(json.loads(path.read_bytes()), binding, incumbent)


@pytest.mark.parametrize("ids", [["1", "1"], ["2", "1"], [""]])
def test_invalid_identity_list_even_with_valid_universe_hash(ids):
    with pytest.raises(ValueError):
        legacy_binding(manifest(ids))


def test_tampering_rejected():
    value = manifest()
    value["realtime_universe"]["market_ids"] = ["1", "3"]
    with pytest.raises(ValueError, match="legacy universe binding invalid"):
        legacy_binding(value)
