"""Synthetic raw source facts; no server or external acquisition."""
import hashlib
import json
import sqlite3
from datetime import datetime, timezone

import pytest

from marketcow.polymarket_contracts import canonical_json, content_sha256
from marketcow.polymarket_live import GammaLiveNormalizer
from marketcow.universe_discovery_seed import build_discovery_seed
from marketcow.universe_discovery_candidate import prepare_discovery_candidate


def seed_args(tmp_path, negative=False):
    rows = [{"id": str(i), "conditionId": f"condition{i}", "events": [{"id": "event"}],
             "clobTokenIds": [str(i*10+1), str(i*10+2)], "outcomes": ["Yes", "No"],
             "active": i != 2, "closed": i == 2} for i in (1, 2)]
    if negative:
        for row in rows:
            row.update(negRisk=True, negRiskMarketID="group", groupItemTitle="Outcome "+row["id"])
    now = datetime(2026, 9, 7, tzinfo=timezone.utc)
    markets = GammaLiveNormalizer.normalize(rows, now)
    path = tmp_path/"raw.jsonl"
    path.write_bytes(b"".join(canonical_json(r)+b"\n" for r in rows))
    universe = {"catalog_revision": "a"*64, "market_ids": ["1", "2"], "market_count": 2}
    universe["universe_id"] = content_sha256(universe)
    manifest = {"catalog_revision": "a"*64, "realtime_universe": universe,
        "catalog_source": {"raw_format": "canonical_jsonl", "raw_payload_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                           "observed_at": now.isoformat(), "source_url": "https://example.invalid/synthetic"}}
    return dict(manifest=manifest, manifest_sha256="b"*64, markets=markets, raw_path=path,
                depth_quantities=["10"], maximum_book_age_ms=5000, maximum_source_bytes=100000,
                maximum_row_bytes=10000, maximum_relation_members=100)


def test_preserves_closed_identity_without_inventing_settlement(tmp_path):
    args = seed_args(tmp_path)
    seed = build_discovery_seed(**args)
    assert [m["identity"]["market_id"] for m in seed["markets"]] == ["1", "2"]
    assert seed["markets"][1]["closed"] is True
    assert seed["settlements"] == {"1": None, "2": None}
    assert seed["maximum_book_age_ms"] == 5000


@pytest.mark.parametrize("field,value", [("maximum_source_bytes", 1), ("maximum_row_bytes", 1)])
def test_bounded_source(tmp_path, field, value):
    args = seed_args(tmp_path); args[field] = value
    with pytest.raises(ValueError, match="byte capacity"):
        build_discovery_seed(**args)


def test_rejects_evidence_change(tmp_path):
    args = seed_args(tmp_path)
    args["manifest"]["catalog_source"]["raw_payload_sha256"] = "0"*64
    with pytest.raises(ValueError, match="source hash"):
        build_discovery_seed(**args)


def test_rejects_selected_coverage_change(tmp_path):
    args = seed_args(tmp_path); args["markets"] = args["markets"][:1]
    with pytest.raises(ValueError, match="identities"):
        build_discovery_seed(**args)


def test_relation_preserves_member_evidence_not_per_market_provenance_equality(tmp_path):
    args = seed_args(tmp_path, negative=True)
    seed = build_discovery_seed(**args)
    assert len(seed["relations"]) == 1
    relation = seed["relations"][0]
    assert relation["member_market_ids"] == ["1", "2"]
    assert relation["expected_member_count"] == 2
    # No book or settlement fabrication is needed to expose relationship facts.
    assert seed["settlements"] == {"1": None, "2": None}


def test_candidate_preparation_is_cold_and_exact(tmp_path):
    source = tmp_path/"source"; source.mkdir()
    args = seed_args(source)
    normalized = source/"normalized.jsonl"
    index = source/"catalog.sqlite"
    offset = 0
    with normalized.open("wb") as stream, sqlite3.connect(index) as db:
        db.execute("CREATE TABLE markets(market_id TEXT,byte_offset INTEGER,byte_length INTEGER,row_sha256 TEXT)")
        for market in args["markets"]:
            body = canonical_json(market.model_dump(mode="json"))
            db.execute("INSERT INTO markets VALUES(?,?,?,?)", (market.identity.market_id, offset, len(body), hashlib.sha256(body).hexdigest()))
            stream.write(body+b"\n"); offset += len(body)+1
    manifest = args["manifest"]
    manifest["catalog_source"].update(raw_path=str(args["raw_path"]), market_count=2)
    for key, path in (("normalized_catalog", normalized), ("catalog_index", index), ("candidate_snapshot", source/"snapshot")):
        if not path.exists(): path.write_bytes(b"synthetic")
        manifest[key] = {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
        path.chmod(0o400)
    args["raw_path"].chmod(0o400)
    (source/"catalog.json").write_text(json.dumps(manifest))
    class Catalog:
        revision = "a"*64
        def get(self, mid):
            return {"market_id": mid, "condition_id": f"condition{mid}",
                    "outcomes": [{"token_id": str(int(mid)*10+j)} for j in (1, 2)], "relations": []}
    target = tmp_path/"candidate"
    report = prepare_discovery_candidate(source_root=source, target_root=target, catalog_source=Catalog(),
        market_ids=("1", "2"), selection_id="synthetic-explicit", policy_version="synthetic-v1",
        maximum_dependencies=4, maximum_tokens=10, maximum_row_bytes=100000,
        maximum_artifact_bytes=1000000, maximum_catalog_copy_bytes=1000000,
        maximum_source_bytes=100000, maximum_relation_members=10, depth_quantities=["10"], maximum_book_age_ms=5000)
    assert report["selected"] == 2 and report["collected_token_count"] == 4
    assert report["metadata_dependency_market_count"] == 0 and report["preheated"] is False
    seed = json.loads((target/"discovery-public-seed.json").read_bytes())
    assert seed["markets"][1]["closed"] is True
    with sqlite3.connect(target/"indexes/latest-state.sqlite3") as db:
        assert db.execute("SELECT COUNT(*) FROM books").fetchone() == (0,)
