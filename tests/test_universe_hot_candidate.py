"""Synthetic frozen source/index; no process, network or account operations."""
import hashlib
import json
import sqlite3
from datetime import datetime, timezone

import pytest

from marketcow.polymarket_contracts import canonical_json, content_sha256
from marketcow.universe_hot_candidate import build_hot_candidate
from marketcow.polymarket_live import GammaLiveNormalizer


@pytest.fixture
def source(tmp_path):
    now = datetime(2026, 9, 7, tzinfo=timezone.utc)
    rows = [dict(id=str(i), conditionId="condition"+str(i), events=[{"id": "event"}],
        clobTokenIds=[str(i*10+1), str(i*10+2)], outcomes=["Yes", "No"], active=i != 2, closed=i == 2)
        for i in (1, 2)]
    raw_path = tmp_path/"raw.jsonl"
    raw_path.write_bytes(b"".join(canonical_json(row)+b"\n" for row in rows))
    args = dict(markets=GammaLiveNormalizer.normalize(rows, now), raw_path=raw_path,
        manifest=dict(catalog_revision="a"*64, catalog_source=dict(raw_format="canonical_jsonl",
            raw_payload_sha256=hashlib.sha256(raw_path.read_bytes()).hexdigest(), observed_at=now.isoformat(),
            source_url="https://example.invalid/synthetic")))
    normalized, index = tmp_path/"normalized.jsonl", tmp_path/"index.sqlite"
    offset = 0
    with normalized.open("wb") as stream, sqlite3.connect(index) as db:
        db.execute("CREATE TABLE markets(market_id TEXT,byte_offset INTEGER,byte_length INTEGER,row_sha256 TEXT)")
        for market in args["markets"]:
            body = canonical_json(market.model_dump(mode="json"))
            db.execute("INSERT INTO markets VALUES(?,?,?,?)", (market.identity.market_id, offset, len(body), hashlib.sha256(body).hexdigest()))
            stream.write(body+b"\n"); offset += len(body)+1
    manifest = args["manifest"]
    manifest["catalog_source"].update(raw_path=str(args["raw_path"]), market_count=2)
    for key, path in (("normalized_catalog", normalized), ("catalog_index", index)):
        manifest[key] = dict(path=str(path), sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    (tmp_path/"catalog.json").write_text(json.dumps(manifest))

    class Catalog:
        revision = "a"*64

        def get(self, mid):
            if mid not in ("1", "2"):
                return None
            return dict(market_id=mid, condition_id="condition"+mid,
                outcomes=[dict(token_id=str(int(mid)*10+i)) for i in (1, 2)], relations=[])

    return dict(source_root=tmp_path, source=Catalog(), market_ids=("1", "2"), selection_sha256="c"*64,
        policy_version="synthetic", protected_market_ids=("2",), parent_selection_id="parent",
        maximum_dependency_markets=10, maximum_metadata_tokens=20, maximum_row_bytes=100000,
        maximum_artifact_bytes=1000000, maximum_source_bytes=100000, maximum_relation_members=10,
        depth_quantities=["10"], maximum_book_age_ms=5000)


@pytest.mark.parametrize("pool", ["live", "discovery"])
def test_real_builder_preserves_null_closed_and_exact_selection(source, pool):
    candidate = build_hot_candidate(pool=pool, **source)
    assert candidate["requested_market_ids"] == ["1", "2"]
    assert candidate["acquisition_market_ids"] == ["1", "2"]
    assert candidate["records"][1]["closed"] is True
    assert all(record["end_at"] is None for record in candidate["records"])
    body = dict(candidate); identity = body.pop("candidate_id")
    assert content_sha256(body) == identity
    if pool == "live":
        assert all(row["end_at"] is None for row in candidate["config"]["configured_markets"])
    else:
        assert candidate["config"]["settlements"] == {"1": None, "2": None}
    assert not (source["source_root"]/"indexes").exists()


def test_hot_builder_rejects_budget_and_parent_without_side_effects(source):
    with pytest.raises(ValueError, match="parent"):
        build_hot_candidate(pool="live", **dict(source, parent_selection_id=None))
    with pytest.raises(ValueError, match="byte capacity"):
        build_hot_candidate(pool="live", **dict(source, maximum_artifact_bytes=100))
    with pytest.raises(ValueError, match="token capacity"):
        build_hot_candidate(pool="live", **dict(source, maximum_metadata_tokens=2))
