"""Actual candidate files/SQLite, synthetic source metadata, no live listener."""
import hashlib
import json
import sqlite3
from datetime import datetime, timezone

import pytest

from marketcow.polymarket_live import GammaLiveNormalizer
from marketcow.polymarket_contracts import canonical_json
from marketcow.universe_live_candidate import prepare_live_candidate


def test_exact_candidate_has_empty_state_and_unknown_date(tmp_path):
    source = tmp_path/"source"; source.mkdir()
    final = tmp_path/"candidate"
    market = GammaLiveNormalizer.normalize([{"id": "1", "conditionId": "condition", "events": [{"id": "event"}],
        "clobTokenIds": ["11", "12"], "outcomes": ["Yes", "No"], "active": True}], datetime.now(timezone.utc))[0]
    body = canonical_json(market.model_dump(mode="json"))
    normalized = source/"normalized.jsonl"; normalized.write_bytes(body+b"\n")
    index = source/"catalog.sqlite"
    with sqlite3.connect(index) as db:
        db.execute("CREATE TABLE markets(market_id TEXT,byte_offset INTEGER,byte_length INTEGER,row_sha256 TEXT)")
        db.execute("INSERT INTO markets VALUES('1',0,?,?)", (len(body), hashlib.sha256(body).hexdigest()))
    manifest = {"catalog_revision": "a"*64}
    for key, path in (("normalized_catalog", normalized), ("catalog_index", index),
                      ("candidate_snapshot", source/"snapshot"), ("catalog_source", source/"raw")):
        if not path.exists(): path.write_text("synthetic")
        path.chmod(0o400)
        pk, hk = ("raw_path", "raw_payload_sha256") if key == "catalog_source" else ("path", "sha256")
        manifest[key] = {pk: str(path), hk: hashlib.sha256(path.read_bytes()).hexdigest()}
    (source/"catalog.json").write_text(json.dumps(manifest))
    class Catalog:
        revision = "a"*64
        def get(self, mid):
            return {"market_id": mid, "condition_id": "condition", "outcomes": [{"token_id": "11"}, {"token_id": "12"}], "relations": []}
    args = dict(source_root=source, target_root=final, catalog_source=Catalog(), market_ids=("1",),
                maximum_dependencies=4, maximum_tokens=10, maximum_row_bytes=100000, maximum_artifact_bytes=1000000,
                maximum_catalog_copy_bytes=1000000)
    report = prepare_live_candidate(**args)
    assert report["selected"] == 1 and report["preheated"] is False and report["applied"] is False
    scope = json.loads((final/"configured-scope.json").read_bytes())
    assert scope["configured_markets"][0]["end_at"] is None
    state = json.loads((final/"state-index.json").read_bytes())
    assert state["path"] == str(final/"indexes/latest-state.sqlite3")
    with sqlite3.connect(final/"indexes/latest-state.sqlite3") as db:
        assert db.execute("SELECT COUNT(*) FROM books").fetchone() == (0,)
    with pytest.raises(ValueError, match="new independent"):
        prepare_live_candidate(**args)
