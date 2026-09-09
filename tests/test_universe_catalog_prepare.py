"""Synthetic frozen Gamma+normalized inputs, not actual full-catalog evidence."""
import hashlib
import json
import secrets
import sqlite3
import socket
import threading
import time
from pathlib import Path

import pytest
import httpx
import uvicorn

from marketcow.universe_catalog_prepare import prepare_catalog
from marketcow.universe_prepared_source import PreparedCatalogSource
from marketcow.universe_control import UniverseControl, wire_bytes
from marketcow.universe_control_server import load_application
from marketcow.universe_control_http import PREFIX
from marketcow.universe_phase1 import selection_sha256
from marketcow.universe_catalog_mapping import map_gamma_record


def test_legacy_manifest_timestamp_normalizes_without_mutating_evidence(inputs, tmp_path):
    prepare_catalog(**inputs)
    with sqlite3.connect(inputs["output"]) as db:
        manifest = json.loads(db.execute("SELECT value FROM metadata WHERE key='manifest'").fetchone()[0])
        manifest["source"]["observed_at"] = "2026-09-07T08:30:00.123456+00:00"
        db.execute("UPDATE metadata SET value=? WHERE key='manifest'", (json.dumps(manifest),))
    before = inputs["output"].read_bytes()
    source = PreparedCatalogSource(inputs["output"], expected_sha256=hashlib.sha256(before).hexdigest(), max_row_bytes=10000)
    try:
        profile = json.loads((Path(__file__).parent/"contracts/phase1/profile.json").read_bytes())
        control = UniverseControl(source, profile, tmp_path/"admit.sqlite", expected_active_selection_id=None)
        assert json.loads(control.snapshot(100))["source"]["observed_at"] == "2026-09-07T08:30:00.123456Z"
    finally:
        source.close()
    assert inputs["output"].read_bytes() == before


def test_record_times_normalize_offsets(inputs):
    normal = json.loads(inputs["normalized_path"].read_text().splitlines()[0])
    raw = json.loads(inputs["raw_path"].read_text().splitlines()[0])
    raw["endDate"] = "2026-09-08T16:30:00+08:00"
    row = map_gamma_record(normal, raw, observed_at="2026-09-07T08:30:00.123456+00:00",
                           metric_unit="USD", contains=lambda _: True)
    assert row["observed_at"] == "2026-09-07T08:30:00.123456Z"
    assert row["end_at"] == "2026-09-08T08:30:00Z"
    assert row["volume_24h"]["observed_at"] == row["observed_at"]
    assert row["liquidity"]["observed_at"] == row["observed_at"]


def test_preparation_normalizes_manifest_timestamp(inputs):
    report = prepare_catalog(**dict(inputs, observed_at="2026-09-07T08:30:00.123456+00:00"))
    assert report["source"]["observed_at"] == "2026-09-07T08:30:00.123456Z"


@pytest.fixture
def inputs(tmp_path):
    raw = [{"id": "1", "events": [{"id": "event", "title": "真实字符 synthetic"}],
            "conditionId": "c1", "active": False, "closed": True,
            "volume24hr": "0", "liquidityNum": "12.50"},
           {"id": "2", "events": [{"id": "event"}], "conditionId": "c2"},
           {"id": "3", "conditionId": "c3"}, {"id": "4"}]
    normal = [{"identity": {"market_id": mid, "event_id": "event", "condition_id": "c"+mid,
                            "outcomes": [{"token_id": "t"+mid, "instrument_id": "i"+mid, "outcome": "YES"}]},
               "relations": [{"relation_id": "group", "revision": "r1", "relation_type": "standard_negative_risk",
                              "outcome_pairs": [{"market_id": i} for i in ("1", "2", "3")], "complete": True}]}
              for mid in ("1", "2", "3")]
    rp, np = tmp_path/"raw.jsonl", tmp_path/"normalized.jsonl"
    rp.write_bytes(b"".join(wire_bytes(row)+b"\n" for row in raw))
    np.write_bytes(b"".join(wire_bytes(row)+b"\n" for row in normal))
    return dict(raw_path=rp, normalized_path=np, output=tmp_path/"prepared.sqlite",
                raw_sha256=hashlib.sha256(rp.read_bytes()).hexdigest(),
                normalized_sha256=hashlib.sha256(np.read_bytes()).hexdigest(),
                revision=hashlib.sha256(b"["+b",".join(wire_bytes(row) for row in normal)+b"]").hexdigest(),
                raw_count=4, normalized_count=3, observed_at="2026-09-07T08:30:00.000Z",
                source_predicate="synthetic-only", traversal_verified=True, metric_unit="USD",
                max_input_row_bytes=10000, max_output_row_bytes=10000, max_records=10, max_database_bytes=1048576)


def test_prepare_mapping_partial_coverage_and_control(inputs, tmp_path):
    report = prepare_catalog(**inputs)
    assert report["unique_count"] == 2 and report["omitted_count"] == 2
    assert report["coverage"]["complete"] is False
    source = PreparedCatalogSource(inputs["output"], expected_sha256=report["prepared_file_sha256"], max_row_bytes=10000)
    try:
        rows = list(source.after(None))
        assert len(rows) == 2 and rows[0]["closed"] is True
        assert rows[1]["active"] is None and rows[1]["liquidity"]["value"] is None
        assert rows[0]["volume_24h"]["value"] == "0"
        assert rows[0]["relations"][0]["missing_market_ids"] == ["3"]
        assert rows[0]["relations"][0]["complete"] is False
        assert source.get("2") == rows[1]
        profile = json.loads((Path(__file__).parent/"contracts/phase1/profile.json").read_bytes())
        control = UniverseControl(source, profile, tmp_path/"admit.sqlite", expected_active_selection_id=None)
        snap = json.loads(control.snapshot(100))
        page = json.loads(control.page(snap["snapshot_id"], snap["first_page_token"], 100))
        assert page["records"] == rows and page["end_of_snapshot"]
    finally:
        source.close()
    with pytest.raises(FileExistsError):
        prepare_catalog(**inputs)


@pytest.mark.parametrize("change", [{"raw_sha256": "0"*64}, {"revision": "0"*64},
                                   {"max_input_row_bytes": 10}, {"max_output_row_bytes": 10}])
def test_prepare_failure_never_publishes(inputs, change):
    with pytest.raises(ValueError):
        prepare_catalog(**dict(inputs, **change))
    assert not inputs["output"].exists()
    assert not list(inputs["output"].parent.glob(".phase1-prepare-*"))


def test_prepared_file_config_and_real_http(inputs, tmp_path):
    report = prepare_catalog(**inputs)
    profile_path = Path(__file__).parent / "contracts/phase1/profile.json"
    profile = json.loads(profile_path.read_bytes())
    caller_file = tmp_path/"callers.json"
    secret = secrets.token_hex(32)
    caller_file.write_bytes(wire_bytes([{"identity": "synthetic-reader", "scopes": ["catalog.read"],
                                        "bearer_sha256": hashlib.sha256(secret.encode()).hexdigest()}]))
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(8)
    port = listener.getsockname()[1]
    config = {"host": "127.0.0.1", "port": port, "profile_path": str(profile_path.resolve()),
              "profile_sha256": selection_sha256(profile), "catalog_path": str(inputs["output"]),
              "catalog_sha256": report["prepared_file_sha256"], "admission_path": str(tmp_path/"admit.sqlite"),
              "callers_path": str(caller_file), "expected_active_selection_id": None,
              "body_timeout_seconds": 2, "maximum_catalog_row_bytes": 10000, "legacy_incumbent": None}
    config_file = tmp_path/"operator.json"
    config_file.write_bytes(wire_bytes(config))
    app, source, _, _ = load_application(config_file)
    server = uvicorn.Server(uvicorn.Config(app, access_log=False, log_level="error", lifespan="off"))
    thread = threading.Thread(target=server.run, kwargs={"sockets": [listener]})
    thread.start()
    try:
        deadline = time.monotonic()+5
        while not server.started and thread.is_alive() and time.monotonic() < deadline:
            time.sleep(.01)
        assert server.started
        with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=3, trust_env=False,
                          headers={"Authorization": f"Bearer {secret}"}) as client:
            snap_response = client.get(PREFIX+"/catalog/snapshot", params={"page_size": 100})
            assert snap_response.status_code == 200
            snap = snap_response.json()
            assert snap["unique_count"] == 2 and not snap["coverage"]["complete"]
            page = client.get(PREFIX+"/catalog/page", params={"snapshot_id": snap["snapshot_id"],
                "page_token": snap["first_page_token"], "limit": 100})
            assert page.status_code == 200 and len(page.json()["records"]) == 2
            forbidden = client.post(PREFIX+"/discovery-selections/admit", json={})
            assert forbidden.status_code == 403
            assert not (tmp_path/"admit.sqlite").exists()
    finally:
        server.should_exit = True
        thread.join(5)
        listener.close()
        source.close()
    assert not thread.is_alive()
