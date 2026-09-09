import json
from pathlib import Path

import pytest

from marketcow.catalog_refresh_worker import RefreshWorker
from marketcow.catalog_publication import CatalogPublication
from marketcow.universe_control import UniverseControl
from tests.test_catalog_capture_prepare import capture


def configuration(tmp_path):
    return dict(
        root=str(tmp_path / "worker"),
        binary="/not-executed",
        maximum_pages=5,
        maximum_bytes=100000,
        maximum_page_bytes=50000,
        maximum_seconds=10,
        request_seconds=5,
        interval_millis=1,
        maximum_records=100,
        maximum_row_bytes=50000,
        maximum_database_bytes=1000000,
        maximum_artifact_bytes=100000000,
        minimum_free_bytes=1,
        metric_unit=None,
        open_interval_seconds=20,
        closed_interval_seconds=100,
        known_interval_seconds=10,
        known_batch_records=5,
        maximum_cycles=2,
        poll_seconds=1,
        retained_change_records=100,
        retained_captures=2,
        snapshot_retention_seconds=1800,
    )


def market(mid="42"):
    return dict(
        id=mid,
        conditionId="0x" + format(int(mid), "064x"),
        events=[{"id": "7"}],
        clobTokenIds=json.dumps([str(int(mid) * 2), str(int(mid) * 2 + 1)]),
        outcomes='["Yes","No"]',
        question="Example?",
        active=True,
        closed=False,
        acceptingOrders=True,
        endDate=None,
    )


def test_ingest_publish_http_snapshot_and_changes(tmp_path):
    worker = RefreshWorker(configuration(tmp_path))
    try:
        root = capture(tmp_path, [market()])
        first = worker.ingest(root)
        provider = CatalogPublication(worker.root / "published", max_row_bytes=50000)
        profile = json.loads((Path(__file__).parent / "contracts/phase1/profile.json").read_bytes())
        app = UniverseControl(
            provider.current(),
            profile,
            tmp_path / "admit.sqlite",
            expected_active_selection_id=None,
            catalog_provider=provider,
        )
        old = json.loads(app.snapshot(1, include_change_sequence=True))
        assert old["schema_version"].endswith(".v2") and old["change_sequence"] == 1
        changes = json.loads(app.changes(0, 1))
        assert changes["events"][0]["market_id"] == "42"
        assert changes["capture_end_sequences"] == {"capture": 1}
        # A new capture closes the same identity. Previously issued pages stay pinned.
        other = tmp_path / "other"
        other.mkdir()
        changed = market()
        changed["closed"] = True
        fresh = capture(other, [changed])
        fresh.rename(other / "next")
        worker.ingest(other / "next")
        new = json.loads(app.snapshot(1, include_change_sequence=True))
        assert new["catalog_revision"] != old["catalog_revision"]
        old_page = json.loads(app.page(old["snapshot_id"], old["first_page_token"], 1))
        new_page = json.loads(app.page(new["snapshot_id"], new["first_page_token"], 1))
        assert old_page["records"][0]["closed"] is False
        assert new_page["records"][0]["closed"] is True
        assert first["sequence"] == 1
    finally:
        worker.close()


def test_upstream_failure_stops_and_keeps_due(tmp_path):
    calls = []

    def fail(*args, **kwargs):
        calls.append(args)
        raise RuntimeError("synthetic upstream failure")

    worker = RefreshWorker(configuration(tmp_path), clock=lambda: 100, execute=fail)
    try:
        with pytest.raises(RuntimeError):
            worker.tick()
        assert len(calls) == 1
        assert worker.state["open_due"] == 0
        assert worker.state["last_error"]["type"] == "RuntimeError"
        assert not (worker.root / "published" / "current.json").exists()
    finally:
        worker.close()


def test_single_writer_and_clock_highwater(tmp_path):
    config = configuration(tmp_path)
    worker = RefreshWorker(config, clock=lambda: 100)
    try:
        with pytest.raises(BlockingIOError):
            RefreshWorker(config)
        worker.state.update(high_water=101)
        with pytest.raises(ValueError, match="clock_regression"):
            worker.tick()
    finally:
        worker.close()


def test_disk_budget_prevents_request(tmp_path):
    config = configuration(tmp_path)
    config["maximum_artifact_bytes"] = 1
    worker = RefreshWorker(config, execute=lambda *_args, **_kwargs: pytest.fail("must not execute"))
    try:
        with pytest.raises(ValueError, match="artifact budget"):
            worker.tick()
    finally:
        worker.close()


def test_real_http_adapter_v2_auth_and_changes(tmp_path):
    import hashlib
    from fastapi.testclient import TestClient
    from marketcow.universe_control_http import Caller, PREFIX, create_control_app

    worker = RefreshWorker(configuration(tmp_path))
    try:
        worker.ingest(capture(tmp_path, [market()]))
        provider = CatalogPublication(worker.root / "published", max_row_bytes=50000)
        profile = json.loads((Path(__file__).parent / "contracts/phase1/profile.json").read_bytes())
        control = UniverseControl(
            provider.current(),
            profile,
            tmp_path / "admission.sqlite",
            expected_active_selection_id=None,
            catalog_provider=provider,
        )
        application = create_control_app(
            control,
            callers=(Caller("test", hashlib.sha256(b"local-test").hexdigest(), frozenset({"catalog.read"})),),
            body_timeout_seconds=1,
        )
        with TestClient(application, client=("127.0.0.1", 45000)) as client:
            assert client.get(PREFIX + "/catalog/changes?after_sequence=0&limit=1").status_code == 401
            headers = {"Authorization": "Bearer local-test"}
            response = client.get(PREFIX + "/catalog/snapshot-v2?page_size=1", headers=headers)
            assert response.status_code == 200 and response.json()["change_sequence"] == 1
            legacy = client.get(PREFIX + '/catalog/snapshot?page_size=1', headers=headers)
            assert legacy.status_code == 200 and legacy.json()['schema_version'].endswith('.v1')
            assert 'change_sequence' not in legacy.json()
            response = client.get(PREFIX + "/catalog/changes?after_sequence=0&limit=1", headers=headers)
            assert response.status_code == 200 and len(response.json()["events"]) == 1
    finally:
        worker.close()


def test_targeted_capture_preserves_other_inventory(tmp_path):
    import hashlib

    worker = RefreshWorker(configuration(tmp_path))
    try:
        worker.ingest(capture(tmp_path, [market(), market("43")]))
        targeted = tmp_path / "targeted"
        targeted.mkdir()
        changed = market()
        changed["closed"] = True
        body = json.dumps(changed).encode()
        (targeted / "page-000001.json").write_bytes(body)
        (targeted / "pages.jsonl").write_text(
            json.dumps(
                dict(
                    page=1,
                    file="page-000001.json",
                    url="https://gamma-api.polymarket.com/markets/42",
                    params=[],
                    received_at="2026-09-10T01:00:01Z",
                    status=200,
                    raw_bytes=len(body),
                    raw_sha256=hashlib.sha256(body).hexdigest(),
                    truncated=False,
                )
            )
            + "\n"
        )
        (targeted / "report.json").write_text(
            json.dumps(
                dict(
                    schema_version="marketcow.catalog-capture.v1",
                    complete=True,
                    error=None,
                    terminal_cursor=None,
                    closed_filter=False,
                    requested_market_ids=["42"],
                    capture_started_at="2026-09-10T01:00:00Z",
                    capture_completed_at="2026-09-10T01:00:02Z",
                    pages=1,
                    retained_raw_bytes=len(body),
                    market_count=1,
                )
            )
        )
        worker.ingest(targeted)
        source = CatalogPublication(worker.root / "published", max_row_bytes=50000).current()
        assert source.count == 2
        assert source.get("42")["closed"] is True
        assert source.get("43")["observed_at"] == "2026-09-09T01:00:01Z"
        prepared = json.loads((tmp_path / "targeted-prepared" / "manifest.json").read_bytes())
        assert prepared["normalized_groups"] == 1
    finally:
        worker.close()


def test_unchanged_capture_skips_normalization(tmp_path):
    worker = RefreshWorker(configuration(tmp_path))
    try:
        worker.ingest(capture(tmp_path, [market()]))
        other = tmp_path / "next-root"
        other.mkdir()
        next_capture = capture(other, [market()])
        next_capture.rename(other / "different")
        result = worker.ingest(other / "different")
        manifest = json.loads((other / "different-prepared" / "manifest.json").read_bytes())
        assert manifest["normalized_groups"] == 0
        assert result["sequence"] == 1
    finally:
        worker.close()


def test_gc_only_owned_expired_artifacts(tmp_path):
    import os

    worker = RefreshWorker(configuration(tmp_path))
    try:
        first = worker.ingest(capture(tmp_path, [market()]))
        old = worker.root / "published" / (first["generation_id"] + ".sqlite")
        os.utime(old, (1, 1))
        other = tmp_path / "second"
        other.mkdir()
        new_capture = capture(other, [market()])
        new_capture.rename(other / "new")
        second = worker.ingest(other / "new")
        # A long-lived generation gets a new retirement grace.
        assert old.stat().st_mtime > 1
        worker.collect_expired()
        assert old.exists()
        os.utime(old, (1, 1))
        sentinel = worker.root / "published" / "account.sqlite"
        sentinel.write_bytes(b"not-owned")
        worker.collect_expired()
        assert not old.exists()
        assert sentinel.read_bytes() == b"not-owned"
        assert (worker.root / "published" / (second["generation_id"] + ".sqlite")).exists()
    finally:
        worker.close()
