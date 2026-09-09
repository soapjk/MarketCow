import hashlib
import json
import pytest

from marketcow.catalog_capture_prepare import prepare_capture


def capture(tmp_path, markets=None):
    root = tmp_path / "capture"
    root.mkdir()
    body = json.dumps({"markets": markets or [], "next_cursor": None}).encode()
    (root / "page-000001.json").write_bytes(body)
    page = dict(
        page=1,
        file="page-000001.json",
        params=[["limit", "100"], ["closed", "false"], ["order", "id"], ["ascending", "true"]],
        received_at="2026-09-09T01:00:01Z",
        status=200,
        raw_bytes=len(body),
        raw_sha256=hashlib.sha256(body).hexdigest(),
        truncated=False,
    )
    (root / "pages.jsonl").write_text(json.dumps(page) + "\n")
    report = dict(
        schema_version="marketcow.catalog-capture.v1",
        complete=True,
        error=None,
        terminal_cursor=None,
        closed_filter=False,
        capture_started_at="2026-09-09T01:00:00Z",
        capture_completed_at="2026-09-09T01:00:02Z",
        pages=1,
        retained_raw_bytes=len(body),
        market_count=len(markets or []),
    )
    (root / "report.json").write_text(json.dumps(report))
    return root


def prepare(root, output):
    return prepare_capture(
        root,
        output,
        max_pages=5,
        max_bytes=100000,
        max_page_bytes=50000,
        max_records=100,
        max_row_bytes=50000,
        max_database_bytes=1000000,
        metric_unit=None,
    )


def test_empty_complete_capture(tmp_path):
    result = prepare(capture(tmp_path), tmp_path / "out")
    assert result["unique_count"] == 0
    assert result["coverage"]["complete"]


def test_corrupt_raw_rejected(tmp_path):
    root = capture(tmp_path)
    (root / "page-000001.json").write_bytes(b"{}")
    with pytest.raises(ValueError, match="hash"):
        prepare(root, tmp_path / "out")
    assert not (tmp_path / "out" / "manifest.json").exists()


def test_failed_capture_rejected(tmp_path):
    root = capture(tmp_path)
    report = json.loads((root / "report.json").read_text())
    report["complete"] = False
    (root / "report.json").write_text(json.dumps(report))
    with pytest.raises(ValueError, match="incomplete"):
        prepare(root, tmp_path / "out")
    assert not (tmp_path / "out").exists()


def test_complete_realistic_mapping(tmp_path):
    market = dict(
        id="42",
        conditionId="0x" + "a" * 64,
        events=[{"id": "7"}],
        clobTokenIds='["111","222"]',
        outcomes='["Yes","No"]',
        question="Example?",
        active=True,
        closed=False,
        acceptingOrders=True,
        endDate=None,
    )
    result = prepare(capture(tmp_path, [market]), tmp_path / "out")
    assert result["unique_count"] == 1
    import sqlite3

    with sqlite3.connect(tmp_path / "out" / "prepared.sqlite") as db:
        row = json.loads(db.execute("SELECT payload FROM records").fetchone()[0])
    assert row["observed_at"] == "2026-09-09T01:00:01Z"
    assert row["end_at"] is None


@pytest.mark.parametrize(
    "field,value",
    [
        ("status", 403),
        ("truncated", True),
        ("params", []),
        ("file", "../outside.json"),
        ("received_at", "2026-09-10T01:00:01Z"),
    ],
)
def test_invalid_page_evidence_never_prepares(tmp_path, field, value):
    root = capture(tmp_path)
    page = json.loads((root / "pages.jsonl").read_bytes())
    page[field] = value
    (root / "pages.jsonl").write_text(json.dumps(page) + "\n")
    with pytest.raises(ValueError):
        prepare(root, tmp_path / "out")
    assert not (tmp_path / "out" / "manifest.json").exists()


def test_false_total_rejected(tmp_path):
    root = capture(tmp_path)
    report = json.loads((root / "report.json").read_bytes())
    report["market_count"] = 1
    (root / "report.json").write_text(json.dumps(report))
    with pytest.raises(ValueError, match="count"):
        prepare(root, tmp_path / "out")
