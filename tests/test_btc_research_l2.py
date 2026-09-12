import hashlib
import json

import pytest

from marketcow.btc_research_l2 import project_archive


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def frame(payload, received="2026-09-12T00:00:00Z"):
    return (
        canonical(
            {
                "schema_version": "marketcow.btc-hour.rust-research-frame.v1",
                "received_at": received,
                "raw_payload": payload,
                "raw_wire_bytes_preserved": False,
                "limitation": "transport parsed JSON before this archive boundary",
            }
        )
        + b"\n"
    )


def capture(tmp_path, frames):
    root = tmp_path / "capture"
    root.mkdir()
    config = {
        "markets": [{"market_id": "1", "condition_id": "0x" + "a" * 64, "token_ids": ["10", "11"]}],
        "maximum_batch_bytes": 1024 * 1024,
        "maximum_total_bytes": 1024 * 1024,
    }
    config_raw = canonical(config)
    archive = b"".join(frames)
    report = {
        "status": "complete",
        "config_sha256": hashlib.sha256(config_raw).hexdigest(),
        "frames_sha256": hashlib.sha256(archive).hexdigest(),
        "bytes": len(archive),
    }
    (root / "config.json").write_bytes(config_raw)
    (root / "frames.jsonl").write_bytes(archive)
    (root / "report.json").write_bytes(canonical(report))
    return root


def test_projects_two_sided_and_never_crosses_gap(tmp_path):
    frames = [
        frame(
            {
                "event_type": "book",
                "asset_id": "10",
                "timestamp": "1",
                "bids": [{"price": "0.4", "size": "2"}],
                "asks": [{"price": "0.6", "size": "3"}],
            }
        ),
        frame(
            {
                "event_type": "book",
                "asset_id": "11",
                "timestamp": "1",
                "bids": [{"price": "0.3", "size": "4"}],
                "asks": [{"price": "0.7", "size": "5"}],
            }
        ),
        frame(
            {
                "event_type": "price_change",
                "timestamp": "2",
                "price_changes": [
                    {"asset_id": "10", "side": "BUY", "price": "0.45", "size": "6"},
                    {"asset_id": "10", "side": "SELL", "price": "0.6", "size": "0"},
                ],
            }
        ),
        frame({"event_type": "source_gap", "asset_id": "10"}),
        frame(
            {
                "event_type": "price_change",
                "timestamp": "3",
                "price_changes": [{"asset_id": "10", "side": "BUY", "price": "0.46", "size": "1"}],
            }
        ),
    ]
    output = tmp_path / "l2"
    report = project_archive(capture(tmp_path, frames), output)
    assert report["l2_rows"] == 3
    assert report["complete_two_sided_market_ids"] == []
    assert report["missing_or_gapped_token_ids"] == ["10"]
    assert report["rejected_unapplied_changes"] == 1
    rows = [json.loads(line) for line in (output / "l2.jsonl").read_bytes().splitlines()]
    assert rows[-1]["local_sequence"] == 2
    assert rows[-1]["kind"] == "delta"
    assert rows[-1]["changes"][0]["price"] == "0.45"
    assert rows[-1]["raw_wire_bytes_preserved"] is False


def test_rejects_changed_archive(tmp_path):
    root = capture(tmp_path, [frame({"event_type": "book", "asset_id": "10", "bids": [], "asks": []})])
    (root / "frames.jsonl").write_bytes(b"{}\n")
    with pytest.raises(ValueError, match="archive_integrity"):
        project_archive(root, tmp_path / "bad")
