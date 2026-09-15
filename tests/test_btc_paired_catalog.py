import hashlib
from pathlib import Path

import pytest

from marketcow.btc_hourly_dataset import canonical
from marketcow.btc_paired_catalog import build_catalog


def fixture(root: Path, market: str, hour: int, *, result: bool = True) -> Path:
    root.mkdir()
    evidence = canonical({"market_id": market, "condition_id": "0x" + market.zfill(64),
                          "observed_at": f"2026-09-10T{hour:02}:00:00Z"})
    parts = {
        "market-evidence.json": evidence,
        "verified-finality.json": b"{}",
        "binance-1m-hour.jsonl": b"one\n",
        "binance-1h-hour.jsonl": b"hour\n",
    }
    part_rows = []
    for name, raw in parts.items():
        (root / name).write_bytes(raw)
        part_rows.append({"file": name, "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()})
    payouts = ["1", "0"] if result else ["0", "1"]
    payload = {
        "schema_version": "marketcow.btc-hour.paired-dataset.v1",
        "market": {"market_id": market, "condition_id": "0x" + market.zfill(64),
                   "start_utc": f"2026-09-10T{hour:02}:00:00Z",
                   "end_utc": f"2026-09-10T{hour + 1:02}:00:00Z",
                   "outcomes": [{"outcome": "Up", "token_id": market + "1", "payout": payouts[0]},
                                {"outcome": "Down", "token_id": market + "2", "payout": payouts[1]}]},
        "binance_result": {"result": result}, "settlement": {"status": "verified_final"},
        "sources": {"market_evidence_sha256": hashlib.sha256(evidence).hexdigest()},
        "parts": part_rows, "limitations": ["historical_l2_absent"],
    }
    identity = hashlib.sha256(canonical(payload)).hexdigest()
    (root / "manifest.json").write_bytes(canonical({**payload, "dataset_id": identity,
                                                     "manifest_payload_sha256": identity}))
    return (root / "manifest.json").resolve()


def test_catalog_verifies_and_sorts_complete_inputs(tmp_path):
    later = fixture(tmp_path / "later", "2", 13)
    earlier = fixture(tmp_path / "earlier", "1", 12, result=False)
    result = build_catalog([later, earlier], (tmp_path / "out/catalog.json").resolve())
    assert [row["market_id"] for row in result["datasets"]] == ["1", "2"]
    assert result["dataset_count"] == 2
    assert result["rule_observed_before_window_count"] == 2
    assert result["complete_for_listed_inputs"] is True
    assert result["historical_market_coverage_complete"] is False


def test_catalog_rejects_tamper_duplicate_and_wrong_result(tmp_path):
    manifest = fixture(tmp_path / "one", "1", 12)
    (manifest.parent / "binance-1h-hour.jsonl").write_bytes(b"tamper")
    with pytest.raises(ValueError, match="paired_part_integrity"):
        build_catalog([manifest], (tmp_path / "tampered.json").resolve())

    manifest = fixture(tmp_path / "two", "2", 13)
    with pytest.raises(ValueError, match="paired_catalog_duplicate"):
        build_catalog([manifest, manifest], (tmp_path / "duplicate.json").resolve())


def test_catalog_requires_absolute_bounded_inputs(tmp_path):
    manifest = fixture(tmp_path / "one", "1", 12)
    with pytest.raises(ValueError, match="paired_catalog_input_capacity"):
        build_catalog([manifest], (tmp_path / "small.json").resolve(), maximum_manifest_bytes=1)
    with pytest.raises(ValueError, match="absolute_manifest_required"):
        build_catalog([Path("manifest.json")], (tmp_path / "relative.json").resolve())
