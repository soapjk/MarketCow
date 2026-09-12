import base64
import hashlib
import json
from pathlib import Path

import pytest

from marketcow.btc_hour_pairing import build_pairing
from marketcow.btc_hourly_dataset import canonical
from marketcow.btc_polymarket_binding import BINANCE_HOUR_RULE


def write(path: Path, value) -> bytes:
    raw = canonical(value)
    path.write_bytes(raw)
    return raw


def fixture(tmp_path: Path):
    evidence_root, archive = tmp_path / "evidence", tmp_path / "archive"
    evidence_root.mkdir()
    raw_market = canonical({"id": "1", "conditionId": "0x" + "a" * 64,
                            "eventStartTime": "2026-09-10T06:00:00Z", "endDate": "2026-09-10T07:00:00Z",
                            "description": BINANCE_HOUR_RULE,
                            "resolutionSource": "https://www.binance.com/en/trade/BTC_USDT",
                            "outcomes": '["Up", "Down"]', "clobTokenIds": '["10", "20"]'})
    digest = hashlib.sha256(raw_market).hexdigest()
    evidence = {"schema_version": "marketcow.polymarket.market-evidence.v1", "market_id": "1",
                "condition_id": "0x" + "a" * 64, "raw_base64": base64.b64encode(raw_market).decode(),
                "raw_sha256": digest, "raw_bytes": len(raw_market), "raw_complete": True,
                "observed_at": "2026-09-10T08:00:00Z",
                "outcomes": [{"outcome": "Up", "token_id": "10"}, {"outcome": "Down", "token_id": "20"}]}
    write(evidence_root / "evidence.json", evidence)
    observation_a, observation_b = b'{"source":"a"}', b'{"source":"b"}'
    (evidence_root / "finality-observation-a.json").write_bytes(observation_a)
    (evidence_root / "finality-observation-b.json").write_bytes(observation_b)
    finality = {"schema_version": "marketcow.polymarket.ctf-finality-quorum.v1", "status": "verified_final",
                "settlement_import_allowed": True, "condition_id": "0x" + "a" * 64,
                "token_ids": ["10", "20"], "payout_numerators": ["0", "1"], "payout_denominator": "1",
                "finality_policy": "two_independent_rpc_finalized_receipts_v1", "collateral": "0x" + "b" * 40,
                "ctf_address": "0x" + "c" * 40,
                "provider_receipts": [
                    {"provider_id": "a", "observation_sha256": hashlib.sha256(observation_a).hexdigest()},
                    {"provider_id": "b", "observation_sha256": hashlib.sha256(observation_b).hexdigest()}]}
    write(evidence_root / "verified-finality.json", finality)
    base = 1789020000000000
    minute_rows = []
    for index in range(60):
        payload = {"open_us": base + index * 60_000_000, "close_us": base + (index + 1) * 60_000_000 - 1,
                   "open": "2" if index == 0 else "1", "high": "2", "low": "1", "close": "1",
                   "volume": "1", "quote_volume": "2", "taker_base_volume": "0.5",
                   "taker_quote_volume": "1", "trades": 2, "final": True}
        minute_rows.append({"interval": "1m", "type": "bar_final", "payload": payload})
    hour_payload = {"open_us": base, "close_us": base + 3_600_000_000 - 1, "open": "2", "high": "2",
                    "low": "1", "close": "1", "volume": "60", "quote_volume": "120",
                    "taker_base_volume": "30", "taker_quote_volume": "60", "trades": 120, "final": True}
    for interval, rows in (("1m", minute_rows), ("1h", [{"interval": "1h", "type": "bar_final", "payload": hour_payload}])):
        root = archive / interval
        root.mkdir(parents=True)
        facts = b"\n".join(canonical(row) for row in rows) + b"\n"
        (root / "facts.jsonl").write_bytes(facts)
        (root / "raw.csv").write_bytes(b"x")
        manifest = {"status": "complete", "coverage_complete": True, "dataset_id": interval,
                    "configuration": {"interval": interval},
                    "parts": [{"file": "facts.jsonl", "bytes": len(facts), "sha256": hashlib.sha256(facts).hexdigest()}]}
        write(root / "manifest.json", manifest)
    return evidence_root, archive


def test_builds_bound_bundle_and_keeps_adapter_limitation(tmp_path):
    evidence, archive = fixture(tmp_path)
    result = build_pairing(evidence, archive, tmp_path / "output", start_utc="2026-09-10T06:00:00Z")
    assert result["market"]["outcomes"][1]["payout"] == "1"
    assert result["binance_result"]["minute_aggregation_matches_hour"] is True
    assert result["settlement"]["adapter_redemption_verified"] is False
    assert result["coverage"]["application_pusd_mapping"] is False
    assert json.loads((tmp_path / "output/manifest.json").read_text())["dataset_id"] == result["dataset_id"]


def test_rejects_finality_disagreement_and_existing_output(tmp_path):
    evidence, archive = fixture(tmp_path)
    finality = json.loads((evidence / "verified-finality.json").read_text())
    finality["payout_numerators"] = ["1", "0"]
    write(evidence / "verified-finality.json", finality)
    with pytest.raises(ValueError, match="disagreement"):
        build_pairing(evidence, archive, tmp_path / "output", start_utc="2026-09-10T06:00:00Z")
    (tmp_path / "output").mkdir()
    with pytest.raises(FileExistsError):
        build_pairing(evidence, archive, tmp_path / "output", start_utc="2026-09-10T06:00:00Z")
