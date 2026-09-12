import hashlib
import json
from datetime import datetime, timezone

import pytest

from marketcow import btc_research_package as subject


def row(number):
    evidence = {
        "schema_version": "marketcow.polymarket.market-evidence.v1",
        "market_id": str(number),
        "condition_id": "0x" + f"{number:064x}",
        "outcomes": [
            {"token_id": str(number * 10), "outcome": "Up"},
            {"token_id": str(number * 10 + 1), "outcome": "Down"},
        ],
    }
    return {
        "evidence": evidence,
        "review": {"market_id": str(number)},
        "binding": {
            "market_id": str(number),
            "condition_id": evidence["condition_id"],
            "up_token": str(number * 10),
            "down_token": str(number * 10 + 1),
        },
    }


@pytest.mark.asyncio
async def test_builds_exact_reviewed_fixed_budget_package(tmp_path, monkeypatch):
    async def discover(*args, **kwargs):
        return [row(1), row(2), row(3)]

    monkeypatch.setattr(subject, "discover", discover)
    root = tmp_path / "package"
    result = await subject.build_package("http://127.0.0.1:1", root, now=datetime(2026, 9, 12, tzinfo=timezone.utc))
    config_raw = (root / "stream-config.json").read_bytes()
    config = json.loads(config_raw)
    assert result["stream_config_sha256"] == hashlib.sha256(config_raw).hexdigest()
    assert config["seconds"] == 1800
    assert len(config["markets"]) == 3
    assert all(value["rule_evidence_path"].startswith(str(root)) for value in config["markets"])
    assert result["subscription_started"] is False


@pytest.mark.asyncio
async def test_incomplete_discovery_does_not_publish_manifest(tmp_path, monkeypatch):
    async def discover(*args, **kwargs):
        return [row(1)]

    monkeypatch.setattr(subject, "discover", discover)
    root = tmp_path / "incomplete"
    with pytest.raises(ValueError, match="three_reviewed_hours_required"):
        await subject.build_package("http://127.0.0.1:1", root, now=datetime(2026, 9, 12, tzinfo=timezone.utc))
    assert not (root / "manifest.json").exists()
