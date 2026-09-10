from pathlib import Path

from marketcow.btc_hourly_dataset import canonical
from marketcow.btc_lifecycle import HourRegistry
import hashlib

import pytest

from marketcow.btc_settlement_monitor import providers, sweep_once
from tests.test_btc_lifecycle import binding


def test_round_runs_two_providers_and_stores_quorum(tmp_path):
    b = dict(binding(), start_utc="2020-01-01T00:00:00Z", end_utc="2020-01-01T01:00:00Z")
    registry = HourRegistry(tmp_path / "registry", maximum_markets=3, maximum_bytes=32768)
    registry.register([b])
    configs = []
    for name in ("a", "b"):
        path = tmp_path / (name + ".json"); path.write_text("{}")
        configs.append({"provider_id": name, "config_path": path})
    calls = []
    def runner(binary, config, condition, tokens, output, timeout):
        calls.append((config, condition, tokens))
        output.write_bytes(canonical({"status": "resolved_unverified"}))
        return 0
    def verifier(observations, **identity):
        assert len(observations) == 2 and identity["condition_id"] == b["condition_id"]
        return {"schema_version": "marketcow.polymarket.ctf-finality-quorum.v1",
                "condition_id": b["condition_id"], "token_ids": [b["up_token"], b["down_token"]],
                "payout_numerators": ["0", "1"], "payout_denominator": "1", "status": "verified_final",
                "finality_policy": "two_independent_rpc_finalized_receipts_v1",
                "provider_receipts": [{"provider_id": "a"}, {"provider_id": "b"}],
                "verified_at": "2026-09-10T00:00:00Z", "settlement_import_allowed": True}
    report = sweep_once(registry, configs, Path("/bin/echo"), tmp_path / "round",
                        maximum_markets=2, seconds=10, runner=runner, verifier=verifier)
    assert report["complete"] and report["markets"][0]["status"] == "verified_final"
    assert len(calls) == 2
    assert registry.plan(__import__("datetime").datetime.now(__import__("datetime").timezone.utc),
                         maximum_subscriptions=3)["settlement_pending"] == []


def test_unresolved_is_preserved_without_quorum_promotion(tmp_path):
    b = dict(binding(), start_utc="2020-01-01T00:00:00Z", end_utc="2020-01-01T01:00:00Z")
    registry = HourRegistry(tmp_path / "registry", maximum_markets=3, maximum_bytes=32768)
    registry.register([b])
    rows = [{"provider_id": x, "config_path": tmp_path / x} for x in ("a", "b")]
    def runner(binary, config, condition, tokens, output, timeout):
        output.write_bytes(b'{"status":"unresolved"}')
        return 0
    report = sweep_once(registry, rows, Path("/bin/echo"), tmp_path / "round",
                        maximum_markets=2, seconds=10, runner=runner,
                        verifier=lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("no quorum")))
    assert report["complete"] and report["markets"][0]["status"] == "unresolved"


def test_provider_manifest_requires_distinct_rpc_endpoints(tmp_path):
    rows = []
    for name, endpoint in (("a", "https://rpc-a.invalid"), ("b", "https://rpc-b.invalid")):
        config = tmp_path / (name + ".json")
        config.write_bytes(canonical({"rpc_endpoint": endpoint}))
        rows.append({"provider_id": name, "config_path": str(config),
                     "config_sha256": hashlib.sha256(config.read_bytes()).hexdigest()})
    manifest = tmp_path / "providers.json"
    manifest.write_bytes(canonical(rows))
    assert len(providers(manifest)) == 2
    rows[1]["config_path"] = rows[0]["config_path"]
    rows[1]["config_sha256"] = rows[0]["config_sha256"]
    manifest.write_bytes(canonical(rows))
    with pytest.raises(ValueError, match="independent_provider"):
        providers(manifest)
