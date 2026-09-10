import hashlib
import sys
from pathlib import Path

import pytest

from marketcow.btc_hourly_dataset import canonical
from marketcow.btc_lifecycle import HourRegistry
from marketcow.btc_settlement_worker import sweep
from tests.test_btc_lifecycle import binding


def fixture_files(root):
    b = dict(binding(), start_utc="2020-01-01T00:00:00Z", end_utc="2020-01-01T01:00:00Z")
    registry = HourRegistry(root / "registry.sqlite", maximum_markets=3, maximum_bytes=32768)
    registry.register([b])
    profile = root / "rpc.json"
    profile.write_bytes(canonical(dict(condition_id=b["condition_id"],
        token_ids_hex=["0x"+format(int(b[k]), "064x") for k in ("up_token", "down_token")],
        rpc_endpoint="https://synthetic.invalid/secret", chain_id="0x89", contract="c",
        code_sha256="a"*64, finality_policy="rpc_finalized_hash_pinned_v1", collateral="c")))
    binary = Path(sys.executable)
    with binary.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    return registry, b, profile, binary, digest


def test_rust_receipt_is_stored_without_finality_promotion(tmp_path):
    registry, b, profile, binary, digest = fixture_files(tmp_path)
    def runner(executable, config, output, timeout):
        assert executable == binary and config == profile and timeout <= 10
        output.write_bytes(canonical(dict(schema_version="marketcow.polymarket.ctf-observation.v1",
            condition_id=b["condition_id"], status="resolved_unverified", observed_at=b["end_utc"],
            settlement_import_allowed=False)))
        return 0
    report = sweep(registry, {b["market_id"]: str(profile)}, binary, digest, tmp_path / "out",
                   market_ids=[b["market_id"]], seconds=10, maximum_markets=1, runner=runner)
    assert report["complete"] and report["observations"][0]["stored"]
    assert report["settlement_import_allowed"] is False
    assert b"secret" not in (tmp_path / "out/report.json").read_bytes()


def test_wrong_binding_fails_before_child(tmp_path):
    registry, b, profile, binary, digest = fixture_files(tmp_path)
    profile.write_bytes(canonical(dict(condition_id="wrong", token_ids_hex=[])))
    with pytest.raises(ValueError, match="identity"):
        sweep(registry, {b["market_id"]: str(profile)}, binary, digest, tmp_path / "out",
              market_ids=[b["market_id"]], seconds=10, maximum_markets=1,
              runner=lambda *args: pytest.fail("must not run"))
    assert not (tmp_path / "out").exists()


def test_reader_failure_preserves_raw_and_does_not_store_success(tmp_path):
    registry, b, profile, binary, digest = fixture_files(tmp_path)
    def runner(executable, config, output, timeout):
        output.write_bytes(b'{"status":"read_failed"}')
        return 1
    report = sweep(registry, {b["market_id"]: str(profile)}, binary, digest, tmp_path / "out",
                   market_ids=[b["market_id"]], seconds=10, maximum_markets=1, runner=runner)
    assert not report["complete"] and not report["observations"][0]["stored"]
    assert (tmp_path / "out/observation-0.json").exists()
