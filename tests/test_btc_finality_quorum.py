import hashlib
from datetime import datetime, timezone

import pytest

from marketcow.btc_finality_quorum import verify_quorum
from marketcow.btc_hourly_dataset import canonical


TOKENS = ["1", "2"]
CONDITION = "0x" + "c" * 64


def observation(path, provider, payouts=(0, 1), mutate=None):
    block = {"hash": "0x" + provider * 64, "number": "0x10", "timestamp": "0x20"}
    methods = ["eth_chainId", "eth_getBlockByNumber", "eth_getCode"] + ["eth_call"] * 8
    evidence = []
    for index, method in enumerate(methods, 1):
        request = {"jsonrpc": "2.0", "id": index, "method": method,
                   "params": [] if index == 1 else (["finalized", False] if index == 2 else
                   (["0x" + "a" * 40, {"blockHash": block["hash"], "requireCanonical": True}]
                    if index == 3 else [{"to": "0x" + "a" * 40}, {"blockHash": block["hash"], "requireCanonical": True}]))}
        result = "0x89" if index == 1 else block if index == 2 else "0x" + "0" * 64
        raw = canonical({"jsonrpc": "2.0", "id": index, "result": result})
        evidence.append({"request": request, "raw_sha256": hashlib.sha256(raw).hexdigest(),
                         "raw_bytes": len(raw), "raw_utf8": raw.decode()})
    value = {"schema_version": "marketcow.polymarket.ctf-observation.v1", "condition_id": CONDITION,
             "chain_id": "0x89", "ctf_address": "0x" + "a" * 40, "collateral": "0x" + "b" * 40,
             "block": block, "status": "resolved_unverified", "settlement_import_allowed": False,
             "standard_ctf_token_binding_verified": True, "payout_numerators": list(map(str, payouts)),
             "payout_denominator": str(sum(payouts)), "observed_at": "2026-09-10T00:00:00Z",
             "bound_tokens": [{"token_id_hex": "0x" + format(int(t), "064x")} for t in TOKENS],
             "evidence": evidence}
    if mutate:
        mutate(value)
    path.write_bytes(canonical(value))
    return {"provider_id": provider, "path": path}


def test_two_independent_bound_receipts_promote_exact_payout(tmp_path):
    rows = [observation(tmp_path / "a", "a"), observation(tmp_path / "b", "b")]
    value = verify_quorum(rows, condition_id=CONDITION, token_ids=TOKENS,
                          now=datetime(2026, 9, 10, tzinfo=timezone.utc))
    assert value["status"] == "verified_final" and value["settlement_import_allowed"] is True
    assert value["payout_numerators"] == ["0", "1"] and len(value["provider_receipts"]) == 2


def test_disagreement_duplicate_provider_and_tamper_fail(tmp_path):
    a = observation(tmp_path / "a", "a")
    b = observation(tmp_path / "b", "b", (1, 0))
    with pytest.raises(ValueError, match="disagreement"):
        verify_quorum([a, b], condition_id=CONDITION, token_ids=TOKENS)
    with pytest.raises(ValueError, match="independent"):
        verify_quorum([a, dict(a)], condition_id=CONDITION, token_ids=TOKENS)
    c = observation(tmp_path / "c", "c", mutate=lambda v: v["evidence"][3].update(raw_sha256="0" * 64))
    with pytest.raises(ValueError, match="identity"):
        verify_quorum([a, c], condition_id=CONDITION, token_ids=TOKENS)


def test_unresolved_and_wrong_token_never_promote(tmp_path):
    a = observation(tmp_path / "a", "a", (0, 0))
    b = observation(tmp_path / "b", "b", (0, 0))
    with pytest.raises(ValueError, match="payout"):
        verify_quorum([a, b], condition_id=CONDITION, token_ids=TOKENS)
    c = observation(tmp_path / "c", "c")
    with pytest.raises(ValueError, match="binding"):
        verify_quorum([observation(tmp_path / "d", "d"), c], condition_id=CONDITION, token_ids=["1", "3"])
