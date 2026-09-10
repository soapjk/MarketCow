"""Promote CTF payouts only after two bounded independent endpoint receipts agree."""
import hashlib
from datetime import datetime, timezone

from .btc_hourly_dataset import canonical
from .universe_live_probe import strict_json


METHODS = ["eth_chainId", "eth_getBlockByNumber", "eth_getCode"] + ["eth_call"] * 8


def _read(path, maximum):
    with path.open("rb") as stream:
        raw = stream.read(maximum + 1)
    if len(raw) > maximum:
        raise ValueError("finality_observation_capacity")
    return raw


def _validate_raw_evidence(value):
    evidence = value.get("evidence")
    if not isinstance(evidence, list) or len(evidence) != len(METHODS):
        raise ValueError("finality_evidence_call_count")
    for index, (row, method) in enumerate(zip(evidence, METHODS, strict=True), 1):
        if set(row) != {"request", "raw_sha256", "raw_bytes", "raw_utf8"}:
            raise ValueError("finality_evidence_schema")
        raw = row["raw_utf8"].encode("utf-8")
        if (len(raw) != row["raw_bytes"] or hashlib.sha256(raw).hexdigest() != row["raw_sha256"]
                or row["request"].get("id") != index or row["request"].get("method") != method):
            raise ValueError("finality_evidence_identity")
        response = strict_json(raw)
        if (response.get("jsonrpc") != "2.0" or response.get("id") != index
                or "result" not in response or "error" in response):
            raise ValueError("finality_rpc_response_identity")
    block = strict_json(evidence[1]["raw_utf8"].encode())["result"]
    if block != value.get("block") or not isinstance(block.get("hash"), str):
        raise ValueError("finality_block_identity")
    pinned = {"blockHash": block["hash"], "requireCanonical": True}
    for row in evidence[2:]:
        params = row["request"].get("params")
        if not isinstance(params, list) or params[-1] != pinned:
            raise ValueError("finality_unpinned_call")


def verify_quorum(observations, *, condition_id, token_ids, maximum_bytes=1048576,
                  now=None):
    """Return a payout receipt; this validates tokens, not adapter redemption."""
    if (not isinstance(observations, list) or not 2 <= len(observations) <= 3
            or len({row.get("provider_id") for row in observations}) != len(observations)
            or any(not isinstance(row.get("provider_id"), str) or not row["provider_id"].isascii()
                   for row in observations)):
        raise ValueError("independent_provider_quorum_required")
    if (not isinstance(token_ids, list) or len(token_ids) != 2 or len(set(token_ids)) != 2
            or any(not token.isdigit() for token in token_ids)):
        raise ValueError("invalid_outcome_token_binding")
    receipts, agreement = [], None
    for row in observations:
        path = row["path"]
        if not path.is_absolute():
            raise ValueError("absolute_observation_path_required")
        raw = _read(path, maximum_bytes)
        value = strict_json(raw)
        expected_hex = ["0x" + format(int(token), "064x") for token in token_ids]
        bound = value.get("bound_tokens")
        if (value.get("schema_version") != "marketcow.polymarket.ctf-observation.v1"
                or value.get("condition_id") != condition_id or value.get("chain_id") != "0x89"
                or value.get("status") != "resolved_unverified" or value.get("settlement_import_allowed") is not False
                or value.get("standard_ctf_token_binding_verified") is not True
                or not isinstance(bound, list) or [item.get("token_id_hex") for item in bound] != expected_hex):
            raise ValueError("finality_observation_binding")
        _validate_raw_evidence(value)
        numerators, denominator = value.get("payout_numerators"), value.get("payout_denominator")
        if (not isinstance(numerators, list) or len(numerators) != 2
                or any(not isinstance(n, str) or not n.isdigit() for n in numerators)
                or not isinstance(denominator, str) or not denominator.isdigit() or int(denominator) <= 0
                or sum(map(int, numerators)) != int(denominator)):
            raise ValueError("invalid_final_payout")
        current = (tuple(numerators), denominator, value.get("collateral"), value.get("ctf_address"))
        if agreement is not None and current != agreement:
            raise ValueError("finality_provider_disagreement")
        agreement = current
        receipts.append({"provider_id": row["provider_id"], "observation_sha256": hashlib.sha256(raw).hexdigest(),
                         "block_hash": value["block"]["hash"], "block_number": value["block"]["number"],
                         "observed_at": value["observed_at"]})
    instant = (now or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return {"schema_version": "marketcow.polymarket.ctf-finality-quorum.v1", "condition_id": condition_id,
            "token_ids": token_ids, "payout_numerators": list(agreement[0]), "payout_denominator": agreement[1],
            "collateral": agreement[2], "ctf_address": agreement[3], "status": "verified_final",
            "finality_policy": "two_independent_rpc_finalized_receipts_v1", "provider_receipts": receipts,
            "verified_at": instant, "settlement_import_allowed": True,
            "limitation": "verifies standard CTF token payouts; adapter redemption execution is outside this receipt"}


def write_receipt(path, value):
    raw = canonical(value)
    with path.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        import os
        os.fsync(stream.fileno())
    return hashlib.sha256(raw).hexdigest()
