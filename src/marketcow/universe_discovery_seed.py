"""Exact Discovery seed from immutable source facts, without old materializations.

Selection is caller-owned. This module neither collects books nor drops closed
markets, and never infers a settlement from a scheduled date or a price.
"""
import hashlib
import json
from datetime import datetime
from pathlib import Path

from marketcow.polymarket_contracts import content_sha256
from marketcow.polymarket_discovery import (
    DiscoveryRelation, DiscoveryRelationMember, PolymarketDiscoveryStore, _decimal_config,
)


def build_discovery_seed(*, manifest, manifest_sha256, markets, raw_path: Path,
                         depth_quantities, maximum_book_age_ms,
                         maximum_source_bytes, maximum_row_bytes, maximum_relation_members):
    """All source bytes hashed while scanning; only selected facts are retained.

    Dependencies remain relation metadata, not silently added collector markets.
    Source-group counts include unselected raw members so omitted identities
    cannot turn a partial relation into a complete arbitrage combination.
    """
    budgets = (maximum_source_bytes, maximum_row_bytes, maximum_relation_members, maximum_book_age_ms)
    if any(type(x) is not int or x <= 0 for x in budgets):
        raise ValueError("explicit Discovery source budgets required")
    policy = list(_decimal_config(depth_quantities))
    source = manifest["catalog_source"]
    if source["raw_format"] != "canonical_jsonl":
        raise ValueError("unsupported bounded raw catalog format")
    universe = dict(manifest["realtime_universe"])
    revision = universe.pop("universe_id")
    if content_sha256(universe) != revision or universe["catalog_revision"] != manifest["catalog_revision"]:
        raise ValueError("universe identity mismatch")
    ids = universe["market_ids"]
    by_id = {m.identity.market_id: m for m in markets}
    if (ids != sorted(set(ids)) or len(markets) != len(by_id) or set(ids) != set(by_id)
            or universe["market_count"] != len(ids) or not 1 <= len(ids) <= 1000):
        raise ValueError("exact Discovery metadata identities required")
    relations = {}
    for market in markets:
        for relation in market.relations:
            if relation.relation_type != "standard_negative_risk":
                continue
            body = relation.model_dump(mode="json")
            old = relations.setdefault(relation.relation_id, body)
            # Per-market provenance legitimately differs across copies. Match
            # all relationship semantics; raw source hashes below retain every
            # contributing member's evidence rather than inventing provenance.
            if {k: v for k, v in old.items() if k != "provenance"} != {k: v for k, v in body.items() if k != "provenance"}:
                raise ValueError("relation copies disagree")
    if sum(len(r["outcome_pairs"]) for r in relations.values()) > maximum_relation_members:
        raise ValueError("relation member capacity")
    group_hashes = {rid: [] for rid in relations}
    selected_raw, seen_raw, digest, total = {}, set(), hashlib.sha256(), 0
    with raw_path.open("rb") as stream:
        while line := stream.readline(maximum_row_bytes + 1):
            total += len(line)
            if len(line) > maximum_row_bytes or total > maximum_source_bytes:
                raise ValueError("raw catalog byte capacity")
            digest.update(line)
            raw = json.loads(line)
            mid = str(raw.get("id") or "")
            rid = PolymarketDiscoveryStore._relation_id_from_raw(raw)
            if mid in by_id or rid in relations:
                if not mid or mid in seen_raw:
                    raise ValueError("duplicate or missing relevant raw identity")
                seen_raw.add(mid)
                if len(seen_raw) > maximum_relation_members + len(ids):
                    raise ValueError("raw relation identity capacity")
            if rid in group_hashes:
                group_hashes[rid].append(content_sha256(raw))
            if mid in by_id:
                if content_sha256(raw) != by_id[mid].raw_payload_sha256:
                    raise ValueError("raw normalized evidence mismatch")
                selected_raw[mid] = raw
    if digest.hexdigest() != source["raw_payload_sha256"] or set(selected_raw) != set(ids):
        raise ValueError("raw source hash or selected coverage mismatch")
    observed = datetime.fromisoformat(source["observed_at"].replace("Z", "+00:00"))
    if observed.tzinfo is None:
        raise ValueError("source time requires timezone")
    settlements = {}
    for mid in ids:
        fact = PolymarketDiscoveryStore._metadata_fact(None, by_id[mid], selected_raw[mid],
            catalog_revision=manifest["catalog_revision"], source_url=source["source_url"],
            catalog_observed_at=observed)
        settlement = PolymarketDiscoveryStore._settlement_from_metadata(fact)
        settlements[mid] = settlement.model_dump(mode="json") if settlement else None
    typed = []
    for rid, body in sorted(relations.items()):
        members = [DiscoveryRelationMember(**{key: pair[key] for key in (
            "market_id", "condition_id", "yes_token_id", "no_token_id", "outcome_label", "pair_revision")})
            for pair in sorted(body["outcome_pairs"], key=lambda p: p["market_id"])]
        hashes = sorted(group_hashes[rid])
        reasons = set(body["missing_fields"])
        if len(members) != len(hashes):
            reasons.add("member_count_mismatch")
        if len(hashes) < 2:
            reasons.add("insufficient_members")
        if not hashes:
            reasons.add("source_relation_group_missing")
        relation = DiscoveryRelation(relation_id=rid,
            member_market_ids=[m.market_id for m in members], members=members,
            expected_member_count=len(hashes), actual_member_count=len(members),
            complete=not reasons and len(members) == len(hashes) and len(hashes) >= 2,
            valid_from=body["valid_from"], valid_to=body["valid_to"], relation_revision=body["revision"],
            catalog_revision=manifest["catalog_revision"], provenance=body["provenance"],
            evidence_sha256=content_sha256({"catalog_revision": manifest["catalog_revision"],
                "relation_id": rid, "revision": body["revision"],
                "members": [m.model_dump(mode="json") for m in members], "raw_member_payload_sha256": hashes}),
            reason_codes=sorted(reasons))
        typed.append(relation.model_dump(mode="json"))
    return {"schema_version": "marketcow.polymarket.discovery-public-seed.v1",
        "catalog_revision": manifest["catalog_revision"], "catalog_manifest_sha256": manifest_sha256,
        "universe_revision": revision, "markets": [by_id[mid].model_dump(mode="json") for mid in ids],
        "relations": typed, "settlements": settlements, "catalog_source": source,
        "depth_quantities": policy, "maximum_book_age_ms": maximum_book_age_ms}
