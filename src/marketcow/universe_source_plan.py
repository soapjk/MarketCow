"""Build an exact Rust source plan and bounded dependency closure from catalog.

No ranking, REST book manufacture, state copying or activation. Missing source
identities are returned explicitly. The selected plan never grows to include
dependency identities; those are a separate collection/resource artifact.
"""
from collections import deque


def build_source_plan(source, *, catalog_revision, pool, market_ids,
                      maximum_dependency_markets, maximum_total_tokens):
    if source.revision != catalog_revision:
        raise ValueError("catalog_revision_mismatch")
    if pool not in ("discovery", "live"):
        raise ValueError("invalid pool")
    if any(type(x) is not int or x < 0 for x in (maximum_dependency_markets, maximum_total_tokens)):
        raise ValueError("explicit dependency budgets required")
    if (not isinstance(market_ids, tuple) or tuple(sorted(set(market_ids))) != market_ids
            or any(not isinstance(x, str) or not x or not x.isascii() for x in market_ids)
            or not 1 <= len(market_ids) <= (1000 if pool == "discovery" else 250)):
        raise ValueError("invalid exact selection")
    selected = set(market_ids)
    queued = set(selected)
    queue = deque(market_ids)
    rows, missing, tokens, relations = {}, [], set(), {}
    while queue:
        mid = queue.popleft()
        record = source.get(mid)
        if record is None:
            if mid in selected:
                raise ValueError("unknown_market_id")
            missing.append(mid)
            continue
        if record["market_id"] != mid:
            raise ValueError("catalog identity mismatch")
        token_ids = [x["token_id"] for x in record["outcomes"]]
        if len(token_ids) != 2 or len(set(token_ids)) != 2 or any(not x for x in token_ids):
            raise ValueError("unsupported source token identity")
        if tokens.intersection(token_ids):
            raise ValueError("duplicate cross-market token identity")
        tokens.update(token_ids)
        if len(tokens) > maximum_total_tokens:
            raise ValueError("token capacity exceeded")
        rows[mid] = {"market_id": mid, "condition_id": record["condition_id"], "token_ids": token_ids}
        for relation in record["relations"]:
            rid = relation["relation_id"]
            previous = relations.get(rid)
            if previous is not None and previous != relation:
                raise ValueError("relation identity conflict")
            relations[rid] = relation
            for dependency in relation["member_market_ids"]:
                if dependency in queued:
                    continue
                # Account for unresolved members too; absent metadata cannot
                # bypass dependency admission or create an unbounded queue.
                if len(queued)-len(selected) >= maximum_dependency_markets:
                    raise ValueError("dependency capacity exceeded")
                queued.add(dependency)
                queue.append(dependency)
    return {
        "plan": {"schema_version": "marketcow.polymarket.rust-"+
                 ("discovery" if pool == "discovery" else "scoped")+"-source-plan.v1",
                 "catalog_revision": catalog_revision, "markets": [rows[mid] for mid in market_ids]},
        "dependency_markets": [rows[mid] for mid in sorted(set(rows)-selected)],
        "missing_dependency_market_ids": sorted(missing),
        "relations": [relations[rid] for rid in sorted(relations)],
        "total_token_count": len(tokens),
    }
