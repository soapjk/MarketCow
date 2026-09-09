"""Scope artifacts for a running Rust owner; no data root, unit or process clone."""
import copy
import hashlib
import json
from pathlib import Path

from marketcow.polymarket_contracts import content_sha256, canonical_json
from marketcow.polymarket_configured_scope import PolymarketConfiguredScope
from marketcow.universe_discovery_seed import build_discovery_seed
from marketcow.universe_live_candidate import read_static_markets
from marketcow.universe_source_plan import build_source_plan


def build_hot_candidate(*, source_root, source, pool, market_ids, selection_sha256,
                        policy_version, protected_market_ids, parent_selection_id,
                        maximum_dependency_markets, maximum_metadata_tokens,
                        maximum_row_bytes, maximum_artifact_bytes,
                        maximum_source_bytes, maximum_relation_members,
                        depth_quantities, maximum_book_age_ms):
    root = Path(source_root).resolve(strict=True)
    ids = tuple(market_ids)
    protected = tuple(protected_market_ids)
    if (protected != tuple(sorted(set(protected))) or not set(protected) <= set(ids)
            or not policy_version or len(selection_sha256) != 64
            or any(c not in "0123456789abcdef" for c in selection_sha256)):
        raise ValueError("invalid hot selection/protection identity")
    if pool == "live" and not parent_selection_id:
        raise ValueError("explicit Live parent selection required")
    plan = build_source_plan(source, catalog_revision=source.revision, pool=pool, market_ids=ids,
        maximum_dependency_markets=maximum_dependency_markets, maximum_total_tokens=maximum_metadata_tokens)
    with (root/"catalog.json").open("rb") as stream:
        manifest_raw = stream.read(maximum_artifact_bytes+1)
    if len(manifest_raw) > maximum_artifact_bytes:
        raise ValueError("catalog manifest byte capacity")
    manifest = json.loads(manifest_raw)
    if manifest["catalog_revision"] != source.revision:
        raise ValueError("hot catalog views differ")
    all_ids = sorted(row["market_id"] for row in plan["plan"]["markets"]+plan["dependency_markets"])
    markets = read_static_markets(root, all_ids, maximum_row_bytes)
    for row in plan["plan"]["markets"]+plan["dependency_markets"]:
        market = markets[row["market_id"]]
        if (row["condition_id"] != market.identity.condition_id
                or set(row["token_ids"]) != {x.token_id for x in market.identity.outcomes}):
            raise ValueError("hot metadata identity mismatch")
    if pool == "live":
        configured = [dict(**row, end_at=markets[row["market_id"]].end_at.isoformat().replace("+00:00", "Z")
                           if markets[row["market_id"]].end_at else None) for row in plan["plan"]["markets"]]
        identity = dict(catalog_revision=source.revision, configured_markets=configured, mode="shadow")
        config = PolymarketConfiguredScope.model_validate(dict(**identity,
            schema_version="marketcow.polymarket.scope-discovery.v1", configured_market_count=len(configured),
            active_scope_id=content_sha256(identity))).model_dump(mode="json")
        acquisition = all_ids
    else:
        virtual = copy.deepcopy(manifest)
        universe = {"schema_version": "marketcow.polymarket.realtime-universe.v1",
            "catalog_revision": source.revision, "market_ids": list(ids), "market_count": len(ids),
            "token_count": len(ids)*2, "maximum_market_count": 1000,
            "source_market_count": manifest["catalog_source"]["market_count"], "eligible_market_count": len(ids),
            "policy_version": policy_version, "selection_id": selection_sha256, "selection_owner": "tradude",
            "eligibility_semantics": "explicit_selection_identity_only_not_trade_eligibility"}
        universe["universe_id"] = content_sha256(universe)
        virtual["realtime_universe"] = universe
        seed = build_discovery_seed(manifest=virtual, manifest_sha256=hashlib.sha256(manifest_raw).hexdigest(),
            markets=[markets[mid] for mid in ids], raw_path=Path(manifest["catalog_source"]["raw_path"]),
            depth_quantities=depth_quantities, maximum_book_age_ms=maximum_book_age_ms,
            maximum_source_bytes=maximum_source_bytes, maximum_row_bytes=maximum_row_bytes,
            maximum_relation_members=maximum_relation_members)
        config = dict(catalog_revision=source.revision, universe_revision=universe["universe_id"],
            projection_id=content_sha256({"universe_revision": universe["universe_id"], "selection": selection_sha256,
                                         "depth_quantities": seed["depth_quantities"], "maximum_book_age_ms": maximum_book_age_ms}),
            market_ids=list(ids), relations=seed["relations"], settlements=seed["settlements"],
            policy=dict(quantities=seed["depth_quantities"], maximum_book_age_ms=maximum_book_age_ms))
        acquisition = list(ids)
    candidate = dict(schema_version="marketcow.hot-scope-candidate.v1", pool=pool,
        catalog_revision=source.revision, selection_sha256=selection_sha256, parent_selection_id=parent_selection_id,
        requested_market_ids=list(ids), protected_market_ids=list(protected),
        acquisition_market_ids=acquisition, records=[markets[mid].model_dump(mode="json") for mid in all_ids],
        missing_dependency_market_ids=plan["missing_dependency_market_ids"], config=config)
    candidate["candidate_id"] = content_sha256(candidate)
    if len(canonical_json(candidate)) > maximum_artifact_bytes:
        raise ValueError("hot candidate byte capacity")
    return candidate
