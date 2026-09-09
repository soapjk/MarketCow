"""Build a caller-selected cold Discovery generation, without activating it."""
import json
import os
import tempfile
from pathlib import Path

from marketcow.polymarket_contracts import content_sha256
from marketcow.universe_catalog_artifacts import clone_catalog_artifacts
from marketcow.universe_discovery_seed import build_discovery_seed
from marketcow.universe_empty_state import initialize_empty_candidate
from marketcow.universe_live_candidate import read_static_markets, write_new
from marketcow.universe_source_plan import build_source_plan


def prepare_discovery_candidate(*, source_root: Path, target_root: Path, catalog_source,
        market_ids, selection_id, policy_version, maximum_dependencies, maximum_tokens,
        maximum_row_bytes, maximum_artifact_bytes, maximum_catalog_copy_bytes,
        maximum_source_bytes, maximum_relation_members, depth_quantities, maximum_book_age_ms):
    source_root = source_root.resolve(strict=True)
    target_root = target_root.absolute()
    if target_root.exists() or target_root.is_symlink() or target_root.is_relative_to(source_root):
        raise ValueError("new independent candidate required")
    if not selection_id or not policy_version:
        raise ValueError("explicit caller selection identity and policy required")
    planned = build_source_plan(catalog_source, catalog_revision=catalog_source.revision, pool="discovery",
        market_ids=market_ids, maximum_dependency_markets=maximum_dependencies, maximum_total_tokens=maximum_tokens)
    stage = Path(tempfile.mkdtemp(prefix=".universe-discovery-", dir=target_root.parent))
    manifest, shared = clone_catalog_artifacts(source_root, stage, target_root,
        maximum_copy_bytes=maximum_catalog_copy_bytes)
    if manifest["catalog_revision"] != catalog_source.revision:
        raise ValueError("source catalog revision mismatch")
    universe = {"schema_version": "marketcow.polymarket.realtime-universe.v1",
        "catalog_revision": catalog_source.revision, "market_ids": list(market_ids),
        "market_count": len(market_ids), "token_count": len(market_ids)*2,
        "maximum_market_count": 1000, "source_market_count": manifest["catalog_source"]["market_count"],
        "eligible_market_count": len(market_ids), "policy_version": policy_version,
        "selection_id": selection_id, "selection_owner": "tradude",
        "eligibility_semantics": "explicit_selection_identity_only_not_trade_eligibility"}
    universe["universe_id"] = content_sha256(universe)
    manifest["realtime_universe"] = universe
    catalog_hash = write_new(stage/"catalog.json", manifest, maximum_artifact_bytes)
    markets = read_static_markets(source_root, market_ids, maximum_row_bytes)
    original = json.loads((source_root/"catalog.json").read_bytes())
    seed = build_discovery_seed(manifest=manifest, manifest_sha256=catalog_hash,
        markets=list(markets.values()), raw_path=Path(original["catalog_source"]["raw_path"]),
        depth_quantities=depth_quantities, maximum_book_age_ms=maximum_book_age_ms,
        maximum_source_bytes=maximum_source_bytes, maximum_row_bytes=maximum_row_bytes,
        maximum_relation_members=maximum_relation_members)
    seed_hash = write_new(stage/"discovery-public-seed.json", seed, maximum_artifact_bytes)
    plan_hash = write_new(stage/"rust-discovery-plan.json", planned["plan"], maximum_artifact_bytes)
    dependency_hash = write_new(stage/"discovery-dependencies.json", planned, maximum_artifact_bytes)
    initialize_empty_candidate(stage, catalog_revision=catalog_source.revision)
    state = json.loads((stage/"state-index.json").read_bytes())
    state["path"] = str(target_root/"indexes/latest-state.sqlite3")
    write_new(stage/"state-index.final.json", state, maximum_artifact_bytes)
    os.replace(stage/"state-index.final.json", stage/"state-index.json")
    report = {"selection_id": selection_id, "universe_revision": universe["universe_id"],
        "plan_sha256": plan_hash, "seed_sha256": seed_hash, "dependency_artifact_sha256": dependency_hash,
        "selected": len(market_ids), "collected_token_count": len(market_ids)*2,
        "metadata_dependency_market_count": len(planned["dependency_markets"]),
        "metadata_closure_token_count": planned["total_token_count"],
        "missing_dependency_market_ids": planned["missing_dependency_market_ids"],
        "shared_catalog_artifacts": shared, "root": str(target_root),
        "new_stream_required": True, "preheated": False, "applied": False}
    write_new(stage/"candidate-preparation.json", report, maximum_artifact_bytes)
    if target_root.exists() or target_root.is_symlink():
        raise ValueError("candidate target appeared during preparation")
    os.rename(stage, target_root)
    fd = os.open(target_root.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    return report
