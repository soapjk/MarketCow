"""Offline exact Live candidate artifacts; no service launch or book copying."""
import hashlib
import json
import os
import sqlite3
import tempfile
from pathlib import Path

from marketcow.polymarket_configured_scope import PolymarketConfiguredScope
from marketcow.polymarket_contracts import canonical_json, content_sha256
from marketcow.polymarket_live import LiveMarket
from marketcow.universe_catalog_artifacts import clone_catalog_artifacts
from marketcow.universe_empty_state import initialize_empty_candidate
from marketcow.universe_source_plan import build_source_plan


def write_new(path, value, maximum):
    raw = canonical_json(value)
    if len(raw) > maximum:
        raise ValueError("candidate artifact byte capacity")
    with path.open("xb") as stream:
        stream.write(raw); stream.flush(); os.fsync(stream.fileno())
    return hashlib.sha256(raw).hexdigest()


def read_static_markets(source_root, ids, maximum_row_bytes):
    """Read only named, hash-bound normalized rows after artifact verification."""
    original = json.loads((source_root/"catalog.json").read_bytes())
    index_path = Path(original["catalog_index"]["path"])
    normalized = Path(original["normalized_catalog"]["path"])
    markets = {}
    with sqlite3.connect(index_path.as_uri()+"?mode=ro", uri=True) as db, normalized.open("rb") as stream:
        db.execute("BEGIN")
        for mid in ids:
            row = db.execute("SELECT byte_offset,byte_length,row_sha256 FROM markets WHERE market_id=?", (mid,)).fetchone()
            if row is None or not 0 < row[1] <= maximum_row_bytes:
                raise ValueError("missing or oversized static market")
            offset, size, sha = row
            stream.seek(offset); raw = stream.read(size)
            if hashlib.sha256(raw).hexdigest() != sha or stream.read(1) != b"\n":
                raise ValueError("static market hash mismatch")
            market = LiveMarket.model_validate_json(raw)
            if market.identity.market_id != mid:
                raise ValueError("static market identity mismatch")
            markets[mid] = market
    return markets


def prepare_live_candidate(*, source_root: Path, target_root: Path, catalog_source,
                           market_ids, maximum_dependencies, maximum_tokens,
                           maximum_row_bytes, maximum_artifact_bytes, maximum_catalog_copy_bytes):
    """Caller holds operator owner lock; failure leaves unpublished stage evidence.

    This operation is not admission or permission to activate. Selection has to
    be validated/protected by the authenticated caller before invoking it.
    """
    source_root = source_root.resolve(strict=True)
    target_root = target_root.absolute()
    if target_root.exists() or target_root.is_symlink() or target_root.is_relative_to(source_root):
        raise ValueError("new independent candidate required")
    if any(type(x) is not int or x <= 0 for x in (maximum_row_bytes, maximum_artifact_bytes)):
        raise ValueError("explicit artifact budgets required")
    planned = build_source_plan(catalog_source, catalog_revision=catalog_source.revision, pool="live",
        market_ids=market_ids, maximum_dependency_markets=maximum_dependencies, maximum_total_tokens=maximum_tokens)
    stage = Path(tempfile.mkdtemp(prefix=".universe-live-", dir=target_root.parent))
    manifest, shared = clone_catalog_artifacts(source_root, stage, target_root,
                                              maximum_copy_bytes=maximum_catalog_copy_bytes)
    if manifest["catalog_revision"] != catalog_source.revision:
        raise ValueError("source catalog revision mismatch")
    manifest.pop("realtime_universe", None)
    catalog_hash = write_new(stage/"catalog.json", manifest, maximum_artifact_bytes)
    # Immutable source index was fully hash-verified by clone_catalog_artifacts.
    ids = sorted({x["market_id"] for x in planned["plan"]["markets"]+planned["dependency_markets"]})
    markets = read_static_markets(source_root, ids, maximum_row_bytes)
    configured = []
    for row in planned["plan"]["markets"]:
        market = markets[row["market_id"]]
        if row["condition_id"] != market.identity.condition_id or set(row["token_ids"]) != {x.token_id for x in market.identity.outcomes}:
            raise ValueError("catalog views disagree")
        configured.append(dict(**row, end_at=market.end_at.isoformat().replace("+00:00", "Z") if market.end_at else None))
    identity = dict(catalog_revision=catalog_source.revision, configured_markets=configured, mode="shadow")
    scope = PolymarketConfiguredScope.model_validate(dict(**identity,
        schema_version="marketcow.polymarket.scope-discovery.v1", configured_market_count=len(configured),
        active_scope_id=content_sha256(identity)))
    scope_hash = write_new(stage/"configured-scope.json", scope.model_dump(mode="json"), maximum_artifact_bytes)
    write_new(stage/"scope-runtime.json", dict(schema_version="marketcow.polymarket.scope-runtime.v1",
        scope_id=scope.active_scope_id, manifest_sha256=scope_hash), maximum_artifact_bytes)
    plan_hash = write_new(stage/"rust-scoped-plan-r1.json", planned["plan"], maximum_artifact_bytes)
    dependency_hash = write_new(stage/"live-bridge-plan-r1.json", dict(
        schema_version="marketcow.polymarket.live-bridge-plan.v1", catalog_revision=catalog_source.revision,
        scope_id=scope.active_scope_id, catalog_manifest_sha256=catalog_hash, catalog_source=manifest["catalog_source"],
        markets=[markets[mid].model_dump(mode="json") for mid in ids]), maximum_artifact_bytes)
    initialize_empty_candidate(stage, catalog_revision=catalog_source.revision)
    state = json.loads((stage/"state-index.json").read_bytes())
    state["path"] = str(target_root/"indexes/latest-state.sqlite3")
    write_new(stage/"state-index.final.json", state, maximum_artifact_bytes)
    os.replace(stage/"state-index.final.json", stage/"state-index.json")
    report = dict(scope_id=scope.active_scope_id, configured_scope_sha256=scope_hash,
        plan_sha256=plan_hash, dependency_plan_sha256=dependency_hash,
        selected=len(configured), dependency_markets=len(planned["dependency_markets"]),
        missing_dependency_market_ids=planned["missing_dependency_market_ids"], shared_catalog_artifacts=shared,
        root=str(target_root), new_stream_required=True, preheated=False, applied=False)
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
