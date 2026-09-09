"""Admission -> bounded cold artifacts -> actual Rust preheat -> registration.

This operator-owned worker never selects desired state or changes a public unit.
Failed jobs remain inspectable and consume a finite slot until explicit cleanup;
an existing target is never silently rebuilt after a crash.
"""
import asyncio
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import shutil
import time

from marketcow.universe_control_server import read_json
from marketcow.universe_discovery_candidate import prepare_discovery_candidate
from marketcow.universe_generation import GenerationStore
from marketcow.universe_generation_preparation import preheat_and_register
from marketcow.universe_live_candidate import write_new
from marketcow.universe_owner import SupervisorOwner
from marketcow.universe_phase1 import _parse_millis, selection_sha256, parse_request_bytes
from marketcow.universe_systemd import DiscoveryUnitBinding, SystemdUnits, UnitArtifact
from marketcow.universe_unit_artifacts import build_unit_pair


class DiscoveryPreparation:
    def __init__(self, control, config):
        fields = {"source_root", "candidate_parent", "artifact_parent", "release_root", "user_unit_root",
            "owner_lock", "generation_store", "template", "binary", "binary_sha256", "maximum_jobs",
            "maximum_generation_bytes", "minimum_free_bytes", "candidate_disk_reserve_bytes",
            "maximum_row_bytes", "maximum_artifact_bytes", "maximum_catalog_copy_bytes", "maximum_source_bytes",
            "maximum_relation_members", "depth_quantities", "maximum_book_age_ms", "preheat_listener",
            "public_listener", "preheat_endpoint", "public_endpoint", "command_timeout_seconds",
            "operation_timeout_seconds", "maximum_runtime_seconds", "full_sync_bytes", "frame_bytes",
            "maximum_frames", "maximum_stream_bytes"}
        if set(config) != fields:
            raise ValueError("explicit preparation operator profile required")
        for key in ("source_root", "candidate_parent", "artifact_parent", "release_root", "user_unit_root",
                    "owner_lock", "generation_store", "binary"):
            if not Path(config[key]).is_absolute():
                raise ValueError("absolute preparation operator paths required")
        for key in fields-set(("source_root", "candidate_parent", "artifact_parent", "release_root", "user_unit_root",
                "owner_lock", "generation_store", "binary", "template", "binary_sha256", "depth_quantities",
                "preheat_listener", "public_listener", "preheat_endpoint", "public_endpoint")):
            if type(config[key]) is not int or config[key] <= 0:
                raise ValueError("positive integer preparation budgets required")
        if config["maximum_jobs"] > 16 or config["operation_timeout_seconds"] > 300:
            raise ValueError("preparation worker capacity exceeded")
        self.control, self.config = control, dict(config)

    def registration(self, generation_id):
        parent = Path(self.config["candidate_parent"])
        jobs = [p for p in parent.iterdir() if p.name.startswith("discovery-")]
        if len(jobs) > self.config["maximum_jobs"]:
            raise ValueError("preparation capacity corrupt")
        for job in jobs:
            if job.is_symlink():
                raise ValueError("unsafe preparation job")
            path = job/"prepared-receipt.json"
            if path.is_file():
                value = read_json(path, self.config["maximum_generation_bytes"])
                if value["response"]["generation_id"] == generation_id:
                    return value["operation"]
        return None

    def prepare(self, caller, request, response_sha256):
        c = self.config
        with SupervisorOwner(Path(c["owner_lock"])):
            admission = self.control.admitted_for_preparation(caller, request, response_sha256)
            digest = request.request_digest()
            key = hashlib.sha256((caller+":"+digest).encode()).hexdigest()
            parent, artifacts = Path(c["candidate_parent"]), Path(c["artifact_parent"])
            job = parent/("discovery-"+key)
            receipt_path = job/"prepared-receipt.json"
            if receipt_path.is_file():
                stored = read_json(receipt_path, c["maximum_generation_bytes"])
                if stored["caller"] != caller or stored["request_digest"] != digest or stored["admission_response_sha256"] != response_sha256:
                    raise ValueError("preparation_retry_conflict")
                return stored["response"]
            if job.exists() or job.is_symlink():
                raise ValueError("preparation_requires_reconciliation")
            if sum(1 for p in parent.iterdir() if p.name.startswith("discovery-")) >= c["maximum_jobs"]:
                raise ValueError("preparation_capacity_exceeded")
            if shutil.disk_usage(parent).free < c["minimum_free_bytes"]+c["candidate_disk_reserve_bytes"]:
                raise ValueError("preparation_disk_reserve_unavailable")
            # A durable reservation precedes artifact work. No automatic retry
            # can create another root for the same request after interruption.
            job.mkdir(mode=0o700)
            write_new(job/"request-binding.json", {"caller": caller, "request_digest": digest,
                "admission_response_sha256": response_sha256, "expires_at": request.expires_at}, 65536)
            directory = artifacts/("discovery-"+key)
            a = self.control.profile["admission"]
            prepared = prepare_discovery_candidate(source_root=Path(c["source_root"]), target_root=job/"source",
                catalog_source=self.control.source, market_ids=tuple(request.selection.market_ids),
                selection_id=request.selection_sha256, policy_version=request.selection.tradude_policy_version,
                maximum_dependencies=a["max_dependency_markets"], maximum_tokens=a["max_total_tokens"],
                maximum_row_bytes=c["maximum_row_bytes"], maximum_artifact_bytes=c["maximum_artifact_bytes"],
                maximum_catalog_copy_bytes=c["maximum_catalog_copy_bytes"], maximum_source_bytes=c["maximum_source_bytes"],
                maximum_relation_members=c["maximum_relation_members"], depth_quantities=c["depth_quantities"],
                maximum_book_age_ms=c["maximum_book_age_ms"])
            template_row = c["template"]
            if set(template_row) != {"name", "path", "sha256"}:
                raise ValueError("invalid registered template")
            template = UnitArtifact(template_row["name"], Path(template_row["path"]), template_row["sha256"])
            template.verify(Path(c["release_root"]))
            pair = build_unit_pair(pool="discovery", template=template, binary=Path(c["binary"]),
                binary_sha256=c["binary_sha256"], candidate_root=job/"source", preparation=prepared, directory=directory,
                preheat_name="marketcow-universe-discovery-"+key[:20]+".service",
                preheat_listener=c["preheat_listener"], public_listener=c["public_listener"],
                preheat_log=job/"preheat.log", public_log=job/"public.log", maximum_runtime_seconds=c["maximum_runtime_seconds"])
            unit_root = Path(c["user_unit_root"])
            (unit_root/pair["preheat"].name).symlink_to(pair["preheat"].path)
            units = SystemdUnits(release_root=Path(c["release_root"]), user_unit_root=unit_root,
                                 command_timeout_seconds=c["command_timeout_seconds"])
            binding = DiscoveryUnitBinding(generation_id="unregistered", scope_id="not-a-live-scope",
                preheat=pair["preheat"], candidate=pair["candidate"], incumbent=template,
                preheat_endpoint=c["preheat_endpoint"], public_endpoint=c["public_endpoint"],
                full_sync_bytes=c["full_sync_bytes"], frame_bytes=c["frame_bytes"], maximum_frames=c["maximum_frames"],
                maximum_stream_bytes=c["maximum_stream_bytes"], projection_id="observed-per-process-at-fullsync",
                universe_revision=prepared["universe_revision"])
            store = GenerationStore(Path(c["generation_store"]), maximum_generations=c["maximum_jobs"],
                                    maximum_record_bytes=c["maximum_generation_bytes"])
            async def preheat():
                await units._command("daemon-reload")
                return await preheat_and_register(store, units, binding, pool="discovery", selection_id=request.selection_sha256,
                    catalog_revision=request.selection.catalog_revision, market_ids=tuple(request.selection.market_ids),
                    dependency_market_ids=tuple(admission["dependency_market_ids"]),
                    protected_market_ids=tuple(p.market_id for p in request.selection.protected_markets), parent_selection_id=None,
                    expires_ms=_parse_millis(request.expires_at), now_ms=lambda: time.time_ns()//1000000,
                    operation_timeout_seconds=c["operation_timeout_seconds"])
            try:
                generation, registered, boundary = asyncio.run(preheat())
            finally:
                store.close()
            operation_config = {"pool": "discovery", "store_path": c["generation_store"], "owner_lock": c["owner_lock"],
                "maximum_generations": c["maximum_jobs"], "maximum_record_bytes": c["maximum_generation_bytes"],
                "release_root": c["release_root"], "user_unit_root": c["user_unit_root"],
                "command_timeout_seconds": c["command_timeout_seconds"], "operation_timeout_seconds": c["operation_timeout_seconds"],
                "binding": json.loads(json.dumps(asdict(registered), default=str))}
            config_path = job/"operation.json"
            write_new(config_path, operation_config, 65536)
            response = {"schema_version": "marketcow.polymarket.discovery-prepared.v1", "status": "prepared",
                "request_id": request.request_id, "admission_id": admission["admission_id"],
                "generation_id": generation.generation_id, "selection_sha256": request.selection_sha256,
                "catalog_revision": request.selection.catalog_revision, "universe_revision": prepared["universe_revision"],
                "requested_market_ids": request.selection.market_ids, "dependency_market_ids": admission["dependency_market_ids"],
                "expires_at": request.expires_at, "preheat_boundary": asdict(boundary), "active": False}
            pending_receipt = job/"prepared-receipt.pending"
            write_new(pending_receipt, {"caller": caller, "request_digest": digest, "admission_response_sha256": response_sha256,
                "response": response, "operation": {"path": str(config_path), "sha256": selection_sha256(operation_config)}},
                c["maximum_generation_bytes"])
            os.link(pending_receipt, receipt_path)
            pending_receipt.unlink()
            descriptor = os.open(job, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            return response


def parse_preparation_request(raw, maximum_bytes):
    if len(raw) > maximum_bytes:
        raise ValueError("preparation request byte budget")
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate preparation key")
            result[key] = value
        return result
    body = json.loads(raw, object_pairs_hook=unique)
    if (set(body) != {"schema_version", "admission_request", "admission_response_sha256"}
            or body["schema_version"] != "marketcow.polymarket.discovery-prepare.v1"):
        raise ValueError("invalid preparation schema")
    digest = body["admission_response_sha256"]
    if not isinstance(digest, str) or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
        raise ValueError("invalid admission response hash")
    request = parse_request_bytes(json.dumps(body["admission_request"], separators=(",", ":")).encode(), maximum_bytes=maximum_bytes)
    return request, digest
