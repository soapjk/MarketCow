"""BG-015 authorization-first validation of a future copied legacy database."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import tempfile
from typing import Any, Callable, Mapping

from .offline_duckdb_import import ALLOWED_TABLES, ImportLimits, OfflineDuckDBImporter
from .offline_full_import import FULL_IMPORT_VERSION, _digest
from .offline_incremental_catchup import CATCHUP_VERSION


COPY_MANIFEST_VERSION = "storage-v2.copy-manifest.v1"
COPY_VALIDATION_VERSION = "storage-v2.copy-validation.v1"
COPY_ACTION = "copy-legacy-database-to-isolated-root"
MAX_FILES = 100
MAX_TOTAL_BYTES = 64 * 1024**3
_ID = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}$")


class CopyValidationError(RuntimeError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class CopyAuthorization:
    authorized: bool
    evidence_id: str
    source_logical_id: str
    copy_action: str
    manifest_sha256: str
    source_path_sha256: str
    allowed_root_sha256: str

    def validate_without_io(self) -> None:
        if not self.authorized:
            raise CopyValidationError("authorization_required")
        if not _ID.fullmatch(self.evidence_id) or not _ID.fullmatch(self.source_logical_id):
            raise CopyValidationError("authorization_invalid")
        if self.copy_action != COPY_ACTION:
            raise CopyValidationError("copy_action_not_authorized")
        for value in (self.manifest_sha256, self.source_path_sha256, self.allowed_root_sha256):
            if not re.fullmatch(r"[0-9a-f]{64}", value):
                raise CopyValidationError("authorization_invalid")


def _sha256_file(path: Path, limit: int) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    try:
        with path.open("rb") as source:
            while chunk := source.read(1024 * 1024):
                size += len(chunk)
                if size > limit:
                    raise CopyValidationError("file_limit_exceeded")
                digest.update(chunk)
    except CopyValidationError:
        raise
    except OSError:
        raise CopyValidationError("file_read_failed") from None
    return digest.hexdigest(), size


def _has_symlink(path: Path) -> bool:
    candidate = Path(path.anchor)
    for part in path.parts[1:]:
        candidate /= part
        try:
            if stat.S_ISLNK(candidate.lstat().st_mode):
                return True
        except OSError:
            return False
    return False


def _atomic(path: Path, document: Mapping[str, Any]) -> None:
    encoded = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=".copy-validation-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(encoded)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


class _StreamEvidence:
    def __init__(self) -> None:
        self.complete: dict[str, Any] | None = None
        self.failed = False

    def write(self, payload: bytes | str) -> None:
        record = json.loads(payload)
        if record.get("type") == "complete":
            self.complete = record
        elif record.get("type") == "failed":
            self.failed = True

    def flush(self) -> None:
        pass


class OfflineCopyValidator:
    """Read-only validation whose authorization check precedes all filesystem I/O."""

    def __init__(
        self,
        limits: ImportLimits | None = None,
        authorization_verifier: Callable[[CopyAuthorization], bool] | None = None,
    ) -> None:
        self.limits = limits or ImportLimits()
        self.authorization_verifier = authorization_verifier

    @staticmethod
    def _manifest_payload(document: dict[str, Any]) -> dict[str, Any]:
        return {key: value for key, value in document.items() if key != "manifest_payload_sha256"}

    @staticmethod
    def _validate_evidence(document: dict[str, Any], version: str, phase: str) -> None:
        unsigned = dict(document)
        checksum = unsigned.pop("checksum", None)
        if checksum != _digest(unsigned) or document.get("version") != version or document.get("phase") != phase:
            raise CopyValidationError("migration_evidence_invalid")

    def validate(
        self,
        *,
        authorization: CopyAuthorization,
        manifest_path: Path,
        allowed_root: Path,
        report_directory: Path,
        mode: str = "sample",
    ) -> dict[str, Any]:
        # This is intentionally the first operation: no Path resolution, stat,
        # directory creation, module factory, connection, or worker exists before it.
        authorization.validate_without_io()
        if self.authorization_verifier is None or not self.authorization_verifier(authorization):
            raise CopyValidationError("authorization_untrusted")
        if mode not in {"sample", "full"}:
            raise CopyValidationError("mode_invalid")
        manifest_path, allowed_root, report_directory = map(Path, (
            manifest_path, allowed_root, report_directory,
        ))
        if not manifest_path.is_absolute() or not allowed_root.is_absolute() or not report_directory.is_absolute():
            raise CopyValidationError("path_invalid")
        if _has_symlink(allowed_root) or _has_symlink(manifest_path) or _has_symlink(report_directory):
            raise CopyValidationError("symlink_rejected")
        try:
            root = allowed_root.resolve(strict=True)
            manifest = manifest_path.resolve(strict=True)
            report = report_directory.resolve(strict=False)
            manifest.relative_to(root)
            report.relative_to(root)
        except (OSError, ValueError):
            raise CopyValidationError("containment_rejected") from None
        if hashlib.sha256(str(root).encode()).hexdigest() != authorization.allowed_root_sha256:
            raise CopyValidationError("allowed_root_binding_mismatch")
        manifest_hash, manifest_size = _sha256_file(manifest, 4 * 1024 * 1024)
        if manifest_hash != authorization.manifest_sha256:
            raise CopyValidationError("manifest_authorization_mismatch")
        try:
            document = json.loads(manifest.read_text())
        except (OSError, UnicodeError, json.JSONDecodeError):
            raise CopyValidationError("manifest_invalid") from None
        if document.get("version") != COPY_MANIFEST_VERSION:
            raise CopyValidationError("manifest_version_unsupported")
        if document.get("source_logical_id") != authorization.source_logical_id:
            raise CopyValidationError("source_binding_mismatch")
        if document.get("authorization_evidence_id") != authorization.evidence_id:
            raise CopyValidationError("authorization_binding_mismatch")
        try:
            copied_at = datetime.fromisoformat(str(document.get("copied_at", "")).replace("Z", "+00:00"))
        except ValueError:
            copied_at = None
        if (
            document.get("copy_action") != COPY_ACTION
            or document.get("authorization_statement") != "separately-authorized-exact-copy"
            or not _ID.fullmatch(str(document.get("allowed_root_logical_id", "")))
            or copied_at is None or copied_at.tzinfo is None
            or document.get("copy_method") not in {"local-synthetic-fixture", "user-authorized-local-copy"}
        ):
            raise CopyValidationError("manifest_contract_invalid")
        payload_sha = _digest(self._manifest_payload(document))
        if document.get("manifest_payload_sha256") != payload_sha:
            raise CopyValidationError("manifest_checksum_mismatch")
        files = document.get("files")
        if not isinstance(files, list) or not 3 <= len(files) <= MAX_FILES:
            raise CopyValidationError("file_inventory_invalid")
        seen: set[str] = set()
        verified: dict[str, tuple[Path, dict[str, Any]]] = {}
        total = 0
        for entry in files:
            if not isinstance(entry, dict) or entry.get("role") in seen:
                raise CopyValidationError("file_inventory_invalid")
            role, relative = entry.get("role"), entry.get("relative_path")
            if not isinstance(role, str) or not isinstance(relative, str) or Path(relative).is_absolute():
                raise CopyValidationError("file_inventory_invalid")
            candidate = root / relative
            if _has_symlink(candidate):
                raise CopyValidationError("symlink_rejected")
            try:
                resolved = candidate.resolve(strict=True)
                resolved.relative_to(root)
            except (OSError, ValueError):
                raise CopyValidationError("file_missing_or_escape") from None
            digest, size = _sha256_file(resolved, min(self.limits.max_file_bytes, MAX_TOTAL_BYTES))
            if digest != entry.get("sha256") or size != entry.get("byte_size"):
                raise CopyValidationError("file_checksum_mismatch")
            total += size
            if total > min(MAX_TOTAL_BYTES, self.limits.max_output_bytes * MAX_FILES):
                raise CopyValidationError("copy_size_limit_exceeded")
            seen.add(role)
            verified[role] = (resolved, entry)
        if set(verified) != {"duckdb", "full_checkpoint", "catchup_checkpoint"}:
            raise CopyValidationError("component_inventory_invalid")
        source_path = verified["duckdb"][0]
        if hashlib.sha256(str(source_path).encode()).hexdigest() != authorization.source_path_sha256:
            raise CopyValidationError("source_path_binding_mismatch")
        full = json.loads(verified["full_checkpoint"][0].read_text())
        catchup = json.loads(verified["catchup_checkpoint"][0].read_text())
        self._validate_evidence(full, FULL_IMPORT_VERSION, "complete")
        self._validate_evidence(catchup, CATCHUP_VERSION, "complete")
        if catchup.get("full_run_id") != full.get("run_id"):
            raise CopyValidationError("migration_evidence_binding_mismatch")
        if catchup.get("source_high_watermark", {}).get("source_fingerprint") != document.get(
            "source_fingerprint"
        ):
            raise CopyValidationError("migration_evidence_binding_mismatch")
        importer = OfflineDuckDBImporter(
            allowed_root=root,
            source=source_path,
            source_label=document.get("source_label", ""),
            limits=self.limits,
        )
        before = importer.inspect()
        if before["source_fingerprint"] != document.get("source_fingerprint"):
            raise CopyValidationError("source_fingerprint_mismatch")
        selected = list(ALLOWED_TABLES if mode == "full" else (
            "provider_health", "raw_artifact_manifest", "market_price_bar",
        ))
        streams = []
        for table in selected:
            sink = _StreamEvidence()
            if importer.stream("extract", table, sink) != 0 or sink.failed or not sink.complete:
                raise CopyValidationError("stream_validation_failed")
            streams.append({
                "table": table,
                "row_count": sink.complete["row_count"],
                "batch_count": sink.complete["batch_count"],
                "data_sha256": sink.complete["data_sha256"],
            })
        after = importer.inspect()
        if after["source_fingerprint"] != before["source_fingerprint"]:
            raise CopyValidationError("source_changed_during_validation")
        report_document = {
            "version": COPY_VALIDATION_VERSION,
            "status": "verified",
            "mode": mode,
            "source_logical_id": authorization.source_logical_id,
            "authorization_evidence_id": authorization.evidence_id,
            "manifest_sha256": manifest_hash,
            "manifest_bytes": manifest_size,
            "source_fingerprint": before["source_fingerprint"],
            "file_count": len(files),
            "total_bytes": total,
            "streams": streams,
            "migration_evidence": {
                "full_run_id": full["run_id"],
                "catchup_run_id": catchup["catchup_run_id"],
            },
        }
        report_document["report_payload_sha256"] = _digest(report_document)
        _atomic(report / "copy-validation.json", report_document)
        return report_document
