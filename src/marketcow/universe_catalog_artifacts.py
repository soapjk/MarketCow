"""Hash-verified immutable catalog sharing for an unpublished runtime root.

Only read-only source inodes may be hardlinked. Source books, recovery state,
logs and cursors are never copied by this operation.
"""
import copy
import hashlib
import json
import os
from pathlib import Path


def clone_catalog_artifacts(source: Path, stage: Path, final: Path, *, maximum_copy_bytes: int):
    source, stage = source.resolve(strict=True), stage.resolve(strict=True)
    final = final.absolute()
    if type(maximum_copy_bytes) is not int or maximum_copy_bytes < 0:
        raise ValueError("explicit catalog copy budget required")
    if stage == source or final == source or final.exists():
        raise ValueError("independent unpublished candidate required")
    with (source/"catalog.json").open("rb") as stream:
        raw = stream.read(2097153)
    if len(raw) > 2097152:
        raise ValueError("catalog manifest byte cap")
    manifest = json.loads(raw)
    result = copy.deepcopy(manifest)
    bindings = [(key, "path", "sha256") for key in
                ("normalized_catalog", "catalog_index", "candidate_snapshot")]
    bindings.append(("catalog_source", "raw_path", "raw_payload_sha256"))
    report = []
    copied_bytes = 0
    for key, path_key, hash_key in bindings:
        path = Path(manifest[key][path_key])
        actual = path.resolve(strict=True)
        if actual != path or not actual.is_relative_to(source) or not actual.is_file():
            raise ValueError("catalog artifact escapes source")
        mutable = bool(actual.stat().st_mode & 0o222)
        source_size = actual.stat().st_size
        if mutable:
            copied_bytes += source_size
            if copied_bytes > maximum_copy_bytes:
                raise ValueError("mutable catalog copy budget exceeded")
        with actual.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        if digest != manifest[key][hash_key]:
            raise ValueError("catalog artifact hash mismatch")
        relative = actual.relative_to(source)
        destination = stage/relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        if mutable:
            with actual.open("rb") as src, destination.open("xb") as dst:
                remaining = source_size
                while remaining:
                    chunk = src.read(min(1048576, remaining))
                    if not chunk:
                        raise ValueError("catalog shrank during copying")
                    dst.write(chunk)
                    remaining -= len(chunk)
                if src.read(1):
                    raise ValueError("catalog grew during copying")
                dst.flush(); os.fsync(dst.fileno())
            destination.chmod(0o400)
        else:
            os.link(actual, destination)
        # Verify the linked inode, rather than assuming a path remained stable.
        with destination.open("rb") as stream:
            if hashlib.file_digest(stream, "sha256").hexdigest() != digest:
                raise ValueError("catalog changed during sharing")
        result[key][path_key] = str(final/relative)
        report.append({"path": str(relative), "sha256": digest, "bytes": destination.stat().st_size,
                       "method": "verified_copy" if mutable else "readonly_hardlink"})
    return result, report
