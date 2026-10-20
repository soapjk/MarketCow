"""Lossless, streaming base + field deltas for immutable normalized catalogs.

Live offset readers keep the pinned materializations. Historical JSONL files can
be removed only after an fsynced archive has reproduced their exact SHA-256.
Observation/provenance changes are patches too; no business fields are ignored.
"""
from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
import fcntl
import gzip
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import threading
import uuid


LOG = logging.getLogger(__name__)
REVISION = re.compile(r"^[a-f0-9]{64}$")
MAX_ROW_BYTES = 16 * 1024 * 1024


def encode(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False,
                      separators=(",", ":")).encode()


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_json(path, payload):
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.pending")
    try:
        with temporary.open("xb") as stream:
            stream.write(encode(payload))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        sync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def signature(path):
    s = path.stat()
    return s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns


def rows(stream):
    previous = None
    while line := stream.readline(MAX_ROW_BYTES + 1):
        if len(line) > MAX_ROW_BYTES or not line.endswith(b"\n"):
            raise ValueError("catalog row exceeds budget or is incomplete")
        record = json.loads(line)
        key = record["identity"]["market_id"]
        if not isinstance(key, str) or not key or (previous is not None and key <= previous):
            raise ValueError("catalog market IDs must be unique and sorted")
        if encode(record) + b"\n" != line:
            raise ValueError("catalog is not canonical JSONL; refusing lossy conversion")
        previous = key
        yield key, record, line


def changes(old, new, path=()):
    if type(old) is not type(new):
        return [[list(path), new]]
    if isinstance(old, dict):
        result = []
        for key in sorted(old.keys() | new.keys()):
            if key not in new:
                result.append([list((*path, key))])
            elif key not in old:
                result.append([list((*path, key)), new[key]])
            else:
                result.extend(changes(old[key], new[key], (*path, key)))
        return result
    if isinstance(old, list) and len(old) == len(new):
        return [patch for i, (a, b) in enumerate(zip(old, new))
                for patch in changes(a, b, (*path, i))]
    return [] if old == new else [[list(path), new]]


def apply(old, patches):
    result = deepcopy(old)
    for patch in patches:
        path = patch[0]
        if not path:
            if len(patch) != 2:
                raise ValueError("invalid root patch")
            result = patch[1]
            continue
        target = result
        for component in path[:-1]:
            target = target[component]
        if len(patch) == 1:
            del target[path[-1]]
        else:
            target[path[-1]] = patch[1]
    return result


def delta_rows(base_rows, stream):
    """Merge a sorted patch stream with base rows, retaining only one of each."""
    base = iter(base_rows)
    old = next(base, None)
    previous = None
    while line := stream.readline(MAX_ROW_BYTES + 1):
        if len(line) > MAX_ROW_BYTES or not line.endswith(b"\n"):
            raise ValueError("invalid delta row")
        key, patches = json.loads(line)
        if not isinstance(key, str) or (previous is not None and key <= previous):
            raise ValueError("delta IDs must be unique and sorted")
        previous = key
        while old is not None and old[0] < key:
            yield old
            old = next(base, None)
        matched = old is not None and old[0] == key
        if patches is None:
            if not matched:
                raise ValueError("delta deletes an absent market")
        else:
            if not matched and (not patches or patches[0][0] != []):
                raise ValueError("delta updates an absent market")
            record = apply(old[1] if matched else None, patches)
            if record["identity"]["market_id"] != key:
                raise ValueError("delta changes market identity")
            yield key, record, encode(record) + b"\n"
        if matched:
            old = next(base, None)
    while old is not None:
        yield old
        old = next(base, None)


class CatalogArchive:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.directory = self.root / "catalog-deltas"
        self.directory.mkdir(exist_ok=True)

    def metadata(self, revision):
        if not REVISION.fullmatch(revision):
            raise ValueError("invalid catalog revision")
        value = json.loads((self.directory / f"{revision}.json").read_bytes())
        if value["revision"] != revision or value["schema"] != "marketcow.catalog-delta.v1":
            raise ValueError("invalid archive metadata")
        return value

    def iter_rows(self, revision, ancestors=()):
        if revision in ancestors or len(ancestors) >= 128:
            raise ValueError("catalog archive cycle or chain budget exceeded")
        meta = self.metadata(revision)
        path = self.directory / f"{revision}.jsonl.gz"
        if digest(path) != meta["archive_sha256"]:
            raise ValueError("catalog archive checksum mismatch")
        with gzip.open(path, "rb") as stream:
            if meta["parent"] is None:
                source = rows(stream)
            else:
                parent = self.metadata(meta["parent"])
                if parent["sha256"] != meta["parent_sha256"]:
                    raise ValueError("catalog parent checksum mismatch")
                source = delta_rows(self.iter_rows(meta["parent"], (*ancestors, revision)), stream)
            sha = hashlib.sha256()
            count = 0
            for row in source:
                sha.update(row[2])
                count += 1
                yield row
            if sha.hexdigest() != meta["sha256"] or count != meta["rows"]:
                raise ValueError("reconstructed catalog checksum mismatch")

    def verify_dependencies(self, revision):
        seen = set()
        expected = None
        while revision is not None:
            if revision in seen or len(seen) >= 128:
                raise ValueError("catalog archive dependency cycle")
            seen.add(revision)
            meta = self.metadata(revision)
            if expected is not None and meta["sha256"] != expected:
                raise ValueError("catalog parent checksum mismatch")
            if digest(self.directory / f"{revision}.jsonl.gz") != meta["archive_sha256"]:
                raise ValueError("catalog archive checksum mismatch")
            expected, revision = meta["parent_sha256"], meta["parent"]

    def restore(self, revision, destination):
        destination = Path(destination)
        temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.pending")
        try:
            with temporary.open("xb") as stream:
                for _, _, line in self.iter_rows(revision):
                    stream.write(line)
                stream.flush()
                os.fsync(stream.fileno())
            # An explicit restore must not overwrite an existing catalog.
            os.link(temporary, destination)
            sync_directory(destination.parent)
        finally:
            temporary.unlink(missing_ok=True)

    def archive(self, path, *, parent=None, parent_path=None):
        path = Path(path)
        revision = path.stem
        if not REVISION.fullmatch(revision) or path.is_symlink():
            raise ValueError("not an immutable catalog file")
        before = signature(path)
        meta_path = self.directory / f"{revision}.json"
        if meta_path.exists():
            meta = self.metadata(revision)
            if digest(path) != meta["sha256"]:
                raise ValueError("existing archive does not match source")
            # Existing receipts alone never authorize deletion: verify the chain.
            for _ in self.iter_rows(revision):
                pass
            return meta
        if parent:
            parent_meta = self.metadata(parent)
            # Infrequent compressed checkpoints bound historical replay work;
            # intermediate revisions still store only changes.
            if parent_meta["depth"] >= 31:
                parent, parent_path, parent_meta = None, None, None
            if parent_path is not None and digest(parent_path) != parent_meta["sha256"]:
                raise ValueError("parent source checksum mismatch")
        else:
            parent_meta = None
        target = self.directory / f"{revision}.jsonl.gz"
        temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.pending")
        sha = hashlib.sha256()
        count = 0
        stats = {"added": 0, "updated": 0, "deleted": 0, "unchanged": 0}
        try:
            with path.open("rb") as source, temporary.open("xb") as raw:
                with gzip.GzipFile(fileobj=raw, mode="wb", filename="", mtime=0, compresslevel=3) as output:
                    if parent is None:
                        for _, _, line in rows(source):
                            sha.update(line)
                            count += 1
                            output.write(line)
                            stats["added"] += 1
                    else:
                        with self._parent_rows(parent, parent_path) as base:
                            old = next(base, None)
                            for key, record, line in rows(source):
                                sha.update(line)
                                count += 1
                                while old is not None and old[0] < key:
                                    output.write(encode([old[0], None]) + b"\n")
                                    stats["deleted"] += 1
                                    old = next(base, None)
                                if old is not None and old[0] == key:
                                    patch = changes(old[1], record)
                                    stats["updated" if patch else "unchanged"] += 1
                                    old = next(base, None)
                                else:
                                    patch = [[[], record]]
                                    stats["added"] += 1
                                if patch:
                                    output.write(encode([key, patch]) + b"\n")
                            while old is not None:
                                output.write(encode([old[0], None]) + b"\n")
                                stats["deleted"] += 1
                                old = next(base, None)
                raw.flush()
                os.fsync(raw.fileno())
            # Verify independently by replaying the completed archive against
            # the exact verified parent materialization (or its archive chain).
            check = hashlib.sha256()
            actual_count = 0
            with gzip.open(temporary, "rb") as delta:
                with self._parent_rows(parent, parent_path) as base:
                    reconstructed = rows(delta) if parent is None else delta_rows(base, delta)
                    for _, _, line in reconstructed:
                        check.update(line)
                        actual_count += 1
            if check.hexdigest() != sha.hexdigest() or actual_count != count or signature(path) != before:
                raise ValueError("catalog changed or delta reconstruction failed")
            if parent_path is not None and digest(parent_path) != parent_meta["sha256"]:
                raise ValueError("parent changed during archive verification")
            os.replace(temporary, target)
            sync_directory(self.directory)
            meta = dict(schema="marketcow.catalog-delta.v1", revision=revision,
                        parent=parent, parent_sha256=parent_meta["sha256"] if parent_meta else None,
                        sha256=sha.hexdigest(), archive_sha256=digest(target), rows=count,
                        depth=parent_meta["depth"] + 1 if parent_meta else 0,
                        source_bytes=before[2], archive_bytes=target.stat().st_size, changes=stats)
            atomic_json(meta_path, meta)
            return meta
        finally:
            temporary.unlink(missing_ok=True)

    @contextmanager
    def _parent_rows(self, revision, path):
        if revision is None:
            yield iter(())
        elif path is not None:
            with Path(path).open("rb") as stream:
                yield iter(rows(stream))
        else:
            yield iter(self.iter_rows(revision))


def pinned_revisions(root):
    pinned = set()
    # Catalog manifests and the durable state can temporarily disagree during
    # publication/recovery. Both remain materialized for existing offset readers.
    for name, field in (("catalog.json", "catalog_revision"), ("state-index.json", "catalog_revision")):
        path = root / name
        if path.exists():
            revision = json.loads(path.read_bytes()).get(field)
            if revision:
                pinned.add(revision)
    return pinned


def compact(root, *, prune=False, progress=None):
    """Offline/maintenance operation, never part of realtime publication."""
    root = Path(root).resolve()
    archive = CatalogArchive(root)
    with (root / ".catalog-archive.lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        files = list((root / "catalogs").glob("*.jsonl"))
        def order(path):
            with path.open("rb") as stream:
                first = stream.readline(MAX_ROW_BYTES + 1)
            return str(json.loads(first).get("observed_at", "")), path.name
        files.sort(key=order)
        head_path = archive.directory / "head.json"
        head = json.loads(head_path.read_bytes())["revision"] if head_path.exists() else None
        results = []
        # Retain the prior full source until its successor is verified. This
        # avoids O(versions²) reconstruction during an initial migration.
        parent_path = next((p for p in files if p.stem == head), None)
        for path in files:
            if (archive.directory / f"{path.stem}.json").exists():
                continue
            before = signature(path)
            meta = archive.archive(path, parent=head, parent_path=parent_path)
            atomic_json(head_path, {"revision": path.stem})
            head = path.stem
            previous = parent_path
            parent_path = path
            results.append(meta)
            if progress:
                progress(meta)
            if prune and previous is not None:
                _prune(root, archive, previous, head)
            if signature(path) != before:
                raise ValueError("catalog source changed after archival")
        if prune:
            for path in files:
                _prune(root, archive, path, head)
        return results


def _prune(root, archive, path, head):
    if not path.exists() or path.stem == head:
        return
    # Import lazily so the archival CLI remains dependency-free when not pruning.
    from .polymarket_live import _publication_lock
    before = signature(path)
    meta = archive.metadata(path.stem)
    if digest(path) != meta["sha256"]:
        raise ValueError("source changed before pruning")
    if digest(archive.directory / f"{path.stem}.jsonl.gz") != meta["archive_sha256"]:
        raise ValueError("archive changed before pruning")
    archive.verify_dependencies(path.stem)
    with _publication_lock(root, exclusive=True):
        if path.stem in pinned_revisions(root) or path.stem == head:
            return
        if signature(path) != before:
            raise ValueError("source changed during pruning")
        path.unlink()
        sync_directory(path.parent)


_maintenance_lock = threading.Lock()
_maintenance_running = False
_maintenance_pending = set()


def request_compaction(root):
    """One process-wide worker, coalesced roots; no full scans on the live path."""
    global _maintenance_running
    with _maintenance_lock:
        if len(_maintenance_pending) >= 8:
            LOG.error("Catalog archival queue is full; leaving immutable source intact")
            return
        _maintenance_pending.add(Path(root).resolve())
        if _maintenance_running:
            return
        _maintenance_running = True
    threading.Thread(target=_maintain, name="catalog-archive", daemon=True).start()


def _maintain():
    global _maintenance_running
    while True:
        with _maintenance_lock:
            if not _maintenance_pending:
                _maintenance_running = False
                return
            root = _maintenance_pending.pop()
        try:
            compact(root, prune=True)
        except Exception:
            LOG.exception("Catalog archival failed; unverified source files retained: %s", root)
