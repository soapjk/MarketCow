"""Immutable catalog generations; a small atomic pointer is the publication step."""

import hashlib
import json
import os
import sqlite3
import threading
import uuid
from pathlib import Path

from marketcow.catalog_incremental import encoded


def atomic_json(path, value):
    temporary = path.with_name("." + path.name + "." + uuid.uuid4().hex)
    try:
        with temporary.open("xb") as stream:
            stream.write(encoded(value))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        temporary.unlink(missing_ok=True)


def publish_generation(store, root: Path, *, capture_report: dict, reader_grace_seconds=None):
    root.mkdir(exist_ok=True)
    if reader_grace_seconds is not None and (type(reader_grace_seconds) is not int or reader_grace_seconds <= 0):
        raise ValueError("explicit reader grace required")
    previous = None
    if (root / "current.json").exists():
        with (root / "current.json").open("rb") as stream:
            body = stream.read(65537)
        if len(body) > 65536:
            raise ValueError("manifest budget")
        previous = json.loads(body)["file"]
        if Path(previous).name != previous or not previous.endswith(".sqlite"):
            raise ValueError("previous generation path invalid")
    identity = uuid.uuid4().hex
    database = root / (identity + ".sqlite")
    store.snapshot(database)
    with database.open("rb") as stream:
        os.fsync(stream.fileno())
        sha = hashlib.file_digest(stream, "sha256").hexdigest()
    with sqlite3.connect(database) as db:
        head = db.execute("SELECT sequence,revision FROM head").fetchone()
        count = db.execute("SELECT count(*) FROM markets").fetchone()[0]
    if head[0] != capture_report["sequence"] or head[1] != capture_report["revision"]:
        raise ValueError("publication capture/head mismatch")
    manifest = dict(
        generation_id=identity,
        file=database.name,
        sha256=sha,
        sequence=head[0],
        revision=head[1],
        unique_count=count,
        capture=capture_report,
        reader_grace_seconds=reader_grace_seconds,
    )
    atomic_json(root / "current.json", manifest)
    # Grace starts at retirement, not original creation (the old generation may
    # have served a snapshot immediately before the pointer changed).
    if previous is not None:
        os.utime(root / previous, None)
    return manifest


class PublishedCatalog:
    def close(self):
        pass  # Connections are scoped to individual reads.

    def __init__(self, root, manifest, max_row_bytes):
        self.path = root / manifest["file"]
        if self.path.name != manifest["generation_id"] + ".sqlite":
            raise ValueError("invalid generation path")
        with self.path.open("rb") as stream:
            if hashlib.file_digest(stream, "sha256").hexdigest() != manifest["sha256"]:
                raise ValueError("generation hash mismatch")
        self.revision, self.count = manifest["revision"], manifest["unique_count"]
        self.change_sequence = manifest["sequence"]
        self.reader_grace_seconds = manifest.get("reader_grace_seconds")
        self.maximum = max_row_bytes
        self.coverage = manifest["capture"]["coverage"]
        self.source = {"name": "polymarket_gamma", "observed_at": manifest["capture"]["completed_at"]}

    def after(self, market_id):
        with sqlite3.connect(self.path.resolve().as_uri() + "?mode=ro", uri=True) as db:
            db.execute("PRAGMA cache_size=-8192")
            for mid, payload in db.execute(
                "SELECT id,substr(payload,1,?) FROM markets WHERE id>? ORDER BY id", (self.maximum + 1, market_id or "")
            ):
                if len(payload) > self.maximum:
                    raise ValueError("catalog row budget")
                record = json.loads(payload)
                if record["market_id"] != mid:
                    raise ValueError("catalog identity mismatch")
                yield record

    def get(self, market_id):
        with sqlite3.connect(self.path.resolve().as_uri() + "?mode=ro", uri=True) as db:
            row = db.execute(
                "SELECT substr(payload,1,?) FROM markets WHERE id=?", (self.maximum + 1, market_id)
            ).fetchone()
            if row is None:
                return None
            if len(row[0]) > self.maximum:
                raise ValueError("catalog row budget")
            return json.loads(row[0])

    def changes(self, after_sequence, limit, byte_budget):
        from marketcow.universe_control import ControlError

        if type(after_sequence) is not int or after_sequence < 0 or type(limit) is not int or limit <= 0:
            raise ControlError("invalid_schema", 400)
        with sqlite3.connect(self.path.resolve().as_uri() + "?mode=ro", uri=True) as db:
            head = db.execute("SELECT sequence,revision,floor FROM head").fetchone()
            if after_sequence < head[2]:
                raise ControlError("catalog_resnapshot_required", 410)
            if after_sequence > head[0]:
                raise ControlError("invalid_schema", 400)
            result = dict(
                schema_version="marketcow.polymarket.catalog-changes.v1",
                events=[],
                capture_end_sequences={},
                next_sequence=after_sequence,
                head_sequence=head[0],
                catalog_revision=head[1],
                has_more=after_sequence < head[0],
            )
            for (payload,) in db.execute(
                "SELECT substr(payload,1,?) FROM changes WHERE sequence>? ORDER BY sequence LIMIT ?",
                (byte_budget + 1, after_sequence, limit),
            ):
                if len(payload) > byte_budget:
                    raise ControlError("response_size_exceeded", 413)
                event = json.loads(payload)
                cid = event["capture_id"]
                candidate = dict(
                    result,
                    events=result["events"] + [event],
                    capture_end_sequences=dict(result["capture_end_sequences"]),
                    next_sequence=event["sequence"],
                    has_more=event["sequence"] < head[0],
                )
                candidate["capture_end_sequences"][cid] = json.loads(
                    db.execute("SELECT report FROM captures WHERE id=?", (cid,)).fetchone()[0]
                )["sequence"]
                if len(encoded(candidate)) > byte_budget:
                    if not result["events"]:
                        raise ControlError("response_size_exceeded", 413)
                    break
                result = candidate
            if len(encoded(result)) > byte_budget:
                raise ControlError("response_size_exceeded", 413)
            return encoded(result)


class CatalogPublication:
    def __init__(self, root: Path, *, max_row_bytes):
        if type(max_row_bytes) is not int or max_row_bytes <= 0:
            raise ValueError("row budget required")
        self.root, self.maximum = root, max_row_bytes
        self.lock = threading.Lock()
        self.identity = self.cached = None

    def current(self):
        with self.lock:
            with (self.root / "current.json").open("rb") as stream:
                body = stream.read(65537)
            if len(body) > 65536:
                raise ValueError("manifest budget")
            manifest = json.loads(body)
            identity = (manifest["generation_id"], manifest["sha256"])
            if identity != self.identity:
                source = PublishedCatalog(self.root, manifest, self.maximum)
                self.cached, self.identity = source, identity
            return self.cached

    def status(self):
        """Publication and failed-refresh state are distinct from book readiness."""
        from marketcow.universe_control import instant

        with (self.root / "current.json").open("rb") as stream:
            body = stream.read(65537)
        if len(body) > 65536:
            raise ValueError("manifest budget")
        manifest = json.loads(body)
        schedule_path = self.root.parent / "schedule.json"
        schedule = None
        if schedule_path.exists():
            with schedule_path.open("rb") as stream:
                body = stream.read(65537)
            if len(body) > 65536:
                raise ValueError("schedule budget")
            schedule = json.loads(body)
        return dict(
            schema_version="marketcow.polymarket.catalog-status.v1",
            generation_id=manifest["generation_id"],
            catalog_revision=manifest["revision"],
            change_sequence=manifest["sequence"],
            unique_count=manifest["unique_count"],
            capture_completed_at=manifest["capture"]["completed_at"],
            coverage=manifest["capture"]["coverage"],
            last_refresh_error=None if schedule is None else schedule["last_error"],
            scheduler_checked_at=None if schedule is None else instant(int(schedule["high_water"] * 1000)),
            freshness_semantics="per_record_observation; publication_is_not_all_records_fresh",
        )
