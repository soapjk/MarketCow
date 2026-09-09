"""Catalog worker only: bounded capture -> prepare -> durable delta -> publish.

Polling is not an upstream change cursor. Explicit open/closed traversal cadence
is persisted; a failed run never advances its next-due time or publishes a pointer.
"""

import fcntl
import hashlib
import json
import shutil
import sqlite3
import subprocess
import time
import uuid
from pathlib import Path

from marketcow.catalog_capture_prepare import prepare_capture, bounded_read
from marketcow.catalog_incremental import CatalogChanges
from marketcow.catalog_publication import atomic_json, publish_generation
from marketcow.universe_prepared_source import PreparedCatalogSource


FIELDS = {
    "root",
    "binary",
    "binary_sha256",
    "maximum_pages",
    "maximum_bytes",
    "maximum_page_bytes",
    "maximum_seconds",
    "request_seconds",
    "interval_millis",
    "maximum_records",
    "maximum_row_bytes",
    "maximum_database_bytes",
    "maximum_artifact_bytes",
    "minimum_free_bytes",
    "metric_unit",
    "open_interval_seconds",
    "closed_interval_seconds",
    "known_interval_seconds",
    "known_batch_records",
    "maximum_cycles",
    "poll_seconds",
    "retained_change_records",
    "retained_captures",
    "snapshot_retention_seconds",
}


def load_config(path):
    config = json.loads(bounded_read(Path(path), 65536))
    if set(config) != FIELDS:
        raise ValueError("exact worker configuration required")
    for key in FIELDS - {"root", "binary", "binary_sha256", "metric_unit"}:
        if type(config[key]) is not int or config[key] <= 0:
            raise ValueError("explicit positive worker budgets required")
    if not all(Path(config[key]).is_absolute() for key in ("root", "binary")):
        raise ValueError("absolute worker paths required")
    if config["metric_unit"] is not None and (not isinstance(config["metric_unit"], str) or not config["metric_unit"]):
        raise ValueError("explicit metric unit or null required")
    with Path(config["binary"]).open("rb") as stream:
        if hashlib.file_digest(stream, "sha256").hexdigest() != config["binary_sha256"]:
            raise ValueError("capture binary hash mismatch")
    return config


class RefreshWorker:
    def __init__(self, config, *, clock=time.time, execute=subprocess.run):
        self.c, self.clock, self.execute = config, clock, execute
        self.root = Path(config["root"])
        self.root.mkdir(exist_ok=True)
        self.owner = (self.root / "worker.lock").open("a+b")
        try:
            fcntl.flock(self.owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            self.owner.close()
            raise
        self.state_path = self.root / "schedule.json"
        self.state = (
            json.loads(bounded_read(self.state_path, 65536))
            if self.state_path.exists()
            else dict(high_water=0, open_due=0, closed_due=0, known_due=0, last_success=None, last_error=None)
        )

    def close(self):
        self.owner.close()

    def storage_check(self):
        # Evidence is not silently removed to make room. Exhaustion is explicit.
        used = sum(p.stat().st_size for p in self.root.rglob("*") if p.is_file())
        reserve = self.c["maximum_bytes"] + 4 * self.c["maximum_database_bytes"]
        if used + reserve > self.c["maximum_artifact_bytes"]:
            raise ValueError("catalog artifact budget exhausted")
        if shutil.disk_usage(self.root).free < self.c["minimum_free_bytes"] + reserve:
            raise ValueError("catalog free disk reserve")

    def collect_expired(self):
        """Only worker-owned named artifacts, never source/account/service roots."""
        runs = []
        for path in self.root.iterdir():
            name = path.name
            if name.startswith("capture-") and len(name) == 40:
                suffix = name[8:]
                if all(c in "0123456789abcdef" for c in suffix) and path.is_dir() and not path.is_symlink():
                    runs.append(path)
        runs.sort(key=lambda path: path.stat().st_mtime, reverse=True)
        for path in runs[self.c["retained_captures"] :]:
            for candidate in (path, path.with_name(path.name + "-prepared")):
                if candidate.exists() and candidate.is_dir() and not candidate.is_symlink():
                    shutil.rmtree(candidate)
            path.with_name(path.name + "-ids.json").unlink(missing_ok=True)
        published = self.root / "published"
        if (published / "current.json").exists():
            current = json.loads(bounded_read(published / "current.json", 65536))["file"]
            cutoff = self.clock() - self.c["snapshot_retention_seconds"]
            for path in published.iterdir():
                if (
                    path.name != current
                    and path.suffix == ".sqlite"
                    and len(path.stem) == 32
                    and all(c in "0123456789abcdef" for c in path.stem)
                    and not path.is_symlink()
                    and path.stat().st_mtime < cutoff
                ):
                    path.unlink()

    def tick(self):
        now = self.clock()
        if now < self.state["high_water"]:
            raise ValueError("clock_regression")
        self.state["high_water"] = now
        atomic_json(self.state_path, self.state)
        # Alternate by overdue deadline so closed reconciliation cannot starve.
        modes = ("open", "closed", "known") if (self.root / "raw-inventory.sqlite").exists() else ("open", "closed")
        mode = min(modes, key=lambda name: self.state[name + "_due"])
        if self.state[mode + "_due"] > now:
            return None
        try:
            self.collect_expired()
            self.storage_check()
            run = self.root / ("capture-" + uuid.uuid4().hex)
            command = [self.c["binary"], "--root", str(run)]
            for field in (
                "maximum_pages",
                "maximum_bytes",
                "maximum_page_bytes",
                "maximum_seconds",
                "request_seconds",
                "interval_millis",
            ):
                command += ["--" + field.replace("_", "-"), str(self.c[field])]
            if mode == "closed":
                command.append("--closed")
            if mode == "known":
                with sqlite3.connect(self.root / "raw-inventory.sqlite") as db:
                    ids = [
                        row[0]
                        for row in db.execute(
                            "SELECT id FROM raw ORDER BY observed,id LIMIT ?",
                            (min(self.c["known_batch_records"], self.c["maximum_pages"]),),
                        )
                    ]
                if not ids:
                    self.state["known_due"] = now + self.c["known_interval_seconds"]
                    atomic_json(self.state_path, self.state)
                    return None
                ids_file = self.root / (run.name + "-ids.json")
                atomic_json(ids_file, ids)
                command += ["--market-ids", str(ids_file)]
            # No shell, retry, proxy alteration or real-time service operation.
            self.execute(
                command,
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=self.c["maximum_seconds"] + self.c["request_seconds"] + 5,
            )
            result = self.ingest(run)
            self.state[mode + "_due"] = self.clock() + self.c[mode + "_interval_seconds"]
            self.state["last_success"], self.state["last_error"] = result, None
            atomic_json(self.state_path, self.state)
            return result
        except Exception as error:
            self.state["last_error"] = {"type": type(error).__name__, "message": str(error)[:512], "at": self.clock()}
            atomic_json(self.state_path, self.state)
            raise  # No automatic retry loop after upstream failure.

    def ingest(self, capture):
        prepared_root = capture.with_name(capture.name + "-prepared")
        report = prepare_capture(
            capture,
            prepared_root,
            max_pages=self.c["maximum_pages"],
            max_bytes=self.c["maximum_bytes"],
            max_page_bytes=self.c["maximum_page_bytes"],
            max_records=self.c["maximum_records"],
            max_row_bytes=self.c["maximum_row_bytes"],
            max_database_bytes=self.c["maximum_database_bytes"],
            metric_unit=self.c["metric_unit"],
            inventory_path=self.root / "raw-inventory.sqlite",
        )
        source = PreparedCatalogSource(
            prepared_root / "prepared.sqlite",
            expected_sha256=report["prepared_file_sha256"],
            max_row_bytes=self.c["maximum_row_bytes"],
        )
        store = CatalogChanges(
            self.root / "catalog.sqlite",
            max_database_bytes=self.c["maximum_database_bytes"],
            max_record_bytes=self.c["maximum_row_bytes"],
            max_batch_records=self.c["maximum_records"],
        )
        try:
            coverage = dict(report["coverage"])
            # Stored inventory includes previous polls; it is not this traversal.
            coverage.update(
                complete=False,
                predicate="observed catalog inventory; explicit open/closed polling",
                incomplete_reasons=sorted(
                    set(coverage["incomplete_reasons"] + ["non_atomic_inventory", "absence_requires_reconciliation"])
                ),
            )
            commit = store.publish(
                source.after(None),
                capture_id=capture.name,
                started_at=report["inventory_observation_start"],
                completed_at=report["capture_completed_at"],
                expected_revision=store.head()["revision"],
                coverage=coverage,
            )
            # Prune only complete old captures. Requests below the floor must
            # resnapshot; no half-capture baseline can become resumable.
            threshold = commit["sequence"] - self.c["retained_change_records"]
            boundary = store.db.execute(
                "SELECT max(CAST(json_extract(report,'$.sequence') AS INTEGER)) "
                "FROM captures WHERE CAST(json_extract(report,'$.sequence') AS INTEGER)<=?",
                (threshold,),
            ).fetchone()[0]
            if boundary is not None and boundary >= store.head()["floor"]:
                store.prune_through(boundary)
            published = publish_generation(
                store,
                self.root / "published",
                capture_report=commit,
                reader_grace_seconds=self.c["snapshot_retention_seconds"],
            )
            return dict(
                generation_id=published["generation_id"],
                sequence=published["sequence"],
                revision=published["revision"],
                capture_id=capture.name,
            )
        finally:
            source.close()
            store.close()


def main():
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--ingest-capture", type=Path, help="Existing complete capture; no network request")
    args = parser.parse_args()
    config = load_config(args.config)
    worker = RefreshWorker(config)
    try:
        if args.ingest_capture is not None:
            if not args.ingest_capture.is_absolute():
                raise ValueError("absolute capture path required")
            worker.storage_check()
            print(json.dumps(worker.ingest(args.ingest_capture)), flush=True)
            return
        for cycle in range(config["maximum_cycles"]):
            result = worker.tick()
            if result is not None:
                print(json.dumps(result), flush=True)
            if cycle + 1 < config["maximum_cycles"]:
                time.sleep(config["poll_seconds"])
    finally:
        worker.close()


if __name__ == "__main__":
    main()
