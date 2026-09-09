"""Pinned read-only phase-1 SQLite generation for the control application."""
import hashlib
import json
import sqlite3
import threading
from pathlib import Path

from marketcow.universe_catalog_mapping import optional_time


class PreparedCatalogSource:
    def __init__(self, path: Path, *, expected_sha256: str, max_row_bytes: int):
        if type(max_row_bytes) is not int or max_row_bytes <= 0:
            raise ValueError("explicit row budget required")
        self.lock = threading.RLock()
        self.maximum = max_row_bytes
        self.db = sqlite3.connect(path.resolve().as_uri()+"?mode=ro", uri=True, check_same_thread=False)
        try:
            self.db.execute("PRAGMA query_only=ON")
            self.db.execute("PRAGMA cache_size=-8192")
            self.db.execute("BEGIN")
            manifest = json.loads(self.db.execute("SELECT value FROM metadata WHERE key='manifest'").fetchone()[0])
            with path.open("rb") as stream:
                if hashlib.file_digest(stream, "sha256").hexdigest() != expected_sha256:
                    raise ValueError("prepared catalog hash mismatch")
            self.revision = manifest["catalog_revision"]
            self.count = self.db.execute("SELECT count(*) FROM records").fetchone()[0]
            if self.count != manifest["unique_count"]:
                raise ValueError("prepared catalog count mismatch")
            # Normalize the wire view only; keep the hashed immutable evidence intact.
            self.source = dict(manifest["source"])
            observed = self.source.get("observed_at")
            if observed is not None:
                normalized = optional_time(observed)
                if normalized is None:
                    raise ValueError("invalid prepared source observation time")
                self.source["observed_at"] = normalized
            self.coverage = manifest["coverage"]
        except BaseException:
            self.db.close()
            raise

    def close(self):
        with self.lock:
            self.db.close()

    def _record(self, row):
        mid, body, digest = row
        if len(body) > self.maximum:
            raise ValueError("prepared row budget exceeded")
        if hashlib.sha256(body).hexdigest() != digest:
            raise ValueError("prepared row hash mismatch")
        record = json.loads(body)
        if record["market_id"] != mid:
            raise ValueError("prepared row identity mismatch")
        return record

    def after(self, market_id):
        with self.lock:
            sql = "SELECT market_id,substr(payload,1,?),sha256 FROM records "
            if market_id is None:
                cursor = self.db.execute(sql+"ORDER BY market_id", (self.maximum+1,))
            else:
                cursor = self.db.execute(sql+"WHERE market_id>? ORDER BY market_id", (self.maximum+1, market_id))
            try:
                for row in cursor:
                    yield self._record(row)
            finally:
                cursor.close()

    def get(self, market_id):
        with self.lock:
            row = self.db.execute("SELECT market_id,substr(payload,1,?),sha256 FROM records WHERE market_id=?",
                                  (self.maximum+1, market_id)).fetchone()
            return None if row is None else self._record(row)
