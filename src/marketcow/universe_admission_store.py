"""Offline admission storage primitive; no collection or activation operations.

The caller supplies an already validated request and deterministic response.
Authentication, catalog admission and response-schema validation belong upstream.
SQLite transactions serialize independent processes. Clock progress commits even
when a business request fails, so a rejected request cannot roll back the fence.
This is a control-plane store, never part of the realtime publication path.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path

from marketcow.universe_phase1 import DiscoverySelectionRequest, _parse_millis


class StoreError(ValueError):
    pass


@dataclass(frozen=True)
class StoreLimits:
    ttl_ms: int
    retention_ms: int
    max_entries: int
    max_response_bytes: int

    def __post_init__(self):
        if any(type(x) is not int or x <= 0 for x in (
            self.ttl_ms, self.retention_ms, self.max_entries, self.max_response_bytes
        )) or self.retention_ms < self.ttl_ms:
            raise ValueError("invalid explicit store limits")


class AdmissionStore:
    def __init__(self, path: Path, limits: StoreLimits):
        self.limits = limits
        self.db = sqlite3.connect(path, isolation_level=None, timeout=5)
        self.db.execute("PRAGMA journal_mode=DELETE")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS clock (singleton INTEGER PRIMARY KEY
                CHECK(singleton=1), high_ms INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS requests (
                caller TEXT NOT NULL, request_id TEXT NOT NULL,
                digest TEXT NOT NULL, retain_until INTEGER NOT NULL,
                response BLOB NOT NULL, response_sha TEXT NOT NULL,
                PRIMARY KEY(caller, request_id));
            CREATE INDEX IF NOT EXISTS requests_retention ON requests(retain_until);
            CREATE TABLE IF NOT EXISTS settings(singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                limits TEXT NOT NULL);
        """)
        try:
            self.db.execute("BEGIN IMMEDIATE")
            encoded = json.dumps(asdict(limits), sort_keys=True, separators=(",", ":"))
            self.db.execute("INSERT OR IGNORE INTO settings VALUES(1,?)", (encoded,))
            if self.db.execute("SELECT limits FROM settings WHERE singleton=1").fetchone()[0] != encoded:
                raise StoreError("store_profile_mismatch")
            self.db.execute("COMMIT")
        except BaseException:
            if self.db.in_transaction:
                self.db.execute("ROLLBACK")
            self.db.close()
            raise

    def close(self):
        self.db.close()

    def record(self, caller: str, request: DiscoverySelectionRequest,
               response: bytes | None, *, now_ms: int) -> bytes | None:
        """Persist or return byte-exact original response; never renew a retry.

        Errors are stable codes. A response is computed before entry, with no
        side effects. No callback is executed while holding the database lock.
        None performs a lookup only, advancing the clock fence but never creating
        an entry. This lets retries return the original before catalog evaluation.
        """
        if not caller or type(now_ms) is not int or now_ms < 0:
            raise StoreError("invalid_schema")
        # Revalidate even model_construct inputs; don't trust a supplied digest.
        request = DiscoverySelectionRequest.model_validate(request.model_dump())
        digest = request.request_digest()
        created = _parse_millis(request.created_at)
        expires = _parse_millis(request.expires_at)
        error = None
        result = response
        self.db.execute("BEGIN IMMEDIATE")
        try:
            high = self.db.execute("SELECT high_ms FROM clock WHERE singleton=1").fetchone()
            if high and now_ms < high[0]:
                raise StoreError("clock_regression")
            self.db.execute("INSERT INTO clock VALUES(1,?) ON CONFLICT(singleton) "
                            "DO UPDATE SET high_ms=excluded.high_ms", (now_ms,))
            # Clock fencing is its own durable commit. A later INSERT failure
            # or process crash must not erase an already observed clock value.
            self.db.execute("COMMIT")
            self.db.execute("BEGIN IMMEDIATE")
            high = self.db.execute("SELECT high_ms FROM clock WHERE singleton=1").fetchone()[0]
            if now_ms < high:
                raise StoreError("clock_regression")
            if expires <= now_ms:
                error = "request_expired"
            elif created > now_ms or expires - created > self.limits.ttl_ms:
                error = "invalid_schema"
            else:
                row = self.db.execute(
                    "SELECT digest,response,response_sha FROM requests WHERE caller=? AND request_id=?",
                    (caller, request.request_id),
                ).fetchone()
                if row:
                    if row[0] != digest:
                        error = "idempotency_conflict"
                    elif hashlib.sha256(row[1]).hexdigest() != row[2]:
                        error = "store_corruption"
                    else:
                        result = row[1]
                elif response is None:
                    pass
                elif type(response) is not bytes or len(response) > self.limits.max_response_bytes:
                    error = "response_size_exceeded"
                else:
                    self.db.execute("DELETE FROM requests WHERE retain_until<=?", (now_ms,))
                    count = self.db.execute("SELECT count(*) FROM requests").fetchone()[0]
                    if count >= self.limits.max_entries:
                        error = "resource_unavailable"
                    else:
                        self.db.execute("INSERT INTO requests VALUES(?,?,?,?,?,?)", (
                            caller, request.request_id, digest,
                            created + self.limits.retention_ms, response,
                            hashlib.sha256(response).hexdigest(),
                        ))
            self.db.execute("COMMIT")
        except BaseException:
            if self.db.in_transaction:
                self.db.execute("ROLLBACK")
            raise
        if error:
            raise StoreError(error)
        return result
