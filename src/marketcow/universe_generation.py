"""Durable generation selection for a direct-Rust supervisor.

This is control-plane intent, NOT a data-plane pointer. A successful CAS does
not claim that a listener has switched. The supervisor must apply the returned
epoch and publish a separate runtime receipt before reporting activation.
No market data, shell commands, account operations or listener writes occur here.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from marketcow.universe_phase1 import selection_sha256


class Generation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    pool: Literal["discovery", "live"]
    selection_id: str = Field(min_length=1)
    catalog_revision: str = Field(pattern=r"^[0-9a-f]{64}$")
    artifact_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    market_ids: tuple[str, ...]
    dependency_market_ids: tuple[str, ...]
    protected_market_ids: tuple[str, ...]
    parent_selection_id: str | None
    # A trusted supervisor assigns a new instance after real baseline validation.
    stream_instance_id: str = Field(min_length=1)
    baseline_cursor: int = Field(ge=0)

    @model_validator(mode="after")
    def identities(self):
        for ids in (self.market_ids, self.dependency_market_ids, self.protected_market_ids):
            if tuple(sorted(set(ids))) != ids or any(not x or not x.isascii() for x in ids):
                raise ValueError("identity arrays must be sorted unique ASCII")
        if not 1 <= len(self.market_ids) <= (1000 if self.pool == "discovery" else 250):
            raise ValueError("requested market capacity")
        if set(self.market_ids) & set(self.dependency_market_ids):
            raise ValueError("dependencies must be counted separately")
        if not set(self.protected_market_ids) <= set(self.market_ids):
            raise ValueError("protected omission")
        if self.pool == "live" and not self.parent_selection_id:
            raise ValueError("live parent selection required")
        if self.pool == "discovery" and self.parent_selection_id is not None:
            raise ValueError("discovery has no parent selection")
        return self

    @property
    def generation_id(self):
        return selection_sha256(self.model_dump(mode="json"))


class GenerationStore:
    """Bounded, process-safe desired-generation CAS with restart fencing.

    Caller admission/authentication, artifact verification, Rust preheating and
    runtime receipts are deliberately separate; never expose register directly
    to an untrusted client as a way to self-certify preparation.
    """
    def __init__(self, path: Path, *, maximum_generations: int, maximum_record_bytes: int):
        if any(type(x) is not int or x <= 0 for x in (maximum_generations, maximum_record_bytes)):
            raise ValueError("explicit generation budgets required")
        self.db = sqlite3.connect(path, isolation_level=None, timeout=5)
        self.db.execute("PRAGMA journal_mode=DELETE")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS generations (
                id TEXT PRIMARY KEY, body BLOB NOT NULL, expires INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS desired (
                pool TEXT PRIMARY KEY, generation TEXT NOT NULL, epoch INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS applied (
                pool TEXT PRIMARY KEY, generation TEXT NOT NULL, epoch INTEGER NOT NULL,
                receipt BLOB NOT NULL, digest TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS clock (id INTEGER PRIMARY KEY CHECK(id=1), high INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS limits (id INTEGER PRIMARY KEY CHECK(id=1), records INTEGER, bytes INTEGER);
        """)
        self.maximum, self.bytes = maximum_generations, maximum_record_bytes
        try:
            self.db.execute("BEGIN IMMEDIATE")
            self.db.execute("INSERT OR IGNORE INTO limits VALUES(1,?,?)", (self.maximum, self.bytes))
            if self.db.execute("SELECT records,bytes FROM limits").fetchone() != (self.maximum, self.bytes):
                raise ValueError("generation profile mismatch")
            self.db.execute("COMMIT")
        except BaseException:
            if self.db.in_transaction:
                self.db.execute("ROLLBACK")
            self.db.close()
            raise

    def close(self):
        self.db.close()

    def _clock(self, now_ms):
        if type(now_ms) is not int or now_ms < 0:
            raise ValueError("invalid clock")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            row = self.db.execute("SELECT high FROM clock WHERE id=1").fetchone()
            if row and now_ms < row[0]:
                raise ValueError("clock_regression")
            self.db.execute("INSERT INTO clock VALUES(1,?) ON CONFLICT(id) DO UPDATE SET high=excluded.high", (now_ms,))
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def register(self, generation: Generation, *, expires_ms: int, now_ms: int):
        generation = Generation.model_validate(generation.model_dump())
        if type(expires_ms) is not int or expires_ms <= now_ms:
            raise ValueError("candidate_expired")
        body = json.dumps(generation.model_dump(mode="json"), sort_keys=True, separators=(",", ":")).encode()
        if len(body) > self.bytes:
            raise ValueError("generation byte capacity")
        self._clock(now_ms)
        self.db.execute("BEGIN IMMEDIATE")
        try:
            old = self.db.execute("SELECT body,expires FROM generations WHERE id=?", (generation.generation_id,)).fetchone()
            if old:
                if old != (body, expires_ms):
                    raise ValueError("generation retry conflict")
            else:
                self.db.execute("DELETE FROM generations WHERE expires<=? AND id NOT IN (SELECT generation FROM desired) AND id NOT IN (SELECT generation FROM applied)", (now_ms,))
                if self.db.execute("SELECT count(*) FROM generations").fetchone()[0] >= self.maximum:
                    raise ValueError("generation capacity")
                self.db.execute("INSERT INTO generations VALUES(?,?,?)", (generation.generation_id, body, expires_ms))
            self.db.execute("COMMIT")
            return generation.generation_id
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def desired(self, pool: str):
        row = self.db.execute("SELECT generation,epoch FROM desired WHERE pool=?", (pool,)).fetchone()
        return None if row is None else {"generation_id": row[0], "epoch": row[1]}

    def applied(self, pool: str):
        row = self.db.execute("SELECT receipt,digest FROM applied WHERE pool=?", (pool,)).fetchone()
        if row is None:
            return None
        receipt = json.loads(row[0])
        if selection_sha256(receipt) != row[1]:
            raise ValueError("runtime_receipt_corrupt")
        return receipt

    def acknowledge(self, pool: str, *, generation_id: str, epoch: int,
                    stream_instance_id: str, baseline_cursor: int,
                    ready_cursor: int, endpoint: str, now_ms: int, identity_kind: str = "stream_instance"):
        """Trusted supervisor acknowledgement after actual HTTP/full-sync/WS ready.

        Never call on process-active alone. A stale observation cannot confirm a
        superseding desired generation. The last applied generation stays pinned
        during preparation/failure and remains available after process restart.
        """
        from urllib.parse import urlsplit
        parsed = urlsplit(endpoint)
        if identity_kind not in ("stream_instance", "discovery_projection"):
            raise ValueError("invalid runtime identity kind")
        if (parsed.scheme not in ("http", "https") or not parsed.hostname
                or parsed.username or parsed.password or parsed.query or parsed.fragment):
            raise ValueError("invalid runtime endpoint")
        if any(type(x) is not int or x < 0 for x in (epoch, baseline_cursor, ready_cursor)) or epoch == 0:
            raise ValueError("invalid runtime boundary")
        self._clock(now_ms)
        self.db.execute("BEGIN IMMEDIATE")
        try:
            if self.desired(pool) != {"generation_id": generation_id, "epoch": epoch}:
                raise ValueError("stale_runtime_receipt")
            row = self.db.execute("SELECT body FROM generations WHERE id=?", (generation_id,)).fetchone()
            generation = Generation.model_validate_json(row[0])
            if generation.generation_id != generation_id or generation.pool != pool:
                raise ValueError("generation_identity_mismatch")
            if not isinstance(stream_instance_id, str) or not stream_instance_id:
                raise ValueError("runtime_instance_mismatch")
            # A real preheat-to-public process handoff creates a fresh instance.
            # The trusted adapter checks scope/catalog/artifact and selected IDs;
            # cursors are comparable only inside the same source instance.
            if (stream_instance_id == generation.stream_instance_id and baseline_cursor < generation.baseline_cursor) or ready_cursor < baseline_cursor:
                raise ValueError("runtime_boundary_regression")
            receipt = {"generation_id": generation_id, "epoch": epoch, "pool": pool,
                       "selection_id": generation.selection_id, "catalog_revision": generation.catalog_revision,
                       "stream_instance_id": stream_instance_id, "baseline_cursor": baseline_cursor,
                       "identity_kind": identity_kind,
                       "ready_cursor": ready_cursor, "endpoint": endpoint,
                       "status": "runtime_applied", "full_sync_required": True}
            previous = self.applied(pool)
            if previous and previous["epoch"] == epoch and previous != receipt:
                raise ValueError("runtime_receipt_conflict")
            body = json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()
            if len(body) > self.bytes:
                raise ValueError("generation byte capacity")
            self.db.execute("INSERT INTO applied VALUES(?,?,?,?,?) ON CONFLICT(pool) DO UPDATE SET generation=excluded.generation,epoch=excluded.epoch,receipt=excluded.receipt,digest=excluded.digest",
                            (pool, generation_id, epoch, body, selection_sha256(receipt)))
            self.db.execute("COMMIT")
            return receipt
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def select(self, pool: str, generation_id: str, *, expected: dict | None,
               protected_market_ids: tuple[str, ...], now_ms: int):
        """CAS desired state, including rollback to a retained generation.

        Epoch prevents A→B→A ABA. Cursors are deliberately never compared across
        instances. Caller must revalidate current position protection on rollback.
        """
        self._clock(now_ms)
        self.db.execute("BEGIN IMMEDIATE")
        try:
            if self.desired(pool) != expected:
                raise ValueError("incumbent_conflict")
            row = self.db.execute("SELECT body,expires FROM generations WHERE id=?", (generation_id,)).fetchone()
            if row is None or row[1] <= now_ms:
                raise ValueError("candidate_expired")
            generation = Generation.model_validate_json(row[0])
            if generation.generation_id != generation_id or generation.pool != pool:
                raise ValueError("generation_identity_mismatch")
            if tuple(sorted(set(protected_market_ids))) != protected_market_ids:
                raise ValueError("invalid protected identities")
            if not set(protected_market_ids) <= set(generation.market_ids):
                raise ValueError("protected_omission")
            if pool == "live":
                # Intent alone is not a running parent. Pin to the verified
                # runtime receipt and reject a transition still in flight.
                parent = self.applied("discovery")
                if parent is None:
                    raise ValueError("parent_selection_mismatch")
                if self.desired("discovery") != {"generation_id": parent["generation_id"], "epoch": parent["epoch"]}:
                    raise ValueError("parent_transition_pending")
                parent_row = self.db.execute("SELECT body FROM generations WHERE id=?", (parent["generation_id"],)).fetchone()
                parent_generation = Generation.model_validate_json(parent_row[0])
                if parent_generation.generation_id != parent["generation_id"] or parent_generation.selection_id != generation.parent_selection_id:
                    raise ValueError("parent_selection_mismatch")
                if parent_generation.catalog_revision != generation.catalog_revision:
                    raise ValueError("parent_catalog_mismatch")
                if not set(generation.market_ids) <= set(parent_generation.market_ids) | set(protected_market_ids):
                    raise ValueError("unprotected_parent_exception")
            epoch = 1 if expected is None else expected["epoch"] + 1
            self.db.execute("INSERT INTO desired VALUES(?,?,?) ON CONFLICT(pool) DO UPDATE SET generation=excluded.generation,epoch=excluded.epoch",
                            (pool, generation_id, epoch))
            self.db.execute("COMMIT")
            return {"generation_id": generation_id, "epoch": epoch, "status": "pending_runtime_application", "full_sync_required": True}
        except BaseException:
            self.db.execute("ROLLBACK")
            raise
