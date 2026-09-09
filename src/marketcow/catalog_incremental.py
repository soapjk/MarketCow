"""Catalog-only durable changes. Never imports collectors, scopes or accounts.

Each input is an already validated phase-1 record. Full-source absence is not a
closure. This store does not claim upstream incremental acquisition or coverage.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sqlite3


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def facts(record):
    value = deepcopy(record)
    value.pop("observed_at", None)
    value.pop("evidence_sha256", None)
    for name in ("volume_24h", "liquidity"):
        if isinstance(value.get(name), dict):
            value[name].pop("observed_at", None)
    return value


def timestamp(text):
    value = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if value.tzinfo is None:
        raise ValueError("timezone required")
    return value


class CatalogChanges:
    def __init__(self, path: Path, *, max_database_bytes: int, max_record_bytes: int, max_batch_records: int):
        for n in (max_database_bytes, max_record_bytes, max_batch_records):
            if type(n) is not int or n <= 0:
                raise ValueError("explicit positive budgets required")
        if max_database_bytes < 4096:
            raise ValueError("database budget too small")
        self.record_cap, self.batch_cap = max_record_bytes, max_batch_records
        self.db = sqlite3.connect(path, isolation_level=None)
        page_size = self.db.execute("PRAGMA page_size").fetchone()[0]
        permitted = max_database_bytes // page_size
        actual = self.db.execute(f"PRAGMA max_page_count={permitted}").fetchone()[0]
        if actual > permitted:
            self.db.close()
            raise ValueError("existing database exceeds budget")
        self.db.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            PRAGMA cache_size=-8192;
            CREATE TABLE IF NOT EXISTS head(
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                sequence INTEGER NOT NULL, revision TEXT NOT NULL, floor INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS markets(
                id TEXT PRIMARY KEY, payload BLOB NOT NULL, facts_sha TEXT NOT NULL,
                observed_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS changes(
                sequence INTEGER PRIMARY KEY, payload BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS captures(
                id TEXT PRIMARY KEY, started TEXT NOT NULL, finished TEXT NOT NULL,
                report BLOB NOT NULL);
        """)
        self.db.execute("INSERT OR IGNORE INTO head VALUES(1,0,?,0)", (digest([]),))

    def close(self):
        self.db.close()

    def head(self):
        row = self.db.execute("SELECT sequence,revision,floor FROM head").fetchone()
        return dict(sequence=row[0], revision=row[1], floor=row[2])

    def publish(self, records, *, capture_id, started_at, completed_at, expected_revision, coverage):
        start, end = timestamp(started_at), timestamp(completed_at)
        if not capture_id or end < start or type(coverage) is not dict:
            raise ValueError("capture metadata invalid")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            before = self.head()
            if before["revision"] != expected_revision:
                raise ValueError("catalog_revision_conflict")
            if self.db.execute("SELECT 1 FROM captures WHERE id=?", (capture_id,)).fetchone():
                raise ValueError("capture_id_reused")
            self.db.execute("CREATE TEMP TABLE IF NOT EXISTS batch_ids(id TEXT PRIMARY KEY)")
            self.db.execute("DELETE FROM batch_ids")
            sequence, revision, changed, count = before["sequence"], before["revision"], 0, 0
            for record in records:
                count += 1
                if count > self.batch_cap:
                    raise ValueError("batch_budget")
                mid = record.get("market_id")
                if not isinstance(mid, str) or not mid:
                    raise ValueError("market_id invalid")
                self.db.execute("INSERT INTO batch_ids VALUES(?)", (mid,))
                observed = timestamp(record["observed_at"])
                if not start <= observed <= end:
                    raise ValueError("record_not_from_capture_window")
                body = encoded(record)
                if len(body) > self.record_cap:
                    raise ValueError("record_budget")
                old = self.db.execute("SELECT payload,facts_sha,observed_at FROM markets WHERE id=?", (mid,)).fetchone()
                if old and observed < timestamp(old[2]):
                    raise ValueError("observation_regression")
                fingerprint = digest(facts(record))
                if not old or fingerprint != old[1]:
                    previous = json.loads(old[0]) if old else None
                    kind = (
                        "market_added"
                        if previous is None
                        else (
                            "market_closed"
                            if record.get("closed") is True and previous.get("closed") is not True
                            else "market_reopened"
                            if record.get("closed") is False and previous.get("closed") is True
                            else "market_updated"
                        )
                    )
                    sequence += 1
                    event = dict(
                        sequence=sequence,
                        kind=kind,
                        market_id=mid,
                        record=record,
                        capture_id=capture_id,
                        base_revision=revision,
                        batch_start_sequence=before["sequence"] + 1,
                    )
                    revision = digest(event)
                    event["catalog_revision"] = revision
                    self.db.execute("INSERT INTO changes VALUES(?,?)", (sequence, encoded(event)))
                    changed += 1
                self.db.execute(
                    "INSERT OR REPLACE INTO markets VALUES(?,?,?,?)", (mid, body, fingerprint, record["observed_at"])
                )
            report = dict(
                capture_id=capture_id,
                started_at=started_at,
                completed_at=completed_at,
                coverage=coverage,
                observed_records=count,
                changed_records=changed,
                sequence=sequence,
                revision=revision,
                absence_semantics="not_observed_is_not_closed",
            )
            self.db.execute(
                "INSERT INTO captures VALUES(?,?,?,?)", (capture_id, started_at, completed_at, encoded(report))
            )
            self.db.execute("UPDATE head SET sequence=?,revision=? WHERE singleton=1", (sequence, revision))
            self.db.execute("COMMIT")
            return report
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def changes(self, *, after_sequence, limit, byte_budget):
        if (
            any(type(n) is not int for n in (after_sequence, limit, byte_budget))
            or after_sequence < 0
            or min(limit, byte_budget) <= 0
        ):
            raise ValueError("changes budgets invalid")
        self.db.execute("BEGIN")
        try:
            head = self.head()
            if after_sequence < head["floor"]:
                raise ValueError("catalog_resnapshot_required")
            if after_sequence > head["sequence"]:
                raise ValueError("future_sequence")
            events, used = [], 0
            for (body,) in self.db.execute(
                "SELECT payload FROM changes WHERE sequence>? ORDER BY sequence LIMIT ?", (after_sequence, limit)
            ):
                if used + len(body) > byte_budget:
                    if not events:
                        raise ValueError("response_size_exceeded")
                    break
                events.append(json.loads(body))
                used += len(body)
            last = events[-1]["sequence"] if events else after_sequence
            # Consumers stage a capture until its committed final waterline. A
            # page boundary is not a relationship-consistency boundary.
            batches = {}
            for event in events:
                cid = event["capture_id"]
                if cid not in batches:
                    report = self.db.execute("SELECT report FROM captures WHERE id=?", (cid,)).fetchone()
                    batches[cid] = json.loads(report[0])["sequence"]
            return dict(
                events=events,
                next_sequence=last,
                head=head,
                has_more=last < head["sequence"],
                capture_end_sequences=batches,
                event_bytes=used,
            )
        finally:
            self.db.execute("ROLLBACK")

    def prune_through(self, sequence):
        """Explicit retention floor, not automatic eviction or a source operation."""
        self.db.execute("BEGIN IMMEDIATE")
        try:
            head = self.head()
            if type(sequence) is not int or not head["floor"] <= sequence <= head["sequence"]:
                raise ValueError("invalid retention floor")
            self.db.execute("DELETE FROM changes WHERE sequence<=?", (sequence,))
            self.db.execute("UPDATE head SET floor=? WHERE singleton=1", (sequence,))
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def snapshot(self, destination: Path):
        """Offline SQLite backup pins a consistent committed state and change waterline.

        Call from the catalog worker, never the Live publishing loop. A separate
        response adapter is still required for the existing HTTP snapshot schema.
        """
        with destination.open("xb"):
            pass
        target = sqlite3.connect(destination)
        try:
            self.db.backup(target)
        finally:
            target.close()
