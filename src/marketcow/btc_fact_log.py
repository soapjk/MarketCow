"""Bounded realtime publication with an independent disk writer.

Callbacks must be nonblocking (for example subscriber.offer); user callbacks are
not executed by this class. Each subscriber has independent count/byte limits.
"""
from __future__ import annotations

import hashlib
import json
import os
import queue
import sqlite3
import threading
import uuid
from collections import deque
from pathlib import Path

from .btc_hourly_dataset import canonical


class Subscriber:
    def __init__(self, count: int, size: int):
        if min(count, size) <= 0:
            raise ValueError("invalid_subscriber_budget")
        self.count, self.size = count, size
        self.items: deque[bytes] = deque()
        self.bytes = 0
        self.error: str | None = None
        self.lock = threading.Lock()

    def offer(self, data: bytes) -> None:
        with self.lock:
            if self.error:
                return
            if len(self.items) >= self.count or self.bytes + len(data) > self.size:
                self.error = "slow_consumer"
                self.items.clear()
                self.bytes = 0
                return
            self.items.append(data)
            self.bytes += len(data)

    def receive(self) -> bytes | None:
        with self.lock:
            if self.error:
                raise RuntimeError(self.error)
            if not self.items:
                return None
            result = self.items.popleft()
            self.bytes -= len(result)
            return result


class FactLog:
    def __init__(self, root: Path, *, maximum_pending: int, maximum_pending_bytes: int,
                 maximum_disk_bytes: int, maximum_subscribers: int, before_write=None):
        if min(maximum_pending, maximum_pending_bytes, maximum_disk_bytes, maximum_subscribers) <= 0:
            raise ValueError("invalid_budget")
        import fcntl
        root.mkdir(parents=True, exist_ok=True)
        self.owner = (root / "owner.lock").open("a+b")
        try:
            fcntl.flock(self.owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            self.owner.close()
            raise
        self.root = root
        self.epoch = uuid.uuid4().hex
        self.maximum_pending = maximum_pending
        self.maximum_pending_bytes = maximum_pending_bytes
        self.maximum_disk_bytes = maximum_disk_bytes
        self.maximum_subscribers = maximum_subscribers
        self.before_write = before_write
        self.lock = threading.Lock()
        self.pending: queue.Queue = queue.Queue(maxsize=maximum_pending)
        self.pending_count = self.pending_bytes = 0
        self.subscribers: list[Subscriber] = []
        self.error: str | None = None
        self.stopped = False
        self.stop = threading.Event()
        self.recovered_continuity = None
        self.startup_validation = None
        db = None
        try:
            db = sqlite3.connect(root / "index.sqlite")
            db.execute("CREATE TABLE IF NOT EXISTS facts(seq INTEGER PRIMARY KEY, offset INTEGER, length INTEGER, sha TEXT)")
            db.execute("""CREATE TABLE IF NOT EXISTS log_state(
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                schema_version INTEGER NOT NULL,
                durable_seq INTEGER NOT NULL,
                durable_end INTEGER NOT NULL,
                durable_sha TEXT,
                continuity_json BLOB,
                continuity_seq INTEGER,
                validation_mode TEXT NOT NULL)""")
            last = db.execute("SELECT seq,offset,length,sha FROM facts ORDER BY seq DESC LIMIT 1").fetchone()
            self.durable = self.published = last[0] if last else 0
            self.disk_bytes = last[1] + last[2] if last else 0
            path = root / "facts.jsonl"
            actual = path.stat().st_size if path.exists() else 0
            if actual != self.disk_bytes:
                raise ValueError("unindexed_or_missing_tail_requires_recovery")
            state = db.execute("SELECT schema_version,durable_seq,durable_end,durable_sha,continuity_json,continuity_seq,validation_mode "
                               "FROM log_state WHERE singleton=1").fetchone()
            # Startup is deliberately O(1): validate the transactionally committed
            # tail only. Full historical validation belongs to audit_committed().
            checkpoint_raw = None
            if last:
                seq, offset, length, sha = last
                if offset < 0 or not isinstance(length, int) or length <= 0 or length > maximum_pending_bytes:
                    raise ValueError("corrupt_index_tail")
                with path.open("rb") as stream:
                    stream.seek(offset)
                    raw = stream.read(length)
                try:
                    value = json.loads(raw)
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise ValueError("corrupt_committed_tail") from exc
                if len(raw) != length or hashlib.sha256(raw).hexdigest() != sha or value.get("sequence") != seq:
                    raise ValueError("corrupt_committed_tail")
                checkpoint = value.get("fact", {}).get("continuity_checkpoint")
                checkpoint_raw = canonical(checkpoint) if checkpoint is not None else None
            if state is None:
                mode = "legacy_tail_bootstrap" if last else "empty"
                db.execute("INSERT INTO log_state VALUES(1,1,?,?,?,?,?,?)",
                           (self.durable, self.disk_bytes, last[3] if last else None,
                            checkpoint_raw, self.durable if checkpoint_raw is not None else None, mode))
                self.startup_validation = {"mode": mode, "records_checked": 1 if last else 0}
            else:
                version, durable_seq, durable_end, durable_sha, checkpoint_raw, checkpoint_seq, mode = state
                if version != 1 or durable_seq != self.durable or durable_end != self.disk_bytes \
                        or durable_sha != (last[3] if last else None):
                    raise ValueError("log_state_tail_mismatch")
                if (checkpoint_raw is None) != (checkpoint_seq is None) \
                        or checkpoint_seq is not None and not 1 <= checkpoint_seq <= durable_seq:
                    raise ValueError("log_state_checkpoint_mismatch")
                self.startup_validation = {"mode": mode, "records_checked": 1 if last else 0}
            if checkpoint_raw is not None:
                from .btc_continuity import Continuity
                validated = Continuity()
                validated.restore(json.loads(checkpoint_raw))
                self.recovered_continuity = validated.checkpoint()
            db.commit()
        except BaseException:
            self.owner.close()
            raise
        finally:
            if db is not None:
                db.close()
        self.writer = threading.Thread(target=self._write, name="btc-fact-writer", daemon=True)
        self.writer.start()

    def subscribe(self, *, count: int, size: int) -> Subscriber:
        with self.lock:
            self.subscribers = [s for s in self.subscribers if not s.error]
            if len(self.subscribers) >= self.maximum_subscribers:
                raise RuntimeError("subscriber_capacity")
            subscriber = Subscriber(count, size)
            self.subscribers.append(subscriber)
            return subscriber

    def publish(self, fact: dict) -> int:
        # No SQLite, file IO, or disk-held mutex on the realtime path.
        with self.lock:
            if self.error or self.stopped:
                raise RuntimeError(self.error or "stopped")
            seq = self.published + 1
            data = canonical({"epoch": self.epoch, "sequence": seq, "fact": fact}) + b"\n"
            if self.pending_count >= self.maximum_pending or self.pending_bytes + len(data) > self.maximum_pending_bytes:
                raise RuntimeError("persistence_capacity")
            if self.disk_bytes + self.pending_bytes + len(data) > self.maximum_disk_bytes:
                raise RuntimeError("archive_capacity")
            self.pending_count += 1
            self.pending_bytes += len(data)
            self.published = seq
            for subscriber in self.subscribers:
                subscriber.offer(data)
            checkpoint = fact.get("continuity_checkpoint")
            checkpoint_raw = canonical(checkpoint) if checkpoint is not None else None
            self.pending.put_nowait((seq, data, checkpoint_raw))
            return seq

    def unsubscribe(self, subscriber: Subscriber) -> None:
        with self.lock:
            if subscriber in self.subscribers:
                self.subscribers.remove(subscriber)
            with subscriber.lock:
                subscriber.error = "subscription_closed"
                subscriber.items.clear()
                subscriber.bytes = 0

    def _write(self) -> None:
        db = None
        try:
            db = sqlite3.connect(self.root / "index.sqlite")
            db.execute("PRAGMA synchronous=FULL")
            with (self.root / "facts.jsonl").open("ab") as stream:
                while not self.stop.is_set() or not self.pending.empty():
                    try:
                        seq, data, checkpoint_raw = self.pending.get(timeout=0.05)
                    except queue.Empty:
                        continue
                    if self.before_write:
                        self.before_write()
                    offset = stream.tell()
                    stream.write(data)
                    stream.flush()
                    os.fsync(stream.fileno())
                    digest = hashlib.sha256(data).hexdigest()
                    db.execute("INSERT INTO facts VALUES(?,?,?,?)", (seq, offset, len(data), digest))
                    db.execute("""INSERT INTO log_state VALUES(1,1,?,?,?,?,?,?)
                        ON CONFLICT(singleton) DO UPDATE SET
                        schema_version=excluded.schema_version,
                        durable_seq=excluded.durable_seq,
                        durable_end=excluded.durable_end,
                        durable_sha=excluded.durable_sha,
                        continuity_json=coalesce(excluded.continuity_json,log_state.continuity_json),
                        continuity_seq=coalesce(excluded.continuity_seq,log_state.continuity_seq),
                        validation_mode=excluded.validation_mode""",
                        (seq, offset + len(data), digest, checkpoint_raw,
                         seq if checkpoint_raw is not None else None, "transactional_tail"))
                    db.commit()
                    with self.lock:
                        self.durable = seq
                        self.disk_bytes += len(data)
                        self.pending_count -= 1
                        self.pending_bytes -= len(data)
        except BaseException as exc:
            with self.lock:
                self.error = "persistence_failed:" + type(exc).__name__
                for subscriber in self.subscribers:
                    with subscriber.lock:
                        subscriber.error = self.error
        finally:
            if db is not None:
                db.close()

    def status(self) -> dict:
        with self.lock:
            return {"epoch": self.epoch, "published": self.published, "durable": self.durable,
                    "pending_count": self.pending_count, "pending_bytes": self.pending_bytes,
                    "error": self.error, "stopped": self.stopped,
                    "startup_validation": self.startup_validation}

    def close(self, timeout: float) -> None:
        with self.lock:
            self.stopped = True
        self.stop.set()
        self.writer.join(timeout)
        if self.writer.is_alive():
            raise TimeoutError("writer_still_running_owner_retained")
        self.owner.close()
        if self.error:
            raise RuntimeError(self.error)


def read_page(root: Path, *, after: int, through: int, limit: int, maximum_bytes: int) -> list[dict]:
    if after < 0 or through < after or limit <= 0 or maximum_bytes <= 0:
        raise ValueError("invalid_read_budget")
    db = sqlite3.connect(f"{(root / 'index.sqlite').resolve().as_uri()}?mode=ro", uri=True)
    result = []
    used = 0
    try:
        db.execute("BEGIN")
        durable = db.execute("SELECT coalesce(max(seq),0) FROM facts").fetchone()[0]
        if through > durable:
            raise ValueError("through_exceeds_durable")
        with (root / "facts.jsonl").open("rb") as source:
            expected_seq = after + 1
            for seq, offset, length, sha in db.execute("SELECT * FROM facts WHERE seq>? AND seq<=? ORDER BY seq LIMIT ?", (after, through, limit)):
                if seq != expected_seq or not isinstance(offset, int) or offset < 0 or not isinstance(length, int) or length <= 0:
                    raise ValueError("corrupt_index_continuity")
                if used + length > maximum_bytes:
                    if not result:
                        raise ValueError("response_size_exceeded")
                    break
                source.seek(offset)
                raw = source.read(length)
                if hashlib.sha256(raw).hexdigest() != sha:
                    raise ValueError("raw_hash_mismatch")
                value = json.loads(raw)
                if value["sequence"] != seq:
                    raise ValueError("sequence_mismatch")
                result.append(value)
                used += length
                expected_seq += 1
            if not result and after < through:
                raise ValueError("corrupt_index_continuity")
    finally:
        db.close()
    return result


def recover_tail(root: Path, *, maximum_tail_bytes: int) -> dict:
    """Preserve uncommitted bytes before truncating; committed corruption is never repaired."""
    import fcntl
    if maximum_tail_bytes <= 0:
        raise ValueError("invalid_budget")
    with (root / "owner.lock").open("a+b") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        db = sqlite3.connect(f"{(root / 'index.sqlite').resolve().as_uri()}?mode=ro", uri=True)
        try:
            last = db.execute("SELECT offset+length FROM facts ORDER BY seq DESC LIMIT 1").fetchone()
            end = last[0] if last else 0
        finally:
            db.close()
        with (root / "facts.jsonl").open("r+b") as stream:
            size = os.fstat(stream.fileno()).st_size
            if size < end:
                raise ValueError("committed_bytes_missing")
            if size - end > maximum_tail_bytes:
                raise ValueError("tail_exceeds_recovery_budget")
            if size == end:
                return {"recovered_bytes": 0, "preserved_file": None}
            stream.seek(end)
            raw = stream.read(maximum_tail_bytes)
            digest = hashlib.sha256(raw).hexdigest()
            preserved = root / ("uncommitted-" + digest + ".raw")
            if preserved.exists():
                if preserved.stat().st_size != len(raw) or hashlib.sha256(preserved.read_bytes()).hexdigest() != digest:
                    raise ValueError("preserved_tail_corrupt")
            else:
                with preserved.open("xb") as output:
                    output.write(raw)
                    output.flush()
                    os.fsync(output.fileno())
            descriptor = os.open(root, os.O_RDONLY)
            try:
                os.fsync(descriptor)
                stream.truncate(end)
                stream.flush()
                os.fsync(stream.fileno())
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            return {"recovered_bytes": len(raw), "preserved_file": preserved.name,
                    "sha256": digest, "requires_source_resync": True}


def audit_committed(root: Path, *, maximum_record_bytes: int) -> dict:
    """Explicit O(N) audit; never called by service startup.

    The audit reads a stable SQLite snapshot up to its durable tail. A concurrently
    appended raw tail is intentionally outside this receipt.
    """
    if maximum_record_bytes <= 0:
        raise ValueError("invalid_budget")
    db = sqlite3.connect(f"{(root / 'index.sqlite').resolve().as_uri()}?mode=ro", uri=True)
    checked = total_bytes = 0
    final_sha = None
    try:
        db.execute("BEGIN")
        state = db.execute("SELECT durable_seq,durable_end,durable_sha FROM log_state WHERE singleton=1").fetchone()
        if state is None:
            last = db.execute("SELECT seq,offset+length,sha FROM facts ORDER BY seq DESC LIMIT 1").fetchone()
            state = last or (0, 0, None)
        durable_seq, durable_end, durable_sha = state
        with (root / "facts.jsonl").open("rb") as source:
            expected_seq = 1
            expected_offset = 0
            for seq, offset, length, sha in db.execute("SELECT seq,offset,length,sha FROM facts ORDER BY seq"):
                if seq > durable_seq:
                    raise ValueError("index_beyond_durable_state")
                if seq != expected_seq or offset != expected_offset or length <= 0:
                    raise ValueError("corrupt_index_continuity")
                if length > maximum_record_bytes:
                    raise ValueError("record_exceeds_budget")
                raw = source.read(length)
                if len(raw) != length or hashlib.sha256(raw).hexdigest() != sha:
                    raise ValueError("corrupt_committed_fact")
                try:
                    value = json.loads(raw)
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise ValueError("corrupt_committed_fact") from exc
                if value.get("sequence") != seq:
                    raise ValueError("corrupt_committed_fact")
                checked += 1
                total_bytes += length
                final_sha = sha
                expected_seq += 1
                expected_offset += length
            if checked != durable_seq or expected_offset != durable_end or final_sha != durable_sha:
                raise ValueError("durable_state_mismatch")
        return {"schema_version": "marketcow.btc-fact-audit.v1", "complete": True,
                "records_checked": checked, "bytes_checked": total_bytes,
                "durable_seq": durable_seq, "durable_end": durable_end,
                "durable_sha": durable_sha}
    finally:
        db.close()
