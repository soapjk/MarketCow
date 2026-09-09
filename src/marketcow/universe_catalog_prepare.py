"""Offline bounded preparation from frozen Gamma/normalized JSONL evidence.

Only a new output file is published. Inputs, realtime roots and manifests are
never modified. Prepared records preserve all eligible metadata identities,
including closed markets; no 1000-market selection is consulted.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from pathlib import Path

from marketcow.universe_catalog_mapping import map_gamma_record, optional_time
from marketcow.universe_control import wire_bytes


def prepare_catalog(*, raw_path: Path, normalized_path: Path, output: Path,
                    raw_sha256: str, normalized_sha256: str, revision: str,
                    raw_count: int, normalized_count: int, observed_at: str,
                    source_predicate: str, traversal_verified: bool, metric_unit: str | None,
                    max_input_row_bytes: int, max_output_row_bytes: int,
                    max_records: int, max_database_bytes: int, observed_by_id=None) -> dict:
    if output.exists():
        raise FileExistsError(output)
    if not source_predicate or type(traversal_verified) is not bool:
        raise ValueError("explicit source coverage required")
    if any(type(x) is not int or x <= 0 for x in (
        max_input_row_bytes, max_output_row_bytes, max_records, max_database_bytes
    )) or max_database_bytes < 4096:
        raise ValueError("explicit positive preparation limits required")
    if not 0 <= normalized_count <= raw_count <= max_records:
        raise ValueError("catalog count budget exceeded")
    # Scratch offset indexes avoid copying raw or normalized payloads into SQLite.
    with tempfile.TemporaryDirectory(prefix=".phase1-prepare-", dir=output.parent) as tmp:
        staging = Path(tmp) / "catalog.sqlite"
        with raw_path.open("rb") as raw_file, normalized_path.open("rb") as norm_file:
            db = sqlite3.connect(staging)
            try:
                db.executescript("PRAGMA page_size=4096; PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL;")
                db.execute(f"PRAGMA max_page_count={max_database_bytes // 4096}")
                db.executescript("""
                    CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
                    CREATE TABLE raw_offsets(id TEXT PRIMARY KEY, off INTEGER, len INTEGER);
                    CREATE TABLE norm_offsets(id TEXT PRIMARY KEY, off INTEGER, len INTEGER, included INTEGER);
                    CREATE TABLE records(market_id TEXT PRIMARY KEY,payload BLOB NOT NULL,sha256 TEXT NOT NULL);
                    CREATE TABLE relations(id TEXT PRIMARY KEY,payload BLOB NOT NULL);
                """)

                def scan(stream, *, normalized):
                    sha, revision_sha = hashlib.sha256(), hashlib.sha256(b"[")
                    count = 0
                    while True:
                        offset = stream.tell()
                        line = stream.readline(max_input_row_bytes + 1)
                        if not line:
                            break
                        if len(line) > max_input_row_bytes or not line.endswith(b"\n") or line == b"\n":
                            raise ValueError("catalog input row invalid or oversized")
                        sha.update(line)
                        if count:
                            revision_sha.update(b",")
                        revision_sha.update(line[:-1])
                        row = json.loads(line, parse_float=str)
                        mid = row["identity"]["market_id"] if normalized else str(row["id"])
                        if not mid:
                            raise ValueError("source market identity missing")
                        if normalized:
                            ref = db.execute("SELECT off,len FROM raw_offsets WHERE id=?", (mid,)).fetchone()
                            if ref is None:
                                raise ValueError("normalized raw evidence missing")
                            raw = json.loads(os.pread(raw_file.fileno(), ref[1], ref[0]), parse_float=str)
                            try:
                                map_gamma_record(row, raw, observed_at=observed_by_id(mid) if observed_by_id else observed_at, metric_unit=metric_unit,
                                                 contains=lambda _mid: True)
                                included = 1
                            except ValueError as error:
                                if str(error) != "source_identity_missing":
                                    raise
                                included = 0
                            db.execute("INSERT INTO norm_offsets VALUES(?,?,?,?)", (mid, offset, len(line), included))
                        else:
                            db.execute("INSERT INTO raw_offsets VALUES(?,?,?)", (mid, offset, len(line)))
                        count += 1
                        if count > max_records:
                            raise ValueError("catalog record budget exceeded")
                        if count % 1000 == 0:
                            db.commit()
                    revision_sha.update(b"]")
                    return count, sha.hexdigest(), revision_sha.hexdigest()

                actual_raw, actual_raw_sha, _ = scan(raw_file, normalized=False)
                if actual_raw != raw_count or actual_raw_sha != raw_sha256:
                    raise ValueError("raw catalog binding mismatch")
                actual_norm, actual_norm_sha, actual_revision = scan(norm_file, normalized=True)
                if (actual_norm != normalized_count or actual_norm_sha != normalized_sha256
                        or actual_revision != revision):
                    raise ValueError("normalized catalog binding mismatch")

                def contains(mid):
                    return db.execute("SELECT 1 FROM norm_offsets WHERE id=? AND included=1", (mid,)).fetchone() is not None

                rows = db.execute("SELECT n.id,n.off,n.len,r.off,r.len FROM norm_offsets n "
                                  "JOIN raw_offsets r ON r.id=n.id WHERE n.included=1 ORDER BY n.id")
                count = 0
                for mid, off, length, raw_off, raw_len in rows:
                    normal = json.loads(os.pread(norm_file.fileno(), length, off), parse_float=str)
                    raw = json.loads(os.pread(raw_file.fileno(), raw_len, raw_off), parse_float=str)
                    record = map_gamma_record(normal, raw, observed_at=observed_by_id(mid) if observed_by_id else observed_at,
                                              metric_unit=metric_unit, contains=contains)
                    body = wire_bytes(record)
                    if len(body) > max_output_row_bytes:
                        raise ValueError("catalog output row budget exceeded")
                    for relation in record["relations"]:
                        encoded = wire_bytes(relation)
                        previous = db.execute("SELECT payload FROM relations WHERE id=?", (relation["relation_id"],)).fetchone()
                        if previous is not None and previous[0] != encoded:
                            raise ValueError("catalog relation copies disagree")
                        db.execute("INSERT OR IGNORE INTO relations VALUES(?,?)", (relation["relation_id"], encoded))
                    db.execute("INSERT INTO records VALUES(?,?,?)", (mid, body, hashlib.sha256(body).hexdigest()))
                    count += 1
                reasons = []
                if not traversal_verified:
                    reasons.append("source_traversal_not_verified")
                if raw_count != normalized_count:
                    reasons.append("raw_records_not_normalized")
                if count != normalized_count:
                    reasons.append("source_event_identity_missing")
                report = {"catalog_revision": revision, "unique_count": count,
                          "raw_count": raw_count, "normalized_count": normalized_count,
                          "omitted_count": raw_count-count,
                          "source": {"name": "polymarket_gamma", "observed_at": optional_time(observed_at)},
                          "coverage": {"predicate": source_predicate + "; source-backed normalized identities",
                                       "complete": not reasons, "incomplete_reasons": reasons},
                          "raw_sha256": raw_sha256, "normalized_sha256": normalized_sha256,
                          "metric_unit": metric_unit}
                db.execute("INSERT INTO metadata VALUES('manifest',?)", (wire_bytes(report),))
                # Keep omitted IDs/reasons as bounded-by-source-count audit facts.
                db.execute("CREATE TABLE omitted AS SELECT r.id, CASE WHEN n.id IS NULL THEN "
                           "'not_normalized' ELSE 'source_event_identity_missing' END AS reason "
                           "FROM raw_offsets r LEFT JOIN norm_offsets n ON r.id=n.id "
                           "WHERE n.id IS NULL OR n.included=0")
                # Check again after mapping; no publication if an input was
                # modified between the hash pass and offset reads.
                raw_file.seek(0)
                norm_file.seek(0)
                if (hashlib.file_digest(raw_file, "sha256").hexdigest() != raw_sha256
                        or hashlib.file_digest(norm_file, "sha256").hexdigest() != normalized_sha256):
                    raise ValueError("catalog source changed during preparation")
                db.executescript("DROP TABLE raw_offsets; DROP TABLE norm_offsets; DROP TABLE relations;")
                db.commit()
                if db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                    raise ValueError("prepared catalog integrity failure")
            finally:
                db.close()
        with staging.open("rb") as stream:
            os.fsync(stream.fileno())
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        # Atomic publish with no overwrite, even if another publisher wins.
        os.link(staging, output)
        directory = os.open(output.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return dict(report, prepared_file_sha256=digest, database_bytes=output.stat().st_size)
