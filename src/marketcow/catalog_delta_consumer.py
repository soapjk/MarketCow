"""Reference local catalog delta consumer, not a strategy or pool controller.

Network transport belongs to the caller. A complete capture commits atomically;
truncated batches remain disk staged and never become visible records.
"""

import json
import sqlite3

from marketcow.catalog_incremental import digest, encoded


class CatalogDeltaConsumer:
    def __init__(self, path, *, max_pending_bytes, max_records):
        if any(type(n) is not int or n <= 0 for n in (max_pending_bytes, max_records)):
            raise ValueError("explicit consumer budgets")
        self.maximum, self.records = max_pending_bytes, max_records
        self.db = sqlite3.connect(path)
        self.db.executescript("""
            PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS head(id INTEGER PRIMARY KEY, seq INTEGER, revision TEXT);
            CREATE TABLE IF NOT EXISTS records(id TEXT PRIMARY KEY, payload BLOB);
            CREATE TABLE IF NOT EXISTS pending(seq INTEGER PRIMARY KEY, capture TEXT, final INTEGER, payload BLOB);
        """)

    def close(self):
        self.db.close()

    def install(self, rows, *, sequence, revision, unique_count):
        if type(sequence) is not int or sequence < 0 or not 0 <= unique_count <= self.records:
            raise ValueError("invalid snapshot bounds")
        with self.db:
            self.db.execute("DELETE FROM records")
            self.db.execute("DELETE FROM pending")
            count = 0
            for row in rows:
                count += 1
                if count > self.records:
                    raise ValueError("snapshot record budget")
                self.db.execute("INSERT INTO records VALUES(?,?)", (row["market_id"], encoded(row)))
            if count != unique_count:
                raise ValueError("snapshot count mismatch")
            self.db.execute("INSERT OR REPLACE INTO head VALUES(1,?,?)", (sequence, revision))

    def head(self):
        row = self.db.execute("SELECT seq,revision FROM head").fetchone()
        if row is None:
            raise ValueError("snapshot required")
        return row

    def ingest(self, response):
        if response.get("schema_version") != "marketcow.polymarket.catalog-changes.v1":
            raise ValueError("changes schema")
        with self.db:
            sequence, revision = self.head()
            last = self.db.execute("SELECT seq,payload FROM pending ORDER BY seq DESC LIMIT 1").fetchone()
            if last:
                sequence, revision = last[0], json.loads(last[1])["catalog_revision"]
            used = self.db.execute("SELECT coalesce(sum(length(payload)),0) FROM pending").fetchone()[0]
            for event in response["events"]:
                if event["sequence"] != sequence + 1 or event["base_revision"] != revision:
                    raise ValueError("changes discontinuity")
                identity = dict(event)
                claimed = identity.pop("catalog_revision")
                if digest(identity) != claimed or event["market_id"] != event["record"]["market_id"]:
                    raise ValueError("changes hash/identity")
                final = response["capture_end_sequences"][event["capture_id"]]
                if type(final) is not int or final < event["sequence"]:
                    raise ValueError("invalid batch boundary")
                prior = self.db.execute("SELECT capture,final FROM pending ORDER BY seq LIMIT 1").fetchone()
                if prior and prior != (event["capture_id"], final):
                    raise ValueError("mixed unfinished capture")
                if not prior and event["batch_start_sequence"] != sequence + 1:
                    raise ValueError("missing capture start")
                payload = encoded(event)
                used += len(payload)
                if used > self.maximum:
                    raise ValueError("pending byte budget")
                self.db.execute(
                    "INSERT INTO pending VALUES(?,?,?,?)", (event["sequence"], event["capture_id"], final, payload)
                )
                sequence, revision = event["sequence"], claimed
                if sequence == final:
                    for (body,) in self.db.execute("SELECT payload FROM pending ORDER BY seq"):
                        value = json.loads(body)
                        self.db.execute(
                            "INSERT OR REPLACE INTO records VALUES(?,?)", (value["market_id"], encoded(value["record"]))
                        )
                    if self.db.execute("SELECT count(*) FROM records").fetchone()[0] > self.records:
                        raise ValueError("record budget")
                    self.db.execute("UPDATE head SET seq=?,revision=?", (sequence, revision))
                    self.db.execute("DELETE FROM pending")
                    used = 0
            if response["next_sequence"] != sequence:
                raise ValueError("changes next sequence mismatch")
            return sequence
