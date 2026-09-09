"""Dirty relation-group normalization; complete sorted artifact export stays streaming."""

import hashlib
import json
import os

from marketcow.polymarket_live import GammaLiveNormalizer, GammaNormalizedCatalog, canonical_json


def initialize(db):
    db.executescript("""
        CREATE TABLE IF NOT EXISTS norm_groups(id TEXT PRIMARY KEY, group_id TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS norm_group_members ON norm_groups(group_id);
        CREATE TABLE IF NOT EXISTS norm_dirty(group_id TEXT PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS norm_cache(id TEXT PRIMARY KEY, group_id TEXT, payload BLOB);
        CREATE INDEX IF NOT EXISTS norm_cache_groups ON norm_cache(group_id);
    """)


def changed(db, mid, raw):
    event = (raw.get("events") or [{}])[0]
    group = raw.get("negRiskMarketID") or raw.get("neg_risk_market_id") or event.get("negRiskMarketID")
    group = "negative:" + str(group) if group and (raw.get("negRisk") or raw.get("neg_risk")) else "market:" + mid
    old = db.execute("SELECT group_id FROM norm_groups WHERE id=?", (mid,)).fetchone()
    if old:
        db.execute("INSERT OR IGNORE INTO norm_dirty VALUES(?)", old)
    db.execute("INSERT OR REPLACE INTO norm_groups VALUES(?,?)", (mid, group))
    db.execute("INSERT OR IGNORE INTO norm_dirty VALUES(?)", (group,))


def normalize(db, output, observed_at, *, max_group_bytes, max_group_records):
    processed = 0
    while row := db.execute("SELECT group_id FROM norm_dirty ORDER BY group_id LIMIT 1").fetchone():
        group = row[0]
        values = []
        used = 0
        for (body,) in db.execute(
            "SELECT r.payload FROM raw r JOIN norm_groups g ON r.id=g.id WHERE g.group_id=? ORDER BY r.id", (group,)
        ):
            used += len(body)
            if used > max_group_bytes or len(values) >= max_group_records:
                raise ValueError("normalization relation group budget")
            values.append(json.loads(body, parse_float=str, parse_int=str))
        result = GammaLiveNormalizer.normalize(values, observed_at)
        with db:
            db.execute("DELETE FROM norm_cache WHERE group_id=?", (group,))
            for market in result:
                db.execute(
                    "INSERT OR REPLACE INTO norm_cache VALUES(?,?,?)",
                    (market.identity.market_id, group, canonical_json(market.model_dump(mode="json"))),
                )
            db.execute("DELETE FROM norm_dirty WHERE group_id=?", (group,))
        processed += 1
    count = 0
    sha = hashlib.sha256()
    revision = hashlib.sha256(b"[")
    with output.open("xb") as stream:
        for (body,) in db.execute("SELECT payload FROM norm_cache ORDER BY id"):
            if count:
                revision.update(b",")
            revision.update(body)
            count += 1
            line = body + b"\n"
            stream.write(line)
            sha.update(line)
        stream.flush()
        os.fsync(stream.fileno())
    revision.update(b"]")
    return GammaNormalizedCatalog(
        output, market_count=count, revision=revision.hexdigest(), sha256=sha.hexdigest()
    ), processed
