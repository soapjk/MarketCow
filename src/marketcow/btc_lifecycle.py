"""Control-path hour registry, independent of realtime subscriptions and disks.

Stores reviewed identities and source observations; never authorizes payout.
Call outside the realtime callback. Capacity exhaustion preserves existing rows.
"""
import hashlib
import sqlite3
from contextlib import contextmanager

from .btc_hourly_dataset import canonical
from .btc_hourly_identity import plan_hours
from .btc_polymarket_binding import timestamp
from .universe_live_probe import strict_json


class HourRegistry:
    def __init__(self, path, *, maximum_markets, maximum_bytes):
        if type(maximum_markets) is not int or maximum_markets <= 0 or type(maximum_bytes) is not int or maximum_bytes <= 0:
            raise ValueError("invalid_registry_budget")
        self.path, self.maximum_markets, self.maximum_bytes = path, maximum_markets, maximum_bytes
        with self.db() as db:
            tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if not tables <= {"hours", "observations"}:
                raise ValueError("dedicated_lifecycle_database_required")
            db.execute("CREATE TABLE IF NOT EXISTS hours(id TEXT PRIMARY KEY, body BLOB NOT NULL, sha TEXT NOT NULL)")
            db.execute("CREATE TABLE IF NOT EXISTS observations(sha TEXT PRIMARY KEY, market_id TEXT NOT NULL, body BLOB NOT NULL)")

    @contextmanager
    def db(self):
        db = sqlite3.connect(self.path, timeout=2)
        try:
            db.execute("PRAGMA synchronous=FULL")
            db.execute("BEGIN IMMEDIATE")
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def _capacity(self, db, additional):
        size = db.execute("SELECT coalesce(sum(length(body)),0) FROM hours").fetchone()[0]
        size += db.execute("SELECT coalesce(sum(length(body)),0) FROM observations").fetchone()[0]
        if size + additional > self.maximum_bytes:
            raise ValueError("lifecycle_payload_capacity")

    def register(self, bindings):
        # Bindings must come from bind_hour, not unreviewed discovery metadata.
        if len(bindings) > self.maximum_markets or len({b["market_id"] for b in bindings}) != len(bindings):
            raise ValueError("lifecycle_identity_capacity")
        with self.db() as db:
            for binding in bindings:
                if binding.get("schema_version") != "marketcow.btc-hour.binding.v1":
                    raise ValueError("binding_schema")
                start, end = timestamp(binding["start_utc"]), timestamp(binding["end_utc"])
                if (end-start).total_seconds() != 3600 or binding["up_token"] == binding["down_token"]:
                    raise ValueError("binding_window_or_tokens")
                raw = canonical(binding)
                digest = hashlib.sha256(raw).hexdigest()
                old = db.execute("SELECT body,sha FROM hours WHERE id=?", (binding["market_id"],)).fetchone()
                if old:
                    if hashlib.sha256(old[0]).hexdigest() != old[1]:
                        raise ValueError("corrupt_lifecycle_binding")
                    if old != (raw, digest):
                        raise ValueError("binding_changed_requires_review")
                    continue
                if db.execute("SELECT count(*) FROM hours").fetchone()[0] >= self.maximum_markets:
                    raise ValueError("lifecycle_identity_capacity")
                self._capacity(db, len(raw))
                db.execute("INSERT INTO hours VALUES(?,?,?)", (binding["market_id"], raw, digest))

    def plan(self, now, *, maximum_subscriptions):
        with self.db() as db:
            rows = db.execute("SELECT body,sha FROM hours ORDER BY id")
            markets = []
            for raw, digest in rows:
                if len(markets) >= self.maximum_markets or len(raw) > self.maximum_bytes or hashlib.sha256(raw).hexdigest() != digest:
                    raise ValueError("corrupt_lifecycle_binding")
                b = strict_json(raw)
                final = False
                for observed, observed_sha in db.execute(
                    "SELECT body,sha FROM observations WHERE market_id=? ORDER BY rowid", (b["market_id"],)
                ):
                    if hashlib.sha256(observed).hexdigest() != observed_sha:
                        raise ValueError("corrupt_settlement_observation")
                    item = strict_json(observed)
                    final = final or (item.get("schema_version") == "marketcow.polymarket.ctf-finality-quorum.v1"
                                      and item.get("status") == "verified_final"
                                      and item.get("settlement_import_allowed") is True)
                markets.append(dict(market_id=b["market_id"], start=timestamp(b["start_utc"]),
                                    end=timestamp(b["end_utc"]), up_token=b["up_token"], down_token=b["down_token"],
                                    rule_review_status="approved",
                                    settlement_status="verified_final" if final else "unverified"))
        return plan_hours(markets, now, maximum_markets=maximum_subscriptions,
                          maximum_pending=self.maximum_markets)

    def observe_settlement(self, market_id, raw):
        if len(raw) > self.maximum_bytes:
            raise ValueError("lifecycle_payload_capacity")
        value = strict_json(raw)
        if (value.get("schema_version") != "marketcow.polymarket.ctf-observation.v1"
                or value.get("status") not in ("unresolved", "resolved_unverified")
                or value.get("settlement_import_allowed") is not False):
            raise ValueError("unsupported_finality_claim")
        timestamp(value["observed_at"])
        digest = hashlib.sha256(raw).hexdigest()
        with self.db() as db:
            row = db.execute("SELECT body,sha FROM hours WHERE id=?", (market_id,)).fetchone()
            if row is None or hashlib.sha256(row[0]).hexdigest() != row[1]:
                raise ValueError("unknown_or_corrupt_hour")
            binding = strict_json(row[0])
            if value["condition_id"] != binding["condition_id"]:
                raise ValueError("settlement_identity_mismatch")
            if db.execute("SELECT 1 FROM observations WHERE sha=?", (digest,)).fetchone():
                saved = db.execute("SELECT market_id,body FROM observations WHERE sha=?", (digest,)).fetchone()
                if saved != (market_id, raw):
                    raise ValueError("observation_binding_conflict")
                return digest
            self._capacity(db, len(raw))
            db.execute("INSERT INTO observations VALUES(?,?,?)", (digest, market_id, raw))
        return digest

    def observe_final_settlement(self, market_id, raw):
        """Persist an independently verified payout without account mutation."""
        if len(raw) > self.maximum_bytes:
            raise ValueError("lifecycle_payload_capacity")
        value = strict_json(raw)
        providers = value.get("provider_receipts")
        payouts, denominator = value.get("payout_numerators"), value.get("payout_denominator")
        if (value.get("schema_version") != "marketcow.polymarket.ctf-finality-quorum.v1"
                or value.get("status") != "verified_final" or value.get("settlement_import_allowed") is not True
                or value.get("finality_policy") != "two_independent_rpc_finalized_receipts_v1"
                or not isinstance(providers, list) or len(providers) < 2
                or len({item.get("provider_id") for item in providers}) != len(providers)
                or not isinstance(payouts, list) or len(payouts) != 2
                or any(not isinstance(item, str) or not item.isdigit() for item in payouts)
                or not isinstance(denominator, str) or not denominator.isdigit() or int(denominator) <= 0
                or sum(map(int, payouts)) != int(denominator)):
            raise ValueError("unsupported_finality_claim")
        timestamp(value["verified_at"])
        digest = hashlib.sha256(raw).hexdigest()
        with self.db() as db:
            row = db.execute("SELECT body,sha FROM hours WHERE id=?", (market_id,)).fetchone()
            if row is None or hashlib.sha256(row[0]).hexdigest() != row[1]:
                raise ValueError("unknown_or_corrupt_hour")
            binding = strict_json(row[0])
            if (value.get("condition_id") != binding["condition_id"]
                    or value.get("token_ids") != [binding["up_token"], binding["down_token"]]):
                raise ValueError("settlement_identity_mismatch")
            old = db.execute("SELECT market_id,body FROM observations WHERE sha=?", (digest,)).fetchone()
            if old:
                if old != (market_id, raw):
                    raise ValueError("observation_binding_conflict")
                return digest
            self._capacity(db, len(raw))
            db.execute("INSERT INTO observations VALUES(?,?,?)", (digest, market_id, raw))
        return digest

    def binding(self, market_id):
        with self.db() as db:
            row = db.execute("SELECT body,sha FROM hours WHERE id=?", (market_id,)).fetchone()
            if row is None or len(row[0]) > self.maximum_bytes or hashlib.sha256(row[0]).hexdigest() != row[1]:
                raise ValueError("unknown_or_corrupt_hour")
            return strict_json(row[0])
