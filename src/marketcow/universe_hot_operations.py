"""Authenticated same-release scope operations; no systemd or data-root swaps."""
import hashlib
import json
import os
from pathlib import Path
import sqlite3
from contextlib import contextmanager

from marketcow.polymarket_contracts import content_sha256, canonical_json
from marketcow.universe_hot_candidate import build_hot_candidate
from marketcow.universe_owner import SupervisorOwner
from marketcow.universe_phase1 import _parse_millis
from marketcow.universe_rust_control import RustScopeClient


class HotScopeOperations:
    def __init__(self, control, config, *, client_factory=RustScopeClient, builder=build_hot_candidate):
        fields = {"store_path", "owner_lock", "source_root", "maximum_candidates", "maximum_artifact_bytes",
            "maximum_row_bytes", "maximum_source_bytes", "maximum_relation_members", "maximum_metadata_tokens",
            "maximum_dependency_markets", "depth_quantities", "maximum_book_age_ms", "candidate_ttl_seconds",
            "legacy_discovery_universe", "legacy_selection_id", "runtimes"}
        if set(config) != fields or set(config["runtimes"]) != {"live", "discovery"}:
            raise ValueError("explicit hot-scope operator configuration required")
        for field in ("store_path", "owner_lock", "source_root"):
            if not Path(config[field]).is_absolute():
                raise ValueError("absolute hot-scope operator paths required")
        for field in fields-{"store_path", "owner_lock", "source_root", "depth_quantities", "legacy_discovery_universe", "legacy_selection_id", "runtimes"}:
            if type(config[field]) is not int or config[field] <= 0:
                raise ValueError("positive explicit hot-scope budget required")
        if config["maximum_candidates"] > 6 or config["candidate_ttl_seconds"] > 3600:
            raise ValueError("hot-scope retention capacity")
        self.control, self.config, self.builder = control, dict(config), builder
        path = Path(config["store_path"])
        descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            if os.fstat(descriptor).st_mode & 0o077:
                raise ValueError("hot candidate store must be private")
        finally:
            os.close(descriptor)
        self.clients = {}
        for pool, row in config["runtimes"].items():
            if set(row) != {"socket_path", "maximum_bytes", "timeout_seconds"}:
                raise ValueError("explicit private Rust endpoint required")
            self.clients[pool] = client_factory(socket_path=Path(row["socket_path"]),
                maximum_bytes=row["maximum_bytes"], timeout_seconds=row["timeout_seconds"])
        with self._db() as db:
            tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if not tables <= {"hot_candidates", "hot_cleanup"}:
                raise ValueError("not a dedicated hot candidate database")
            db.execute("CREATE TABLE IF NOT EXISTS hot_candidates (id TEXT PRIMARY KEY, pool TEXT NOT NULL, caller TEXT NOT NULL, expires_ms INTEGER NOT NULL, body BLOB NOT NULL, sha TEXT NOT NULL, stage TEXT NOT NULL, receipt BLOB)")
            db.execute("CREATE TABLE IF NOT EXISTS hot_cleanup (pool TEXT PRIMARY KEY, receipt BLOB NOT NULL, sha TEXT NOT NULL)")

    @contextmanager
    def _db(self):
        db = sqlite3.connect(self.config["store_path"], timeout=2)
        try:
            db.execute("PRAGMA synchronous=FULL")
            with db:
                yield db
        finally:
            db.close()

    def _rows(self, db, pool):
        rows = db.execute("SELECT id,caller,expires_ms,body,sha,stage,receipt FROM hot_candidates WHERE pool=?", (pool,)).fetchall()
        if len(rows) > self.config["maximum_candidates"]:
            raise ValueError("hot candidate store capacity corrupt")
        checked = []
        for cid, caller, expires, raw, sha, stage, receipt in rows:
            if len(raw) > self.config["maximum_artifact_bytes"] or hashlib.sha256(raw).hexdigest() != sha:
                raise ValueError("hot candidate store hash mismatch")
            candidate = json.loads(raw)
            body = dict(candidate); body.pop("candidate_id")
            if content_sha256(body) != cid or candidate["candidate_id"] != cid:
                raise ValueError("hot candidate content identity mismatch")
            checked.append((candidate, caller, expires, stage, json.loads(receipt) if receipt else None))
        return checked

    @staticmethod
    def _matches(candidate, actual):
        if candidate["pool"] == "live":
            return candidate["config"]["active_scope_id"] == actual.get("scope_id")
        return candidate["config"]["universe_revision"] == actual.get("universe_revision")

    def status(self, pool):
        if pool not in self.clients:
            raise ValueError("invalid hot pool")
        actual = self.clients[pool].status()
        with self._db() as db:
            rows = self._rows(db, pool)
        matches = [candidate for candidate, *_ in rows if self._matches(candidate, actual)]
        if len(matches) > 1:
            raise ValueError("ambiguous active selection binding")
        selection = matches[0]["selection_sha256"] if matches else None
        if (selection is None and pool == "discovery"
                and actual.get("universe_revision") == self.config["legacy_discovery_universe"]):
            selection = self.config["legacy_selection_id"]
        return {"schema_version": "marketcow.hot-scope-status.v1", "pool": pool, "actual": actual,
                "selection_id": selection, "candidate_id": matches[0]["candidate_id"] if matches else None,
                "candidates": [{"candidate_id": c["candidate_id"], "stage": stage, "expires_ms": expires}
                               for c, _, expires, stage, _ in rows]}

    def current_discovery_selection(self):
        selection = self.status("discovery")["selection_id"]
        if not selection:
            raise ValueError("unbound actual Discovery selection")
        return selection

    @staticmethod
    def _lease_identity(caller, lease_id):
        if (not isinstance(lease_id, str) or not lease_id or len(lease_id) > 96
                or any(not (c.isascii() and (c.isalnum() or c in "-_.:")) for c in lease_id)):
            raise ValueError("invalid acquisition lease id")
        return hashlib.sha256(caller.encode()).hexdigest()[:16]+":"+lease_id

    def acquisition_lease_status(self, pool):
        if pool not in self.clients:
            raise ValueError("invalid hot pool")
        return self.clients[pool].request({"operation": "acquisition_lease_status"})

    def acquisition_lease(self, caller, body, action):
        if action not in {"acquire", "renew", "release"}:
            raise ValueError("invalid acquisition lease action")
        common = {"schema_version", "pool", "expected_scope_id", "expected_revision", "lease_id"}
        expected = common | ({"ttl_seconds", "market_ids"} if action == "acquire" else ({"ttl_seconds"} if action == "renew" else set()))
        if (set(body) != expected or body.get("schema_version") != "marketcow.acquisition-lease.v1"
                or body.get("pool") not in self.clients or type(body.get("expected_revision")) is not int
                or body["expected_revision"] <= 0 or not isinstance(body.get("expected_scope_id"), str)):
            raise ValueError("invalid acquisition lease request")
        command = {"operation": action+"_acquisition_lease", "expected_scope_id": body["expected_scope_id"],
            "expected_revision": body["expected_revision"], "lease_id": self._lease_identity(caller, body["lease_id"])}
        if action != "release":
            if type(body["ttl_seconds"]) is not int or body["ttl_seconds"] <= 0:
                raise ValueError("invalid acquisition lease TTL")
            command["ttl_seconds"] = body["ttl_seconds"]
        if action == "acquire":
            ids = body["market_ids"]
            if not isinstance(ids, list) or not ids or any(not isinstance(i, str) or not i for i in ids) or ids != sorted(set(ids)):
                raise ValueError("sorted unique lease market ids required")
            command["market_ids"] = ids
        return self.clients[body["pool"]].request(command)

    def _build(self, *, pool, ids, selection, policy, protected, parent):
        c = self.config
        return self.builder(source_root=c["source_root"], source=self.control.source, pool=pool,
            market_ids=ids, selection_sha256=selection, policy_version=policy,
            protected_market_ids=protected, parent_selection_id=parent,
            **{k: c[k] for k in ("maximum_dependency_markets", "maximum_metadata_tokens", "maximum_row_bytes",
                "maximum_artifact_bytes", "maximum_source_bytes", "maximum_relation_members", "depth_quantities", "maximum_book_age_ms")})

    def _acquisition_commands(self, candidate, expected):
        maximum = self.config["runtimes"][candidate["pool"]]["maximum_bytes"]
        acquired = set(candidate["acquisition_market_ids"])
        def command(records):
            return dict(operation="prepare_acquisition", **expected,
                catalog_revision=candidate["catalog_revision"], records=records,
                evidence_sha256=content_sha256(records),
                acquisition_market_ids=sorted(r["identity"]["market_id"] for r in records if r["identity"]["market_id"] in acquired))
        # Bound the complete wire, including IDs, not just record payload bytes.
        records, size = [], len(canonical_json(command([])))
        for record in candidate["records"]:
            cost = len(canonical_json(record))+len(canonical_json(record["identity"]["market_id"]))+2
            if records and (size+cost > maximum or len(records) == 4096):
                yield command(records)
                records, size = [], len(canonical_json(command([])))
            if size+cost > maximum:
                raise ValueError("single metadata record exceeds runtime command capacity")
            records.append(record); size += cost
        if records:
            yield command(records)

    def prepare_discovery(self, caller, request, admission_response_sha256):
        with SupervisorOwner(Path(self.config["owner_lock"])):
            self.control.admitted_for_preparation(caller, request, admission_response_sha256)
            candidate = self._build(pool="discovery", ids=tuple(request.selection.market_ids),
                selection=request.selection_sha256, policy=request.selection.tradude_policy_version,
                protected=tuple(p.market_id for p in request.selection.protected_markets), parent=None)
            expires = min(_parse_millis(request.expires_at), self.control.wall_ms()+self.config["candidate_ttl_seconds"]*1000)
            return self._prepare(caller, candidate, expires)

    def _parent_ids(self, selection):
        status = self.status("discovery")
        if selection != status["selection_id"]:
            raise ValueError("parent Discovery selection changed")
        with self._db() as db:
            parent = next((c for c, *_ in self._rows(db, "discovery") if self._matches(c, status["actual"])), None)
        if parent:
            return set(parent["requested_market_ids"])
        with (Path(self.config["source_root"])/"catalog.json").open("rb") as stream:
            raw = stream.read(self.config["maximum_artifact_bytes"]+1)
        if len(raw) > self.config["maximum_artifact_bytes"]:
            raise ValueError("legacy parent byte capacity")
        universe = dict(json.loads(raw)["realtime_universe"])
        uid = universe.pop("universe_id")
        if uid != self.config["legacy_discovery_universe"] or content_sha256(universe) != uid:
            raise ValueError("legacy parent binding changed")
        return set(universe["market_ids"])

    def prepare_live(self, caller, body):
        fields = {"schema_version", "catalog_revision", "parent_selection_id", "market_ids", "protected_market_ids",
            "protected_exceptions", "policy_version", "expires_ms", "expected_scope_id", "expected_revision"}
        if (set(body) != fields or body["schema_version"] != "marketcow.hot-live-prepare.v1"
                or body["catalog_revision"] != self.control.source.revision or type(body["expires_ms"]) is not int
                or type(body["expected_revision"]) is not int or body["expected_revision"] <= 0
                or not isinstance(body["market_ids"], list) or not isinstance(body["protected_market_ids"], list)
                or not self.control.wall_ms() < body["expires_ms"] <= self.control.wall_ms()+self.config["candidate_ttl_seconds"]*1000):
            raise ValueError("invalid Live preparation identity/expiry")
        with SupervisorOwner(Path(self.config["owner_lock"])):
            actual = self.clients["live"].status()
            if actual.get("scope_id") != body["expected_scope_id"] or actual.get("revision") != body["expected_revision"]:
                raise ValueError("Live incumbent changed")
            parent = self._parent_ids(body["parent_selection_id"])
            exceptions = body["protected_exceptions"]
            if (not isinstance(exceptions, list) or any(set(row) != {"market_id", "reasons"}
                    or not row["reasons"] or row["reasons"] != sorted(set(row["reasons"]))
                    or any(not isinstance(reason, str) or not reason or not reason.isascii() for reason in row["reasons"]) for row in exceptions)):
                raise ValueError("invalid protected exception reasons")
            exception_ids = [row["market_id"] for row in exceptions]
            outside = set(body["market_ids"])-parent
            if exception_ids != sorted(set(exception_ids)) or set(exception_ids) != outside or not outside <= set(body["protected_market_ids"]):
                raise ValueError("Live membership outside parent without exact protection")
            candidate = self._build(pool="live", ids=tuple(body["market_ids"]), selection=content_sha256(body),
                policy=body["policy_version"], protected=tuple(body["protected_market_ids"]), parent=body["parent_selection_id"])
            return self._prepare(caller, candidate, body["expires_ms"])

    def _prepare(self, caller, candidate, expires, *, resume=False):
        pool, cid = candidate["pool"], candidate["candidate_id"]
        actual = self.clients[pool].status()
        expected = dict(expected_scope_id=actual["scope_id" if pool == "live" else "projection_id"], expected_revision=actual["revision"])
        with self._db() as db:
            if db.execute("SELECT 1 FROM hot_cleanup WHERE pool=?", (pool,)).fetchone():
                raise RuntimeError("pending cleanup must be reconciled before preparation")
            rows = self._rows(db, pool)
            existing = next((row for row in rows if row[0]["candidate_id"] == cid), None)
            if existing:
                if existing[1] != caller or existing[2] <= self.control.wall_ms():
                    raise ValueError("candidate caller/expiry conflict")
                if existing[3] == "prepared":
                    return existing[4]
                if not resume or existing[3] != "preparing":
                    raise RuntimeError("candidate requires status reconciliation")
            elif len(rows) >= self.config["maximum_candidates"] or any(row[3] in ("preparing", "prepared") for row in rows):
                raise ValueError("candidate capacity")
            raw = canonical_json(candidate)
            if len(raw) > self.config["maximum_artifact_bytes"]:
                raise ValueError("candidate byte capacity")
            if not existing:
                db.execute("INSERT INTO hot_candidates VALUES (?,?,?,?,?,?,?,NULL)",
                    (cid, pool, caller, expires, raw, hashlib.sha256(raw).hexdigest(), "preparing"))
        chunks = []
        for command in self._acquisition_commands(candidate, expected):
            installed = self.clients[pool].request(command)
            if installed.get("acquisition_installed") is not True or installed.get("publication_applied") is not False:
                raise RuntimeError("acquisition did not produce an installation receipt")
            chunks.append(dict(request_sha256=content_sha256(command), response_sha256=content_sha256(installed),
                               metadata_count=len(command["records"]), acquisition_count=len(command["acquisition_market_ids"])))
        prepared = self.clients[pool].request(dict(operation="prepare_publication", **expected, config=candidate["config"]))
        if prepared.get("publication_applied") is not False:
            raise RuntimeError("prepare unexpectedly published")
        if expires <= self.control.wall_ms():
            raise RuntimeError("candidate expired after preparation; explicit cleanup required")
        response = dict(schema_version="marketcow.hot-scope-prepared.v1", candidate_id=cid, pool=pool,
            selection_id=candidate["selection_sha256"], expires_ms=expires, expected=expected,
            requested_market_ids=candidate["requested_market_ids"], acquisition={"chunks": chunks, "all_installed": True},
            publication_applied=False, actual=prepared["actual"])
        with self._db() as db:
            db.execute("UPDATE hot_candidates SET stage='prepared',receipt=? WHERE id=?", (canonical_json(response), cid))
        return response

    def reconcile(self, caller, body):
        fields = {"schema_version", "pool", "candidate_id", "expected_scope_id", "expected_revision"}
        if (set(body) != fields or body["schema_version"] != "marketcow.hot-scope-reconcile.v1"
                or body["pool"] not in self.clients or type(body["expected_revision"]) is not int
                or body["expected_revision"] <= 0):
            raise ValueError("invalid explicit reconciliation")
        with SupervisorOwner(Path(self.config["owner_lock"])):
            actual = self.clients[body["pool"]].status()
            with self._db() as db:
                row = next((r for r in self._rows(db, body["pool"]) if r[0]["candidate_id"] == body["candidate_id"]), None)
                if row is None or row[1] != caller:
                    raise ValueError("unknown or foreign candidate")
                if self._matches(row[0], actual):
                    db.execute("UPDATE hot_candidates SET stage='active' WHERE id=?", (body["candidate_id"],))
                    return dict(schema_version="marketcow.hot-scope-reconciled.v1", stage="active", actual=actual,
                        candidate_id=body["candidate_id"], selection_id=row[0]["selection_sha256"], publication_applied_by_this_operation=False)
            key = "scope_id" if body["pool"] == "live" else "projection_id"
            if actual[key] != body["expected_scope_id"] or actual["revision"] != body["expected_revision"]:
                raise ValueError("reconciliation incumbent changed")
            if row[0]["pool"] == "live":
                self._parent_ids(row[0]["parent_selection_id"])
            # Explicit operator/consumer request only. Replay of acquisition is
            # identity-idempotent; this path NEVER replays publication.
            return self._prepare(caller, row[0], row[2], resume=True)

    def activate(self, caller, body):
        fields = {"schema_version", "pool", "candidate_id", "expected_scope_id", "expected_revision", "protected_market_ids"}
        if (set(body) != fields or body["schema_version"] != "marketcow.hot-scope-activate.v1"
                or body["pool"] not in self.clients or type(body["expected_revision"]) is not int
                or body["expected_revision"] <= 0):
            raise ValueError("invalid hot activation request")
        with SupervisorOwner(Path(self.config["owner_lock"])):
            with self._db() as db:
                row = next((row for row in self._rows(db, body["pool"]) if row[0]["candidate_id"] == body["candidate_id"]), None)
            if row is None or row[1] != caller or row[2] <= self.control.wall_ms() or row[3] != "prepared":
                raise ValueError("candidate not prepared for caller or expired")
            candidate = row[0]
            if candidate["pool"] == "live":
                self._parent_ids(candidate["parent_selection_id"])
            protected = body["protected_market_ids"]
            if (not isinstance(protected, list) or protected != sorted(set(protected))
                    or not set(candidate["protected_market_ids"]) <= set(protected)
                    or not set(protected) <= set(candidate["requested_market_ids"])):
                raise ValueError("protected omission")
            result = self.clients[body["pool"]].request(dict(operation="publish_scope",
                expected_scope_id=body["expected_scope_id"], expected_revision=body["expected_revision"], config=candidate["config"]))
            if result.get("publication_applied") is not True or not self._matches(candidate, result["actual"]):
                raise RuntimeError("actual publication receipt differs")
            with self._db() as db:
                db.execute("UPDATE hot_candidates SET stage='active' WHERE id=?", (candidate["candidate_id"],))
            return {"schema_version": "marketcow.hot-scope-activated.v1", "candidate_id": candidate["candidate_id"],
                "selection_id": candidate["selection_sha256"], "actual": result["actual"], "new_full_sync_required": True}

    def retire(self, caller, body):
        if (set(body) != {"schema_version", "pool", "candidate_id"}
                or body["schema_version"] != "marketcow.hot-scope-retire.v1" or body["pool"] not in self.clients):
            raise ValueError("invalid hot retirement request")
        pool = body["pool"]
        with SupervisorOwner(Path(self.config["owner_lock"])):
            actual = self.clients[pool].status()
            with self._db() as db:
                rows = self._rows(db, pool)
            row = next((row for row in rows if row[0]["candidate_id"] == body["candidate_id"]), None)
            if row is None or row[1] != caller or self._matches(row[0], actual):
                raise ValueError("unknown, foreign or active candidate cannot retire")
            if row[3] == "retiring":
                receipt = row[4]
                ticket = receipt["retirement_ticket"]
                inventory = actual.get("admitted_market_ids")
                absent = isinstance(inventory, list) and not set(receipt["removed_market_ids"]).intersection(inventory)
                persisted = type(actual.get("retirement_persisted")) is int and actual["retirement_persisted"] >= ticket
                restarted = (actual.get("stream_instance_id", actual.get("projection_id")) != receipt.get("instance")
                             and actual.get("retirement_submitted") == 0 and absent)
                done = ((persisted or restarted) and (absent or inventory is None)
                        and actual.get("acquisition", {}).get("retiring_shards") == 0)
                if done:
                    with self._db() as db:
                        db.execute("DELETE FROM hot_candidates WHERE id=?", (body["candidate_id"],))
                elif (actual.get("retirement_submitted", 0) < ticket and isinstance(inventory, list)
                      and set(receipt["removed_market_ids"]) <= set(inventory)):
                    # An explicit retry can finish a persisted but unsubmitted
                    # retirement. Rust rechecks current/grace references.
                    self.clients[pool].request(dict(operation="retire_acquisition", market_ids=receipt["removed_market_ids"],
                        expected_scope_id=actual["scope_id" if pool == "live" else "projection_id"], expected_revision=actual["revision"]))
                return {"schema_version": "marketcow.hot-scope-retirement.v1", "candidate_id": body["candidate_id"],
                        "resources_released": done, "actual": actual}
            preserved = set()
            for candidate, _, _, stage, _ in rows:
                if candidate["candidate_id"] != body["candidate_id"] and (self._matches(candidate, actual) or stage in ("preparing", "prepared")):
                    preserved.update(record["identity"]["market_id"] for record in candidate["records"])
            removed = sorted({record["identity"]["market_id"] for record in row[0]["records"]}-preserved)
            # Collection may already have removed an obsolete candidate's
            # exclusive metadata. Do not resend unknown identities to Rust.
            if isinstance(actual.get("admitted_market_ids"), list):
                removed = sorted(set(removed).intersection(actual["admitted_market_ids"]))
            if not removed:
                with self._db() as db:
                    db.execute("DELETE FROM hot_candidates WHERE id=?", (body["candidate_id"],))
                return {"schema_version": "marketcow.hot-scope-retirement.v1", "candidate_id": body["candidate_id"],
                        "resources_released": True, "shared_resources_retained": True}
            receipt = {"retirement_ticket": actual["retirement_submitted"]+1, "removed_market_ids": removed,
                       "instance": actual.get("stream_instance_id", actual.get("projection_id"))}
            with self._db() as db:
                db.execute("UPDATE hot_candidates SET stage='retiring',receipt=? WHERE id=?", (canonical_json(receipt), body["candidate_id"]))
            result = self.clients[pool].request(dict(operation="retire_acquisition", market_ids=removed,
                expected_scope_id=actual["scope_id" if pool == "live" else "projection_id"], expected_revision=actual["revision"]))
            if result.get("retirement_queued") is not True:
                raise RuntimeError("retirement not queued")
            after = self.clients[pool].status()
            return {"schema_version": "marketcow.hot-scope-retirement.v1", "candidate_id": body["candidate_id"],
                    "resources_released": False, "retirement": receipt, "actual": after}

    def collect(self, caller, body):
        """Explicit bounded GC also covers the pre-controller legacy membership."""
        if (set(body) != {"schema_version", "pool", "expected_scope_id", "expected_revision"}
                or body["schema_version"] != "marketcow.hot-scope-collect.v1"
                or body["pool"] not in self.clients or type(body["expected_revision"]) is not int):
            raise ValueError("invalid scope collection request")
        pool = body["pool"]
        with SupervisorOwner(Path(self.config["owner_lock"])):
            actual = self.clients[pool].status()
            key = "scope_id" if pool == "live" else "projection_id"
            if actual[key] != body["expected_scope_id"] or actual["revision"] != body["expected_revision"]:
                raise ValueError("collection incumbent changed")
            admitted, referenced = actual.get("admitted_market_ids"), actual.get("referenced_market_ids")
            if not isinstance(admitted, list) or not isinstance(referenced, list) or not actual.get("source_readable"):
                raise RuntimeError("runtime reference inventory unavailable")
            with self._db() as db:
                pending = db.execute("SELECT receipt,sha FROM hot_cleanup WHERE pool=?", (pool,)).fetchone()
                if pending:
                    if len(pending[0]) > self.config["maximum_artifact_bytes"] or hashlib.sha256(pending[0]).hexdigest() != pending[1]:
                        raise ValueError("cleanup receipt corrupt")
                    receipt = json.loads(pending[0])
                    absent = not set(receipt["removed_market_ids"]).intersection(admitted)
                    persisted = (type(actual.get("retirement_persisted")) is int
                                 and actual["retirement_persisted"] >= receipt["ticket"])
                    restarted = (actual.get("stream_instance_id", actual.get("projection_id")) != receipt["instance"]
                                 and actual.get("retirement_submitted") == 0)
                    released = absent and (persisted or restarted) and actual.get("acquisition", {}).get("retiring_shards") == 0
                    if released:
                        db.execute("DELETE FROM hot_cleanup WHERE pool=?", (pool,))
                    elif actual.get("retirement_submitted", 0) < receipt["ticket"] and set(receipt["removed_market_ids"]) <= set(admitted):
                        # The caller explicitly retries collect. Never replay a
                        # publication; Rust revalidates current/grace references.
                        reserved = set(referenced)
                        for candidate, _, expires, stage, _ in self._rows(db, pool):
                            if stage in ("preparing", "prepared") and expires > self.control.wall_ms():
                                reserved.update(r["identity"]["market_id"] for r in candidate["records"])
                        if reserved.intersection(receipt["removed_market_ids"]):
                            raise RuntimeError("cleanup intent now referenced; explicit reconciliation required")
                        self.clients[pool].request(dict(operation="retire_acquisition", market_ids=receipt["removed_market_ids"],
                            expected_scope_id=actual[key], expected_revision=actual["revision"]))
                    return dict(schema_version="marketcow.hot-scope-collected.v1", resources_released=released,
                                retirement=receipt, actual=actual)
                keep = set(referenced)
                for candidate, _, expires, stage, _ in self._rows(db, pool):
                    if self._matches(candidate, actual) or (stage in ("preparing", "prepared") and expires > self.control.wall_ms()):
                        keep.update(record["identity"]["market_id"] for record in candidate["records"])
                removed = sorted(set(admitted)-keep)
                if not removed:
                    return dict(schema_version="marketcow.hot-scope-collected.v1", resources_released=True, removed_market_ids=[], actual=actual)
                if len(removed) > 4096:
                    removed = removed[:4096]  # Explicit one bounded GC batch per call.
                receipt = dict(caller=caller, removed_market_ids=removed, ticket=actual["retirement_submitted"]+1,
                    instance=actual.get("stream_instance_id", actual.get("projection_id")))
                raw = canonical_json(receipt)
                db.execute("INSERT INTO hot_cleanup VALUES (?,?,?)", (pool, raw, hashlib.sha256(raw).hexdigest()))
            result = self.clients[pool].request(dict(operation="retire_acquisition", market_ids=removed,
                expected_scope_id=actual[key], expected_revision=actual["revision"]))
            if result.get("retirement_queued") is not True:
                raise RuntimeError("collection requires reconciliation")
            return dict(schema_version="marketcow.hot-scope-collected.v1", resources_released=False, retirement=receipt,
                        actual=self.clients[pool].status())
