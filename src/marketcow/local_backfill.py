from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Mapping, Sequence

from psycopg import sql
from psycopg.types.json import Jsonb

from .clickhouse_shadow import _market
from .clickhouse_writer import normalize_bar
from .local_backup import _assert_no_sensitive, _fsync_dir, _hash, _json


BACKFILL_VERSION = "storage-v2.backfill.v1"
MAX_BATCH_SIZE = 5000
MAX_DOMAINS = 32
MAX_ERRORS = 50
MAX_RECONCILE_ROWS = 100000


@dataclass(frozen=True)
class Domain:
    table: str
    key: tuple[str, ...]
    json_columns: tuple[str, ...] = ()


POSTGRES_DOMAINS = (
    Domain("ingestion_runs", ("run_id",)),
    Domain("provider_health", ("provider",)),
    Domain("raw_artifact_manifest", ("artifact_id",), ("metadata_json",)),
    Domain("economic_calendar_event", ("event_id",), ("payload_json",)),
    Domain("economic_indicator_latest", ("indicator_id",), ("payload_json",)),
    Domain("earnings_calendar_event", ("event_id",), ("payload_json",)),
    Domain("tushare_request", ("request_id",), ("params_json", "response_fields_json")),
    Domain("tushare_data_row", ("request_id", "row_index"), ("payload_json",)),
    Domain("fundamental_snapshot", ("symbol", "report_period")),
    Domain("fundamental_snapshot_history", ("version_id",)),
    Domain("financial_statement_rows", ("symbol", "statement", "report_date", "source"),
           ("payload_json",)),
    Domain("baostock_snapshot", ("symbol", "report_period"), ("payload_json",)),
    Domain("tdx_financial_snapshot", ("symbol", "report_period")),
    Domain("tdx_financial_snapshot_history", ("version_id",)),
    Domain("validation_result", ("symbol", "report_period", "metric", "source_a", "source_b")),
    Domain("funnel_metrics", ("symbol",)),
)


@dataclass(frozen=True)
class BackfillTargets:
    root: Path
    postgres: Any
    clickhouse: Any
    writer: Any
    canonical_builder: Any
    profile: str = "development"
    allowed_root: Path | None = None
    contract_gate: Callable[[], Mapping[str, Any]] | None = None


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(dir=path.parent, prefix=".backfill-")
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(json.dumps(value, sort_keys=True, indent=2).encode())
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_dir(path.parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _iso(value: Any) -> str:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat(timespec="milliseconds")


def _bounded_error(error: Exception) -> str:
    text = str(error).replace("\n", " ")[:1000]
    if "://" in text or text.startswith("/"):
        return "[REDACTED]"
    return text


class LocalStorageBackfill:
    """Development-only, checkpointed DuckDB to Storage V2 migration drill."""

    def __init__(
        self, source: Any, targets: BackfillTargets, batch_size: int = 1000,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if targets.profile not in {"development", "test"}:
            raise ValueError("backfill is development/test-only")
        if not 1 <= batch_size <= MAX_BATCH_SIZE:
            raise ValueError("backfill batch_size must be between 1 and 5000")
        if targets.allowed_root is None:
            raise ValueError("backfill requires an explicit allowed root")
        supplied = Path(targets.root)
        if supplied.is_symlink():
            raise ValueError("backfill root must not be a symlink")
        self.root = supplied.resolve()
        allowed = Path(targets.allowed_root).resolve()
        try:
            self.root.relative_to(allowed)
        except ValueError as error:
            raise ValueError("backfill root escapes allowed root") from error
        names = (self.root.name, str(targets.postgres.schema), str(targets.clickhouse.database))
        if any("production" in value.lower() for value in names):
            raise ValueError("backfill rejects production targets")
        if not all(value.endswith(("development", "test")) for value in names):
            raise ValueError("backfill targets must be development/test isolated")
        source_path = Path(source.path)
        if "production" in str(source_path).lower():
            raise ValueError("backfill rejects production source paths")
        self.source = source
        self.targets = targets
        self.batch_size = batch_size
        self.clock = clock
        self.state = self.root / ".storage-v2-backfill"
        self.checkpoint_path = self.state / "checkpoint.json"
        self.snapshot_path = self.state / "source-snapshot.duckdb"

    @staticmethod
    def _sign(value: Dict[str, Any]) -> None:
        value.pop("checksum", None)
        value["checksum"] = _hash(_json(value))

    @classmethod
    def _validate_checkpoint(cls, value: Mapping[str, Any]) -> None:
        unsigned = dict(value)
        checksum = unsigned.pop("checksum", None)
        if checksum != _hash(_json(unsigned)):
            raise ValueError("backfill checkpoint checksum mismatch")
        if value.get("version") != BACKFILL_VERSION:
            raise ValueError("backfill checkpoint version mismatch")

    def _source_fingerprint(self, path: Path | None = None) -> str:
        database = path or Path(self.source.path)
        digest = hashlib.sha256()
        with open(database, "rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _logical_fingerprint(self, path: Path | None = None) -> str:
        """Fingerprint bounded logical rows, not mutable DuckDB file bytes."""
        import duckdb

        database = Path(path or self.source.path)
        payload = []
        with duckdb.connect(str(database), read_only=True) as connection:
            for domain in (*POSTGRES_DOMAINS, Domain(
                "market_price_bar",
                ("symbol", "interval", "adjustment", "timestamp", "source"),
            )):
                columns = self._columns(connection, domain.table)
                count = connection.execute(
                    f'SELECT count(*) FROM "{domain.table}"'
                ).fetchone()[0]
                if count > MAX_RECONCILE_ROWS:
                    raise ValueError("logical fingerprint exceeds bounded row limit")
                order = ", ".join(f'"{key}"' for key in domain.key)
                rows = connection.execute(
                    f'SELECT {", ".join(chr(34) + name + chr(34) for name in columns)} '
                    f'FROM "{domain.table}" ORDER BY {order}'
                ).fetchall()
                payload.append((domain.table, count, self._rows_checksum(
                    columns, rows, domain.json_columns
                )))
        return _hash(_json(payload))

    def _preflight(self) -> None:
        source_path = Path(self.source.path).resolve()
        if source_path.is_symlink() or not source_path.is_file():
            raise ValueError("backfill source must be a regular DuckDB file")
        self.targets.postgres.migrate()
        self.targets.clickhouse.migrate()
        with self.targets.postgres.connection() as connection:
            versions = {row["version"] if isinstance(row, Mapping) else row[0]
                        for row in connection.execute(
                            "SELECT version FROM schema_migrations"
                        ).fetchall()}
        if versions != {1, 2, 3, 4}:
            raise ValueError("PostgreSQL migrations are incomplete or unknown")
        client = self.targets.clickhouse._require_client()
        versions = {row[0] for row in client.query(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).result_rows}
        if versions != {1, 2, 3, 4}:
            raise ValueError("ClickHouse migrations are incomplete or unknown")
        if not self.checkpoint_path.exists():
            with self.targets.postgres.connection() as connection:
                occupied = sum(int(connection.execute(
                    sql.SQL("SELECT count(*) AS count FROM {}").format(
                        sql.Identifier(domain.table)
                    )
                ).fetchone()["count"]) for domain in POSTGRES_DOMAINS)
            click_occupied = client.query(
                "SELECT (SELECT count() FROM market_bar_raw FINAL) + "
                "(SELECT count() FROM market_bar_canonical FINAL)"
            ).result_rows[0][0]
            if occupied or click_occupied:
                raise ValueError("backfill targets must be empty for a new run")
        usage = shutil.disk_usage(self.root.parent)
        if usage.free < source_path.stat().st_size * 2 + 16 * 1024 * 1024:
            raise ValueError("insufficient local capacity for backfill snapshot")

    def _load_or_initialize(self) -> Dict[str, Any]:
        if self.checkpoint_path.exists():
            checkpoint = json.loads(self.checkpoint_path.read_text())
            self._validate_checkpoint(checkpoint)
            if checkpoint["source_path_hash"] != _hash(str(Path(self.source.path).resolve()).encode()):
                raise ValueError("backfill checkpoint source binding mismatch")
            if checkpoint["targets"] != self._target_ids():
                raise ValueError("backfill checkpoint target binding mismatch")
            return checkpoint
        self.state.mkdir(parents=True, exist_ok=True)
        with self.source.connect() as connection:
            connection.execute("CHECKPOINT")
        shutil.copy2(self.source.path, self.snapshot_path)
        with self.source.connect() as connection:
            watermark = connection.execute(
                "SELECT COALESCE(MAX(ingested_at), '') FROM market_price_bar"
            ).fetchone()[0]
        snapshot_fingerprint = self._source_fingerprint(self.snapshot_path)
        binding = {
            "version": BACKFILL_VERSION,
            "source_fingerprint": snapshot_fingerprint,
            "targets": self._target_ids(),
        }
        checkpoint: Dict[str, Any] = {
            **binding,
            "run_id": _hash(_json(binding)),
            "source_path_hash": _hash(str(Path(self.source.path).resolve()).encode()),
            "snapshot_watermark": watermark or None,
            "phase": "backfill",
            "domains": {},
            "catchup_passes": 0,
            "last_live_fingerprint": None, "completion_fingerprint": None,
            "errors": [],
        }
        self._save(checkpoint)
        return checkpoint

    def _target_ids(self) -> Dict[str, str]:
        return {
            "postgres_schema": str(self.targets.postgres.schema),
            "clickhouse_database": str(self.targets.clickhouse.database),
        }

    def _save(self, checkpoint: Dict[str, Any]) -> None:
        self._sign(checkpoint)
        _atomic_json(self.checkpoint_path, checkpoint)

    @staticmethod
    def _columns(connection: Any, table: str) -> list[str]:
        return [row[1] for row in connection.execute(
            f"PRAGMA table_info('{table}')"
        ).fetchall()]

    def _scan(self, path: Path, domain: Domain, after: Sequence[Any] | None):
        import duckdb

        with duckdb.connect(str(path), read_only=True) as connection:
            columns = self._columns(connection, domain.table)
            projection = ", ".join(f'"{name}"' for name in columns)
            order = ", ".join(f'"{name}"' for name in domain.key)
            parameters: list[Any] = []
            where = ""
            if after is not None:
                marks = ", ".join("?" for _ in domain.key)
                where = f" WHERE ({order}) > ({marks})"
                parameters.extend(after)
            parameters.append(self.batch_size)
            rows = connection.execute(
                f'SELECT {projection} FROM "{domain.table}"{where} '
                f"ORDER BY {order} LIMIT ?", parameters,
            ).fetchall()
            return columns, rows

    def _upsert_postgres(self, domain: Domain, columns: list[str], rows: list[Any]) -> None:
        if not rows:
            return
        statement = sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}").format(
            sql.Identifier(domain.table),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
            sql.SQL(", ").join(map(sql.Identifier, domain.key)),
            sql.SQL(", ").join(
                sql.SQL("{}=EXCLUDED.{}").format(sql.Identifier(name), sql.Identifier(name))
                for name in columns if name not in domain.key
            ),
        )
        with self.targets.postgres.connection() as connection:
            for row in rows:
                values = []
                for name, value in zip(columns, row):
                    if name in domain.json_columns:
                        if isinstance(value, str):
                            value = json.loads(value or "{}")
                        value = Jsonb(value or {})
                    values.append(value)
                connection.execute(statement, values)

    @staticmethod
    def _raw_row(columns: list[str], row: Sequence[Any]) -> Dict[str, Any]:
        item = dict(zip(columns, row))
        bar_time = item.get("bar_at") or datetime.fromtimestamp(
            int(item["timestamp"]), timezone.utc
        ).isoformat()
        provenance = {"market": None}
        return {
            "symbol": item["symbol"], "market": _market(item["symbol"], provenance),
            "interval": item["interval"], "adjustment": item["adjustment"],
            "bar_time": bar_time, "open": item["open"], "high": item["high"],
            "low": item["low"], "close": item["close"],
            "raw_close": item.get("raw_close"),
            "adjustment_factor": item.get("adjustment_factor"),
            "volume": item["volume"], "amount": item.get("amount"),
            "source": item["source"],
            "source_sequence": item.get("source_sequence") or str(item["timestamp"]),
            "observed_at": item.get("observed_at") or item["ingested_at"],
            "ingested_at": item["ingested_at"],
            "raw_artifact_id": item.get("raw_artifact_id"),
        }

    def _copy_domain(self, checkpoint: Dict[str, Any], path: Path, domain: Domain,
                     phase: str, fault_hook: Any) -> int:
        name = f"postgres:{domain.table}:{phase}"
        state = checkpoint["domains"].setdefault(name, {"after": None, "rows": 0, "done": False})
        while not state["done"]:
            if fault_hook:
                fault_hook("before_batch", name)
            columns, rows = self._scan(path, domain, state["after"])
            self._upsert_postgres(domain, columns, rows)
            if fault_hook:
                fault_hook("after_write", name)
            if rows:
                positions = [columns.index(key) for key in domain.key]
                state["after"] = [rows[-1][position] for position in positions]
                state["rows"] += len(rows)
            state["done"] = len(rows) < self.batch_size
            self._save(checkpoint)
            if fault_hook:
                fault_hook("after_checkpoint", name)
        return state["rows"]

    def _copy_market(self, checkpoint: Dict[str, Any], path: Path, phase: str,
                     fault_hook: Any) -> int:
        domain = Domain("market_price_bar", ("symbol", "interval", "adjustment", "timestamp", "source"))
        name = f"clickhouse:raw:{phase}"
        state = checkpoint["domains"].setdefault(
            name, {"after": None, "rows": 0, "done": False, "ranges": {},
                   "canonical_done": False}
        )
        while not state["done"]:
            columns, rows = self._scan(path, domain, state["after"])
            normalized = [self._raw_row(columns, row) for row in rows]
            if fault_hook:
                fault_hook("before_batch", name)
            outcome = self.targets.writer.write("raw", normalized)
            if outcome["spooled"]:
                raise RuntimeError("ClickHouse raw batch was spooled; retry after recovery")
            for item in normalized:
                key = "\x1f".join((item["symbol"], item["interval"], item["adjustment"]))
                boundary = state["ranges"].setdefault(
                    key, {"start": item["bar_time"], "end": item["bar_time"]}
                )
                boundary["start"] = min(boundary["start"], item["bar_time"])
                boundary["end"] = max(boundary["end"], item["bar_time"])
            if fault_hook:
                fault_hook("after_write", name)
            if rows:
                positions = [columns.index(key) for key in domain.key]
                state["after"] = [rows[-1][position] for position in positions]
                state["rows"] += len(rows)
            state["done"] = len(rows) < self.batch_size
            self._save(checkpoint)
            if fault_hook:
                fault_hook("after_checkpoint", name)
        if state["canonical_done"]:
            return state["rows"]
        for key, boundary in sorted(state["ranges"].items()):
            symbol, interval, adjustment = key.split("\x1f")
            result = self.targets.canonical_builder.rebuild(
                symbol, interval, adjustment, boundary["start"], boundary["end"], 50000
            )
            if result["status"] != "ok":
                raise RuntimeError("canonical rebuild did not complete")
        state["canonical_done"] = True
        self._save(checkpoint)
        return state["rows"]

    def _active_raw_rows(self) -> list[Dict[str, Any]]:
        import duckdb

        with duckdb.connect(str(self.source.path), read_only=True) as source:
            count = source.execute("SELECT count(*) FROM market_price_bar").fetchone()[0]
            if count > MAX_RECONCILE_ROWS:
                raise ValueError("market canonicalization exceeds bounded row limit")
            columns = self._columns(source, "market_price_bar")
            rows = source.execute(
                "SELECT * FROM market_price_bar ORDER BY symbol, interval, adjustment, "
                "timestamp, source"
            ).fetchall()
        return [normalize_bar("raw", self._raw_row(columns, row)) for row in rows]

    def _finalize_canonical(self, fault_hook: Any = None) -> list[Dict[str, Any]]:
        """Create one deterministic canonical generation from the final raw set."""
        raw = self._active_raw_rows()
        expected, _, _ = self.targets.canonical_builder.build_rows(raw, [])
        if fault_hook:
            fault_hook("before_write", "clickhouse:canonical:final")
        self.targets.clickhouse._require_client().command(
            "TRUNCATE TABLE market_bar_canonical"
        )
        outcome = self.targets.writer.write("canonical", expected)
        if outcome["spooled"] or outcome["written"] != len(expected):
            raise RuntimeError("final canonical generation did not complete")
        if fault_hook:
            fault_hook("after_write", "clickhouse:canonical:final")
        return expected

    def run(self, fault_hook: Any = None, max_catchup_passes: int = 10) -> Dict[str, Any]:
        started = self.clock()
        if not 1 <= max_catchup_passes <= 100:
            raise ValueError("max_catchup_passes must be between 1 and 100")
        self._preflight()
        self.state.mkdir(parents=True, exist_ok=True)
        with (self.state / "backfill.lock").open("a+") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            try:
                checkpoint = self._load_or_initialize()
                if checkpoint["phase"] == "backfill":
                    for domain in POSTGRES_DOMAINS:
                        self._copy_domain(checkpoint, self.snapshot_path, domain, "snapshot", fault_hook)
                    self._copy_market(checkpoint, self.snapshot_path, "snapshot", fault_hook)
                    checkpoint["phase"] = "catchup"
                    self._save(checkpoint)
                stable = checkpoint.get("phase") == "complete"
                reconciliation = None
                for _ in range(max_catchup_passes):
                    fingerprint_before = self._logical_fingerprint()
                    pass_id = checkpoint["catchup_passes"] + 1
                    for domain in POSTGRES_DOMAINS:
                        self._copy_domain(checkpoint, Path(self.source.path), domain,
                                          f"catchup-{pass_id}", fault_hook)
                    self._copy_market(checkpoint, Path(self.source.path), f"catchup-{pass_id}", fault_hook)
                    fingerprint_after = self._logical_fingerprint()
                    checkpoint["catchup_passes"] = pass_id
                    checkpoint["last_live_fingerprint"] = fingerprint_after
                    stable = fingerprint_before == fingerprint_after
                    checkpoint["phase"] = "canonicalize" if stable else "catchup"
                    self._save(checkpoint)
                    if not stable:
                        continue
                    expected_canonical = self._finalize_canonical(fault_hook)
                    checkpoint["phase"] = "reconcile"
                    self._save(checkpoint)
                    before_reconcile = self._logical_fingerprint()
                    if before_reconcile != checkpoint["last_live_fingerprint"]:
                        checkpoint["phase"] = "catchup"
                        self._save(checkpoint)
                        stable = False
                        continue
                    reconciliation = self.reconcile(expected_canonical)
                    after_reconcile = self._logical_fingerprint()
                    if after_reconcile != before_reconcile:
                        checkpoint["phase"] = "catchup"
                        self._save(checkpoint)
                        stable = False
                        continue
                    checkpoint["completion_fingerprint"] = after_reconcile
                    break
                if not stable or reconciliation is None:
                    raise RuntimeError("catch-up did not reach a stable source watermark")
                if reconciliation["status"] != "ok":
                    raise RuntimeError(
                        "backfill reconciliation failed: " +
                        json.dumps(reconciliation["mismatches"][:MAX_ERRORS], sort_keys=True)
                    )
                checkpoint["phase"] = "complete"
                self._save(checkpoint)
                report = self._report(
                    checkpoint, reconciliation, max(0.0, self.clock() - started)
                )
                _atomic_json(self.state / "report.json", report)
                return report
            except Exception as error:
                if self.checkpoint_path.exists():
                    checkpoint = json.loads(self.checkpoint_path.read_text())
                    checkpoint["errors"] = (checkpoint.get("errors", []) + [
                        _bounded_error(error)
                    ])[-MAX_ERRORS:]
                    self._save(checkpoint)
                raise
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    @staticmethod
    def _canonical_value(value: Any) -> Any:
        if isinstance(value, datetime):
            return _iso(value)
        if isinstance(value, str) and "T" in value:
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                pass
            else:
                if parsed.tzinfo is not None:
                    return _iso(parsed)
        if isinstance(value, (bytes, bytearray)):
            return value.hex()
        if isinstance(value, float):
            return format(value, ".15g")
        if isinstance(value, Mapping):
            return {str(key): LocalStorageBackfill._canonical_value(item)
                    for key, item in sorted(value.items())}
        if isinstance(value, (list, tuple)):
            return [LocalStorageBackfill._canonical_value(item) for item in value]
        return value

    @classmethod
    def _rows_checksum(cls, columns: Sequence[str], rows: Iterable[Sequence[Any]],
                       json_columns: Sequence[str] = ()) -> str:
        json_names = set(json_columns)
        def normalize(name: str, value: Any) -> Any:
            if name in json_names and isinstance(value, str):
                try:
                    value = json.loads(value)
                except json.JSONDecodeError:
                    pass
            return cls._canonical_value(value)
        normalized = [dict(zip(columns, (cls._canonical_value(value) for value in row)))
                      for row in rows]
        normalized = [
            {name: normalize(name, value) for name, value in item.items()}
            for item in normalized
        ]
        return _hash(_json(normalized))

    def _canonical_matches(
        self, expected: Sequence[Mapping[str, Any]], actual: Sequence[Sequence[Any]],
    ) -> bool:
        columns = list(self.targets.writer.repository.CANONICAL_COLUMNS)
        expected_rows = [tuple(row.get(column) for column in columns) for row in expected]
        return (len(expected_rows) == len(actual) and
                self._rows_checksum(columns, expected_rows) ==
                self._rows_checksum(columns, actual))

    def reconcile(self, expected_canonical: Sequence[Mapping[str, Any]] | None = None) -> Dict[str, Any]:
        import duckdb

        mismatches = []
        domains = []
        with duckdb.connect(str(self.source.path), read_only=True) as source:
            for domain in POSTGRES_DOMAINS:
                columns = self._columns(source, domain.table)
                order = ", ".join(f'"{key}"' for key in domain.key)
                source_count = source.execute(
                    f'SELECT count(*) FROM "{domain.table}"'
                ).fetchone()[0]
                if source_count > MAX_RECONCILE_ROWS:
                    raise ValueError("reconciliation domain exceeds bounded row limit")
                source_rows = source.execute(
                    f'SELECT {", ".join(map(lambda x: chr(34)+x+chr(34), columns))} '
                    f'FROM "{domain.table}" ORDER BY {order}'
                ).fetchall()
                with self.targets.postgres.connection() as target:
                    target_rows = target.execute(sql.SQL("SELECT {} FROM {} ORDER BY {}").format(
                        sql.SQL(", ").join(map(sql.Identifier, columns)),
                        sql.Identifier(domain.table),
                        sql.SQL(", ").join(map(sql.Identifier, domain.key)),
                    )).fetchall()
                if target_rows and isinstance(target_rows[0], Mapping):
                    target_rows = [tuple(row[name] for name in columns) for row in target_rows]
                left = self._rows_checksum(columns, source_rows, domain.json_columns)
                right = self._rows_checksum(columns, target_rows, domain.json_columns)
                ok = source_count == len(target_rows) and left == right
                domains.append({"domain": domain.table, "rows": len(source_rows), "status": "ok" if ok else "mismatch"})
                if not ok and len(mismatches) < MAX_ERRORS:
                    mismatches.append({"domain": domain.table, "source_rows": len(source_rows),
                                       "target_rows": len(target_rows)})
            market_rows = source.execute("SELECT COUNT(*) FROM market_price_bar").fetchone()[0]
            if market_rows > MAX_RECONCILE_ROWS:
                raise ValueError("market reconciliation exceeds bounded row limit")
            raw_columns = self._columns(source, "market_price_bar")
            raw_source_rows = source.execute(
                "SELECT * FROM market_price_bar ORDER BY symbol, interval, adjustment, "
                "timestamp, source"
            ).fetchall()
        normalized_raw = [normalize_bar("raw", self._raw_row(raw_columns, row))
                          for row in raw_source_rows]
        raw_contract_columns = list(self.targets.writer.repository.RAW_COLUMNS)
        expected_raw = [tuple(row.get(column) for column in raw_contract_columns)
                        for row in normalized_raw]
        raw_result = self.targets.clickhouse._require_client().query(
            "SELECT " + ", ".join(raw_contract_columns) +
            " FROM market_bar_raw FINAL ORDER BY symbol, interval, adjustment, "
            "bar_time, source"
        )
        raw_actual = raw_result.result_rows
        click_rows = self.targets.clickhouse._require_client().query(
            "SELECT count() FROM market_bar_raw FINAL"
        ).result_rows[0][0]
        raw_checksum_ok = self._rows_checksum(raw_contract_columns, expected_raw) == \
            self._rows_checksum(raw_contract_columns, raw_actual)
        if market_rows != click_rows or not raw_checksum_ok:
            mismatches.append({"domain": "market_bar_raw", "source_rows": market_rows,
                               "target_rows": click_rows})
        domains.append({"domain": "market_bar_raw", "rows": market_rows,
                        "status": "ok" if market_rows == click_rows and raw_checksum_ok
                        else "mismatch"})
        expected_canonical = list(expected_canonical if expected_canonical is not None else
                                  self.targets.canonical_builder.build_rows(normalized_raw, [])[0])
        canonical_columns = list(self.targets.writer.repository.CANONICAL_COLUMNS)
        canonical_actual = self.targets.clickhouse._require_client().query(
            "SELECT " + ", ".join(canonical_columns) +
            " FROM market_bar_canonical FINAL ORDER BY symbol, interval, adjustment, bar_time"
        ).result_rows
        canonical_ok = self._canonical_matches(expected_canonical, canonical_actual)
        domains.append({"domain": "market_bar_canonical", "rows": len(expected_canonical),
                        "status": "ok" if canonical_ok else "mismatch"})
        if not canonical_ok:
            mismatches.append({"domain": "market_bar_canonical",
                               "source_rows": len(expected_canonical),
                               "target_rows": len(canonical_actual),
                               "reason": "business key/content/provenance/version checksum"})
        if self.targets.contract_gate is not None:
            gate = dict(self.targets.contract_gate())
            if gate.get("status") != "ok":
                mismatches.append({"domain": "query_contracts", "status": "mismatch"})
            domains.append({"domain": "query_contracts", "rows": int(gate.get("checks", 0)),
                            "status": gate.get("status", "mismatch")})
        return {"status": "ok" if not mismatches else "mismatch", "domains": domains,
                "mismatches": mismatches, "allowlist": []}

    def _report(self, checkpoint: Mapping[str, Any], reconciliation: Mapping[str, Any],
                duration_seconds: float = 0.0):
        report = {
            "version": BACKFILL_VERSION,
            "status": "complete",
            "run_id": checkpoint["run_id"],
            "source_snapshot": checkpoint["source_fingerprint"],
            "snapshot_watermark": checkpoint["snapshot_watermark"],
            "targets": checkpoint["targets"],
            "catchup_passes": checkpoint["catchup_passes"],
            "batches": sum(
                (int(item.get("rows", 0)) + self.batch_size - 1) // self.batch_size
                for item in checkpoint.get("domains", {}).values()
            ),
            "duration_seconds": round(duration_seconds, 6),
            "lag": (0 if checkpoint.get("completion_fingerprint") and
                     checkpoint.get("completion_fingerprint") ==
                     checkpoint.get("last_live_fingerprint") else 1),
            "domains": reconciliation["domains"],
            "mismatches": reconciliation["mismatches"],
            "recovery": "rerun the same bounded command; atomic checkpoints resume the current domain",
        }
        _assert_no_sensitive(report, "backfill report")
        return report
