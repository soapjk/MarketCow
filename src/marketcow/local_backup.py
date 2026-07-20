from __future__ import annotations

import fcntl
import hashlib
import hmac
import json
import os
import re
import shutil
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping


MANIFEST_VERSION = "storage-v2.backup-manifest.v1"
REQUIRED_COMPONENTS = frozenset({
    "postgresql", "clickhouse", "duckdb", "cold_archive", "spool", "cursor_key",
})
MAX_FILES = 10000
_SENSITIVE_KEY = re.compile(
    r"(?i)^(?:password|passwd|passphrase|secret|token|api[_-]?key|"
    r"access[_-]?key(?:[_-]?id)?|authorization|cookie|set-cookie|dsn)$"
)
_SENSITIVE_TEXT = re.compile(
    r"(?i)(?:postgres(?:ql)?|clickhouse|mysql|mariadb|mongodb(?:\+srv)?|redis)://"
    r"[^\s/]+@|(?:password|passwd|passphrase|secret|token|api[_-]?key|"
    r"access[_-]?key(?:[_-]?id)?|authorization|cookie|set-cookie|dsn)"
    r"[\"']?\s*[:=]\s*[\"']?\s*(?!null\b|none\b|\[redacted\]\b)[^\s,}\]]+"
)


def _assert_no_sensitive(value: Any, context: str = "backup") -> None:
    """Reject credential-bearing structured or textual data without key-name false positives."""
    if isinstance(value, Mapping):
        for raw_key, item in value.items():
            key = str(raw_key)
            if _SENSITIVE_KEY.fullmatch(key.strip()):
                if item not in (None, "", "[REDACTED]"):
                    raise ValueError(f"{context} contains sensitive text")
            if _SENSITIVE_TEXT.search(key):
                raise ValueError(f"{context} contains sensitive text")
            _assert_no_sensitive(item, context)
        return
    if isinstance(value, (list, tuple)):
        for item in value:
            _assert_no_sensitive(item, context)
        return
    if isinstance(value, str) and _SENSITIVE_TEXT.search(value):
        raise ValueError(f"{context} contains sensitive text")


def _assert_payload_safe(data: bytes, context: str) -> None:
    """Inspect JSON structurally and other UTF-8 payloads as bounded credential text."""
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        value = text
    _assert_no_sensitive(value, context)


def _hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _json(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), default=str).encode("utf-8")


def _fsync_dir(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _seal_cursor_key(plaintext: bytes, wrapping_key: bytes, context: bytes) -> bytes:
    if len(wrapping_key) < 32:
        raise ValueError("backup wrapping key must contain at least 32 bytes")
    nonce = hmac.new(wrapping_key, b"nonce:" + context + plaintext,
                     hashlib.sha256).digest()[:16]
    stream = bytearray()
    counter = 0
    while len(stream) < len(plaintext):
        stream.extend(hmac.new(
            wrapping_key, b"stream:" + nonce + counter.to_bytes(8, "big"), hashlib.sha256
        ).digest())
        counter += 1
    ciphertext = bytes(left ^ right for left, right in zip(plaintext, stream))
    tag = hmac.new(wrapping_key, b"tag:" + nonce + ciphertext, hashlib.sha256).digest()
    return b"MCBK1" + nonce + tag + ciphertext


def _verify_sealed(data: bytes, wrapping_key: bytes) -> bool:
    if len(data) < 53 or not data.startswith(b"MCBK1"):
        return False
    nonce, tag, ciphertext = data[5:21], data[21:53], data[53:]
    expected = hmac.new(wrapping_key, b"tag:" + nonce + ciphertext,
                        hashlib.sha256).digest()
    return hmac.compare_digest(tag, expected)


def _utc(value: str) -> str:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("backup watermark timestamps must include timezone")
    return parsed.astimezone(timezone.utc).isoformat()


@dataclass(frozen=True)
class BackupComponent:
    name: str
    kind: str
    version: str
    files: Mapping[str, bytes]
    watermark: Mapping[str, Any]
    canonical_rebuildable: bool = False

    @classmethod
    def json(cls, name: str, kind: str, version: str, value: Any,
             watermark: Mapping[str, Any], canonical_rebuildable: bool = False):
        return cls(name, kind, version, {"logical.json": _json(value)}, watermark,
                   canonical_rebuildable)

    @classmethod
    def tree(cls, name: str, kind: str, version: str, root: Path,
             allowed_root: Path, watermark: Mapping[str, Any],
             canonical_rebuildable: bool = False):
        root, allowed = Path(root).resolve(), Path(allowed_root).resolve()
        try:
            root.relative_to(allowed)
        except ValueError as error:
            raise ValueError("backup source escapes storage root") from error
        files: Dict[str, bytes] = {}
        for path in sorted(root.rglob("*")):
            if path.is_symlink():
                raise ValueError("backup source must not contain symlinks")
            if path.is_file():
                try:
                    path.resolve().relative_to(root)
                except ValueError as error:
                    raise ValueError("backup source file escapes component root") from error
                files[path.relative_to(root).as_posix()] = path.read_bytes()
                if len(files) > MAX_FILES:
                    raise ValueError("backup component file count exceeds limit")
        return cls(name, kind, version, files, watermark, canonical_rebuildable)

    @classmethod
    def postgresql(cls, database: Any, captured_at: str):
        from psycopg import sql
        payload: Dict[str, Any] = {}
        with database.pool.connection() as connection:
            tables = connection.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema=%s AND table_type='BASE TABLE' ORDER BY table_name",
                [database.schema],
            ).fetchall()
            for item in tables:
                table = item["table_name"] if isinstance(item, dict) else item[0]
                query = sql.SQL(
                    "SELECT row_to_json(item)::text AS payload FROM {}.{} item "
                    "ORDER BY row_to_json(item)::text"
                ).format(sql.Identifier(database.schema), sql.Identifier(table))
                rows = connection.execute(query).fetchall()
                payload[table] = [
                    json.loads(row["payload"] if isinstance(row, dict) else row[0])
                    for row in rows
                ]
        return cls.json("postgresql", "logical-json", "postgresql-schema-v1", payload,
                        {"captured_at": captured_at,
                         "table_count": len(payload)})

    @classmethod
    def clickhouse(cls, database: Any, captured_at: str):
        allowed = {"schema_migrations", "market_bar_raw", "market_bar_canonical"}
        tables = sorted(
            str(row[0]) for row in database.client.query("SHOW TABLES").result_rows
            if str(row[0]) in allowed
        )
        payload = {}
        for table in tables:
            result = database.client.query(f"SELECT * FROM {table}")
            payload[table] = {
                "columns": list(result.column_names),
                "rows": sorted(result.result_rows, key=lambda row: repr(tuple(row))),
            }
        return cls.json(
            "clickhouse", "logical-json", "clickhouse-schema-v1", payload,
            {"captured_at": captured_at, "table_count": len(payload)},
            canonical_rebuildable=True,
        )


class LocalStorageBackup:
    """Atomic local-only backup bundle generator and pre-restore verifier."""

    def __init__(self, backup_root: Path, storage_root: Path, wrapping_key: bytes,
                 profile: str = "development") -> None:
        if profile != "development":
            raise ValueError("Storage V2 backup is development-only")
        self.storage_root = Path(storage_root).resolve()
        self.backup_root = Path(backup_root).resolve()
        try:
            self.backup_root.relative_to(self.storage_root)
        except ValueError as error:
            raise ValueError("backup root must remain inside storage root") from error
        if len(wrapping_key) < 32:
            raise ValueError("backup wrapping key must contain at least 32 bytes")
        self.wrapping_key = bytes(wrapping_key)
        self.backup_root.mkdir(parents=True, exist_ok=True)
        self.staging = self.backup_root / ".staging"
        self.staging.mkdir(exist_ok=True)

    def create(self, components: Iterable[BackupComponent], snapshot_at: str,
               mode: str = "full", base_backup_id: str | None = None,
               fault_hook: Any = None) -> Dict[str, Any]:
        if mode not in {"full", "incremental"}:
            raise ValueError("backup mode must be full or incremental")
        if mode == "incremental" and not base_backup_id:
            raise ValueError("incremental backup requires base backup id")
        normalized = {component.name: component for component in components}
        if set(normalized) != REQUIRED_COMPONENTS:
            missing = sorted(REQUIRED_COMPONENTS - set(normalized))
            extra = sorted(set(normalized) - REQUIRED_COMPONENTS)
            raise ValueError(f"backup component set mismatch missing={missing} extra={extra}")
        lock_path = self.backup_root / ".backup.lock"
        with lock_path.open("a+") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            try:
                return self._create_locked(normalized, snapshot_at, mode, base_backup_id,
                                           fault_hook)
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    def _create_locked(self, components: Mapping[str, BackupComponent], snapshot_at: str,
                       mode: str, base_backup_id: str | None,
                       fault_hook: Any) -> Dict[str, Any]:
        component_document = []
        payloads: Dict[str, bytes] = {}
        snapshot_at = _utc(snapshot_at)
        snapshot_bytes = snapshot_at.encode()
        captured = []
        for name in sorted(components):
            component = components[name]
            if not component.version or not component.kind or not component.files:
                raise ValueError(f"backup component {name} is incomplete")
            if "captured_at" not in component.watermark:
                raise ValueError(f"backup component {name} watermark is incomplete")
            captured_at = _utc(str(component.watermark["captured_at"]))
            if captured_at > snapshot_at:
                raise ValueError("component watermark is after backup snapshot")
            captured.append(captured_at)
            entries = []
            for relative, original in sorted(component.files.items()):
                path = Path(relative)
                if path.is_absolute() or ".." in path.parts or path.name in {"", "."}:
                    raise ValueError("backup component relative path is unsafe")
                data = bytes(original)
                if name == "cursor_key":
                    data = _seal_cursor_key(data, self.wrapping_key,
                                            snapshot_bytes + relative.encode())
                    target = f"components/{name}/{relative}.sealed"
                    mode_bits = "0600"
                else:
                    _assert_payload_safe(data, f"backup component {name}")
                    target = f"components/{name}/{relative}"
                    mode_bits = "0640"
                payloads[target] = data
                entries.append({"path": target, "sha256": _hash(data),
                                "bytes": len(data), "mode": mode_bits})
            component_document.append({
                "name": name, "kind": component.kind, "version": component.version,
                "watermark": {**dict(component.watermark), "captured_at": captured_at},
                "canonical_rebuildable": component.canonical_rebuildable,
                "files": entries,
            })
        manifest = {
            "manifest_version": MANIFEST_VERSION, "mode": mode,
            "base_backup_id": base_backup_id, "snapshot_at": snapshot_at,
            "rpo_assumption": "RPO: component watermarks captured at explicit local snapshot",
            "rto_assumption": "RTO: local restore drill target <= 60 minutes",
            "canonical_boundary": (
                "canonical may be rebuilt only from verified raw+spool through recorded watermark"
            ),
            "cross_component_watermark": {
                "earliest_captured_at": min(captured),
                "latest_captured_at": max(captured),
                "snapshot_at": snapshot_at,
            },
            "components": component_document,
        }
        _assert_no_sensitive(manifest, "backup manifest")
        manifest["backup_id"] = _hash(_json(manifest))[:24]
        manifest["manifest_payload_sha256"] = _hash(_json(manifest))
        _assert_no_sensitive(manifest, "backup manifest")
        backup_id = manifest["backup_id"]
        final = self.backup_root / backup_id
        if final.exists():
            return self.verify(final)
        stage = self.staging / f"{backup_id}-{uuid.uuid4().hex}"
        stage.mkdir()
        try:
            for relative, data in payloads.items():
                target = stage / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                with target.open("wb") as handle:
                    handle.write(data)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.chmod(target, 0o600 if relative.startswith("components/cursor_key/")
                         else 0o640)
            manifest_path = stage / "manifest.json"
            with manifest_path.open("wb") as handle:
                handle.write(json.dumps(manifest, sort_keys=True, indent=2).encode())
                handle.flush()
                os.fsync(handle.fileno())
            for directory in sorted(
                {path.parent for path in stage.rglob("*") if path.is_file()},
                key=lambda item: len(item.parts), reverse=True,
            ):
                _fsync_dir(directory)
            _fsync_dir(stage)
            _fsync_dir(self.staging)
            if fault_hook:
                fault_hook("before_publish")
            os.replace(stage, final)
            _fsync_dir(self.staging)
            _fsync_dir(self.backup_root)
            if fault_hook:
                fault_hook("after_publish")
            return self.verify(final)
        finally:
            if stage.exists():
                shutil.rmtree(stage, ignore_errors=True)

    def verify(self, artifact: Path) -> Dict[str, Any]:
        supplied = Path(artifact)
        if supplied.is_symlink():
            raise ValueError("backup artifact must not be a symlink")
        artifact = supplied.resolve()
        try:
            artifact.relative_to(self.backup_root)
        except ValueError as error:
            raise ValueError("backup artifact escapes backup root") from error
        manifest_path = artifact / "manifest.json"
        if manifest_path.is_symlink():
            raise ValueError("backup manifest must not be a symlink")
        try:
            manifest = json.loads(manifest_path.read_text())
        except (OSError, json.JSONDecodeError) as error:
            raise ValueError("backup manifest missing or corrupt") from error
        if manifest.get("manifest_version") != MANIFEST_VERSION:
            raise ValueError("unsupported backup manifest version")
        signature = manifest.pop("manifest_payload_sha256", None)
        if _hash(_json(manifest)) != signature:
            raise ValueError("backup manifest checksum mismatch")
        _assert_no_sensitive(manifest, "backup manifest")
        backup_id = manifest.get("backup_id")
        identity_document = dict(manifest)
        identity_document.pop("backup_id", None)
        components = manifest.get("components", [])
        names = {item.get("name") for item in components}
        if names != REQUIRED_COMPONENTS or len(components) != len(REQUIRED_COMPONENTS):
            raise ValueError("backup component set is incomplete")
        captured = []
        expected = {"manifest.json"}
        entry_count = 0
        for component in components:
            if not component.get("version") or "watermark" not in component:
                raise ValueError("backup component metadata incomplete")
            captured.append(_utc(component["watermark"]["captured_at"]))
            for item in component.get("files", []):
                entry_count += 1
                relative = item["path"]
                expected.add(relative)
                path = artifact / relative
                if path.is_symlink() or not path.is_file():
                    raise ValueError("backup file missing or symlinked")
                try:
                    path.resolve().relative_to(artifact)
                except ValueError as error:
                    raise ValueError("backup file escapes artifact") from error
                data = path.read_bytes()
                if len(data) != item["bytes"] or _hash(data) != item["sha256"]:
                    raise ValueError("backup file checksum mismatch")
                actual_mode = path.stat().st_mode & 0o777
                if actual_mode != int(item["mode"], 8):
                    raise ValueError("backup file permission mismatch")
                if component["name"] == "cursor_key":
                    if not _verify_sealed(data, self.wrapping_key):
                        raise ValueError("cursor key sealed payload authentication failed")
                else:
                    _assert_payload_safe(data, "backup")
        if backup_id != _hash(_json(identity_document))[:24] or backup_id != artifact.name:
            raise ValueError("backup id and directory mismatch")
        expected_watermark = {
            "earliest_captured_at": min(captured),
            "latest_captured_at": max(captured),
            "snapshot_at": _utc(manifest["snapshot_at"]),
        }
        if manifest.get("cross_component_watermark") != expected_watermark:
            raise ValueError("backup cross-component watermark mismatch")
        actual = {path.relative_to(artifact).as_posix() for path in artifact.rglob("*")
                  if path.is_file()}
        if actual != expected or entry_count != len(expected) - 1:
            raise ValueError("backup file inventory mismatch")
        return {**manifest, "manifest_payload_sha256": signature,
                "status": "verified", "artifact_path": str(artifact)}
