import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import marketcow.offline_copy_validator as copy_validator

from marketcow.offline_copy_validator import (
    COPY_ACTION,
    COPY_MANIFEST_VERSION,
    CopyAuthorization,
    CopyValidationError,
    OfflineCopyValidator,
)
from marketcow.offline_duckdb_import import ImportLimits, OfflineDuckDBImporter
from marketcow.offline_full_import import FULL_IMPORT_VERSION, _digest
from marketcow.offline_incremental_catchup import CATCHUP_VERSION
from marketcow.storage import Warehouse


class OfflineCopyValidatorTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.root = Path(self.folder.name).resolve()
        self.source = self.root / "synthetic-test" / "legacy.duckdb"
        Warehouse(self.source)
        self.limits = ImportLimits(
            max_file_bytes=64 * 1024**2, max_rows=1000, batch_rows=10,
            timeout_seconds=20,
        )
        self.source_evidence = OfflineDuckDBImporter(
            allowed_root=self.root, source=self.source, source_label="test-fixture",
            limits=self.limits,
        ).inspect()
        self.full_path = self.root / "evidence" / "full.json"
        self.catchup_path = self.root / "evidence" / "catchup.json"
        self.manifest_path = self.root / "copy" / "manifest.json"
        self.report = self.root / "reports"
        self.full_path.parent.mkdir()
        full = {
            "version": FULL_IMPORT_VERSION, "phase": "complete", "run_id": "full-run",
            "targets": {"postgres_schema": "copy_test", "clickhouse_database": "copy_test"},
        }
        self._sign_write(self.full_path, full)
        catchup = {
            "version": CATCHUP_VERSION, "phase": "complete", "catchup_run_id": "catchup-run",
            "full_run_id": "full-run",
            "source_high_watermark": {"source_fingerprint": self.source_evidence["source_fingerprint"]},
        }
        self._sign_write(self.catchup_path, catchup)
        self.authorization = self._write_manifest()

    def tearDown(self):
        self.folder.cleanup()

    @staticmethod
    def _sign_write(path, document):
        document = dict(document)
        document.pop("checksum", None)
        document["checksum"] = _digest(document)
        path.write_text(json.dumps(document, sort_keys=True, separators=(",", ":")))

    def _file(self, role, path):
        payload = path.read_bytes()
        return {
            "role": role, "relative_path": str(path.relative_to(self.root)),
            "byte_size": len(payload), "sha256": hashlib.sha256(payload).hexdigest(),
        }

    def _write_manifest(self, **changes):
        self.manifest_path.parent.mkdir(exist_ok=True)
        document = {
            "version": COPY_MANIFEST_VERSION,
            "source_logical_id": "synthetic-old-main-copy",
            "source_label": "test-fixture",
            "copied_at": "2026-07-21T00:00:00Z",
            "copy_method": "local-synthetic-fixture",
            "copy_action": COPY_ACTION,
            "authorization_statement": "separately-authorized-exact-copy",
            "authorization_evidence_id": "test-authorization-1",
            "allowed_root_logical_id": "t9-isolated-test-root",
            "source_fingerprint": self.source_evidence["source_fingerprint"],
            "files": [
                self._file("duckdb", self.source),
                self._file("full_checkpoint", self.full_path),
                self._file("catchup_checkpoint", self.catchup_path),
            ],
        }
        document.update(changes)
        document["manifest_payload_sha256"] = _digest(document)
        encoded = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
        self.manifest_path.write_bytes(encoded)
        return CopyAuthorization(
            authorized=True,
            evidence_id="test-authorization-1",
            source_logical_id="synthetic-old-main-copy",
            copy_action=COPY_ACTION,
            manifest_sha256=hashlib.sha256(encoded).hexdigest(),
            source_path_sha256=hashlib.sha256(str(self.source.resolve()).encode()).hexdigest(),
            allowed_root_sha256=hashlib.sha256(str(self.root).encode()).hexdigest(),
        )

    def validate(self, **changes):
        arguments = {
            "authorization": self.authorization,
            "manifest_path": self.manifest_path,
            "allowed_root": self.root,
            "report_directory": self.report,
            "mode": "sample",
        }
        arguments.update(changes)
        return OfflineCopyValidator(
            self.limits, authorization_verifier=lambda value: value == self.authorization,
        ).validate(**arguments)

    def test_sample_and_full_success_are_read_only_bounded_and_redacted(self):
        before = (self.source.read_bytes(), self.source.stat().st_mtime_ns)
        sample = self.validate()
        self.assertEqual(sample["status"], "verified")
        self.assertEqual(len(sample["streams"]), 3)
        full = self.validate(mode="full")
        self.assertEqual(len(full["streams"]), 19)
        self.assertEqual((self.source.read_bytes(), self.source.stat().st_mtime_ns), before)
        serialized = json.dumps(full)
        self.assertNotIn(str(self.root), serialized)
        self.assertNotIn("password", serialized.lower())
        stored = json.loads((self.report / "copy-validation.json").read_text())
        checksum = stored.pop("report_payload_sha256")
        self.assertEqual(checksum, _digest(stored))

    def test_authorization_fails_before_any_path_read_or_directory_creation(self):
        denied = CopyAuthorization(False, "", "", "", "", "", "")
        with patch.object(Path, "open", side_effect=AssertionError("filesystem touched")):
            with self.assertRaisesRegex(CopyValidationError, "authorization_required"):
                OfflineCopyValidator().validate(
                    authorization=denied,
                    manifest_path=self.root / "missing.json",
                    allowed_root=self.root,
                    report_directory=self.root / "must-not-exist",
                )
        self.assertFalse((self.root / "must-not-exist").exists())
        with patch.object(Path, "open", side_effect=AssertionError("filesystem touched")):
            with self.assertRaisesRegex(CopyValidationError, "authorization_untrusted"):
                OfflineCopyValidator().validate(
                    authorization=self.authorization,
                    manifest_path=self.manifest_path,
                    allowed_root=self.root,
                    report_directory=self.root / "must-not-exist",
                )

    def test_manifest_external_binding_rejects_tamper_and_resign(self):
        document = json.loads(self.manifest_path.read_text())
        document["copied_at"] = "2099-01-01T00:00:00Z"
        document.pop("manifest_payload_sha256")
        document["manifest_payload_sha256"] = _digest(document)
        self.manifest_path.write_text(json.dumps(document, sort_keys=True, separators=(",", ":")))
        with self.assertRaisesRegex(CopyValidationError, "manifest_authorization_mismatch"):
            self.validate()

    def test_atomic_manifest_replacement_between_snapshot_and_parse_fails_closed(self):
        replacement = self.root / "replacement-manifest.json"
        document = json.loads(self.manifest_path.read_text())
        document["copied_at"] = "2099-01-01T00:00:00Z"
        document.pop("manifest_payload_sha256")
        document["manifest_payload_sha256"] = _digest(document)
        replacement.write_text(json.dumps(document, sort_keys=True, separators=(",", ":")))
        original = copy_validator._json_from_snapshot

        def replace_then_parse(path, snapshot, code):
            if path == self.manifest_path.resolve():
                replacement.replace(path)
            return original(path, snapshot, code)

        with (
            patch.object(copy_validator, "_json_from_snapshot", side_effect=replace_then_parse),
            patch.object(copy_validator, "OfflineDuckDBImporter", side_effect=AssertionError("online work")),
        ):
            with self.assertRaisesRegex(CopyValidationError, "file_changed_after_read"):
                self.validate()
        self.assertFalse((self.report / "copy-validation.json").exists())

    def test_atomic_checkpoint_replacement_between_inventory_and_parse_fails_closed(self):
        replacement = self.root / "replacement-full.json"
        full = json.loads(self.full_path.read_text())
        full["run_id"] = "attacker-run"
        self._sign_write(replacement, full)
        original = copy_validator._json_from_snapshot

        def replace_then_parse(path, snapshot, code):
            if path == self.full_path.resolve():
                replacement.replace(path)
            return original(path, snapshot, code)

        with (
            patch.object(copy_validator, "_json_from_snapshot", side_effect=replace_then_parse),
            patch.object(copy_validator, "OfflineDuckDBImporter", side_effect=AssertionError("online work")),
        ):
            with self.assertRaisesRegex(CopyValidationError, "file_changed_after_read"):
                self.validate()
        self.assertFalse((self.report / "copy-validation.json").exists())

    def test_wrong_source_file_hash_missing_and_symlink_are_rejected(self):
        self.source.write_bytes(self.source.read_bytes() + b"tamper")
        with self.assertRaises(CopyValidationError) as caught:
            self.validate()
        self.assertEqual(caught.exception.code, "file_checksum_mismatch")
        self.assertNotIn(str(self.source), str(caught.exception))

    def test_unknown_manifest_version_and_migration_evidence_fail_closed(self):
        self.authorization = self._write_manifest(version="future")
        with self.assertRaisesRegex(CopyValidationError, "manifest_version_unsupported"):
            self.validate()
        catchup = json.loads(self.catchup_path.read_text())
        catchup["version"] = "future"
        catchup.pop("checksum")
        self._sign_write(self.catchup_path, catchup)
        self.authorization = self._write_manifest()
        with self.assertRaisesRegex(CopyValidationError, "migration_evidence_invalid"):
            self.validate()

    def test_path_escape_and_size_limit_fail_closed(self):
        outside = self.root.parent / "outside-copy.duckdb"
        outside.write_bytes(b"x")
        try:
            self.authorization = self._write_manifest(files=[
                {"role": "duckdb", "relative_path": "../outside-copy.duckdb", "byte_size": 1,
                 "sha256": hashlib.sha256(b"x").hexdigest()},
                self._file("full_checkpoint", self.full_path),
                self._file("catchup_checkpoint", self.catchup_path),
            ])
            with self.assertRaisesRegex(CopyValidationError, "file_missing_or_escape"):
                self.validate()
        finally:
            outside.unlink(missing_ok=True)

    def test_symlink_and_capacity_bound_are_rejected(self):
        linked = self.root / "evidence" / "linked-full.json"
        linked.symlink_to(self.full_path)
        self.authorization = self._write_manifest(files=[
            self._file("duckdb", self.source),
            {**self._file("full_checkpoint", self.full_path),
             "relative_path": str(linked.relative_to(self.root))},
            self._file("catchup_checkpoint", self.catchup_path),
        ])
        with self.assertRaisesRegex(CopyValidationError, "symlink_rejected"):
            self.validate()
        self.authorization = self._write_manifest()
        tiny = ImportLimits(max_file_bytes=1024, max_rows=1000, batch_rows=10)
        with self.assertRaisesRegex(CopyValidationError, "file_limit_exceeded"):
            OfflineCopyValidator(
                tiny, authorization_verifier=lambda value: value == self.authorization,
            ).validate(
                authorization=self.authorization, manifest_path=self.manifest_path,
                allowed_root=self.root, report_directory=self.report,
            )

    def test_source_change_during_stream_cannot_publish_report(self):
        fake = Mock()
        fake.inspect.side_effect = [
            {"source_fingerprint": self.source_evidence["source_fingerprint"]},
            {"source_fingerprint": "changed"},
        ]
        def stream(_command, table, sink):
            sink.write(json.dumps({
                "type": "complete", "table": table, "row_count": 0,
                "batch_count": 0, "data_sha256": "0" * 64,
            }))
            return 0
        fake.stream.side_effect = stream
        with patch("marketcow.offline_copy_validator.OfflineDuckDBImporter", return_value=fake):
            with self.assertRaisesRegex(CopyValidationError, "source_changed_during_validation"):
                self.validate()
        self.assertFalse((self.report / "copy-validation.json").exists())


if __name__ == "__main__":
    unittest.main()
