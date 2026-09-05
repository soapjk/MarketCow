from __future__ import annotations

import hashlib
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.prepare_polymarket_scoped_live import (
    _clone_verified,
    _verified_local_path,
)


class PreparePolymarketScopedLiveTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_cross_root_catalog_binding_is_rejected(self):
        source_root = self.root / "source"
        outside = self.root / "outside.jsonl"
        (source_root / "catalogs").mkdir(parents=True)
        outside.write_bytes(b"outside\n")

        with self.assertRaisesRegex(ValueError, "escapes source root"):
            _verified_local_path(source_root, outside, "catalogs")

    def test_corrupt_source_is_rejected_without_partial_destination(self):
        source = self.root / "source.jsonl"
        destination = self.root / "target" / "catalog.jsonl"
        source.write_bytes(b"authoritative\n")

        with self.assertRaisesRegex(ValueError, "source artifact hash mismatch"):
            _clone_verified(source, destination, "0" * 64)

        self.assertFalse(destination.exists())
        self.assertFalse(destination.with_name(".catalog.jsonl.preparing").exists())

    def test_existing_target_with_different_hash_is_never_overwritten(self):
        source = self.root / "source.jsonl"
        destination = self.root / "target" / "catalog.jsonl"
        source.write_bytes(b"authoritative\n")
        destination.parent.mkdir()
        destination.write_bytes(b"previous-generation\n")
        expected = hashlib.sha256(source.read_bytes()).hexdigest()

        with self.assertRaisesRegex(ValueError, "target artifact differs"):
            _clone_verified(source, destination, expected)

        self.assertEqual(destination.read_bytes(), b"previous-generation\n")
