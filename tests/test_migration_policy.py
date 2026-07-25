from __future__ import annotations

import unittest

from marketcow.migration_policy import validate_migration_history


MIGRATIONS = [(1, "one", "sql"), (2, "two", "sql"), (3, "three", "sql")]


class MigrationPolicyTest(unittest.TestCase):
    def test_empty_and_known_prefixes_can_upgrade(self):
        self.assertEqual(validate_migration_history([], MIGRATIONS, "test"), set())
        self.assertEqual(
            validate_migration_history([(1, "one"), (2, "two")], MIGRATIONS, "test"),
            {1, 2},
        )
        self.assertEqual(
            validate_migration_history([(1, b"one")], MIGRATIONS, "test"),
            {1},
        )

    def test_rejects_gap_newer_schema_and_description_drift(self):
        cases = (
            ([(1, "one"), (3, "three")], "contiguous"),
            ([(1, "one"), (4, "future")], "newer"),
            ([(1, "changed")], "descriptions"),
            ([(1, "one"), (1, "one")], "duplicate"),
        )
        for applied, message in cases:
            with self.subTest(applied=applied), self.assertRaisesRegex(
                RuntimeError, message
            ):
                validate_migration_history(applied, MIGRATIONS, "test")
