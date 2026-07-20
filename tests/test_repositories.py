import tempfile
import unittest
from pathlib import Path

from marketcow.duckdb_repositories import LocalArtifactStore, create_duckdb_repositories
from marketcow.repositories import (
    ArtifactStore,
    FundamentalRepository,
    MarketBarRepository,
    MetadataRepository,
)
from marketcow.storage import Warehouse


class DuckDBRepositoryTest(unittest.TestCase):
    def test_warehouse_is_exposed_through_explicit_domain_contracts(self):
        with tempfile.TemporaryDirectory() as folder:
            warehouse = Warehouse(Path(folder) / "warehouse.duckdb")
            repositories = create_duckdb_repositories(warehouse)

            self.assertIsInstance(repositories.metadata, MetadataRepository)
            self.assertIsInstance(repositories.fundamentals, FundamentalRepository)
            self.assertIsInstance(repositories.market_bars, MarketBarRepository)
            self.assertIsInstance(repositories.artifacts, ArtifactStore)
            self.assertIs(repositories.metadata, warehouse)
            self.assertIs(repositories.fundamentals, warehouse)
            self.assertIs(repositories.market_bars, warehouse)
            self.assertIsInstance(repositories.artifacts, LocalArtifactStore)

    def test_local_artifact_store_writes_body_and_manifest_atomically_for_callers(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            warehouse = Warehouse(root / "warehouse.duckdb")
            store = LocalArtifactStore(warehouse)

            manifest = store.write_json(
                root / "raw",
                "fixture",
                {"value": 1},
                "test",
                "https://example.test/data",
                "payload",
                "2026-07-20T00:00:00+00:00",
                "2026-07-20T00:00:01+00:00",
                {"scope": "contract"},
            )

            self.assertTrue(Path(manifest["storage_path"]).exists())
            saved = store.latest_artifact("fixture")
            self.assertIsNotNone(saved)
            self.assertEqual(saved["artifact_id"], manifest["artifact_id"])


if __name__ == "__main__":
    unittest.main()
