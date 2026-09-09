import copy
import pytest

from marketcow.catalog_delta_consumer import CatalogDeltaConsumer
from marketcow.catalog_incremental import CatalogChanges, digest
from marketcow.catalog_publication import CatalogPublication, publish_generation
from tests.test_catalog_incremental import row, publish


def test_paginated_atomic_delta_and_restart(tmp_path):
    store = CatalogChanges(
        tmp_path / "source.sqlite", max_database_bytes=1000000, max_record_bytes=10000, max_batch_records=10
    )
    consumer = CatalogDeltaConsumer(tmp_path / "consumer.sqlite", max_pending_bytes=20000, max_records=10)
    try:
        consumer.install([], sequence=0, revision=digest([]), unique_count=0)
        report = publish(store, [row(), row("2")])
        publish_generation(store, tmp_path / "published", capture_report=report)
        source = CatalogPublication(tmp_path / "published", max_row_bytes=10000).current()
        import json

        first = json.loads(source.changes(0, 1, 10000))
        assert consumer.ingest(first) == 1
        assert consumer.head()[0] == 0
        assert consumer.db.execute("SELECT count(*) FROM records").fetchone()[0] == 0
        consumer.close()
        consumer = CatalogDeltaConsumer(tmp_path / "consumer.sqlite", max_pending_bytes=20000, max_records=10)
        second = json.loads(source.changes(1, 1, 10000))
        corrupt = copy.deepcopy(second)
        corrupt["events"][0]["record"]["question"] = "tampered"
        with pytest.raises(ValueError, match="hash"):
            consumer.ingest(corrupt)
        assert consumer.ingest(second) == 2
        assert consumer.head()[0] == 2
        assert consumer.db.execute("SELECT count(*) FROM records").fetchone()[0] == 2
    finally:
        consumer.close()
        store.close()
