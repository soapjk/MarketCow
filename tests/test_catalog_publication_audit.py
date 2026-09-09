import copy

import httpx
import pytest

from scripts.audit_catalog_publication import audit


def transport(*, duplicate=False):
    calls = []
    snapshot = dict(schema_version="marketcow.polymarket.catalog-snapshot.v1",
                    snapshot_id="one", catalog_revision="revision", first_page_token="first", unique_count=1)

    def handle(request):
        calls.append(request)
        assert request.method == "GET"
        name = request.url.path.rsplit("/", 1)[-1]
        if name == "status":
            value = {"catalog_revision": "revision"}
        elif name == "snapshot":
            value = snapshot
        elif name == "snapshot-v2":
            value = dict(snapshot, schema_version="marketcow.polymarket.catalog-snapshot.v2", change_sequence=1)
        elif name == "changes":
            value = {"next_sequence": 1, "events": []}
        else:
            rows = [{"market_id": "42", "relations": []}]
            if duplicate:
                rows.append(copy.deepcopy(rows[0]))
            value = dict(snapshot_id="one", catalog_revision="revision", page_token="first",
                         records=rows, end_of_snapshot=True, next_page_token=None)
        return httpx.Response(200, json=value)

    return httpx.MockTransport(handle), calls


def test_audit_get_only_full_traversal():
    mock, calls = transport()
    with httpx.Client(transport=mock, base_url="http://127.0.0.1") as client:
        result = audit(client, maximum_bytes=10000, maximum_pages=5, maximum_seconds=10)
    assert result["passed"] and result["unique_count"] == 1
    assert len(calls) == 6


def test_audit_duplicate_rejected_before_remaining_requests():
    mock, calls = transport(duplicate=True)
    with httpx.Client(transport=mock, base_url="http://127.0.0.1") as client:
        with pytest.raises(ValueError, match="duplicate"):
            audit(client, maximum_bytes=10000, maximum_pages=5, maximum_seconds=10)
    assert len(calls) == 3


def test_audit_byte_budget_stops():
    mock, calls = transport()
    with httpx.Client(transport=mock, base_url="http://127.0.0.1") as client:
        with pytest.raises(ValueError, match="byte budget"):
            audit(client, maximum_bytes=1, maximum_pages=5, maximum_seconds=10)
    assert len(calls) == 1
