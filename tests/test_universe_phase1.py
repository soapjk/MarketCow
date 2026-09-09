from __future__ import annotations

import hashlib

import pytest

from marketcow.universe_phase1 import (
    DiscoverySelectionRequest,
    canonical_json_bytes,
    selection_sha256,
    parse_request_bytes,
)


def _request(**overrides):
    selection = {
        "catalog_revision": "a" * 64,
        "market_ids": ["10", "2"],
        "tradude_policy_version": "policy-v1",
        "expected_active_selection_id": None,
        "protected_markets": [],
        "resource_profile_id": "phase1-r3-test-v1",
        "resource_profile_sha256": "b" * 64,
    }
    payload = {
        "schema_version": "tradude.marketcow.discovery-selection-request.v1",
        "request_id": "1700000000000:" + "c" * 32,
        "created_at": "2023-11-14T22:13:20.000Z",
        "expires_at": "2023-11-14T22:28:20.000Z",
        "selection": selection,
        "selection_sha256": selection_sha256(selection),
    }
    payload.update(overrides)
    return payload


def test_canonical_vector_is_stable():
    value = {"expected_active_selection_id": None, "market_ids": ["10", "2"]}
    assert canonical_json_bytes(value) == b'{"expected_active_selection_id":null,"market_ids":["10","2"]}'
    assert hashlib.sha256(canonical_json_bytes(value)).hexdigest() == (
        "9a9ec68086057462e9c36cc8d46592d4892ae5dbe1eee76444acf891691e1d6f"
    )


def test_request_validates_timestamp_and_digest():
    request = DiscoverySelectionRequest.model_validate(_request())
    assert request.request_digest() == hashlib.sha256(canonical_json_bytes(_request())).hexdigest()


@pytest.mark.parametrize("field,value", [
    ("selection_sha256", "0" * 64),
    ("request_id", "1700000000001:" + "c" * 32),
    ("expires_at", "2023-11-14T22:13:19.000Z"),
])
def test_request_rejects_identity_mismatch(field, value):
    with pytest.raises(ValueError):
        DiscoverySelectionRequest.model_validate(_request(**{field: value}))


def test_selection_rejects_unsorted_or_protected_outside():
    selection = _request()["selection"]
    selection["market_ids"] = ["2", "10"]
    with pytest.raises(ValueError):
        DiscoverySelectionRequest.model_validate(_request(selection=selection))


@pytest.mark.parametrize("value", [1.1, float("nan"), {"x": "非ASCII"}, {1: "x"}])
def test_canonical_rejects_outside_profile(value):
    with pytest.raises(ValueError):
        canonical_json_bytes(value)


@pytest.mark.parametrize("body", [b'{"x":1,"x":2}', b'{"x":NaN}', b'"\xff"'])
def test_strict_json_rejects_ambiguous_inputs(body):
    with pytest.raises(ValueError):
        parse_request_bytes(body, maximum_bytes=4096)


def test_byte_parser_matches_validated_request():
    body = canonical_json_bytes(_request())
    assert parse_request_bytes(body, maximum_bytes=len(body)).request_digest() == hashlib.sha256(body).hexdigest()
    with pytest.raises(ValueError):
        parse_request_bytes(body, maximum_bytes=len(body)-1)


@pytest.mark.parametrize("schema", [None, "wrong"])
def test_schema_required_and_exact(schema):
    payload = _request()
    if schema is None:
        del payload["schema_version"]
    else:
        payload["schema_version"] = schema
    with pytest.raises(ValueError):
        DiscoverySelectionRequest.model_validate(payload)
