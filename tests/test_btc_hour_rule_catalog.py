import hashlib
import json

import pytest

from marketcow.btc_hour_rule_catalog import build_rule_catalog
from marketcow.btc_hourly_dataset import canonical
from marketcow.btc_polymarket_binding import BINANCE_HOUR_RULE


def market(identity="1", start="2026-09-10T12:00:00Z", end="2026-09-10T13:00:00Z"):
    return {"id": identity, "conditionId": "0x" + identity.zfill(64),
            "question": "Bitcoin Up or Down - September 10, 8AM ET",
            "description": BINANCE_HOUR_RULE,
            "resolutionSource": "https://www.binance.com/en/trade/BTC_USDT",
            "eventStartTime": start, "endDate": end,
            "outcomes": '["Up", "Down"]', "clobTokenIds": f'["{identity}1", "{identity}2"]',
            "createdAt": "2026-09-09T12:00:00Z", "updatedAt": "2026-09-10T12:01:00Z",
            "active": True, "closed": False, "acceptingOrders": True}


def write_source(path, values):
    raw = b"".join(canonical(value) + b"\n" for value in values)
    path.write_bytes(raw)
    return hashlib.sha256(raw).hexdigest(), len(raw)


def test_extracts_only_exact_hour_and_preserves_locator(tmp_path):
    source = (tmp_path / "source.jsonl").resolve()
    wrong = market("2")
    wrong["description"] += " changed"
    digest, size = write_source(source, [{"id": "other"}, wrong, market()])
    output = (tmp_path / "out/catalog.json").resolve()
    result = build_rule_catalog(source, output, source_sha256=digest,
                                source_observed_at="2026-09-10T12:30:00Z",
                                maximum_source_bytes=size)
    assert result["record_count"] == 1
    assert result["during_window_count"] == 1
    assert result["contiguous_segments"] == [{"start_utc": "2026-09-10T12:00:00Z",
                                               "end_utc": "2026-09-10T13:00:00Z",
                                               "record_count": 1}]
    assert result["missing_hour_count_between_first_and_last"] == 0
    row = result["records"][0]
    raw = source.read_bytes()[row["raw_locator"]["offset"]:
                              row["raw_locator"]["offset"] + row["raw_locator"]["length"]]
    assert hashlib.sha256(raw).hexdigest() == row["raw_sha256"]
    assert row["outcomes"] == [{"outcome": "Up", "token_id": "11"},
                               {"outcome": "Down", "token_id": "12"}]


def test_source_hash_capacity_and_duplicates_fail_closed(tmp_path):
    source = (tmp_path / "source.jsonl").resolve()
    digest, size = write_source(source, [market(), market()])
    with pytest.raises(ValueError, match="rule_catalog_duplicate"):
        build_rule_catalog(source, (tmp_path / "duplicate.json").resolve(), source_sha256=digest,
                           source_observed_at="2026-09-10T11:00:00Z", maximum_source_bytes=size)
    with pytest.raises(ValueError, match="rule_catalog_source_capacity"):
        build_rule_catalog(source, (tmp_path / "small.json").resolve(), source_sha256=digest,
                           source_observed_at="2026-09-10T11:00:00Z",
                           maximum_source_bytes=size - 1)
    with pytest.raises(ValueError, match="rule_catalog_source_hash"):
        build_rule_catalog(source, (tmp_path / "hash.json").resolve(), source_sha256="0" * 64,
                           source_observed_at="2026-09-10T11:00:00Z", maximum_source_bytes=size)


def test_invalid_matching_identity_is_not_silently_skipped(tmp_path):
    source = (tmp_path / "source.jsonl").resolve()
    value = market()
    value["clobTokenIds"] = json.dumps(["same", "same"])
    digest, size = write_source(source, [value])
    with pytest.raises(ValueError, match="rule_catalog_identity"):
        build_rule_catalog(source, (tmp_path / "out.json").resolve(), source_sha256=digest,
                           source_observed_at="2026-09-10T11:00:00Z", maximum_source_bytes=size)


def test_catalog_quantifies_time_gaps(tmp_path):
    source = (tmp_path / "source.jsonl").resolve()
    digest, size = write_source(source, [market("1", "2026-09-10T12:00:00Z", "2026-09-10T13:00:00Z"),
                                         market("2", "2026-09-10T15:00:00Z", "2026-09-10T16:00:00Z")])
    result = build_rule_catalog(source, (tmp_path / "out.json").resolve(), source_sha256=digest,
                                source_observed_at="2026-09-10T11:00:00Z", maximum_source_bytes=size)
    assert result["missing_hour_count_between_first_and_last"] == 2
    assert [segment["record_count"] for segment in result["contiguous_segments"]] == [1, 1]
