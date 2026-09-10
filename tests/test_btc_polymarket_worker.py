import json

import pytest

from marketcow.btc_fact_log import read_page
from marketcow.btc_polymarket_worker import run
from marketcow.btc_lifecycle import HourRegistry
from tests.test_btc_polymarket_binding import sample


def configuration(path):
    evidence, review = sample()
    config = dict(schema_version="marketcow.btc-hour.capture-config.v1",
                  endpoint="http://127.0.0.1:8793", scope_id="scope",
                  reviewed_markets=[dict(evidence=evidence, review=review)], seconds=30,
                  maximum_frames=10, maximum_total_bytes=65536, maximum_frame_bytes=8192,
                  maximum_fullsync_bytes=32768, maximum_pending=10,
                  maximum_pending_bytes=131072, maximum_disk_bytes=262144)
    path.write_text(json.dumps(config))


def test_entry_preserves_received_facts_and_terminal_receipt(tmp_path):
    config = tmp_path / "config.json"
    configuration(config)
    async def fake(endpoint, scope, bindings, publish, **budgets):
        assert bindings[0]["up_token"] == "10" and budgets["maximum_frames"] == 10
        publish({"synthetic": True, "source_cursor": 100})
        return dict(ready_observed=True, frames=1)
    output = tmp_path / "capture"
    registry = HourRegistry(tmp_path / "lifecycle.sqlite", maximum_markets=3, maximum_bytes=16384)
    report = run(config, output, capture_function=fake, registry=registry)
    assert report["lifecycle_plan"]["activation_performed"] is False
    assert report["status"] == "capture_complete"
    assert report["log"]["published"] == report["log"]["durable"] == 1
    assert read_page(output / "facts", after=0, through=1, limit=1, maximum_bytes=1024)[0]["fact"]["synthetic"]
    with pytest.raises(FileExistsError):
        run(config, output, capture_function=fake)


def test_failure_keeps_facts_and_does_not_leak_exception_url(tmp_path):
    config = tmp_path / "config.json"
    configuration(config)
    async def fail(endpoint, scope, bindings, publish, **budgets):
        publish({"synthetic": True})
        raise RuntimeError("https://private.example/?credential=secret")
    output = tmp_path / "capture"
    report = run(config, output, capture_function=fail)
    assert report["status"] == "failed" and report["error"] == "RuntimeError"
    assert report["log"]["durable"] == 1
    assert "credential" not in (output / "report.json").read_text()
