import hashlib

from marketcow.btc_dataset_export import export
from marketcow.btc_fact_log import FactLog


def make(root):
    return FactLog(root, maximum_pending=10, maximum_pending_bytes=8192,
                   maximum_disk_bytes=16384, maximum_subscribers=1)


def run(root, output, mode="observed", **overrides):
    config = dict(through=2, start="2026-09-07T00:00:00Z", end="2026-09-08T00:00:00Z",
                  source="binance_global_spot", mode=mode, maximum_records=10, maximum_bytes=8192)
    config.update(overrides)
    return export(root, output, **config)


def test_fixed_prefix_reproduces_after_append_and_missing_time_not_invented(tmp_path):
    root = tmp_path / "log"
    log = make(root)
    for at in ("2026-09-07T01:00:00Z", None):
        log.publish(dict(source="binance_global_spot", first_received_at=at, event_at="2026-09-07T01:00:00Z"))
    log.close(2)
    first = run(root, tmp_path / "first")
    assert first["status"] == "export_complete" and first["selected"] == first["missing_time"] == 1
    log = make(root)
    log.publish(dict(source="binance_global_spot", first_received_at="2026-09-07T02:00:00Z"))
    log.close(2)
    second = run(root, tmp_path / "second")
    assert first["dataset_id"] == second["dataset_id"]
    assert first["facts_sha256"] == hashlib.sha256((tmp_path / "second/facts.jsonl").read_bytes()).hexdigest()
    research = run(root, tmp_path / "research", "event_time_research")
    assert research["selected"] == 2 and not research["coverage_complete"]


def test_future_watermark_and_small_budget_fail_with_manifest(tmp_path):
    root = tmp_path / "log"
    log = make(root)
    log.publish(dict(source="binance_global_spot", event_at="2026-09-07T01:00:00Z"))
    log.close(2)
    assert run(root, tmp_path / "future")["status"] == "incomplete"
    assert run(root, tmp_path / "small", through=1, maximum_bytes=1)["status"] == "incomplete"
