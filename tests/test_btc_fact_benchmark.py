import pytest

from marketcow.btc_fact_benchmark import measure


def test_measurement_checks_both_watermarks_and_committed_pages(tmp_path):
    report = measure(tmp_path / "run", 10)
    assert report["error"] is None
    assert report["blocked_state"]["durable"] == 0
    assert report["blocked_state"]["published"] == 10
    assert report["final_state"]["durable"] == report["verified_committed_count"] == 10
    assert report["slow_error"] == "slow_consumer"
    assert report["latency_ns"]["max"] >= report["latency_ns"]["p95"] > 0


def test_benchmark_bounds_before_creation(tmp_path):
    with pytest.raises(ValueError):
        measure(tmp_path / "run", 10001)
    assert not (tmp_path / "run").exists()
