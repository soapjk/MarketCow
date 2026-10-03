from fastapi.testclient import TestClient
import threading
import time

from marketcow.btc_fact_log import FactLog
from marketcow.btc_research_read import create_read_app


class Bootstrap:
    def status(self, durable):
        return {"ready": durable >= 1, "durable_watermark": durable}

    def checkpoint(self, durable):
        return {
            "schema_version": "marketcow.binance-finalized-1m-window.v1",
            "checkpoint_sequence": 1,
            "durable_watermark": durable,
            "bars": [{"open_time_ms": 1}],
        }


def test_asof_never_invents_receipt(tmp_path):
    log = FactLog(tmp_path, maximum_pending=3, maximum_pending_bytes=4096,
                  maximum_disk_bytes=8192, maximum_subscribers=1)
    log.publish({"event_at": "2026-09-07T01:00:00Z", "first_received_at": None})
    log.close(2)
    with TestClient(create_read_app(tmp_path, maximum_records=3, maximum_bytes=4096)) as client:
        base = "/v1/btc-research/facts?after=0&through=1&limit=3"
        result = client.get(base).json()
        assert result["records"] == [] and result["next_after"] == 1
        assert len(client.get(base + "&mode=event_time_research").json()["records"]) == 1
        assert client.get(base + "&asof=2026-09-07").status_code == 400
        assert client.post(base).status_code == 405


def test_live_stream_bypasses_blocked_disk_and_releases_slot(tmp_path):
    gate = threading.Event()
    log = FactLog(tmp_path, maximum_pending=3, maximum_pending_bytes=4096,
                  maximum_disk_bytes=8192, maximum_subscribers=1,
                  before_write=lambda: gate.wait(5))
    app = create_read_app(tmp_path, maximum_records=3, maximum_bytes=4096, live_log=log)
    try:
        with TestClient(app) as client:
            for sequence in (1, 2):
                with client.websocket_connect("/v1/btc-research/stream") as ws:
                    hello = ws.receive_json()
                    assert hello["replay"] is False and hello["continuity"] == "unverified"
                    log.publish({"n": sequence})
                    assert ws.receive_json()["sequence"] == sequence
                    assert client.get("/v1/btc-research/status").json()["durable"] == 0
            assert not log.subscribers
    finally:
        gate.set()
        log.close(2)


def test_status_and_stream_expose_bootstrap_durability(tmp_path):
    log = FactLog(tmp_path, maximum_pending=3, maximum_pending_bytes=4096,
                  maximum_disk_bytes=8192, maximum_subscribers=1)
    log.publish({"first_received_at": "2026-09-23T00:00:00Z"})
    deadline = time.monotonic() + 2
    while log.status()["durable"] < 1 and time.monotonic() < deadline:
        time.sleep(.001)
    assert log.status()["durable"] == 1
    app = create_read_app(
        tmp_path, maximum_records=3, maximum_bytes=4096,
        live_log=log, bootstrap=Bootstrap(),
    )
    try:
        with TestClient(app) as client:
            status = client.get("/v1/btc-research/status").json()
            assert status["btc_1m_bootstrap"]["ready"] is True
            assert client.get("/v1/btc-research/bootstrap").json() == {
                "ready": True, "durable_watermark": 1,
            }
            checkpoint = client.get("/v1/btc-research/finalized-1m-window").json()
            assert checkpoint["checkpoint_sequence"] == 1
            assert checkpoint["durable_watermark"] == 1
            assert checkpoint["epoch"] == log.epoch
            with client.websocket_connect("/v1/btc-research/stream") as ws:
                hello = ws.receive_json()
                assert hello["btc_1m_bootstrap"]["ready"] is True
    finally:
        log.close(2)


def test_bootstrap_endpoint_requires_attached_state(tmp_path):
    with TestClient(create_read_app(
        tmp_path, maximum_records=3, maximum_bytes=4096,
    )) as client:
        assert client.get("/v1/btc-research/bootstrap").status_code == 503
        assert client.get("/v1/btc-research/finalized-1m-window").status_code == 503
