from fastapi.testclient import TestClient
import threading

from marketcow.btc_fact_log import FactLog
from marketcow.btc_research_read import create_read_app


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
