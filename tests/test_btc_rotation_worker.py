import hashlib
import json

from marketcow.btc_rotation_worker import read_config, run


def evidence(market="1"):
    condition = "0x" + "a" * 64
    raw = json.dumps({
        "id": market, "conditionId": condition,
        "slug": "bitcoin-up-or-down-september-10-2026-5pm-et",
        "question": "Bitcoin Up or Down", "description": "Binance BTC/USDT 1H Close >= Open.",
        "startDate": "2026-09-10T21:00:00Z", "endDate": "2026-09-10T22:00:00Z",
        "outcomes": '["Up","Down"]', "clobTokenIds": '["10","20"]',
    }, separators=(",", ":")).encode()
    projected = {
        "schema_version": "marketcow.polymarket.market-evidence.v1",
        "market_id": market, "condition_id": condition, "raw_base64": __import__("base64").b64encode(raw).decode(),
        "raw_sha256": hashlib.sha256(raw).hexdigest(), "raw_complete": True,
        "raw_bytes": len(raw), "observed_at": "2026-09-10T21:00:01Z",
        "outcomes": [{"token_id": "10", "outcome": "Up"},
                     {"token_id": "20", "outcome": "Down"}],
    }
    review = {
        "schema_version": "marketcow.btc-hour.rule-review.v1", "status": "approved",
        "market_id": market, "raw_sha256": projected["raw_sha256"],
        "source_instrument": "BINANCE_SPOT:BTCUSDT", "comparison": "final_close_gte_open",
        "start_utc": "2026-09-10T21:00:00Z", "end_utc": "2026-09-10T22:00:00Z",
    }
    return {"evidence": projected, "review": review}


def configuration(secret_path):
    secret = secret_path.read_bytes()
    return {
        "schema_version": "marketcow.btc-hour.rotation-config.v1",
        "management_endpoint": "http://127.0.0.1:18898",
        "discovery_endpoint": "http://127.0.0.1:8795",
        "live_endpoint": "http://127.0.0.1:8793", "caller": "tradude",
        "bearer_file": str(secret_path), "bearer_sha256": hashlib.sha256(secret).hexdigest(),
        "discovery_request": {"schema_version": "tradude.marketcow.discovery-selection-request.v1"},
        "live_request": {"schema_version": "marketcow.hot-live-prepare.v1"},
        "reviewed_markets": [evidence()],
        "control_timeout_seconds": 20, "maximum_control_response_bytes": 1048576,
        "discovery_capture": {"maximum_bytes": 8388608, "timeout": 20},
        "live_capture": {"seconds": 10, "maximum_frames": 20, "maximum_total_bytes": 1048576,
                         "maximum_frame_bytes": 262144, "maximum_fullsync_bytes": 524288},
        "persistence": {"maximum_pending": 100, "maximum_pending_bytes": 1048576,
                        "maximum_disk_bytes": 8388608},
    }


def test_config_binds_secret_without_exposing_it(tmp_path):
    secret = tmp_path / "secret"
    secret.write_text("private-value\n")
    config = tmp_path / "config.json"
    config.write_text(json.dumps(configuration(secret)))
    value, bindings, bearer, digest = read_config(config)
    assert bearer == "private-value" and bindings[0]["market_id"] == "1"
    assert digest == hashlib.sha256(config.read_bytes()).hexdigest()
    value["bearer_sha256"] = "0" * 64
    config.write_text(json.dumps(value))
    try:
        read_config(config)
    except ValueError as exc:
        assert str(exc) == "rotation_bearer_identity"
    else:
        raise AssertionError("secret identity mismatch accepted")


def test_run_persists_capture_and_closes_operations(tmp_path):
    secret = tmp_path / "secret"
    secret.write_text("private-value")
    config = tmp_path / "config.json"
    config.write_text(json.dumps(configuration(secret)))
    made = []

    class Operations:
        def __init__(self, *args, **kwargs):
            made.append(self)
            self.closed = False

        def close(self):
            self.closed = True

    async def rotation(operations, caller, discovery, live, bindings, discovery_endpoint,
                       live_endpoint, publish, attempt_root, **budgets):
        attempt_root.mkdir()
        publish({"event_type": "ready", "first_received_at": "2026-09-10T00:00:00Z"})
        return {"stage": "capture_complete", "caller": caller,
                "market_id": bindings[0]["market_id"]}

    report = run(config, tmp_path / "output", operations_factory=Operations, rotation=rotation)
    assert report["status"] == "capture_complete" and report["fact_log"]["durable"] == 1
    assert made[0].closed is True
    stored = json.loads((tmp_path / "output/report.json").read_bytes())
    assert stored == report and "private-value" not in json.dumps(stored)
