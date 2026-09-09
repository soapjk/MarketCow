"""Offline only: no official endpoints are contacted."""
from datetime import datetime, timezone
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import hashlib
import io
import json
import urllib.error

import pytest


spec = spec_from_file_location("history_probe", Path(__file__).parents[1] /
                              "scripts/probe_official_history.py")
probe = module_from_spec(spec)
spec.loader.exec_module(probe)
NOW = datetime(2026, 9, 8, tzinfo=timezone.utc)


def test_past_seven_days():
    start, end = probe.history_window("2026-09-01T00:00:00Z", "2026-09-08T00:00:00Z", NOW)
    assert (end-start).total_seconds() == 604800


@pytest.mark.parametrize("start,end", [
    ("2026-09-06T00:00:00Z", "2026-09-13T00:00:00Z"),
    ("2026-09-08T00:00:00Z", "2026-09-08T00:00:00Z"),
    ("2026-09-08T00:00:00Z", "2026-09-07T00:00:00Z"),
    ("2026-08-31T00:00:00Z", "2026-09-08T00:00:00Z"),
    ("2026-09-01T00:00:00", "2026-09-08T00:00:00Z"),
    ("2026-09-01T00:00:00+00:00", "2026-09-08T00:00:00Z"),
])
def test_invalid_windows(start, end):
    with pytest.raises(ValueError):
        probe.history_window(start, end, NOW)


def arguments(tmp_path, kind="availability_probe"):
    return ["--output", str(tmp_path / "evidence"), "--start-utc", "2023-01-01T00:00:00Z",
            "--end-utc", "2023-01-02T00:00:00Z", "--research-type", kind]


class Response(io.BytesIO):
    status = 200
    headers = {}


def fake_transport(monkeypatch, replies):
    calls = []

    class Opener:
        def open(self, request, timeout):
            calls.append(request.full_url)
            reply = replies.pop(0)
            if isinstance(reply, Exception):
                raise reply
            if isinstance(reply, Response):
                return reply
            return Response(json.dumps(reply).encode())

    monkeypatch.setattr(probe.urllib.request, "build_opener", lambda *args: Opener())
    return calls


def test_research_type_rejected_before_io(tmp_path, monkeypatch):
    calls = fake_transport(monkeypatch, [])
    with pytest.raises(ValueError, match="research_type_mismatch"):
        probe.main(arguments(tmp_path, "single_match"))
    assert calls == []
    assert not (tmp_path / "evidence").exists()


def test_http_failure_stops_and_records_version(tmp_path, monkeypatch):
    error = urllib.error.HTTPError("https://example.invalid", 403, "Forbidden", {},
                                   io.BytesIO(b"error code: 1010\n"))
    calls = fake_transport(monkeypatch, [error])
    probe.main(arguments(tmp_path))
    report = json.loads((tmp_path / "evidence/report.json").read_text())
    assert len(calls) == 1
    assert report["requests"][0]["status"] == 403
    assert report["requests"][0]["raw_complete"]
    assert report["code_sha256"] == hashlib.sha256(Path(probe.__file__).read_bytes()).hexdigest()
    assert report["configuration"]["research_type"] == "availability_probe"
    assert report["sample"]["paired_label_verified"] is False


def test_wrong_market_stops_before_clob(tmp_path, monkeypatch):
    calls = fake_transport(monkeypatch, [{"id": "wrong"}])
    probe.main(arguments(tmp_path))
    report = json.loads((tmp_path / "evidence/report.json").read_text())
    assert len(calls) == 1
    assert "market_identity_mismatch" in report["error"]


@pytest.mark.parametrize("condition,tokens", [
    ("0x" + "b" * 64, ["1", "2"]),
    ("0x" + "a" * 64, ["1", "3"]),
    ("0x" + "a" * 64, ["1", "1"]),
])
def test_wrong_clob_identity_stops_before_history(tmp_path, monkeypatch, condition, tokens):
    gamma = {"id": "1088482", "conditionId": "0x" + "a" * 64,
             "endDate": "2099-01-01T00:00:00Z", "clobTokenIds": '["1","2"]'}
    clob = {"condition_id": condition, "tokens": [{"token_id": t} for t in tokens]}
    calls = fake_transport(monkeypatch, [gamma, clob])
    probe.main(arguments(tmp_path))
    report = json.loads((tmp_path / "evidence/report.json").read_text())
    assert len(calls) == 2
    assert "identity_mismatch" in report["error"]


@pytest.mark.parametrize("ticks", [[0, 70, 70], [0, 0, 70, 70]])
def test_deadline_before_open_no_request(tmp_path, monkeypatch, ticks):
    calls = fake_transport(monkeypatch, [])
    values = iter(ticks)
    monkeypatch.setattr(probe.time, "monotonic", lambda: next(values))
    probe.main(arguments(tmp_path))
    report = json.loads((tmp_path / "evidence/report.json").read_text())
    assert calls == []
    assert "deadline" in report["error"] or any(
        r.get("error") == "TimeoutError" for r in report["requests"])


def test_wall_clock_regression_between_requests(tmp_path, monkeypatch):
    calls = fake_transport(monkeypatch, [{"id": "1088482", "conditionId": "0x" + "a" * 64}])

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 9, 7 if calls else 8, tzinfo=timezone.utc)

    monkeypatch.setattr(probe, "datetime", Clock)
    probe.main(arguments(tmp_path))
    report = json.loads((tmp_path / "evidence/report.json").read_text())
    assert len(calls) == 1
    assert "wall_clock_regression" in report["error"]


def test_total_deadline_after_first_response_stops_second_open(tmp_path, monkeypatch):
    ticks = [0]

    class ExpiringResponse(Response):
        def close(self):
            ticks[0] = 70
            super().close()

    response = ExpiringResponse(json.dumps({"id": "1088482",
                                           "conditionId": "0x" + "a" * 64}).encode())
    calls = fake_transport(monkeypatch, [response])
    monkeypatch.setattr(probe.time, "monotonic", lambda: ticks[0])
    probe.main(arguments(tmp_path))
    report = json.loads((tmp_path / "evidence/report.json").read_text())
    assert len(calls) == 1
    assert report["requests"][0]["raw_complete"]
    assert "total_deadline_before_request" in report["error"]


@pytest.mark.parametrize("size", [1048575, 1048576, 1048577])
def test_total_byte_boundary_no_extra_read(tmp_path, monkeypatch, size):
    class CountingResponse(Response):
        actual_read = 0
        reads = 0

        def read1(self, maximum):
            self.reads += 1
            data = super().read1(maximum)
            self.actual_read += len(data)
            return data

    response = CountingResponse(b" " * size)
    calls = fake_transport(monkeypatch, [response])
    probe.main(arguments(tmp_path))
    report = json.loads((tmp_path / "evidence/report.json").read_text())
    assert len(calls) == 1
    assert response.actual_read == min(size, 1048576) == report["total_bytes"]
    assert report["requests"][0]["raw_complete"] == (size < 1048576)
    if size >= 1048576:
        assert response.reads == 64
        assert report["requests"][0]["stop_reason"] == "byte_budget_eof_unverified"


def test_frozen_identity_only_calls_history(tmp_path, monkeypatch):
    payload = json.dumps({"id": "1088482", "conditionId": "0x" + "a" * 64,
                          "clobTokenIds": '["1","2"]', "outcomes": '["Yes","No"]',
                          "endDate": "2099-01-01T00:00:00Z"})
    evidence = {"market_id": "1088482", "raw_response": payload,
                "raw_response_sha256": hashlib.sha256(payload.encode()).hexdigest(),
                "observed_at": "2026-09-06T00:00:00Z", "source_url": "synthetic",
                "settlement": {"payouts": [{"token_id": "1", "outcome": "Yes"},
                                           {"token_id": "2", "outcome": "No"}]}}
    path = tmp_path / "frozen.json"
    path.write_text(json.dumps({"evidence": evidence}))
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    calls = fake_transport(monkeypatch, [{"history": []}, {"history": []}])
    probe.main(arguments(tmp_path) + ["--frozen-identity", str(path),
                                     "--frozen-identity-sha256", digest])
    assert len(calls) == 2 and all("/prices-history?" in url for url in calls)
    report = json.loads((tmp_path / "evidence/report.json").read_text())
    assert report["budget"]["requests"] == 2
    assert report["pairing"]["clob_condition_id"] is None
    with pytest.raises(ValueError, match="hash"):
        probe.frozen_identity(path, "0" * 64)


def test_environment_proxy_passed_to_opener(tmp_path, monkeypatch):
    seen = []
    monkeypatch.setattr(probe.urllib.request, "getproxies",
                        lambda: {"https": "http://example.invalid:8888"})

    class Opener:
        def open(self, request, timeout):
            raise OSError("synthetic proxy failure: confidential")

    def build(*handlers):
        seen.extend(handlers)
        return Opener()

    monkeypatch.setattr(probe.urllib.request, "build_opener", build)
    probe.main(arguments(tmp_path))
    assert seen[0].proxies == {"https": "http://example.invalid:8888"}
    report_text = (tmp_path / "evidence/report.json").read_text()
    report = json.loads(report_text)
    assert len(report["requests"]) == 1
    assert report["configuration"]["environment_proxy_enabled"] is True
    assert report["configuration"]["direct_fallback_on_proxy_failure"] is False
    assert "confidential" not in report_text
    assert "example.invalid" not in report_text
