import io
import json
import urllib.error
from unittest.mock import Mock

import pytest

from marketcow import btc_archive_download as module


def test_http_failure_stops_without_followup(tmp_path, monkeypatch):
    opener = Mock()
    opener.open.side_effect = urllib.error.HTTPError("https://data.binance.vision", 403, "denied", {}, None)
    monkeypatch.setattr(module.urllib.request, "build_opener", lambda *_: opener)
    report = module.download("2026-09-07", tmp_path / "capture")
    assert opener.open.call_count == 1
    assert report["imports"] == [] and report["error"].startswith("HTTPError")
    assert json.loads((tmp_path / "capture/report.json").read_text())["error"] == report["error"]


def test_expired_deadline_never_opens_request(tmp_path, monkeypatch):
    ticks = iter([0, 121])
    monkeypatch.setattr(module.time, "monotonic", lambda: next(ticks))
    opener = Mock()
    monkeypatch.setattr(module.urllib.request, "build_opener", lambda *_: opener)
    report = module.download("2026-09-07", tmp_path / "capture")
    opener.open.assert_not_called()
    assert report["error"] == "TimeoutError:total_deadline"


def test_checksum_body_limit_includes_detection_byte(tmp_path, monkeypatch):
    class Response(io.BytesIO):
        status = 200
    response = Response(b"x" * 5000)
    opener = Mock()
    opener.open.return_value = response
    monkeypatch.setattr(module.urllib.request, "build_opener", lambda *_: opener)
    report = module.download("2026-09-07", tmp_path / "capture")
    assert report["error"] == "ValueError:body_budget_exceeded"
    assert report["requests"][0]["bytes"] == 4097
    assert opener.open.call_count == 1 and response.closed
    assert report["imports"] == []


def test_future_day_rejected_before_output_or_network(tmp_path, monkeypatch):
    opener = Mock()
    monkeypatch.setattr(module.urllib.request, "build_opener", opener)
    with pytest.raises(ValueError):
        module.download("2999-01-01", tmp_path / "capture")
    assert not (tmp_path / "capture").exists()
    opener.assert_not_called()


def test_redirect_handler_does_not_follow():
    assert module.NoRedirect().redirect_request(None, None, 302, "redirect", {}, "https://example.com") is None
