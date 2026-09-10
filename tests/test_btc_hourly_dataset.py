import hashlib
import json
import zipfile
from datetime import datetime, timedelta, timezone

import pytest

from marketcow.btc_hourly_dataset import EPOCH, import_archive, visible_asof


NOW = datetime(2026, 9, 9, tzinfo=timezone.utc)
START = (datetime(2026, 9, 7, tzinfo=timezone.utc) - EPOCH) // timedelta(microseconds=1)


def row(index):
    opened = START + index * 3_600_000_000
    return f"{opened},100,102,99,101,3,{opened + 3_600_000_000 - 1},303,2,1,101,0\n"


def run(tmp_path, body=None, **overrides):
    archive = tmp_path / "BTCUSDT-1h-2026-09-07.zip"
    with zipfile.ZipFile(archive, "w") as target:
        target.writestr(archive.stem + ".csv", body if body is not None else "".join(row(i) for i in range(24)))
    checksum = tmp_path / "checksum"
    checksum.write_text(hashlib.sha256(archive.read_bytes()).hexdigest() + "  " + archive.name)
    args = dict(archive=archive, checksum=checksum, output=tmp_path / "out", interval="1h",
                day="2026-09-07", captured_at=NOW, maximum_zip_bytes=65536,
                maximum_uncompressed_bytes=65536, maximum_records=24)
    args.update(overrides)
    return import_archive(**args)


def test_full_day_offsets_and_asof(tmp_path):
    manifest = run(tmp_path)
    assert manifest["coverage_complete"] and manifest["records"] == 24
    raw = (tmp_path / "out/raw.csv").read_bytes()
    for line in (tmp_path / "out/facts.jsonl").read_bytes().splitlines():
        fact = json.loads(line)
        loc = fact["raw_locator"]
        original = raw[loc["offset"]:loc["offset"] + loc["length"]]
        assert hashlib.sha256(original).hexdigest() == fact["raw_sha256"]
        assert not visible_asof(fact, NOW, "observed")
        assert visible_asof(fact, NOW, "event_time_research")
        assert not visible_asof(fact, EPOCH, "event_time_research")


def test_missing_not_filled(tmp_path):
    result = run(tmp_path, row(1))
    assert result["records"] == 1 and not result["coverage_complete"]
    assert len(result["gaps"]) == 2


@pytest.mark.parametrize("body", [row(0) + row(0), row(1) + row(0), row(0).replace(",102,99,", ",98,99,"), row(0).replace(",3,", ",NaN,"), "bad\n"])
def test_corrupt_retains_no_success_manifest(tmp_path, body):
    with pytest.raises(ValueError):
        run(tmp_path, body)
    assert not (tmp_path / "out/manifest.json").exists()


@pytest.mark.parametrize("override", [{"maximum_records": 1}, {"maximum_zip_bytes": 1},
                                      {"maximum_uncompressed_bytes": 1},
                                      {"captured_at": datetime(2026, 9, 7, tzinfo=timezone.utc)},
                                      {"captured_at": datetime(2026, 9, 9)}])
def test_limits_and_time(tmp_path, override):
    with pytest.raises(ValueError):
        run(tmp_path, **override)
    assert not (tmp_path / "out/manifest.json").exists()


def test_no_overwrite(tmp_path):
    run(tmp_path)
    previous = (tmp_path / "out/manifest.json").read_bytes()
    with pytest.raises(FileExistsError):
        run(tmp_path)
    assert (tmp_path / "out/manifest.json").read_bytes() == previous


def test_checksum_mismatch_before_output(tmp_path):
    wrong = tmp_path / "wrong"
    wrong.write_text("0" * 64 + "  BTCUSDT-1h-2026-09-07.zip")
    with pytest.raises(ValueError, match="checksum_mismatch"):
        run(tmp_path, checksum=wrong)
    assert not (tmp_path / "out").exists()


def test_millisecond_input_is_not_guessed(tmp_path):
    with pytest.raises(ValueError, match="window"):
        run(tmp_path, row(0).replace(str(START), str(START // 1000)))


def test_unknown_asof_mode_and_naive_clock():
    with pytest.raises(ValueError):
        visible_asof({}, NOW, "guess")
    with pytest.raises(ValueError):
        visible_asof({}, datetime(2026, 9, 9), "observed")
