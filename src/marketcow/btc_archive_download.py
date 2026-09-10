"""Download exactly one public Binance SPOT day (1m + 1h), with fixed bounds."""
import argparse
import hashlib
import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .btc_hourly_dataset import import_archive


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def download(day: str, root: Path) -> dict:
    start = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if start < datetime(2025, 1, 1, tzinfo=timezone.utc) or start + timedelta(days=1) > datetime.now(timezone.utc):
        raise ValueError("unsupported_or_incomplete_day")
    root.mkdir(parents=True, exist_ok=False)
    deadline = time.monotonic() + 120
    opener = urllib.request.build_opener(NoRedirect())
    report = {"day": day, "requests": [], "imports": [], "error": None}
    total = 0
    try:
        for interval in ("1m", "1h"):
            filename = f"BTCUSDT-{interval}-{day}.zip"
            for name, maximum in ((filename + ".CHECKSUM", 4096), (filename, 64 * 1024 * 1024)):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("total_deadline")
                url = f"https://data.binance.vision/data/spot/daily/klines/BTCUSDT/{interval}/{name}"
                entry = {"url": url, "started_at": datetime.now(timezone.utc).isoformat(), "bytes": 0, "sha256": None}
                report["requests"].append(entry)
                digest = hashlib.sha256()
                # The partial file remains on every failure. No automatic retry.
                with opener.open(urllib.request.Request(url), timeout=min(20, remaining)) as response, (root / name).open("xb") as output:
                    entry["status"] = response.status
                    while True:
                        if time.monotonic() >= deadline:
                            raise TimeoutError("total_deadline")
                        raw = response.read(min(65536, maximum - entry["bytes"] + 1))
                        if not raw:
                            break
                        entry["bytes"] += len(raw)
                        total += len(raw)
                        if entry["bytes"] > maximum or total > 128 * 1024 * 1024:
                            raise ValueError("body_budget_exceeded")
                        output.write(raw)
                        digest.update(raw)
                    output.flush()
                    os.fsync(output.fileno())
                entry["sha256"] = digest.hexdigest()
                entry["received_at"] = datetime.now(timezone.utc).isoformat()
                time.sleep(1)
            manifest = import_archive(root / filename, root / (filename + ".CHECKSUM"), root / interval,
                                      interval=interval, day=day, captured_at=datetime.now(timezone.utc),
                                      maximum_zip_bytes=64 * 1024 * 1024,
                                      maximum_uncompressed_bytes=128 * 1024 * 1024,
                                      maximum_records=1440 if interval == "1m" else 24)
            report["imports"].append(manifest)
    except Exception as exc:
        report["error"] = type(exc).__name__ + ":" + str(exc)
    finally:
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        report["code_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        (root / "report.json").write_text(json.dumps(report, sort_keys=True))
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = download(args.day, args.output)
    print(json.dumps({"requests": len(report["requests"]), "error": report["error"],
                      "imports": len(report["imports"])}))
    if report["error"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
