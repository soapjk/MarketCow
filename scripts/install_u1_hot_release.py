"""Explicit coordinated software replacement; never a market-scope activation.

Run only after the operator has received the current Paper pause receipt.
Keeps original unit contents, market roots and all account data; no auto rollback.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

from prepare_u1_hot_release import R, UNITS, sha, write


def state(name):
    result = subprocess.check_output(["systemctl", "--user", "show", name,
        "-p", "MainPID", "-p", "ActiveState", "-p", "Result", "-p", "ExecMainStatus"], text=True, timeout=5)
    return dict(line.split("=", 1) for line in result.splitlines())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release", type=Path, required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--paper-pause-reference", required=True,
        help="Exact message ID when available, otherwise channel ID plus peer observation UTC")
    args = parser.parse_args()
    root = args.release.resolve(strict=True)
    assert root.parent == R/"releases" and root == args.release
    assert args.paper_pause_reference.startswith(("channel_message:", "channel:"))
    assert sha(root/"manifest.json") == args.manifest_sha256
    manifest = json.loads((root/"manifest.json").read_bytes())
    names = ["marketcow-phase1-control.service", "marketcow-polymarket-collector.service",
             "marketcow-polymarket-discovery.service"]
    assert set(manifest["units"]) == set(names)
    for relative, digest in manifest["files"].items():
        path = root/relative
        assert path.resolve(strict=True).is_relative_to(root) and sha(path) == digest
    for name in names:
        target = UNITS/name
        assert sha(target) == manifest["units"][name]["incumbent_sha256"]
    os.umask(0o077)
    evidence = root/"installation"
    evidence.mkdir(mode=0o700)  # A failed attempt must be inspected, never blindly rerun.
    report = dict(started_at_ns=time.time_ns(), pause_reference=args.paper_pause_reference,
        manifest_sha256=args.manifest_sha256, no_account_operations=True, steps=[])
    for name in names:
        write(evidence/(name+".before"), (UNITS/name).read_text())
    try:
        for name in names:
            subprocess.run(["systemctl", "--user", "stop", name], check=True, timeout=65)
            stopped = state(name)
            assert stopped["MainPID"] == "0", stopped
            report["steps"].append(dict(operation="stop", unit=name, result=stopped))
        for name in names:
            target = UNITS/name
            assert sha(target) == manifest["units"][name]["incumbent_sha256"]
            candidate = Path(manifest["units"][name]["candidate_path"])
            assert sha(candidate) == manifest["units"][name]["candidate_sha256"]
            temporary = UNITS/("."+name+".hot-install")
            temporary.symlink_to(candidate)
            os.replace(temporary, target)
        directory = os.open(UNITS, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        subprocess.run(["systemctl", "--user", "daemon-reload"], check=True, timeout=10)
        for name in names[1:]+names[:1]:
            subprocess.run(["systemctl", "--user", "start", name], check=True, timeout=45)
            report["steps"].append(dict(operation="start", unit=name, result=state(name)))
        report["units_started"] = True
        report["http_ws_verified"] = False  # Never infer data readiness from systemd.
    except Exception as error:
        report["error"] = repr(error)
        raise
    finally:
        report["ended_at_ns"] = time.time_ns()
        write(evidence/"receipt.json", report)
        print(json.dumps(dict(receipt=str(evidence/"receipt.json"),
            receipt_sha256=hashlib.sha256((evidence/"receipt.json").read_bytes()).hexdigest(),
            units_started=report.get("units_started", False), http_ws_verified=False)))


if __name__ == "__main__":
    main()
