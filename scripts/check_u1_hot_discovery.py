"""Finite real REST hot-membership check on the existing diagnostic source root.

No formal unit/root/port is changed; selections are labelled operational test
inputs, not Tradude ranking. No accounts or trading clients.
"""
import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import shlex
import socket
import sqlite3
import subprocess
import time
import traceback
from urllib.parse import urlencode

import httpx

from marketcow.polymarket_contracts import content_sha256
from marketcow.universe_live_candidate import read_static_markets
from marketcow.universe_rust_control import RustScopeClient
from websockets.asyncio.client import connect

R = Path("/mnt/p44pro/marketcow-shadow-v3-runtime/linux")
ROOT = R/"universe-discovery-candidate-r1"


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def unit_status(unit):
    return dict(line.split("=", 1) for line in subprocess.check_output(
        ["systemctl", "--user", "show", unit, "-p", "Result", "-p", "ExecMainStatus", "-p", "MainPID", "-p", "MemoryPeak"],
        text=True, timeout=5).splitlines() if "=" in line)


async def observe(base, private, config, report, unit):
    deadline = time.monotonic()+65
    async with httpx.AsyncClient(trust_env=False, timeout=3) as http:
        while True:
            if unit_status(unit)["MainPID"] == "0":
                raise RuntimeError("candidate exited before control became ready")
            try:
                actual = await asyncio.to_thread(private.status)
                if actual["source_readable"] and actual["source_cursor"] > 0:
                    break
            except (OSError, ValueError, RuntimeError):
                pass
            if time.monotonic() >= deadline:
                raise TimeoutError("managed source startup")
            await asyncio.sleep(.2)
        report["initial"] = actual
        prefix = base+"/v1/prediction-markets/polymarket/live/discovery"
        initial = await http.get(prefix+"/full-sync")
        initial.raise_for_status()
        assert len(initial.content) <= 67108864
        report["initial_fullsync_sha256"] = hashlib.sha256(initial.content).hexdigest()
        old = initial.json()
        assert sorted(m["market_id"] for m in old["markets"]) == report["expected_initial_market_ids"]
        expected = dict(expected_scope_id=actual["projection_id"], expected_revision=actual["revision"])
        records = report.pop("records")
        prepared = await asyncio.to_thread(private.request, dict(operation="prepare_acquisition", **expected,
            catalog_revision=config["catalog_revision"], records=records, evidence_sha256=content_sha256(records),
            acquisition_market_ids=config["market_ids"]))
        assert prepared["acquisition_installed"] and not prepared["publication_applied"]
        report["acquisition"] = prepared
        unchanged = await asyncio.to_thread(private.status)
        assert unchanged["projection_id"] == actual["projection_id"] and unchanged["revision"] == actual["revision"]
        report["before_publish"] = unchanged
        report["prepare"] = await asyncio.to_thread(private.request,
            dict(operation="prepare_publication", **expected, config=config))
        activated = await asyncio.to_thread(private.request, dict(operation="publish_scope", **expected, config=config))
        assert activated["publication_applied"] and activated["actual"]["projection_id"] == config["projection_id"]
        report["activation"] = activated
        after = await http.get(prefix+"/full-sync")
        after.raise_for_status()
        report["new_fullsync_sha256"] = hashlib.sha256(after.content).hexdigest()
        assert sorted(m["market_id"] for m in after.json()["markets"]) == config["market_ids"]
        # Consume this exact baseline; do not reserve a third full-sync lease.
        baseline = after.json(); cursor = baseline["boundary_cursor"]
        start_cursor = cursor; frames = wire_bytes = 0; observed_new = False
        uri = prefix.replace("http://", "ws://", 1)+"/stream?"+urlencode(dict(projection_id=config["projection_id"], after_cursor=cursor))
        async with asyncio.timeout(max(1, deadline-time.monotonic())):
            async with connect(uri, proxy=None, max_size=16777216, max_queue=1, close_timeout=1) as ws:
                while frames < 2000 and wire_bytes < 67108864:
                    raw = await ws.recv(); encoded = raw.encode() if isinstance(raw, str) else raw
                    frames += 1; wire_bytes += len(encoded)
                    assert wire_bytes <= 67108864
                    frame = json.loads(encoded)
                    assert frame["schema_version"] == "marketcow.polymarket.discovery-events.v3" and not frame["resync_required"]
                    assert frame["projection_id"] == config["projection_id"] and frame["universe_revision"] == config["universe_revision"]
                    assert frame["catalog_revision"] == config["catalog_revision"] and frame["after_cursor"] == cursor
                    assert cursor <= frame["next_cursor"] <= frame["boundary_cursor"]
                    cursor = frame["next_cursor"]
                    for item in frame["items"]:
                        if item["type"] == "market_update" and item["payload"]["market_id"] == report["new_acquisition_market_id"]:
                            observed_new = True
                            report["new_market_delta"] = item
                            report["new_market_wire_sha256"] = hashlib.sha256(encoded).hexdigest()
                    if observed_new and cursor > start_cursor:
                        break
        assert observed_new and cursor > start_cursor
        report["stream"] = dict(initial_cursor=start_cursor, final_cursor=cursor, frames=frames, bytes=wire_bytes, new_market_update=observed_new)
        try:
            await asyncio.to_thread(private.request, dict(operation="publish_scope", **expected, config=config))
            raise AssertionError("stale CAS succeeded")
        except ValueError as error:
            report["stale_cas_rejected"] = str(error)
        # The configured grace is 2s. The old range is no longer referenced
        # before requesting deletion of its source-only identities.
        await asyncio.sleep(2.1)
        current = await asyncio.to_thread(private.status)
        removed = sorted(set(current["admitted_market_ids"])-set(current["referenced_market_ids"]))
        if removed:
            report["retirement"] = await asyncio.to_thread(private.request, dict(operation="retire_acquisition",
                market_ids=removed, expected_scope_id=current["projection_id"], expected_revision=current["revision"]))
            ticket = current["retirement_submitted"]+1
            retirement_deadline = min(deadline, time.monotonic()+15)
            while True:
                current = await asyncio.to_thread(private.status)
                if current["retirement_persisted"] >= ticket and current["acquisition"]["retiring_shards"] == 0:
                    break
                if time.monotonic() >= retirement_deadline:
                    raise TimeoutError("retirement disk/task receipt")
                await asyncio.sleep(.2)
            assert not set(removed).intersection(current["admitted_market_ids"])
            report["retired_market_count"] = len(removed)
        report["final_runtime"] = await asyncio.to_thread(private.status)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", required=True)
    parser.add_argument("--binary-sha256", required=True)
    parser.add_argument("--resume-state", type=Path)
    args = parser.parse_args()
    if not args.run.isascii() or not args.run.isalnum():
        raise ValueError("run identity")
    os.umask(0o077)
    name = "universe-hot-discovery-"+args.run
    unit = "marketcow-"+name+".service"
    output = R/"logs"/(name+"-report.json")
    if output.exists():
        raise ValueError("refuse overwrite")
    private_root = ROOT/("control-"+args.run)
    private_root.mkdir(mode=0o700)  # No new source root; explicit run evidence only.
    endpoint = private_root/"s.sock"
    journal = args.resume_state or private_root/"scope.json"
    if args.resume_state:
        assert journal.resolve(strict=True).is_relative_to(ROOT.resolve())
    binary = R/"target/release/marketcow-discovery-collector"
    assert sha(binary) == args.binary_sha256
    with socket.socket() as check:
        check.bind(("127.0.0.1", 18900))
    processes = subprocess.check_output(["ps", "-eo", "args="], text=True)
    assert not any(str(ROOT) in line and "--root" in line for line in processes.splitlines())
    manifest = json.loads((ROOT/"catalog.json").read_bytes())
    seed = json.loads((ROOT/"discovery-public-seed.json").read_bytes())
    initial_ids = sorted(manifest["realtime_universe"]["market_ids"])
    static = read_static_markets(ROOT, initial_ids, 2097152)
    retained = initial_ids[9:18] if args.resume_state else initial_ids[:9]
    assert len(retained) == 9
    index = R/"phase1/catalog-r1.sqlite"
    with sqlite3.connect(index.as_uri()+"?mode=ro", uri=True) as db:
        db.execute("PRAGMA query_only=ON")
        rows = db.execute("SELECT market_id FROM records WHERE json_extract(payload,'$.active')=1 AND json_extract(payload,'$.closed')=0 ORDER BY CAST(market_id AS INTEGER) DESC LIMIT 2000").fetchall()
    candidates = read_static_markets(ROOT, [mid for (mid,) in rows if mid not in static], 2097152)
    eligible = [m for m in candidates.values() if m.lifecycle_state == "active"
                and not any(r.relation_type == "standard_negative_risk" for r in m.relations)][:100]
    added = None
    # Find one reproducible real book for this acquisition diagnostic. This is
    # not strategy ranking or a production all-healthy admission requirement.
    transport = httpx.HTTPTransport(proxy=httpx.Proxy("http://127.0.0.1:17890"))
    with httpx.Client(transport=transport, trust_env=False, timeout=4) as source_http:
        for offset in range(0, len(eligible), 20):
            batch = eligible[offset:offset+20]
            request = [{"token_id": outcome.token_id} for m in batch for outcome in m.identity.outcomes]
            with source_http.stream("POST", "https://clob.polymarket.com/books", json=request) as response:
                response.raise_for_status(); raw = bytearray()
                for chunk in response.iter_bytes(65536):
                    if len(raw)+len(chunk) > 2097152:
                        raise ValueError("source fixture byte capacity")
                    raw.extend(chunk)
            tokens = {book["asset_id"] for book in json.loads(raw)}
            added = next((m for m in batch if {o.token_id for o in m.identity.outcomes} <= tokens), None)
            if added:
                break
    assert added is not None
    static[added.identity.market_id] = added
    ids = sorted(retained+[added.identity.market_id])
    relations = [r for r in seed["relations"] if set(r["member_market_ids"]).intersection(ids)]
    config = dict(catalog_revision=manifest["catalog_revision"], market_ids=ids, relations=relations,
        settlements={mid: seed["settlements"].get(mid) for mid in ids},
        policy=dict(quantities=seed["depth_quantities"], maximum_book_age_ms=5000))
    config["universe_revision"] = content_sha256({"diagnostic": name, "market_ids": ids})
    config["projection_id"] = content_sha256(config)
    report = dict(passed=False, synthetic_selection_not_ranking=True, binary_sha256=args.binary_sha256,
        formal_changed=False, started_at_ns=time.time_ns(), new_acquisition_market_id=added.identity.market_id,
        records=[static[mid].model_dump(mode="json") for mid in ids], config=config)
    report["expected_initial_market_ids"] = sorted(json.loads(journal.read_bytes())["state"]["active_config"]["market_ids"]) if args.resume_state else initial_ids
    report["restart_from_journal"] = bool(args.resume_state)
    formal = subprocess.check_output(["systemctl", "--user", "cat", "marketcow-polymarket-discovery.service"], text=True)
    start, = [line for line in formal.splitlines() if line.startswith("ExecStart=")]
    command = shlex.split(start.split("=", 1)[1])
    binary_at, = [i for i, value in enumerate(command) if value.endswith("/marketcow-discovery-collector")]
    command = command[binary_at:]; command[0] = str(binary)
    updates = {"--root": str(ROOT), "--plan": str(ROOT/"rust-discovery-plan.json"),
        "--plan-sha256": sha(ROOT/"rust-discovery-plan.json"), "--discovery-seed": str(ROOT/"discovery-public-seed.json"),
        "--discovery-seed-sha256": sha(ROOT/"discovery-public-seed.json"), "--discovery-listen": "127.0.0.1:18900",
        "--poll-seconds": "2"}
    for flag, value in updates.items():
        assert command.count(flag) == 1
        command[command.index(flag)+1] = value
    command += ["--scope-control-socket", str(endpoint), "--scope-control-bytes", "16777216",
        "--scope-control-timeout-seconds", "20", "--scope-retire-grace-seconds", "2",
        "--acquisition-token-budget", "4096", "--acquisition-socket-budget", "128", "--scope-state-file", str(journal),
        "--scope-state-bytes", "134217728"]
    try:
        subprocess.run(["systemd-run", "--user", "--unit="+unit, "--property=MemoryMax=4G",
            "--property=RuntimeMaxSec=100", "--property=KillSignal=SIGINT", "--property=TimeoutStopSec=20",
            "--property=StandardOutput=append:"+str(R/"logs"/(name+".log")),
            "--property=StandardError=append:"+str(R/"logs"/(name+".log")), "/usr/bin/env",
            "-u", "ALL_PROXY", "-u", "all_proxy", "HTTPS_PROXY=http://127.0.0.1:17890",
            "HTTP_PROXY=http://127.0.0.1:17890", "NO_PROXY=127.0.0.1,localhost", *command], check=True, timeout=5)
        asyncio.run(observe("http://127.0.0.1:18900", RustScopeClient(socket_path=endpoint,
            maximum_bytes=16777216, timeout_seconds=20), config, report, unit))
        report["passed"] = True
    except Exception as error:
        report["error"] = repr(error)
        report["traceback"] = traceback.format_exc(limit=6)
    finally:
        report.pop("records", None)
        try:
            subprocess.run(["systemctl", "--user", "stop", unit], check=True, timeout=25)
            report["stopped"] = unit_status(unit)
            assert report["stopped"]["MainPID"] == "0" and report["stopped"]["Result"] == "success" and report["stopped"]["ExecMainStatus"] == "0"
            assert not endpoint.exists()
            report["journal_sha256"] = sha(journal)
            with sqlite3.connect((ROOT/"indexes/latest-state.sqlite3").as_uri()+"?mode=ro", uri=True) as db:
                db.execute("BEGIN")
                assert db.execute("PRAGMA quick_check").fetchone() == ("ok",)
                metadata = dict(db.execute("SELECT key,value FROM metadata"))
                tail = db.execute("SELECT cursor,payload,sha256 FROM recent_events ORDER BY cursor DESC LIMIT 1").fetchone()
                assert tail[0] == int(metadata["latest_cursor"]) and hashlib.sha256(tail[1]).hexdigest() == tail[2]
                assert int(metadata["recent_event_bytes"]) <= int(metadata["bounded_history_bytes"])
                report["durable"] = dict(latest_cursor=tail[0], tail_sha256=tail[2], retained_bytes=metadata["recent_event_bytes"])
        except Exception as error:
            report["cleanup_error"] = repr(error); report["passed"] = False
        report["ended_at_ns"] = time.time_ns()
        with output.open("x") as stream:
            json.dump(report, stream, sort_keys=True); stream.flush(); os.fsync(stream.fileno())
    print(json.dumps({k: report.get(k) for k in ("passed", "error", "traceback", "cleanup_error", "stream", "durable", "stopped")}, sort_keys=True))
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
