"""Finite same-process Live250 CAS and new-token WS check; no formal/Paper changes."""
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
from websockets.asyncio.client import connect

from marketcow.polymarket_contracts import content_sha256
from marketcow.universe_live_candidate import read_static_markets
from marketcow.universe_rust_control import RustScopeClient
from check_u1_hot_discovery import R, sha, unit_status

ROOT = R/"public-rust-candidate-r1"


async def observe(private, config, record, report, unit, prefix):
    deadline = time.monotonic()+70
    base = "http://127.0.0.1:8794/v1/prediction-markets/polymarket/live"
    async with httpx.AsyncClient(trust_env=False, timeout=5) as http:
        while True:
            if unit_status(unit)["MainPID"] == "0":
                raise RuntimeError("Live candidate exited during startup")
            try:
                actual = await asyncio.to_thread(private.status)
                if actual["source_readable"] and actual["source_cursor"] > 0:
                    break
            except (OSError, ValueError, RuntimeError):
                pass
            if time.monotonic() >= deadline:
                raise TimeoutError("Live startup")
            await asyncio.sleep(.2)
        report["initial"] = actual
        initial = await http.get(base+"/full-sync", params={"scope_id": actual["scope_id"]})
        initial.raise_for_status()
        assert len(initial.content) <= 134217728
        baseline = initial.json()
        assert len(baseline["bootstrap"]["markets"]) == 250
        report["old_fullsync_sha256"] = hashlib.sha256(initial.content).hexdigest()
        old_book_tokens = set(baseline["snapshot"]["books"])
        expected = dict(expected_scope_id=actual["scope_id"], expected_revision=actual["revision"])
        old_uri = base.replace("http://", "ws://", 1)+"/stream?"+urlencode(dict(scope_id=actual["scope_id"], after_cursor=baseline["cursor"]))
        async with connect(old_uri, proxy=None, max_size=67108864, max_queue=1, close_timeout=1) as old:
            cursor = baseline["cursor"]; ready = False; old_count = 0; old_bytes = 0
            while not ready:
                raw = await asyncio.wait_for(old.recv(), max(1, deadline-time.monotonic()))
                old_bytes += len(raw.encode() if isinstance(raw, str) else raw); old_count += 1
                assert old_count <= 20000 and old_bytes <= 67108864
                frame = json.loads(raw)
                assert frame["type"] != "error", frame
                if frame["type"] == "event":
                    assert frame["event"]["cursor"] == frame["cursor"] > cursor; cursor = frame["cursor"]
                elif frame["type"] == "ready":
                    assert frame["stream_instance_id"] == baseline["stream_instance_id"]
                    ready = True
            # Drain old scope while preparation runs; it must not depend on a
            # complete batch, new market, or disk publication acknowledgement.
            async def drain_old():
                nonlocal cursor, old_count, old_bytes
                while True:
                    raw = await old.recv(); old_count += 1
                    old_bytes += len(raw.encode() if isinstance(raw, str) else raw)
                    assert old_count <= 40000 and old_bytes <= 134217728
                    frame = json.loads(raw)
                    if frame["type"] == "error":
                        report["old_scope_end"] = frame
                        assert frame["code"] == "polymarket_scope_changed"
                        return
                    if frame["type"] == "event":
                        assert frame["event"]["cursor"] == frame["cursor"] > cursor; cursor = frame["cursor"]
            drain = asyncio.create_task(drain_old())
            try:
                report["acquisition"] = await asyncio.to_thread(private.request, dict(operation="prepare_acquisition", **expected,
                    catalog_revision=config["catalog_revision"], records=[record], evidence_sha256=content_sha256([record]),
                    acquisition_market_ids=[record["identity"]["market_id"]]))
                assert report["acquisition"]["acquisition_installed"] and not report["acquisition"]["publication_applied"]
                unchanged = await asyncio.to_thread(private.status)
                assert unchanged["scope_id"] == actual["scope_id"] and unchanged["revision"] == actual["revision"]
                report["prepare"] = await asyncio.to_thread(private.request, dict(operation="prepare_publication", **expected, config=config))
                report["activation"] = await asyncio.to_thread(private.request, dict(operation="publish_scope", **expected, config=config))
                await asyncio.wait_for(drain, 10)
                report["old_scope_cursor"] = cursor
                assert report["activation"]["actual"]["stream_instance_id"] == baseline["stream_instance_id"]
            finally:
                if not drain.done():
                    drain.cancel()
                await asyncio.gather(drain, return_exceptions=True)
        new = await http.get(base+"/full-sync", params={"scope_id": config["active_scope_id"]})
        new.raise_for_status()
        assert len(new.content) <= 134217728
        with prefix.with_suffix(".fullsync.json").open("xb") as output:
            output.write(new.content)
        baseline = new.json()
        assert sorted(m["identity"]["market_id"] for m in baseline["bootstrap"]["markets"]) == sorted(m["market_id"] for m in config["configured_markets"])
        report["new_fullsync_sha256"] = hashlib.sha256(new.content).hexdigest()
        tokens = {outcome["token_id"] for outcome in record["identity"]["outcomes"]}
        baseline_books = {token: baseline["snapshot"]["books"][token] for token in tokens
                          if token in baseline["snapshot"]["books"] and token not in old_book_tokens}
        report["new_market_baseline_books"] = baseline_books
        # An authoritative unchanged/empty book may arrive during preparation.
        # It is already evidence in full-sync, not a promised future delta.
        cursor = baseline["cursor"]; ready = False; seen_new = bool(baseline_books); counts = {}; total = 0
        recovery_facts = []
        uri = base.replace("http://", "ws://", 1)+"/stream?"+urlencode(dict(scope_id=config["active_scope_id"], after_cursor=cursor))
        async with asyncio.timeout(max(1, deadline-time.monotonic())):
            async with connect(uri, proxy=None, max_size=67108864, max_queue=1, close_timeout=1) as ws:
                with prefix.with_suffix(".ws.jsonl").open("xb") as output:
                    while sum(counts.values()) < 30000 and total < 67108864:
                        raw = await ws.recv(); encoded = raw.encode() if isinstance(raw, str) else raw
                        if total+len(encoded)+1 > 67108864:
                            raise ValueError("Live wire evidence budget")
                        output.write(encoded+b"\n"); total += len(encoded)+1
                        frame = json.loads(encoded); kind = frame["type"]; counts[kind] = counts.get(kind, 0)+1
                        assert kind != "error", frame
                        if kind == "event":
                            assert frame["cursor"] == frame["event"]["cursor"] > cursor
                            cursor = frame["cursor"]
                            event = frame["event"]
                            if event["market_id"] == record["identity"]["market_id"] and event["event_type"] == "book":
                                seen_new = True
                            if event["market_id"] == record["identity"]["market_id"] and event["event_type"] == "recovery_started":
                                if len(recovery_facts) < 2:
                                    recovery_facts.append(event)
                        elif kind == "ready":
                            assert frame["stream_instance_id"] == baseline["stream_instance_id"]
                            ready = True
                        report["stream_progress"] = dict(counts=dict(counts), bytes=total, cursor=cursor,
                            new_market_book=seen_new, new_market_recovery_facts=recovery_facts)
                        if ready and (seen_new or recovery_facts) and counts.get("book_confirmations", 0) and counts.get("event", 0) >= 100:
                            break
        assert ready and (seen_new or recovery_facts) and counts.get("book_confirmations", 0)
        report["stream"] = report["stream_progress"]
        report["new_market_trade_eligibility_verified"] = False
        report["final_runtime"] = await asyncio.to_thread(private.status)
        pid = unit_status(unit)["MainPID"]
        report["resources"] = [line for line in Path(f"/proc/{pid}/status").read_text().splitlines()
                               if line.startswith(("VmRSS:", "VmHWM:", "Threads:"))]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", required=True)
    parser.add_argument("--binary-sha256", required=True)
    parser.add_argument("--added-market-id", required=True)
    parser.add_argument("--resume-state", type=Path)
    args = parser.parse_args()
    assert args.run.isascii() and args.run.isalnum() and args.added_market_id.isdigit()
    os.umask(0o077)
    unit = "marketcow-universe-hot-live-"+args.run+".service"
    prefix = R/"logs"/("universe-hot-live-"+args.run)
    assert not prefix.with_suffix(".report.json").exists()
    private_root = ROOT/("control-"+args.run); private_root.mkdir(mode=0o700)
    endpoint = private_root/"s.sock"; journal = args.resume_state or private_root/"scope.json"
    if args.resume_state:
        assert journal.resolve(strict=True).is_relative_to(ROOT.resolve())
    binary = R/"target/release/marketcow-discovery-collector"
    assert sha(binary) == args.binary_sha256
    with socket.socket() as check:
        check.bind(("127.0.0.1", 8794))
    command_lines = subprocess.check_output(["ps", "-eo", "args="], text=True)
    assert not any(str(ROOT) in line and "--root" in line for line in command_lines.splitlines())
    config = (json.loads(journal.read_bytes())["state"]["active_config"] if args.resume_state
              else json.loads((ROOT/"configured-scope.json").read_bytes()))
    assert args.added_market_id not in {row["market_id"] for row in config["configured_markets"]}
    market = read_static_markets(ROOT, [args.added_market_id], 2097152)[args.added_market_id]
    assert not any(r.relation_type == "standard_negative_risk" for r in market.relations)
    row = dict(market_id=market.identity.market_id, condition_id=market.identity.condition_id,
               token_ids=[o.token_id for o in market.identity.outcomes], end_at=market.end_at.isoformat().replace("+00:00", "Z") if market.end_at else None)
    config["configured_markets"] = sorted(config["configured_markets"][1:]+[row], key=lambda row: row["market_id"])
    config["active_scope_id"] = content_sha256({k: config[k] for k in ("catalog_revision", "configured_markets", "mode")})
    formal = subprocess.check_output(["systemctl", "--user", "cat", "marketcow-polymarket-collector.service"], text=True)
    start, = [line for line in formal.splitlines() if line.startswith("ExecStart=")]
    command = shlex.split(start.split("=", 1)[1]); at, = [i for i, v in enumerate(command) if v.endswith("/marketcow-discovery-collector")]
    command = command[at:]; command[0] = str(binary)
    for flag, value in {"--root": str(ROOT), "--plan": str(ROOT/"rust-scoped-plan-r1.json"),
        "--plan-sha256": sha(ROOT/"rust-scoped-plan-r1.json"), "--configured-scope": str(ROOT/"configured-scope.json"),
        "--configured-scope-sha256": sha(ROOT/"configured-scope.json"), "--dependency-plan": str(ROOT/"live-bridge-plan-r1.json"),
        "--dependency-plan-sha256": sha(ROOT/"live-bridge-plan-r1.json"), "--public-listen": "127.0.0.1:8794"}.items():
        assert command.count(flag) == 1; command[command.index(flag)+1] = value
    command += ["--scope-control-socket", str(endpoint), "--scope-control-bytes", "16777216", "--scope-state-bytes", "134217728",
        "--scope-control-timeout-seconds", "20", "--scope-retire-grace-seconds", "2", "--acquisition-token-budget", "4096",
        "--acquisition-socket-budget", "16", "--scope-state-file", str(journal)]
    report = dict(passed=False, formal_changed=False, test_selection_not_strategy=True, binary_sha256=args.binary_sha256,
                  added_market_id=args.added_market_id, started_at_ns=time.time_ns())
    try:
        subprocess.run(["systemd-run", "--user", "--unit="+unit, "--property=MemoryMax=4G", "--property=RuntimeMaxSec=110",
            "--property=KillSignal=SIGINT", "--property=TimeoutStopSec=25", "--property=StandardOutput=append:"+str(prefix.with_suffix(".log")),
            "--property=StandardError=append:"+str(prefix.with_suffix(".log")), "/usr/bin/env", "-u", "ALL_PROXY", "-u", "all_proxy",
            "HTTPS_PROXY=http://127.0.0.1:17890", "HTTP_PROXY=http://127.0.0.1:17890", "NO_PROXY=127.0.0.1,localhost", *command], check=True, timeout=5)
        asyncio.run(observe(RustScopeClient(socket_path=endpoint, maximum_bytes=16777216, timeout_seconds=20),
            config, market.model_dump(mode="json"), report, unit, prefix))
        report["passed"] = True
    except Exception as error:
        report.update(error=repr(error), traceback=traceback.format_exc(limit=6))
    finally:
        try:
            subprocess.run(["systemctl", "--user", "stop", unit], check=True, timeout=30)
            report["stopped"] = unit_status(unit)
            assert report["stopped"]["MainPID"] == "0" and report["stopped"]["Result"] == "success" and report["stopped"]["ExecMainStatus"] == "0"
            assert not endpoint.exists()
            report["journal_sha256"] = sha(journal)
            with sqlite3.connect((ROOT/"indexes/latest-state.sqlite3").as_uri()+"?mode=ro", uri=True) as db:
                db.execute("BEGIN"); assert db.execute("PRAGMA quick_check").fetchone() == ("ok",)
                metadata = dict(db.execute("SELECT key,value FROM metadata"))
                tail = db.execute("SELECT cursor,payload,sha256 FROM recent_events ORDER BY cursor DESC LIMIT 1").fetchone()
                assert tail[0] == int(metadata["latest_cursor"]) and hashlib.sha256(tail[1]).hexdigest() == tail[2]
                assert int(metadata["recent_event_bytes"]) <= int(metadata["bounded_history_bytes"])
                report["durable"] = dict(latest_cursor=tail[0], sha256=tail[2], retained_bytes=metadata["recent_event_bytes"])
        except Exception as error:
            report.update(cleanup_error=repr(error), passed=False)
        report["ended_at_ns"] = time.time_ns()
        with prefix.with_suffix(".report.json").open("x") as output:
            json.dump(report, output, sort_keys=True); output.flush(); os.fsync(output.fileno())
    print(json.dumps({k: report.get(k) for k in ("passed", "error", "traceback", "cleanup_error", "stream", "resources", "durable", "stopped")}))
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
