"""Run two isolated, bounded, read-only Polymarket WS route probes on U1."""

import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time
from datetime import datetime, timezone
from urllib.request import urlopen

import yaml


ROOT = Path("/mnt/p44pro/marketcow-shadow-v3-runtime/linux")
PROXY = ROOT / "polymarket-proxy"
STATUS = "http://192.168.124.3:8793/v1/prediction-markets/polymarket/research/btc-5m/status"
PYTHON = ROOT / "tradude/source/.venv/bin/python3"
PROBE = Path(__file__).with_name("probe_u1_ws_age.py")
DURATION = 90
ROUTES = (
    ("sg", "🇸🇬 Pro-新加坡-BGP-01|v202605", 17893),
    ("uk", "🇬🇧 英国-Pro-01|v202605", 17896),
    ("fr", "🇫🇷 法国-Pro-01|v202605", 17894),
    ("us", "🇺🇸 Pro-美国-BGP-01|v202605", 17897),
)


def monitor_route_sockets(sidecars, output: Path, stop: threading.Event) -> None:
    """Sample only this probe's TCP sockets; never delay either receive loop."""
    with (output / "tcp-samples.jsonl").open("x", encoding="utf-8") as evidence:
        while not stop.is_set():
            try:
                result = subprocess.run(
                    ["ss", "-tinp"], capture_output=True, text=True, timeout=2, check=True
                )
                lines = result.stdout.splitlines()
                at_ns = time.time_ns()
                for label, process, _ in sidecars:
                    marker = f"pid={process.pid},"
                    for index, line in enumerate(lines):
                        if marker in line:
                            evidence.write(json.dumps({
                                "route": label,
                                "sampled_at_ns": at_ns,
                                "socket": line.strip(),
                                "tcp_info": lines[index + 1].strip() if index + 1 < len(lines) else "",
                            }, separators=(",", ":")) + "\n")
                evidence.flush()
            except (OSError, subprocess.SubprocessError) as error:
                evidence.write(json.dumps({"sampled_at_ns": time.time_ns(), "error": str(error)}) + "\n")
                evidence.flush()
            stop.wait(1)


def port_available(port: int) -> bool:
    with socket.socket() as sock:
        try:
            sock.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


def current_assets(duration: int, current_only: bool) -> tuple[str, list[str], str, str]:
    with urlopen(STATUS, timeout=5) as response:
        status = json.load(response)
    markets = status["profile"]["markets"]
    now = datetime.now(timezone.utc).timestamp()
    current = next(m for m in markets if m["role"] == "current")
    next_market = next(m for m in markets if m["role"] == "next")
    remaining = datetime.fromisoformat(current["end_at"].replace("Z", "+00:00")).timestamp() - now
    if current_only and remaining < duration + 20:
        raise RuntimeError("current market does not have enough time left")
    market = current if current_only or remaining > duration + 20 else next_market
    return market["market_id"], [o["token_id"] for o in market["outcomes"]], market["start_at"], market["end_at"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration-seconds", type=int, default=DURATION)
    parser.add_argument("--route", choices=[label for label, _, _ in ROUTES], action="append")
    parser.add_argument("--current-only", action="store_true", help="wait for an active market with enough time remaining")
    parser.add_argument("--align-next-window", action="store_true", help="start reception at the next market window")
    parser.add_argument("--rust-probe", type=Path, help="use the raw Rust probe binary instead of Python")
    args = parser.parse_args()
    if not 0 < args.duration_seconds <= 300:
        parser.error("duration must be between 1 and 300 seconds")
    if args.current_only and args.align_next_window:
        parser.error("--current-only and --align-next-window are mutually exclusive")
    if args.rust_probe and not args.rust_probe.is_file():
        parser.error(f"Rust probe binary not found: {args.rust_probe}")
    routes = tuple(route for route in ROUTES if args.route is None or route[0] in args.route)
    os.umask(0o077)
    if not all(port_available(port) for _, _, port in routes):
        raise RuntimeError("one of the isolated proxy ports is already in use")
    config = yaml.safe_load((PROXY / "config.yaml").read_text())
    nodes = {node["name"]: node for node in config["proxies"]}
    if args.align_next_window:
        with urlopen(STATUS, timeout=5) as response:
            status = json.load(response)
        market = next(m for m in status["profile"]["markets"] if m["role"] == "next")
        market_id = market["market_id"]
        assets = [o["token_id"] for o in market["outcomes"]]
        market_start, market_end = market["start_at"], market["end_at"]
    elif args.current_only:
        deadline = time.monotonic() + 330
        while True:
            try:
                market_id, assets, market_start, market_end = current_assets(args.duration_seconds, True)
                break
            except RuntimeError as error:
                if str(error) != "current market does not have enough time left" or time.monotonic() >= deadline:
                    raise
                time.sleep(1)
    else:
        market_id, assets, market_start, market_end = current_assets(args.duration_seconds, False)
    output = ROOT / "diagnostics" / (("ws-rust-pro-" if args.rust_probe else "ws-europe-pro-") + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    output.mkdir(mode=0o700)
    (output / "manifest.json").write_text(json.dumps({"market_id": market_id, "asset_ids": assets, "market_start": market_start, "market_end": market_end, "duration_seconds": args.duration_seconds, "routes": [name for name, _, _ in routes], "current_only": args.current_only, "align_next_window": args.align_next_window, "probe": "rust" if args.rust_probe else "python"}, indent=2))
    sidecars = []
    probes = []
    monitor_stop = threading.Event()
    monitor = None
    with tempfile.TemporaryDirectory(prefix="ws-europe-pro-", dir=ROOT / "tmp") as temp:
        try:
            for label, name, port in routes:
                if name not in nodes:
                    raise RuntimeError(f"missing configured node: {name}")
                route_dir = output / label
                route_dir.mkdir(mode=0o700)
                config_dir = Path(temp) / label
                config_dir.mkdir(mode=0o700)
                local = dict(config)
                local.update({
                    "mixed-port": port,
                    "proxies": [nodes[name]],
                    "proxy-groups": [{"name": "POLYMARKET", "type": "select", "proxies": [name]}],
                    "rules": ["DOMAIN-SUFFIX,polymarket.com,POLYMARKET", "MATCH,REJECT"],
                    "profile": {"store-selected": False},
                    "log-level": "warning",
                })
                config_path = config_dir / "config.yaml"
                config_path.write_text(yaml.safe_dump(local, allow_unicode=True))
                proxy_log = (route_dir / "proxy.log").open("wb")
                sidecar = subprocess.Popen([str(PROXY / "mihomo"), "-d", str(config_dir), "-f", str(config_path)], stdout=proxy_log, stderr=subprocess.STDOUT)
                sidecars.append((label, sidecar, proxy_log))
            for _, _, port in routes:
                for _ in range(100):
                    try:
                        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                            break
                    except OSError:
                        time.sleep(0.05)
                else:
                    raise RuntimeError(f"isolated proxy did not listen on {port}")
            if args.align_next_window:
                start_at = datetime.fromisoformat(market_start.replace("Z", "+00:00")).timestamp()
                while time.time() < start_at + 1:
                    time.sleep(min(0.2, start_at + 1 - time.time()))
            for label, _, port in routes:
                route_dir = output / label
                run_log = (route_dir / "run.log").open("wb")
                command = ([str(args.rust_probe)] if args.rust_probe else [str(PYTHON), str(PROBE)]) + ["--duration-seconds", str(args.duration_seconds), "--proxy-port", str(port), "--output", str(route_dir / "raw-messages.jsonl")]
                for asset in assets:
                    command += ["--asset-id", asset]
                probe = subprocess.Popen(command, stdout=run_log, stderr=subprocess.STDOUT)
                probes.append((probe, run_log))
            monitor = threading.Thread(
                target=monitor_route_sockets,
                args=(sidecars, output, monitor_stop),
                daemon=True,
            )
            monitor.start()
            for probe, _ in probes:
                try:
                    probe.wait(timeout=args.duration_seconds + 25)
                except subprocess.TimeoutExpired:
                    probe.terminate()
                    probe.wait(timeout=5)
        finally:
            monitor_stop.set()
            if monitor is not None:
                monitor.join(timeout=3)
            for probe, log in probes:
                if probe.poll() is None:
                    probe.terminate()
                    probe.wait(timeout=5)
                log.close()
            for _, sidecar, log in sidecars:
                if sidecar.poll() is None:
                    sidecar.terminate()
                    sidecar.wait(timeout=5)
                log.close()
    print(json.dumps({"output": str(output), "market_id": market_id, "probe_exit_codes": {routes[i][0]: p.returncode for i, (p, _) in enumerate(probes)}, "sidecars_stopped": all(p.poll() is not None for _, p, _ in sidecars)}))


if __name__ == "__main__":
    main()
