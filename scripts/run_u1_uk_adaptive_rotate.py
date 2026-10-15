"""Isolated five-minute UK-Pro A/B WebSocket rotation experiment on U1."""

import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from urllib.request import urlopen

import yaml


ROOT = Path("/mnt/p44pro/marketcow-shadow-v3-runtime/linux")
PROXY = ROOT / "polymarket-proxy"
PROBE = ROOT / "diagnostics/ws-dual-rotate-1wmvjHdZ/target/release/adaptive_rotate"
STATUS = "http://192.168.124.3:8793/v1/prediction-markets/polymarket/research/btc-5m/status"
NODE = "🇬🇧 英国-Pro-01|v202605"
PORT = 17896


def main() -> None:
    os.umask(0o077)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", PORT))
    with urlopen(STATUS, timeout=5) as response:
        status = json.load(response)
    market = next(m for m in status["profile"]["markets"] if m["role"] == "next")
    start_ms = int(datetime.fromisoformat(market["start_at"].replace("Z", "+00:00")).timestamp() * 1000)
    if start_ms - int(time.time() * 1000) < 15000:
        raise RuntimeError("next window begins too soon to prepare the isolated route")
    assets = [o["token_id"] for o in market["outcomes"]]
    if len(assets) != 2 or not PROBE.is_file():
        raise RuntimeError("missing two-token market or probe")

    private = yaml.safe_load((PROXY / "config.yaml").read_text())
    node = next((n for n in private["proxies"] if n["name"] == NODE), None)
    if node is None:
        raise RuntimeError("UK Pro node not configured")
    output = ROOT / "diagnostics" / ("ws-uk-adaptive-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    output.mkdir(mode=0o700)
    (output / "manifest.json").write_text(json.dumps({
        "route": "UK Pro", "proxy_port": PORT, "market_id": market["market_id"],
        "market_start": market["start_at"], "market_end": market["end_at"],
        "asset_ids": assets, "start_at_ms": start_ms, "duration_seconds": 300,
        "probe": str(PROBE), "prepare_age_ms": 1200, "switch_age_ms": 1500,
        "switch_dwell_ms": 500,
    }, indent=2))
    with tempfile.TemporaryDirectory(prefix="ws-uk-adaptive-", dir=ROOT / "tmp") as temporary:
        config_dir = Path(temporary)
        local = dict(private)
        local.update({
            "mixed-port": PORT,
            "proxies": [node],
            "proxy-groups": [{"name": "POLYMARKET", "type": "select", "proxies": [NODE]}],
            "rules": ["DOMAIN-SUFFIX,polymarket.com,POLYMARKET", "MATCH,REJECT"],
            "profile": {"store-selected": False},
            "log-level": "warning",
        })
        (config_dir / "config.yaml").write_text(yaml.safe_dump(local, allow_unicode=True))
        with (output / "proxy.log").open("wb") as proxy_log, (output / "run.jsonl").open("wb") as run_log:
            sidecar = subprocess.Popen([str(PROXY / "mihomo"), "-d", str(config_dir), "-f", str(config_dir / "config.yaml")], stdout=proxy_log, stderr=subprocess.STDOUT)
            try:
                for _ in range(100):
                    if sidecar.poll() is not None:
                        raise RuntimeError("isolated UK sidecar exited before listening")
                    try:
                        with socket.create_connection(("127.0.0.1", PORT), timeout=0.2):
                            break
                    except OSError:
                        time.sleep(0.05)
                else:
                    raise RuntimeError("isolated UK sidecar did not listen")
                command = [str(PROBE), "--proxy-host", "127.0.0.1", "--proxy-port", str(PORT),
                           "--start-at-ms", str(start_ms), "--duration-seconds", "300"]
                for asset in assets:
                    command += ["--asset-id", asset]
                probe = subprocess.Popen(command, stdout=run_log, stderr=subprocess.STDOUT)
                try:
                    probe.wait(timeout=max(330, (start_ms / 1000 - time.time()) + 330))
                except subprocess.TimeoutExpired:
                    probe.terminate()
                    probe.wait(timeout=5)
                run_log.flush()
                result = {"output": str(output), "probe_exit_code": probe.returncode,
                          "sidecar_alive_after_probe": sidecar.poll() is None}
            finally:
                if sidecar.poll() is None:
                    sidecar.terminate()
                    sidecar.wait(timeout=5)
    result["sidecar_stopped"] = sidecar.poll() is not None
    (output / "result.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result))


if __name__ == "__main__":
    main()
