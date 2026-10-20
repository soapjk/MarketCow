#!/usr/bin/env python3
"""Count U1 public Internet traffic and send one daily ntfy alert.

The nftables rules only observe packets addressed to this host. They use an
independent table with accept policies and never alter packet disposition.
Run ``check`` as the service user and ``run`` as root (normally via systemd).
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
from typing import Callable
from urllib import error, request
from zoneinfo import ZoneInfo


TABLE = "u1_external_traffic"
COUNTERS = ("ingress_v4", "egress_v4", "ingress_v6", "egress_v6")
DEFAULT_STATE = Path("/var/lib/u1-external-traffic/state.json")
DEFAULT_THRESHOLD = 50 * 1000**3
DEFAULT_INTERVAL = 60
TIMEZONE = ZoneInfo("Asia/Shanghai")

# A packet is public when its remote endpoint is outside these ranges. The
# inbound destination may be private because U1 is behind NAT, so only source
# is classified for ingress and only destination for egress.
NON_PUBLIC_V4 = (
    "0.0.0.0/8, 10.0.0.0/8, 100.64.0.0/10, 127.0.0.0/8, "
    "169.254.0.0/16, 172.16.0.0/12, 192.0.0.0/24, 192.0.2.0/24, "
    "192.168.0.0/16, 198.18.0.0/15, 198.51.100.0/24, "
    "203.0.113.0/24, 224.0.0.0/4, 240.0.0.0/4"
)
NON_PUBLIC_V6 = (
    "::/128, ::1/128, 100::/64, 2001:db8::/32, fc00::/7, "
    "fe80::/10, ff00::/8"
)


@dataclass(frozen=True)
class Snapshot:
    ingress_v4: int
    egress_v4: int
    ingress_v6: int
    egress_v6: int

    @property
    def ingress(self) -> int:
        return self.ingress_v4 + self.ingress_v6

    @property
    def egress(self) -> int:
        return self.egress_v4 + self.egress_v6


@dataclass
class DailyState:
    schema_version: int
    day: str
    last: dict[str, int]
    ingress_bytes: int = 0
    egress_bytes: int = 0
    notified: bool = False
    notification_attempts: int = 0

    @property
    def total_bytes(self) -> int:
        return self.ingress_bytes + self.egress_bytes


def nft_ruleset() -> str:
    return f"""table inet {TABLE} {{
    counter ingress_v4 {{ }}
    counter egress_v4 {{ }}
    counter ingress_v6 {{ }}
    counter egress_v6 {{ }}

    chain count_input {{
        type filter hook input priority raw; policy accept;
        iifname != \"lo\" ip saddr != {{ {NON_PUBLIC_V4} }} counter name ingress_v4
        iifname != \"lo\" ip6 saddr != {{ {NON_PUBLIC_V6} }} counter name ingress_v6
    }}

    chain count_output {{
        type filter hook output priority raw; policy accept;
        oifname != \"lo\" ip daddr != {{ {NON_PUBLIC_V4} }} counter name egress_v4
        oifname != \"lo\" ip6 daddr != {{ {NON_PUBLIC_V6} }} counter name egress_v6
    }}
}}
"""


def run_nft(*args: str, stdin: str | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["nft", *args], input=stdin, text=True, check=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )


def ensure_counters() -> None:
    probe = subprocess.run(
        ["nft", "list", "table", "inet", TABLE], text=True,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    if probe.returncode != 0:
        run_nft("-f", "-", stdin=nft_ruleset())
    # Refuse partial or foreign tables instead of replacing firewall state.
    snapshot = read_snapshot()
    if set(asdict(snapshot)) != set(COUNTERS):
        raise RuntimeError(f"nft table {TABLE} does not contain the expected counters")


def parse_nft_json(document: str) -> Snapshot:
    payload = json.loads(document)
    values: dict[str, int] = {}
    for entry in payload.get("nftables", []):
        counter = entry.get("counter")
        if not isinstance(counter, dict) or counter.get("table") != TABLE:
            continue
        name = counter.get("name")
        if name in COUNTERS:
            values[name] = int(counter["bytes"])
    missing = set(COUNTERS) - set(values)
    if missing:
        raise RuntimeError("missing nft counters: " + ", ".join(sorted(missing)))
    return Snapshot(**values)


def read_snapshot() -> Snapshot:
    result = run_nft("-j", "list", "table", "inet", TABLE)
    return parse_nft_json(result.stdout)


def load_state(path: Path) -> DailyState | None:
    try:
        payload = json.loads(path.read_text())
    except FileNotFoundError:
        return None
    if payload.get("schema_version") != 1:
        raise RuntimeError(f"unsupported state schema in {path}")
    last = payload.get("last")
    if not isinstance(last, dict) or set(last) != set(COUNTERS):
        raise RuntimeError(f"invalid counter state in {path}")
    return DailyState(**payload)


def save_state(path: Path, state: DailyState) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, temporary = tempfile.mkstemp(prefix=".state-", dir=path.parent)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w") as stream:
            json.dump(asdict(state), stream, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.close(descriptor)
        except OSError:
            pass
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def counter_delta(current: int, previous: int) -> int:
    if current < 0 or previous < 0:
        raise ValueError("counter values must be non-negative")
    # A smaller value means the nft table was recreated or the counter wrapped.
    # Bytes already sampled remain in DailyState; count only the new value.
    return current - previous if current >= previous else current


def update_state(
    state: DailyState | None, snapshot: Snapshot, day: str
) -> DailyState:
    current = asdict(snapshot)
    if state is None or state.day != day:
        # Never assign an interval spanning midnight to the wrong day. At the
        # default cadence this deliberately leaves at most 60 seconds uncounted.
        return DailyState(schema_version=1, day=day, last=current)
    state.ingress_bytes += sum(
        counter_delta(current[name], state.last[name])
        for name in ("ingress_v4", "ingress_v6")
    )
    state.egress_bytes += sum(
        counter_delta(current[name], state.last[name])
        for name in ("egress_v4", "egress_v6")
    )
    state.last = current
    return state


def human_bytes(value: int) -> str:
    return f"{value / 1000**3:.2f} GB"


def send_ntfy(message: str, *, timeout: float = 10.0) -> None:
    url = os.environ.get("U1_TRAFFIC_NTFY_URL")
    if not url:
        raise RuntimeError("U1_TRAFFIC_NTFY_URL is not configured")
    headers = {
        "Content-Type": "text/plain; charset=utf-8",
        "Title": "U1 traffic alert",
        "Priority": "high",
        "Tags": "warning,chart_with_upwards_trend",
    }
    token = os.environ.get("U1_TRAFFIC_NTFY_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    outgoing = request.Request(
        url, data=message.encode("utf-8"), headers=headers, method="POST"
    )
    try:
        with request.urlopen(outgoing, timeout=timeout) as response:
            if not 200 <= response.status < 300:
                raise RuntimeError(f"ntfy returned HTTP {response.status}")
    except error.URLError as exc:
        raise RuntimeError(f"ntfy delivery failed: {exc.reason}") from exc


def sample_once(
    state_path: Path,
    threshold: int,
    notifier: Callable[[str], None] = send_ntfy,
    *,
    now: datetime | None = None,
    snapshot: Snapshot | None = None,
) -> DailyState:
    moment = now or datetime.now(TIMEZONE)
    if moment.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    counters = snapshot or read_snapshot()
    state = update_state(load_state(state_path), counters, moment.astimezone(TIMEZONE).date().isoformat())
    if state.total_bytes >= threshold and not state.notified:
        state.notification_attempts += 1
        save_state(state_path, state)
        message = (
            f"U1 public Internet traffic for {state.day} reached "
            f"{human_bytes(state.total_bytes)} "
            f"(in {human_bytes(state.ingress_bytes)}, "
            f"out {human_bytes(state.egress_bytes)}), above "
            f"{human_bytes(threshold)}."
        )
        try:
            notifier(message)
        except Exception:
            # Persist the measurement but leave notified false so the next
            # interval retries delivery.
            save_state(state_path, state)
            raise
        state.notified = True
    save_state(state_path, state)
    return state


def check_environment(state_path: Path) -> int:
    report = {
        "python": sys.version.split()[0],
        "nft": shutil.which("nft"),
        "effective_uid": os.geteuid(),
        "state_path": str(state_path),
        "state_exists": state_path.exists(),
        "ntfy_url_configured": bool(os.environ.get("U1_TRAFFIC_NTFY_URL")),
        "ntfy_token_configured": bool(os.environ.get("U1_TRAFFIC_NTFY_TOKEN")),
        "timezone": str(TIMEZONE),
        "threshold_bytes": DEFAULT_THRESHOLD,
    }
    errors = []
    if not report["nft"]:
        errors.append("nft executable is missing")
    if os.geteuid() != 0:
        errors.append("root is required to read and create nft counters")
    print(json.dumps({"ok": not errors, "errors": errors, **report}, indent=2))
    return 0 if not errors else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ("check", "once", "run"):
        command = subparsers.add_parser(name)
        command.add_argument("--state", type=Path, default=DEFAULT_STATE)
    for name in ("once", "run"):
        command = subparsers.choices[name]
        command.add_argument("--threshold-gb", type=float, default=50.0)
    subparsers.choices["run"].add_argument(
        "--interval", type=int, default=DEFAULT_INTERVAL
    )
    args = parser.parse_args()
    if args.command == "check":
        return check_environment(args.state)
    if os.geteuid() != 0:
        parser.error("once/run must execute as root to access nftables")
    if args.threshold_gb <= 0:
        parser.error("--threshold-gb must be positive")
    ensure_counters()
    threshold = int(args.threshold_gb * 1000**3)
    if args.command == "once":
        state = sample_once(args.state, threshold)
        print(json.dumps(asdict(state), sort_keys=True))
        return 0
    if args.interval < 1:
        parser.error("--interval must be positive")
    while True:
        try:
            state = sample_once(args.state, threshold)
            print(
                f"{datetime.now(TIMEZONE).isoformat()} day={state.day} "
                f"in={state.ingress_bytes} out={state.egress_bytes} "
                f"notified={state.notified}",
                flush=True,
            )
        except Exception as exc:
            print(f"monitor error: {exc}", file=sys.stderr, flush=True)
        time.sleep(args.interval)


if __name__ == "__main__":
    raise SystemExit(main())
