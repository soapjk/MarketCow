#!/usr/bin/env python3
from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

from dotenv import load_dotenv


DEFAULT_PROJECT_DIR = Path("/Volumes/T9/projects/marketcow")
DEFAULT_DATA_ROOT = Path("/Volumes/T9/data/marketcow/production")
LOOPBACK_HOSTS = {"127.0.0.1", "::1", "localhost"}


@dataclass(frozen=True)
class Service:
    name: str
    command: tuple[str, ...]


def _required_path(environment: Mapping[str, str], name: str) -> Path:
    raw = environment.get(name, "").strip()
    if not raw:
        raise ValueError(f"{name} is required for the production Polymarket scope")
    path = Path(raw)
    if not path.is_absolute():
        raise ValueError(f"{name} must be an absolute path")
    return path.resolve(strict=True)


def build_services(
    project_dir: Path,
    environment: Mapping[str, str],
    *,
    python: str = sys.executable,
) -> tuple[Service, ...]:
    """Build all non-storage processes in the production data-center stack."""
    project_dir = project_dir.resolve(strict=True)
    data_root = Path(environment.get("MARKETCOW_HOME", str(DEFAULT_DATA_ROOT)))
    if not data_root.is_absolute():
        raise ValueError("MARKETCOW_HOME must be an absolute path")
    live_root = data_root / "prediction-markets" / "polymarket-live"

    host = environment.get("MARKETCOW_HOST", "127.0.0.1")
    if host not in LOOPBACK_HOSTS:
        raise ValueError("production MarketCow APIs must bind to loopback")
    api_port = int(environment.get("MARKETCOW_PORT", "8790"))
    read_api_port = int(environment.get("MARKETCOW_POLYMARKET_READ_API_PORT", "8791"))
    stream_host = "127.0.0.1"
    stream_port = 8794
    for name, port in (
        ("MARKETCOW_PORT", api_port),
        ("MARKETCOW_POLYMARKET_READ_API_PORT", read_api_port),
        ("Polymarket live stream port", stream_port),
    ):
        if not 1 <= port <= 65535:
            raise ValueError(f"{name} must be in [1, 65535]")
    if len({api_port, read_api_port, stream_port}) != 3:
        raise ValueError("MarketCow API, read API and live stream ports must differ")

    manifest = _required_path(environment, "MARKETCOW_POLYMARKET_SCOPE_MANIFEST")
    report = _required_path(environment, "MARKETCOW_POLYMARKET_SELECTION_REPORT")
    candidates = _required_path(environment, "MARKETCOW_POLYMARKET_CANDIDATE_SNAPSHOT")
    tradude_worktree = _required_path(environment, "MARKETCOW_POLYMARKET_TRADUDE_WORKTREE")
    consumer_age = environment.get("MARKETCOW_POLYMARKET_CONSUMER_MAXIMUM_BOOK_AGE_SECONDS", "5.0")
    delivery_headroom = environment.get("MARKETCOW_POLYMARKET_MINIMUM_DELIVERY_HEADROOM_SECONDS", "1.0")
    stream_uri = f"ws://{stream_host}:{stream_port}"

    collector = Service(
        "polymarket-collector",
        (
            python,
            str(project_dir / "scripts" / "run_polymarket_live_paper_scope.py"),
            "--root",
            str(live_root),
            "--scope-manifest",
            str(manifest),
            "--selection-report",
            str(report),
            "--candidate-snapshot",
            str(candidates),
            "--tradude-worktree",
            str(tradude_worktree),
            "--snapshot-refresh-seconds",
            environment.get("MARKETCOW_POLYMARKET_SNAPSHOT_REFRESH_SECONDS", "2.0"),
        ),
    )
    shared_api = Service(
        "shared-api",
        (
            python,
            "-m",
            "marketcow",
            "--profile",
            environment.get("MARKETCOW_PROFILE", "production"),
            "start",
            "--host",
            host,
            "--port",
            str(api_port),
        ),
    )
    read_api = Service(
        "polymarket-read-api",
        (
            python,
            str(project_dir / "scripts" / "run_polymarket_live_read_api.py"),
            "--root",
            str(live_root),
            "--host",
            "127.0.0.1",
            "--port",
            str(read_api_port),
            "--stable-snapshot-max-book-age-seconds",
            environment.get("MARKETCOW_POLYMARKET_STABLE_SNAPSHOT_MAX_BOOK_AGE_SECONDS", "3.5"),
            "--consumer-maximum-book-age-seconds",
            consumer_age,
            "--minimum-delivery-headroom-seconds",
            delivery_headroom,
            "--stable-read-wait-seconds",
            environment.get("MARKETCOW_POLYMARKET_STABLE_READ_WAIT_SECONDS", "6.0"),
            "--stable-read-poll-seconds",
            environment.get("MARKETCOW_POLYMARKET_STABLE_READ_POLL_SECONDS", "0.025"),
            "--executor-workers",
            environment.get("MARKETCOW_POLYMARKET_READ_EXECUTOR_WORKERS", "4"),
            "--live-stream-uri",
            stream_uri,
        ),
    )
    return collector, shared_api, read_api


def supervise(
    services: Sequence[Service],
    *,
    project_dir: Path,
    environment: Mapping[str, str],
    poll_seconds: float = 0.25,
    shutdown_seconds: float = 10.0,
) -> int:
    processes: list[tuple[Service, subprocess.Popen[bytes]]] = []
    stopping = False

    def request_stop(signum: int, _frame: object) -> None:
        nonlocal stopping
        stopping = True
        print(f"marketcow_production_stop_requested signal={signum}", flush=True)

    previous_handlers = {signum: signal.signal(signum, request_stop) for signum in (signal.SIGTERM, signal.SIGINT)}
    try:
        for service in services:
            process = subprocess.Popen(
                service.command,
                cwd=project_dir,
                env=dict(environment),
            )
            processes.append((service, process))
            print(
                f"marketcow_production_service_started name={service.name} pid={process.pid}",
                flush=True,
            )
        while not stopping:
            for service, process in processes:
                returncode = process.poll()
                if returncode is not None:
                    print(
                        "marketcow_production_required_service_exited "
                        f"name={service.name} pid={process.pid} returncode={returncode}",
                        file=sys.stderr,
                        flush=True,
                    )
                    return returncode if returncode != 0 else 1
            time.sleep(poll_seconds)
        return 0
    finally:
        for _service, process in reversed(processes):
            if process.poll() is None:
                process.terminate()
        deadline = time.monotonic() + shutdown_seconds
        for _service, process in reversed(processes):
            remaining = max(0.0, deadline - time.monotonic())
            try:
                process.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                process.kill()
        for _service, process in reversed(processes):
            process.wait()
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


def main() -> None:
    project_dir = Path(os.environ.get("MARKETCOW_PROJECT_DIR", str(DEFAULT_PROJECT_DIR)))
    env_file = Path(os.environ["MARKETCOW_ENV_FILE"])
    load_dotenv(env_file, override=False)
    environment = dict(os.environ)
    services = build_services(project_dir, environment)
    raise SystemExit(supervise(services, project_dir=project_dir, environment=environment))


if __name__ == "__main__":
    main()
