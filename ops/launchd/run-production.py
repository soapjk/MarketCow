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
    environment: Mapping[str, str] | None = None


def _required_path(
    environment: Mapping[str, str], name: str, *, preserve_executable_symlink: bool = False,
) -> Path:
    raw = environment.get(name, "").strip()
    if not raw:
        raise ValueError(f"{name} is required for the production Polymarket scope")
    path = Path(raw)
    if not path.is_absolute():
        raise ValueError(f"{name} must be an absolute path")
    if not path.exists():
        raise FileNotFoundError(path)
    return path.absolute() if preserve_executable_symlink else path.resolve(strict=True)


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
    discovery_root = data_root / "prediction-markets" / "polymarket-discovery"
    rust_root = data_root / "prediction-markets" / "polymarket-rust"

    host = environment.get("MARKETCOW_HOST", "127.0.0.1")
    if host not in LOOPBACK_HOSTS:
        raise ValueError("production MarketCow APIs must bind to loopback")
    api_port = int(environment.get("MARKETCOW_PORT", "8790"))
    rust_port = int(environment.get("MARKETCOW_POLYMARKET_RUST_PORT", "8796"))
    discovery_stream_port = int(environment.get(
        "MARKETCOW_POLYMARKET_DISCOVERY_STREAM_PORT", "8795"
    ))
    for name, port in (
        ("MARKETCOW_PORT", api_port),
        ("Polymarket internal Rust data-plane port", rust_port),
        ("Polymarket internal discovery stream port", discovery_stream_port),
    ):
        if not 1 <= port <= 65535:
            raise ValueError(f"{name} must be in [1, 65535]")
    if len({api_port, rust_port, discovery_stream_port}) != 3:
        raise ValueError("MarketCow API and internal stream ports must differ")

    rust_binary = _required_path(environment, "MARKETCOW_RUST_BINARY")
    rust_scope = _required_path(environment, "MARKETCOW_POLYMARKET_RUST_SCOPE_FILE")
    rust_scope_registry = _required_path(
        environment, "MARKETCOW_POLYMARKET_SCOPE_REGISTRY_ROOT"
    )
    fee_semantics = _required_path(
        environment, "MARKETCOW_POLYMARKET_FEE_SEMANTICS_POLICY"
    )
    tradude_worktree = _required_path(environment, "MARKETCOW_POLYMARKET_TRADUDE_WORKTREE")
    tradude_python = _required_path(
        environment,
        "MARKETCOW_TRADUDE_PYTHON",
        preserve_executable_symlink=True,
    )
    if not os.access(tradude_python, os.X_OK):
        raise ValueError("MARKETCOW_TRADUDE_PYTHON must be executable")
    opportunity_config = _required_path(
        environment, "MARKETCOW_POLYMARKET_OPPORTUNITY_CONTROLLER_CONFIG"
    )
    refresh_config = _required_path(
        environment, "MARKETCOW_POLYMARKET_UNIVERSE_REFRESH_CONFIG"
    )
    rust_scope_id = environment.get("MARKETCOW_RUST_SCOPE_ID", "").strip()
    rust_admin_token = environment.get("MARKETCOW_RUST_ADMIN_TOKEN", "").strip()
    if len(rust_scope_id) != 64 or any(
        value not in "0123456789abcdef" for value in rust_scope_id
    ):
        raise ValueError("MARKETCOW_RUST_SCOPE_ID must be a lowercase SHA-256")
    if not rust_admin_token:
        raise ValueError("MARKETCOW_RUST_ADMIN_TOKEN is required")
    depth_notionals = [
        value.strip() for value in environment.get(
            "MARKETCOW_POLYMARKET_DISCOVERY_DEPTH_NOTIONALS", ""
        ).split(",") if value.strip()
    ]
    if not depth_notionals:
        raise ValueError(
            "MARKETCOW_POLYMARKET_DISCOVERY_DEPTH_NOTIONALS is required"
        )
    discovery_collector = Service(
        "polymarket-discovery-collector",
        (
            python,
            str(project_dir / "scripts" / "run_polymarket_live.py"),
            "--root",
            str(discovery_root),
            "--shard-size",
            environment.get("MARKETCOW_POLYMARKET_DISCOVERY_SHARD_SIZE", "500"),
            "--max-websocket-connections",
            environment.get(
                "MARKETCOW_POLYMARKET_DISCOVERY_MAX_WEBSOCKET_CONNECTIONS", "32"
            ),
            "--live-stream-port",
            str(discovery_stream_port),
            "--fee-semantics-policy",
            str(fee_semantics),
        ),
    )

    rust_environment = dict(environment)
    rust_environment.update({
        "MARKETCOW_RUST_PROFILE": "production",
        "MARKETCOW_RUST_ROLE": "polymarket_data_plane",
        "MARKETCOW_RUST_BIND": f"127.0.0.1:{rust_port}",
        "MARKETCOW_RUST_STORAGE_ROOT": str(rust_root),
        "MARKETCOW_RUST_SCOPE_ID": rust_scope_id,
        "MARKETCOW_RUST_SHADOW": "false",
        "MARKETCOW_POLYMARKET_LIVE_ENABLED": "true",
        "MARKETCOW_POLYMARKET_SCOPE_FILE": str(rust_scope),
        "MARKETCOW_POLYMARKET_SCOPE_REGISTRY_ROOT": str(rust_scope_registry),
        "MARKETCOW_REAL_ORDER_SUBMISSION_ENABLED": "false",
    })
    rust_data_plane = Service(
        "polymarket-rust-data-plane",
        (str(rust_binary), "serve"),
        rust_environment,
    )
    opportunity_controller = Service(
        "polymarket-opportunity-controller",
        (
            str(tradude_python),
            str(tradude_worktree / "examples/polymarket/run_opportunity_scope_controller.py"),
            "--config", str(opportunity_config),
            "--follow", "--retry-seconds",
            environment.get("MARKETCOW_POLYMARKET_CONTROLLER_RETRY_SECONDS", "5"),
        ),
        {
            **environment,
            "PYTHONPATH": str(tradude_worktree),
        },
    )
    universe_activator = Service(
        "polymarket-universe-activator",
        (
            python,
            str(project_dir / "scripts/migration/auto_refresh_polymarket_dynamic_universe.py"),
            "--config", str(refresh_config),
            "--follow", "--poll-seconds",
            environment.get("MARKETCOW_POLYMARKET_ACTIVATOR_POLL_SECONDS", "2"),
        ),
        {
            **environment,
            "PYTHONPATH": os.pathsep.join((str(project_dir), str(project_dir / "src"))),
        },
    )
    gateway_environment = dict(environment)
    gateway_environment.update({
        "MARKETCOW_POLYMARKET_RUST_DATA_PLANE_URL": f"http://127.0.0.1:{rust_port}",
        "MARKETCOW_POLYMARKET_LIVE_STREAM_URI": "",
    })
    unified_api = Service(
        "unified-api",
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
        gateway_environment,
    )
    return (
        discovery_collector,
        rust_data_plane,
        unified_api,
        opportunity_controller,
        universe_activator,
    )


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
                env=dict(service.environment or environment),
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
    # A managed virtualenv may contain an editable install left by an older
    # worktree. Production children must always import the selected main
    # checkout, independent of site-packages state.
    environment["PYTHONPATH"] = str(project_dir.resolve(strict=True) / "src")
    services = build_services(project_dir, environment)
    raise SystemExit(supervise(services, project_dir=project_dir, environment=environment))


if __name__ == "__main__":
    main()
