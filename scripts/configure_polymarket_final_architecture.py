#!/usr/bin/env python3
"""Materialize the one-entry-point Polymarket production configuration locally."""

from __future__ import annotations

import argparse
import json
import os
import secrets
import subprocess
from pathlib import Path
from typing import Any


SCOPE_SCHEMA = "marketcow.polymarket.rust-live-scope.v4"
UNIVERSE_ID_LENGTH = 64


def _atomic_write(path: Path, body: bytes, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("xb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _read_env(path: Path) -> tuple[list[str], dict[str, str]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    values: dict[str, str] = {}
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return lines, values


def _update_env(path: Path, updates: dict[str, str]) -> None:
    lines, _ = _read_env(path)
    remaining = dict(updates)
    output: list[str] = []
    for line in lines:
        stripped = line.strip()
        key = stripped.split("=", 1)[0].strip() if "=" in stripped else ""
        if key in remaining and not stripped.startswith("#"):
            output.append(f"{key}={remaining.pop(key)}")
        else:
            output.append(line)
    if remaining:
        if output and output[-1]:
            output.append("")
        output.append("# Unified Polymarket final architecture (managed locally)")
        output.extend(f"{key}={value}" for key, value in sorted(remaining.items()))
    _atomic_write(path, ("\n".join(output) + "\n").encode())


def _scope_payload(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_bytes())
    universe = payload.get("universe") or {}
    scope_id = payload.get("scope_id")
    if (
        payload.get("schema_version") != SCOPE_SCHEMA
        or not isinstance(scope_id, str)
        or len(scope_id) != UNIVERSE_ID_LENGTH
        or any(value not in "0123456789abcdef" for value in scope_id)
        or universe.get("universe_id") != scope_id
        or int(universe.get("generation", 0)) <= 0
        or int(payload.get("market_count", 0)) <= 0
        or int(payload.get("token_count", 0)) != 2 * int(payload["market_count"])
    ):
        raise ValueError(f"invalid Rust v4 Polymarket seed scope: {path}")
    universe_schema = universe.get("schema_version")
    if universe_schema == "marketcow.polymarket.universe.v1":
        universe["schema_version"] = "marketcow.polymarket.universe.v2"
        # V1 carried ranking exclusions that are not part of the active scope.
        # V2 exclusions are reserved for MarketCow validation failures.
        universe["excluded_markets"] = []
    elif universe_schema != "marketcow.polymarket.universe.v2":
        raise ValueError(f"unsupported dynamic universe seed schema: {path}")
    return payload


def _choose_seed(support_dir: Path, explicit: Path | None) -> Path:
    if explicit is not None:
        _scope_payload(explicit.resolve(strict=True))
        return explicit.resolve()
    candidates = list(
        support_dir.glob(
            "polymarket-universe-refresh/*/work/generation-*-candidate.json"
        )
    )
    valid: list[tuple[int, int, Path]] = []
    for candidate in candidates:
        try:
            payload = _scope_payload(candidate)
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            continue
        valid.append((
            int(payload["universe"]["generation"]),
            candidate.stat().st_mtime_ns,
            candidate,
        ))
    if not valid:
        raise RuntimeError(
            "no valid Rust v4 seed scope exists; set MARKETCOW_POLYMARKET_SEED_SCOPE"
        )
    return max(valid, key=lambda value: (value[0], value[1]))[2]


def configure(
    *,
    project_dir: Path,
    support_dir: Path,
    data_root: Path,
    env_file: Path,
    rust_binary: Path,
    explicit_seed: Path | None = None,
) -> dict[str, str]:
    project_dir = project_dir.resolve(strict=True)
    support_dir = support_dir.resolve(strict=True)
    env_file = env_file.resolve(strict=True)
    rust_binary = rust_binary.resolve(strict=True)
    _, current = _read_env(env_file)
    data_root = Path(current.get("MARKETCOW_HOME", str(data_root))).resolve()
    architecture_root = support_dir / "polymarket-final-architecture"
    registry_root = architecture_root / "scope-registry"
    work_root = architecture_root / "universe-refresh"
    runtime_root = architecture_root / "tradude-controller"
    active_scope = architecture_root / "active-scope.json"
    if active_scope.exists():
        payload = _scope_payload(active_scope)
    else:
        seed = _choose_seed(support_dir, explicit_seed)
        payload = _scope_payload(seed)
    _atomic_write(
        active_scope,
        json.dumps(payload, indent=2, sort_keys=True).encode() + b"\n",
    )
    scope_id = payload["scope_id"]
    registry_scope = registry_root / f"{scope_id}.json"
    _atomic_write(registry_scope, active_scope.read_bytes())

    canonical_tradude_root = Path("/Volumes/T9/projects/trade/tradude")
    configured_tradude_root = Path(current.get(
        "MARKETCOW_POLYMARKET_TRADUDE_WORKTREE", str(canonical_tradude_root),
    ))
    configured_entrypoint = (
        configured_tradude_root
        / "examples/polymarket/run_opportunity_scope_controller.py"
    )
    tradude_root = (
        configured_tradude_root
        if configured_entrypoint.is_file()
        else canonical_tradude_root
    ).resolve(strict=True)
    canonical_tradude_python = Path("/Volumes/T9/projects/trade/.venv/bin/python")
    configured_tradude_python = Path(current.get(
        "MARKETCOW_TRADUDE_PYTHON", str(canonical_tradude_python),
    ))
    def usable_tradude_python(candidate: Path) -> bool:
        if not candidate.is_file() or not os.access(candidate, os.X_OK):
            return False
        try:
            result = subprocess.run(
                [
                    str(candidate),
                    "-c",
                    "import pyarrow; import domains.prediction_markets.discovery.opportunity_controller",
                ],
                env={**os.environ, "PYTHONPATH": str(tradude_root)},
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=30,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            return False
        return result.returncode == 0

    tradude_python = (
        configured_tradude_python
        if usable_tradude_python(configured_tradude_python)
        else canonical_tradude_python
    ).absolute()
    controller_entrypoint = (
        tradude_root / "examples/polymarket/run_opportunity_scope_controller.py"
    )
    if not controller_entrypoint.is_file() or not usable_tradude_python(tradude_python):
        raise RuntimeError("Tradude opportunity controller is not executable")

    api_port = int(current.get("MARKETCOW_PORT", "8790"))
    rust_port = int(current.get("MARKETCOW_POLYMARKET_RUST_PORT", "8796"))
    selection_path = work_root / "tradude-selection.json"
    controller_config = architecture_root / "opportunity-controller.yaml"
    controller_payload = {
        "schema": "tradude.prediction_market.opportunity_scope_controller.v1",
        "marketcow": {
            "discovery_base_url": f"http://127.0.0.1:{api_port}",
            "live_base_url": f"http://127.0.0.1:{api_port}",
            "timeout_seconds": 30,
            "maximum_page_bytes": 16_777_216,
            "maximum_scope_bytes": 8_388_608,
            "maximum_full_sync_bytes": 104_857_600,
            "maximum_stream_frame_bytes": 16_777_216,
            "page_size": 500,
        },
        "prefilter": {
            "target_quantity": "5",
            "maximum_taker_fee_rate": "0.10",
            "fee_exponent": 1,
            "fee_quantum": "0.00001",
            "complete_set_payout": "1",
            "reverse_complete_set_collateral": "1",
            "negative_risk_payout": "1",
            "logical_guaranteed_payout": "1",
            "complete_set_hold_to_resolution": True,
        },
        "selection": {
            "market_limit": 100,
            "maximum_book_age_ns": 60_000_000_000,
            "maximum_capital_release_duration_ns": 2_592_000_000_000_000,
            "minimum_expected_net_edge": "0",
            "minimum_return_on_capital": "0",
            "net_edge_weight": "1",
            "return_on_capital_weight": "10",
            "release_speed_weight": "1",
            "incumbent_market_bonus": "100",
        },
        "activation": {
            "minimum_switch_interval_ns": 300_000_000_000,
            "minimum_score_improvement": "0.01",
            "minimum_selected_opportunity_count": 1,
            "force_switch_for_ineligible_active_market": True,
        },
        "resolution_latency_assumptions": [{
            "resolution_source": "uma",
            "model_version": "polymarket-uma-documented-dispute-window-v1",
            "quantile": "documented_dispute_upper_bound",
            "latency_ns": 518_400_000_000_000,
            "evidence_hashes": [
                "66fa553d169f1e332cae778690e5e653aa0500a8f817bae480f441054ebc0625"
            ],
        }],
        "output": {
            "selection_path": str(selection_path),
            "status_path": str(runtime_root / "status.json"),
        },
    }
    _atomic_write(
        controller_config,
        json.dumps(controller_payload, indent=2, sort_keys=True).encode() + b"\n",
    )

    refresh_config = architecture_root / "universe-refresh.json"
    refresh_payload = {
        "schema_version": "marketcow.polymarket.universe-auto-refresh-config.v3",
        "service_url": f"http://127.0.0.1:{rust_port}",
        "candidate_manifest": str(selection_path),
        "catalog_manifest": str(
            data_root / "prediction-markets/polymarket-discovery/catalog.json"
        ),
        "startup_scope": str(active_scope),
        "scope_registry_root": str(registry_root),
        "work_root": str(work_root),
        "audit_result": str(work_root / "latest-result.json"),
        "target_market_count": 100,
        "minimum_market_count": 1,
        "clob_books_endpoint": "https://clob.polymarket.com/books",
        "request_timeout_seconds": 60,
        "retry_seconds": 60,
    }
    _atomic_write(
        refresh_config,
        json.dumps(refresh_payload, indent=2, sort_keys=True).encode() + b"\n",
    )

    admin_token = current.get("MARKETCOW_RUST_ADMIN_TOKEN") or secrets.token_urlsafe(48)
    updates = {
        "MARKETCOW_PORT": str(api_port),
        "MARKETCOW_POLYMARKET_RUST_PORT": str(rust_port),
        "MARKETCOW_POLYMARKET_DISCOVERY_STREAM_PORT": current.get(
            "MARKETCOW_POLYMARKET_DISCOVERY_STREAM_PORT", "8795"
        ),
        "MARKETCOW_POLYMARKET_DISCOVERY_DEPTH_NOTIONALS": current.get(
            "MARKETCOW_POLYMARKET_DISCOVERY_DEPTH_NOTIONALS", "10,25,50"
        ),
        "MARKETCOW_RUST_BINARY": str(rust_binary),
        "MARKETCOW_RUST_SCOPE_ID": scope_id,
        "MARKETCOW_RUST_ADMIN_TOKEN": admin_token,
        "MARKETCOW_POLYMARKET_RUST_SCOPE_FILE": str(active_scope),
        "MARKETCOW_POLYMARKET_SCOPE_REGISTRY_ROOT": str(registry_root),
        "MARKETCOW_POLYMARKET_FEE_SEMANTICS_POLICY": str(
            project_dir / "ops/polymarket/fee-semantics-v2.json"
        ),
        "MARKETCOW_POLYMARKET_TRADUDE_WORKTREE": str(tradude_root),
        "MARKETCOW_TRADUDE_PYTHON": str(tradude_python),
        "MARKETCOW_POLYMARKET_OPPORTUNITY_CONTROLLER_CONFIG": str(
            controller_config
        ),
        "MARKETCOW_POLYMARKET_UNIVERSE_REFRESH_CONFIG": str(refresh_config),
    }
    _update_env(env_file, updates)
    return {
        "scope_id": scope_id,
        "scope_file": str(active_scope),
        "controller_config": str(controller_config),
        "refresh_config": str(refresh_config),
        "public_port": str(api_port),
        "rust_internal_port": str(rust_port),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-dir", type=Path, required=True)
    parser.add_argument("--support-dir", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--rust-binary", type=Path, required=True)
    parser.add_argument("--seed-scope", type=Path)
    arguments = parser.parse_args()
    seed_scope = arguments.seed_scope
    if seed_scope is None and os.environ.get("MARKETCOW_POLYMARKET_SEED_SCOPE"):
        seed_scope = Path(os.environ["MARKETCOW_POLYMARKET_SEED_SCOPE"])
    result = configure(
        project_dir=arguments.project_dir,
        support_dir=arguments.support_dir,
        data_root=arguments.data_root,
        env_file=arguments.env_file,
        rust_binary=arguments.rust_binary,
        explicit_seed=seed_scope,
    )
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
