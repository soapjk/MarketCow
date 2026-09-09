"""Operator-only finite runtime driver, never an unauthenticated HTTP action.

Requires a previously registered desired generation and immutable unit binding.
The same owner lock must guard all commands that select or apply generations.
No default source roots, units, endpoints, budgets or deployment action.
"""
import argparse
import asyncio
import json
import time
from pathlib import Path

from marketcow.universe_control_server import read_json
from marketcow.universe_generation import GenerationStore
from marketcow.universe_owner import SupervisorOwner
from marketcow.universe_phase1 import selection_sha256
from marketcow.universe_runtime_supervisor import RuntimeSupervisor
from marketcow.universe_systemd import (
    DiscoveryUnitBinding, LiveUnitBinding, SystemdDiscoveryRuntime,
    SystemdLiveRuntime, SystemdUnits, UnitArtifact,
)


def parse_binding(value, pool):
    value = dict(value)
    for name in ("preheat", "candidate", "incumbent"):
        row = value[name]
        if set(row) != {"name", "path", "sha256"} or not Path(row["path"]).is_absolute():
            raise ValueError("invalid registered unit binding")
        value[name] = UnitArtifact(row["name"], Path(row["path"]), row["sha256"])
    kind = LiveUnitBinding if pool == "live" else DiscoveryUnitBinding
    return kind(**value)


async def execute(config_path, config_sha256, action):
    config = read_json(config_path, 65536)
    if selection_sha256(config) != config_sha256:
        raise ValueError("runtime operator config hash mismatch")
    fields = {"pool", "store_path", "owner_lock", "maximum_generations", "maximum_record_bytes",
              "release_root", "user_unit_root", "command_timeout_seconds", "operation_timeout_seconds", "binding"}
    if set(config) != fields or config["pool"] not in ("discovery", "live") or action not in ("apply", "reconcile"):
        raise ValueError("invalid runtime operator config/action")
    for field in ("store_path", "owner_lock", "release_root", "user_unit_root"):
        if not Path(config[field]).is_absolute():
            raise ValueError("absolute operator paths required")
    # Do not create an empty database in place of a missing prepared store.
    if not Path(config["store_path"]).is_file():
        raise ValueError("registered generation store missing")
    binding = parse_binding(config["binding"], config["pool"])
    with SupervisorOwner(Path(config["owner_lock"])):
        store = GenerationStore(Path(config["store_path"]), maximum_generations=config["maximum_generations"],
                                maximum_record_bytes=config["maximum_record_bytes"])
        try:
            units = SystemdUnits(release_root=Path(config["release_root"]),
                                 user_unit_root=Path(config["user_unit_root"]),
                                 command_timeout_seconds=config["command_timeout_seconds"])
            runtime_type = SystemdLiveRuntime if config["pool"] == "live" else SystemdDiscoveryRuntime
            supervisor = RuntimeSupervisor(store, runtime_type(units, binding),
                                           operation_timeout_seconds=config["operation_timeout_seconds"])
            method = supervisor.apply if action == "apply" else supervisor.reconcile
            return await method(config["pool"], now_ms=lambda: time.time_ns() // 1_000_000)
        finally:
            store.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--config-sha256", required=True, help="Canonical operator config SHA256")
    parser.add_argument("--action", choices=("apply", "reconcile"), required=True)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(execute(args.config, args.config_sha256, args.action)), sort_keys=True))


if __name__ == "__main__":
    main()
