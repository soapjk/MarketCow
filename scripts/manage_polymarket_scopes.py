#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from marketcow.polymarket_scopes import (
    PolymarketScopeRegistry,
    build_replacement_scope_manifest,
)


def _document(path: Path) -> dict:
    value = json.loads(path.resolve(strict=True).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Prepare, accept, atomically activate, or roll back Polymarket scopes",
    )
    parser.add_argument("--registry-root", required=True, type=Path)
    commands = parser.add_subparsers(dest="command", required=True)
    generate = commands.add_parser("generate")
    generate.add_argument("--current-manifest", required=True, type=Path)
    generate.add_argument("--candidate-snapshot", required=True, type=Path)
    generate.add_argument("--output", required=True, type=Path)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--manifest", required=True, type=Path)
    accept = commands.add_parser("accept")
    accept.add_argument("--scope-id", required=True)
    accept.add_argument("--evidence", required=True, type=Path)
    activate = commands.add_parser("activate")
    activate.add_argument("--scope-id", required=True)
    activate.add_argument("--grace-seconds", type=int, default=300)
    rollback = commands.add_parser("rollback")
    rollback.add_argument("--grace-seconds", type=int, default=300)
    commands.add_parser("status")
    arguments = parser.parse_args()
    registry = PolymarketScopeRegistry(arguments.registry_root)
    if arguments.command == "generate":
        result = build_replacement_scope_manifest(
            _document(arguments.current_manifest),
            _document(arguments.candidate_snapshot),
            generated_at_ns=time.time_ns(),
        )
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        with arguments.output.open("x", encoding="utf-8") as stream:
            json.dump(result, stream, indent=2, sort_keys=True)
            stream.write("\n")
    elif arguments.command == "prepare":
        result = registry.prepare(_document(arguments.manifest))
    elif arguments.command == "accept":
        result = registry.accept(arguments.scope_id, _document(arguments.evidence))
    elif arguments.command == "activate":
        result = registry.activate(
            arguments.scope_id, grace_seconds=arguments.grace_seconds,
        )
    elif arguments.command == "rollback":
        result = registry.rollback(grace_seconds=arguments.grace_seconds)
    else:
        result = registry.active() or {"status": "not_configured"}
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
