from __future__ import annotations

import argparse
import ipaddress
import json
import os
import sys
from typing import Any, Sequence

import uvicorn

from .config import PROFILES, Settings


def is_loopback_host(host: str) -> bool:
    try:
        return host.lower() == "localhost" or ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def initialize(settings: Settings) -> dict[str, Any]:
    settings.validate_preflight()
    return {"status": "ready", "storage": "postgresql-clickhouse"}


def diagnose(settings: Settings) -> dict[str, Any]:
    from .factory import create_online_repositories
    from .health import HealthEvaluator

    resources = None
    try:
        resources = create_online_repositories(settings)
        health = HealthEvaluator().evaluate(resources.health_snapshot())
        return {
            "status": "ready" if health["ready"] else "attention",
            "checks": {"storage": health, "network": {
                "checked": False,
                "message": "upstream access is checked only on explicit requests",
            }},
        }
    except Exception:
        return {"status": "attention", "checks": {"storage": {
            "status": "unavailable", "ready": False,
            "reason": "dependency_probe_failed",
        }}}
    finally:
        if resources is not None:
            try:
                resources.close()
            except Exception:
                pass


def build_parser(settings: Settings) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="marketcow")
    parser.add_argument("--profile", choices=tuple(sorted(PROFILES)), default=settings.profile)
    commands = parser.add_subparsers(dest="command", required=True)
    start = commands.add_parser("start")
    start.add_argument("--host", default=settings.host)
    start.add_argument("--port", type=int, default=settings.port)
    commands.add_parser("init")
    commands.add_parser("doctor")
    spool = commands.add_parser("spool")
    actions = spool.add_subparsers(dest="spool_action", required=True)
    for action in ("status", "audit", "quarantine-corrupt", "retry-dead", "replay"):
        command = actions.add_parser(action)
        command.add_argument("--limit", type=int, default=100)
    listing = actions.add_parser("list")
    listing.add_argument("kind", choices=(
        "wal-pending", "wal-replayed", "raw-intents", "raw-processing",
        "scheduler-pending", "scheduler-processing", "scheduler-failed", "quarantine",
    ))
    listing.add_argument("--limit", type=int, default=100)
    cleanup = actions.add_parser("cleanup-replayed")
    cleanup.add_argument("--retention-seconds", type=int, required=True)
    cleanup.add_argument("--limit", type=int, default=100)
    return parser


def operate_spool(settings: Settings, action: str, limit: int = 100,
                  kind: str = "", retention_seconds: int = 0) -> dict[str, Any]:
    if settings.profile == "production" and action not in {"status", "audit", "list"}:
        raise ValueError("mutating spool operations are development/test-only")
    from .clickhouse_writer import LocalClickHouseSpool
    from .spool_operator import SpoolOperator

    spool = LocalClickHouseSpool(
        settings.clickhouse_spool_path, settings.storage_root,
        settings.clickhouse_spool_quota_bytes, settings.clickhouse_spool_warning_ratio,
    )
    operator = SpoolOperator(spool)
    if action == "status":
        return {"status": "ok", "spool": spool.diagnostics(limit), "audit": operator.audit(limit)}
    if action == "list":
        return operator.list_items(kind, limit)
    if action == "audit":
        return operator.audit(limit)
    if action == "quarantine-corrupt":
        return operator.quarantine_corrupt(limit)
    if action == "retry-dead":
        return operator.retry_scheduler_failed(limit)
    if action == "cleanup-replayed":
        return operator.cleanup_replayed(retention_seconds, limit)
    if action == "replay":
        from .service import FundamentalService
        service = FundamentalService(settings)
        try:
            return {"status": "ok", "replay": service.online_resources.writer.replay(limit)}
        finally:
            service.close()
    raise ValueError("unknown spool action")


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv) or ["start"]
    if arguments[0] in {"--host", "--port"}:
        arguments.insert(0, "start")
    profile = None
    for index, argument in enumerate(arguments):
        if argument.startswith("--profile="):
            profile = argument.split("=", 1)[1]
        elif argument == "--profile" and index + 1 < len(arguments):
            profile = arguments[index + 1]
    settings = Settings.from_env(profile)
    args = build_parser(settings).parse_args(arguments)
    try:
        settings.validate_preflight()
        if args.command == "init":
            print(json.dumps(initialize(settings), ensure_ascii=False, indent=2))
            return 0
        if args.command == "doctor":
            result = diagnose(settings)
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0 if result["status"] == "ready" else 1
        if args.command == "spool":
            result = operate_spool(
                settings, args.spool_action, args.limit,
                getattr(args, "kind", ""), getattr(args, "retention_seconds", 0),
            )
            print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
            return 0 if result.get("status") == "ok" else 2
        if not is_loopback_host(args.host) and os.getenv(
            "MARKETCOW_ALLOW_NON_LOOPBACK", ""
        ).lower() not in {"1", "true", "yes"}:
            raise ValueError("refusing non-loopback host without explicit override")
        os.environ["MARKETCOW_PROFILE"] = settings.profile
        uvicorn.run("marketcow.api:create_app", host=args.host, port=args.port, factory=True)
        return 0
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
