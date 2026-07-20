from __future__ import annotations

import argparse
import importlib
import ipaddress
import json
import os
import sys
from pathlib import Path
from typing import Any, Callable, Sequence

import duckdb
import uvicorn

from .config import Settings
from .service import FundamentalService
from .storage import Warehouse


REQUIRED_MODULES = (
    "akshare",
    "baostock",
    "duckdb",
    "fastapi",
    "mootdx",
    "pandas",
    "requests",
    "uvicorn",
)


def is_loopback_host(host: str) -> bool:
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def initialize(settings: Settings) -> dict[str, Any]:
    settings.raw_path.mkdir(parents=True, exist_ok=True)
    (settings.raw_path.parent / "tdx/financial").mkdir(parents=True, exist_ok=True)
    Warehouse(settings.database_path)
    return {
        "status": "ready",
        "database": str(settings.database_path),
        "raw_path": str(settings.raw_path),
    }


def _database_status(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"ok": False, "path": str(path), "message": "not initialized; run marketcow init"}
    required_tables = {"fundamental_snapshot", "ingestion_runs", "market_quote_latest", "tdx_financial_snapshot"}
    try:
        with duckdb.connect(str(path), read_only=True) as connection:
            tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
            missing = sorted(required_tables - tables)
            counts = {
                "fundamentals": connection.execute("SELECT COUNT(*) FROM fundamental_snapshot").fetchone()[0]
                if "fundamental_snapshot" in tables
                else 0,
                "quotes": connection.execute("SELECT COUNT(*) FROM market_quote_latest").fetchone()[0]
                if "market_quote_latest" in tables
                else 0,
                "tdx_periods": connection.execute(
                    "SELECT COUNT(DISTINCT report_period) FROM tdx_financial_snapshot"
                ).fetchone()[0]
                if "tdx_financial_snapshot" in tables
                else 0,
            }
    except Exception as exc:
        return {"ok": False, "path": str(path), "message": str(exc)}
    return {
        "ok": not missing,
        "path": str(path),
        "missing_tables": missing,
        "data": counts,
    }


def diagnose(settings: Settings) -> dict[str, Any]:
    modules: dict[str, dict[str, Any]] = {}
    for name in REQUIRED_MODULES:
        try:
            importlib.import_module(name)
            modules[name] = {"ok": True}
        except Exception as exc:
            modules[name] = {"ok": False, "error": str(exc)}
    raw_exists = settings.raw_path.is_dir()
    raw_writable = raw_exists and os.access(settings.raw_path, os.W_OK)
    database = _database_status(settings.database_path)
    checks = {
        "python": {
            "ok": sys.version_info >= (3, 11),
            "version": ".".join(str(item) for item in sys.version_info[:3]),
        },
        "dependencies": {
            "ok": all(item["ok"] for item in modules.values()),
            "modules": modules,
        },
        "database": database,
        "raw_storage": {
            "ok": raw_writable,
            "path": str(settings.raw_path),
            "message": "writable" if raw_writable else "not initialized or not writable",
        },
        "network": {
            "checked": False,
            "message": "upstream access is checked only when a quote or sync command is requested",
        },
    }
    ready = all(check.get("ok", True) for name, check in checks.items() if name != "network")
    return {"status": "ready" if ready else "attention", "checks": checks}


def sync_cn(
    settings: Settings,
    report_period: str = "",
    tdx_periods: int = 4,
    include_valuation: bool = True,
    skip_fundamentals: bool = False,
    skip_tdx: bool = False,
    progress: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    if skip_fundamentals and skip_tdx:
        raise ValueError("sync-cn has nothing to do: do not combine --skip-fundamentals and --skip-tdx")
    initialize(settings)
    service = FundamentalService(settings)
    result: dict[str, Any] = {"status": "success", "steps": {}, "errors": []}
    announce = progress or (lambda message: None)
    if not skip_fundamentals:
        announce("Refreshing the low-frequency A-share fundamentals snapshot...")
        try:
            result["steps"]["fundamentals"] = service.refresh_market_fundamentals(
                report_period, include_valuation
            )
        except Exception as exc:
            result["steps"]["fundamentals"] = {"status": "failed", "error": str(exc)}
            result["errors"].append({"step": "fundamentals", "error": str(exc)})
    if not skip_tdx:
        announce("Syncing up to {0} recent TDX financial periods...".format(tdx_periods))
        try:
            result["steps"]["tdx_financials"] = service.sync_tdx_financials(tdx_periods)
        except Exception as exc:
            result["steps"]["tdx_financials"] = {"status": "failed", "error": str(exc)}
            result["errors"].append({"step": "tdx_financials", "error": str(exc)})
    if result["errors"]:
        successful_steps = [step for step in result["steps"].values() if step.get("status") == "success"]
        result["status"] = "partial" if successful_steps else "failed"
    result["data"] = diagnose(settings)["checks"]["database"].get("data", {})
    return result


def build_parser(settings: Settings) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="marketcow",
        description="Run and maintain the local market data API",
    )
    parser.add_argument(
        "--profile", choices=("production", "development"), default=settings.profile,
        help="runtime profile; must appear before the command",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    start = commands.add_parser("start", help="start the local HTTP API")
    start.add_argument("--host", default=settings.host)
    start.add_argument("--port", type=int, default=settings.port)

    commands.add_parser("init", help="create the local database and data directories")
    commands.add_parser("doctor", help="check installation, storage, dependencies, and local data coverage")

    sync = commands.add_parser("sync-cn", help="run an explicit low-frequency China-market data sync")
    sync.add_argument("--report-period", default="", help="YYYYMMDD; defaults to the latest broadly available period")
    sync.add_argument("--tdx-periods", type=int, default=4, choices=range(1, 41), metavar="1..40")
    sync.add_argument("--without-valuation", action="store_true", help="skip the full-market valuation snapshot")
    sync.add_argument("--skip-fundamentals", action="store_true")
    sync.add_argument("--skip-tdx", action="store_true")
    spool = commands.add_parser("spool", help="inspect or explicitly operate the development WAL/spool")
    spool_actions = spool.add_subparsers(dest="spool_action", required=True)
    for action in ("status", "audit", "migrate-legacy", "quarantine-corrupt",
                   "retry-dead", "replay"):
        command = spool_actions.add_parser(action)
        command.add_argument("--limit", type=int, default=100)
    listing = spool_actions.add_parser("list")
    listing.add_argument("kind", choices=(
        "wal-pending", "wal-replayed", "raw-intents", "raw-processing",
        "scheduler-pending", "scheduler-processing", "scheduler-failed", "quarantine",
    ))
    listing.add_argument("--limit", type=int, default=100)
    cleanup = spool_actions.add_parser("cleanup-replayed")
    cleanup.add_argument("--retention-seconds", type=int, required=True)
    cleanup.add_argument("--limit", type=int, default=100)
    return parser


def operate_spool(settings: Settings, action: str, limit: int = 100,
                  kind: str = "", retention_seconds: int = 0) -> dict[str, Any]:
    if settings.profile != "development":
        raise ValueError("spool operator is development-only")
    from .clickhouse_writer import LocalClickHouseSpool
    from .spool_operator import SpoolOperator

    if action in {"status", "list", "audit"} and not settings.clickhouse_spool_path.exists():
        empty = {"status": "ok", "present": False, "root": str(settings.clickhouse_spool_path)}
        if action == "list":
            empty.update({"kind": kind, "items": [], "truncated": False})
        return empty
    spool = LocalClickHouseSpool(
        settings.clickhouse_spool_path, settings.storage_root,
        settings.clickhouse_spool_quota_bytes, settings.clickhouse_spool_warning_ratio,
    )
    operator = SpoolOperator(spool)
    if action == "status":
        audit = operator.audit(limit)
        return {"status": audit["status"], "spool": spool.diagnostics(limit),
                "audit": audit}
    if action == "list":
        return operator.list_items(kind, limit)
    if action == "audit":
        return operator.audit(limit)
    if action == "migrate-legacy":
        return operator.migrate_legacy(limit)
    if action == "quarantine-corrupt":
        return operator.quarantine_corrupt(limit)
    if action == "retry-dead":
        return operator.retry_scheduler_failed(limit)
    if action == "cleanup-replayed":
        return operator.cleanup_replayed(retention_seconds, limit)
    if action == "replay":
        if not settings.clickhouse_enabled:
            raise ValueError("spool replay requires MARKETCOW_CLICKHOUSE_ENABLED")
        service = FundamentalService(settings)
        scheduler = getattr(service.market_bar_repository, "background_scheduler", None)
        if scheduler:
            scheduler.pause()
        try:
            writer = getattr(service.market_bar_repository, "writer", None)
            if writer is None:
                raise ValueError("ClickHouse writer is not assembled")
            replay = writer.replay(limit)
            unhealthy = (replay["failed"] or replay["quarantined"] or
                         replay["callback_failed"] or replay["lock_busy"])
            return {"status": "partial" if unhealthy else "ok", "replay": replay}
        finally:
            if scheduler:
                scheduler.resume()
            service.close()
    raise ValueError("unknown spool action")


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if not arguments:
        arguments = ["start"]
    elif arguments[0] in {"--host", "--port"}:
        arguments.insert(0, "start")
    profile = None
    for index, argument in enumerate(arguments):
        if argument.startswith("--profile="):
            profile = argument.split("=", 1)[1]
            break
        if argument == "--profile" and index + 1 < len(arguments):
            profile = arguments[index + 1]
            break
    settings = Settings.from_env(profile)
    parser = build_parser(settings)
    args = parser.parse_args(arguments)

    try:
        settings.validate_runtime_isolation()
        if args.command == "init":
            print(json.dumps(initialize(settings), ensure_ascii=False, indent=2))
            return 0
        if args.command == "doctor":
            result = diagnose(settings)
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0 if result["status"] == "ready" else 1
        if args.command == "sync-cn":
            print(
                "Starting an explicit low-frequency China-market sync; existing TDX files are reused.",
                file=sys.stderr,
            )
            result = sync_cn(
                settings,
                report_period=args.report_period,
                tdx_periods=args.tdx_periods,
                include_valuation=not args.without_valuation,
                skip_fundamentals=args.skip_fundamentals,
                skip_tdx=args.skip_tdx,
                progress=lambda message: print(message, file=sys.stderr),
            )
            print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
            return 0 if result["status"] == "success" else 1
        if args.command == "spool":
            result = operate_spool(
                settings, args.spool_action, args.limit,
                getattr(args, "kind", ""), getattr(args, "retention_seconds", 0),
            )
            print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
            return 0 if result.get("status") == "ok" else 2
        if not is_loopback_host(args.host):
            allowed = os.getenv("MARKETCOW_ALLOW_NON_LOOPBACK", "").lower() in {"1", "true", "yes"}
            if not allowed:
                parser.error(
                    "refusing a non-loopback host because admin endpoints have no authentication; "
                    "set MARKETCOW_ALLOW_NON_LOOPBACK=1 only in a trusted network"
                )
        initialize(settings)
        os.environ["MARKETCOW_PROFILE"] = settings.profile
        uvicorn.run("marketcow.api:app", host=args.host, port=args.port)
        return 0
    except Exception as exc:
        print("error: {0}".format(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
