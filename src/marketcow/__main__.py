from __future__ import annotations

import argparse
import ipaddress
import json
import os
import sys
import time
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
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
    service_account = commands.add_parser("service-account")
    service_actions = service_account.add_subparsers(
        dest="service_account_action", required=True
    )
    generate = service_actions.add_parser("generate")
    generate.add_argument("--id", required=True)
    generate.add_argument("--role", choices=("viewer", "operator"), default="operator")
    generate.add_argument(
        "--scopes", default="history:read,history:write",
        help="comma-separated capability scopes",
    )
    csv_import = commands.add_parser("import-bars")
    csv_import.add_argument("--file", required=True)
    csv_import.add_argument(
        "--config", required=True,
        help="JSON file containing the versioned CSV import declaration",
    )
    csv_import.add_argument("--dry-run", action="store_true")
    csv_import.add_argument("--idempotency-key", default="")
    csv_import.add_argument("--chunk-rows", type=int, default=100000)
    csv_import.add_argument("--max-attempts", type=int, default=3)
    csv_import.add_argument(
        "--evidence-output", default="",
        help=(
            "create a redacted JSON smoke-test evidence file; "
            "the file must not already exist"
        ),
    )
    return parser


def _write_csv_import_evidence(
    output: str, payload: dict[str, Any]
) -> None:
    if not output:
        return
    target = Path(output).expanduser().resolve()
    if not target.parent.is_dir():
        raise ValueError("CSV import evidence parent directory does not exist")
    encoded = json.dumps(
        payload, ensure_ascii=False, indent=2, sort_keys=True, default=str
    )
    with target.open("x", encoding="utf-8") as stream:
        stream.write(encoded)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


def _public_csv_job(job: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value for key, value in job.items()
        if key not in {"storage_path", "request_json"}
    }


def import_csv_bars(settings: Settings, args: Any) -> dict[str, Any]:
    from .csv_import import CsvImportRequest
    from .csv_import_service import create_csv_import_service
    from .service import FundamentalService

    config_path = Path(args.config).expanduser().resolve(strict=True)
    allowed_root = (settings.allowed_root or settings.storage_root).resolve()
    if not config_path.is_relative_to(allowed_root):
        raise ValueError("CSV import config is outside MARKETCOW_ALLOWED_ROOT")
    declaration = CsvImportRequest.from_dict(json.loads(
        config_path.read_text(encoding="utf-8")
    ))
    cli_settings = replace(settings, clickhouse_background_canonical=False)
    service = FundamentalService(cli_settings)
    imports = create_csv_import_service(cli_settings, service)
    try:
        if args.dry_run:
            report = imports.dry_run(args.file, declaration)
            evidence = {
                "schema": "marketcow.csv-import-smoke-evidence.v1",
                "generated_at": datetime.now(timezone.utc).isoformat(
                    timespec="microseconds"
                ),
                "mode": "dry-run",
                "contract_version": report["contract_version"],
                "file": report["file"],
                "source": report["source"],
                "profile": report["profile"],
                "namespace": report["namespace"],
                "interval": report["interval"],
                "adjustment": report["adjustment"],
                "validation": {
                    key: value for key, value in report.items()
                    if key not in {
                        "file", "source", "profile", "namespace",
                        "interval", "adjustment",
                    }
                },
            }
            _write_csv_import_evidence(args.evidence_output, evidence)
            return report
        key = args.idempotency_key.strip()
        if not key:
            raise ValueError("--idempotency-key is required without --dry-run")
        job, created = imports.create_import(
            args.file, declaration, idempotency_key=key,
            chunk_rows=args.chunk_rows, max_attempts=args.max_attempts,
        )
        job_id = str(job["job_id"])
        while True:
            current = imports.get(job_id)
            if current is None:
                raise RuntimeError("CSV import job disappeared")
            if current["status"] in {"succeeded", "failed", "canceled"}:
                request_json = dict(current.get("request_json") or {})
                evidence = {
                    "schema": "marketcow.csv-import-smoke-evidence.v1",
                    "generated_at": datetime.now(timezone.utc).isoformat(
                        timespec="microseconds"
                    ),
                    "mode": "formal-import",
                    "created": created,
                    "file": (
                        request_json.get("dry_run_report", {}).get("file")
                    ),
                    "manifest": request_json.get("manifest"),
                    "job": _public_csv_job(current),
                    "quality_report": current.get("quality_report_json"),
                }
                _write_csv_import_evidence(args.evidence_output, evidence)
                return {"created": created, "job": _public_csv_job(current)}
            time.sleep(0.2)
    finally:
        imports.close()
        service.close()


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
        if args.command == "service-account":
            from .admin_auth import generate_service_api_key, load_service_accounts

            api_key, key_hash = generate_service_api_key(args.id)
            scopes = [scope.strip().lower() for scope in args.scopes.split(",") if scope.strip()]
            declaration = {
                args.id.strip().lower(): {
                    "role": args.role,
                    "key_hash": key_hash,
                    "scopes": scopes,
                    "enabled": True,
                }
            }
            load_service_accounts(json.dumps(declaration))
            print(json.dumps({
                "api_key": api_key,
                "service_accounts_json": declaration,
                "warning": "The API key is shown once. Store it in the calling service.",
            }, ensure_ascii=False, indent=2))
            return 0
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
        if args.command == "import-bars":
            result = import_csv_bars(settings, args)
            print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
            status = result.get("status") or (result.get("job") or {}).get("status")
            return 0 if status in {"valid", "succeeded"} else 2
        if not is_loopback_host(args.host) and os.getenv(
            "MARKETCOW_ALLOW_NON_LOOPBACK", ""
        ).lower() not in {"1", "true", "yes"}:
            raise ValueError("refusing non-loopback host without explicit override")
        os.environ["MARKETCOW_PROFILE"] = settings.profile
        os.environ["MARKETCOW_RUNTIME_HOST"] = args.host
        os.environ["MARKETCOW_RUNTIME_PORT"] = str(args.port)
        uvicorn.run("marketcow.api:create_app", host=args.host, port=args.port, factory=True)
        return 0
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
