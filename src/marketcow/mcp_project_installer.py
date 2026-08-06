from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import selectors
import stat
import subprocess
import sys
import tempfile
import tomllib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit, urlunsplit
from urllib.request import ProxyHandler, Request, build_opener


INSTALLER_VERSION = "1.1.0"
MANAGED_BEGIN = "# BEGIN MARKETCOW MCP MANAGED v1"
MANAGED_END = "# END MARKETCOW MCP MANAGED v1"
SERVER_TABLE = "[mcp_servers.marketcow]"
REQUIRED_TOOLS = frozenset({
    "service_health",
    "search_instruments",
    "get_instrument",
    "get_quotes",
    "get_market_bars",
    "get_canonical_bars",
    "get_fundamental",
    "get_financial_statements",
    "get_dividends",
    "get_exposure_facts",
})
PROTOCOL_VERSION = "2025-11-25"
STDIO_RESPONSE_TIMEOUT_SECONDS = 10.0


class InstallerError(RuntimeError):
    pass


@dataclass(frozen=True)
class WorkspacePaths:
    workspace: Path
    codex_dir: Path
    config: Path
    backups: Path


@dataclass(frozen=True)
class VerificationResult:
    transport: str
    marketcow_version: str
    tool_count: int
    tools: tuple[str, ...]
    missing_tools: tuple[str, ...]
    extra_tools: tuple[str, ...]
    service_health: Mapping[str, Any]

    def payload(self) -> dict[str, Any]:
        return {
            "transport": self.transport,
            "marketcow_version": self.marketcow_version,
            "tool_count": self.tool_count,
            "tools": list(self.tools),
            "minimum_required_tools": sorted(REQUIRED_TOOLS),
            "missing_tools": list(self.missing_tools),
            "extra_tools": list(self.extra_tools),
            "service_health": dict(self.service_health),
        }


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def _mode_is_writable(path: Path) -> bool:
    return bool(path.stat().st_mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH))


def validate_workspace(raw_workspace: str, *, codex_home: str | None = None) -> WorkspacePaths:
    supplied = Path(raw_workspace).expanduser()
    if not supplied.is_absolute():
        raise InstallerError("--workspace must be an explicit absolute path")
    try:
        workspace = supplied.resolve(strict=True)
    except FileNotFoundError as exc:
        raise InstallerError("--workspace must be an existing directory") from exc
    if not workspace.is_dir():
        raise InstallerError("--workspace must be an existing directory")
    home = Path.home().resolve()
    user_codex_home = (home / ".codex").resolve()
    active_codex_home = Path(
        codex_home or os.environ.get("CODEX_HOME") or user_codex_home
    ).expanduser().resolve()
    protected_codex_homes = {active_codex_home, user_codex_home}
    if workspace in {Path("/"), home, *protected_codex_homes}:
        raise InstallerError(
            "--workspace cannot be /, HOME, CODEX_HOME, or the user Codex config directory"
        )
    if any(
        protected in workspace.parents or workspace in protected.parents
        for protected in protected_codex_homes
    ):
        raise InstallerError(
            "--workspace cannot contain or be inside a Codex config directory"
        )
    if not _mode_is_writable(workspace) or not os.access(workspace, os.W_OK):
        raise InstallerError("--workspace is not writable")
    codex_dir = workspace / ".codex"
    if codex_dir.exists():
        if codex_dir.is_symlink() or not codex_dir.is_dir():
            raise InstallerError("<workspace>/.codex must be a real directory")
        if not _mode_is_writable(codex_dir) or not os.access(codex_dir, os.W_OK):
            raise InstallerError("<workspace>/.codex is not writable")
    return WorkspacePaths(
        workspace=workspace,
        codex_dir=codex_dir,
        config=codex_dir / "config.toml",
        backups=codex_dir / "backups" / "marketcow-mcp",
    )


def _managed_span(content: str) -> tuple[int, int] | None:
    begins = [match.start() for match in re.finditer(re.escape(MANAGED_BEGIN), content)]
    ends = [match.end() for match in re.finditer(re.escape(MANAGED_END), content)]
    if not begins and not ends:
        return None
    if len(begins) != 1 or len(ends) != 1 or begins[0] >= ends[0]:
        raise InstallerError("MarketCow managed block markers are malformed; restore a backup")
    start = begins[0]
    if start > 0 and content[start - 1] == "\n":
        start -= 1
    end = ends[0]
    if end < len(content) and content[end] == "\n":
        end += 1
    return start, end


def _without_managed_block(content: str) -> tuple[str, str | None]:
    span = _managed_span(content)
    if span is None:
        return content, None
    start, end = span
    return content[:start] + content[end:], content[start:end]


def _parse_unmanaged_config(content: str) -> Mapping[str, Any]:
    unmanaged, _ = _without_managed_block(content)
    try:
        parsed = tomllib.loads(unmanaged) if unmanaged.strip() else {}
    except tomllib.TOMLDecodeError as exc:
        raise InstallerError(f"existing config.toml is invalid TOML: {exc}") from exc
    servers = parsed.get("mcp_servers") or {}
    if isinstance(servers, Mapping) and "marketcow" in servers:
        raise InstallerError(
            "unmanaged [mcp_servers.marketcow] already exists; rename or remove it "
            "manually after reviewing the configuration, then retry"
        )
    return parsed


def _toml_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def validate_service_url(value: str) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path.rstrip("/") != "/mcp"
    ):
        raise InstallerError("--service-url must be an http(s) URL ending in /mcp")
    return urlunsplit((parsed.scheme, parsed.netloc, "/mcp", "", ""))


def _api_base(service_url: str) -> str:
    parsed = urlsplit(service_url)
    return urlunsplit((parsed.scheme, parsed.netloc, "", "", "")).rstrip("/")


def validate_stdio_command(value: str) -> Path:
    command = Path(value).expanduser()
    if not command.is_absolute():
        raise InstallerError("--stdio-command must be an absolute executable path")
    try:
        resolved = command.resolve(strict=True)
    except FileNotFoundError as exc:
        raise InstallerError("--stdio-command does not exist") from exc
    if not resolved.is_file() or not os.access(resolved, os.X_OK):
        raise InstallerError("--stdio-command must be an executable file")
    return resolved


def managed_block(
    *,
    transport: str,
    service_url: str,
    observed_version: str,
    stdio_command: Path | None = None,
) -> str:
    lines = [
        MANAGED_BEGIN,
        f"# installer_version = {INSTALLER_VERSION}",
        f"# observed_marketcow_version = {observed_version}",
        f"# transport = {transport}",
        SERVER_TABLE,
    ]
    if transport == "http":
        lines.extend((
            f"url = {_toml_string(service_url)}",
            "enabled = true",
        ))
    else:
        if stdio_command is None:
            raise InstallerError("stdio transport requires --stdio-command")
        lines.extend((
            f"command = {_toml_string(str(stdio_command))}",
            "enabled = true",
            "",
            "[mcp_servers.marketcow.env]",
            f"MARKETCOW_MCP_BASE_URL = {_toml_string(_api_base(service_url))}",
        ))
    lines.append(MANAGED_END)
    return "\n".join(lines) + "\n"


def merge_managed_block(content: str, block: str) -> str:
    unmanaged, _ = _without_managed_block(content)
    _parse_unmanaged_config(unmanaged)
    prefix = unmanaged
    if prefix and not prefix.endswith("\n"):
        prefix += "\n"
    if prefix and not prefix.endswith("\n\n"):
        prefix += "\n"
    merged = prefix + block
    try:
        tomllib.loads(merged)
    except tomllib.TOMLDecodeError as exc:
        raise InstallerError(f"generated project config is invalid TOML: {exc}") from exc
    return merged


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="wb", prefix=f".{path.name}.", dir=path.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
        try:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
            os.chmod(temporary, 0o600)
        except Exception:
            temporary.unlink(missing_ok=True)
            raise
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _ensure_backup_directory(paths: WorkspacePaths, *, create: bool) -> None:
    if paths.codex_dir.exists() and (
        paths.codex_dir.is_symlink() or not paths.codex_dir.is_dir()
    ):
        raise InstallerError("<workspace>/.codex must be a real directory")
    if create:
        paths.codex_dir.mkdir(mode=0o700, exist_ok=True)
    backup_parent = paths.backups.parent
    if backup_parent.exists() and (
        backup_parent.is_symlink() or not backup_parent.is_dir()
    ):
        raise InstallerError("<workspace>/.codex/backups must be a real directory")
    if create:
        backup_parent.mkdir(mode=0o700, exist_ok=True)
    elif not backup_parent.is_dir():
        raise InstallerError("no MarketCow backups exist for this workspace")
    if paths.backups.exists() and (
        paths.backups.is_symlink() or not paths.backups.is_dir()
    ):
        raise InstallerError("MarketCow backup directory must be a real directory")
    if create:
        paths.backups.mkdir(mode=0o700, exist_ok=True)
    elif not paths.backups.is_dir():
        raise InstallerError("no MarketCow backups exist for this workspace")


def _backup(paths: WorkspacePaths, original: bytes | None) -> Path:
    _ensure_backup_directory(paths, create=True)
    identity = _sha256(original or b"")[:12]
    suffix = "toml" if original is not None else "absent"
    backup = paths.backups / f"config.{_utc_stamp()}.{identity}.{suffix}"
    _atomic_write(backup, original or b"")
    return backup


def _request_json(
    url: str,
    *,
    method: str = "GET",
    payload: Mapping[str, Any] | None = None,
    headers: Mapping[str, str] | None = None,
    timeout: float = 10,
) -> Mapping[str, Any]:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    request_headers = {"Accept": "application/json", **dict(headers or {})}
    if body is not None:
        request_headers["Content-Type"] = "application/json"
    request = Request(url, data=body, method=method, headers=request_headers)
    try:
        opener = build_opener(ProxyHandler({}))
        with opener.open(request, timeout=timeout) as response:
            result = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")[:1000]
        raise InstallerError(f"HTTP {exc.code} from {url}: {detail}") from exc
    except (URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
        raise InstallerError(f"cannot read JSON from {url}: {exc}") from exc
    if not isinstance(result, Mapping):
        raise InstallerError(f"invalid JSON object from {url}")
    return result


def _mcp_result(payload: Mapping[str, Any], request_id: int) -> Mapping[str, Any]:
    if payload.get("id") != request_id:
        raise InstallerError("MCP response id does not match request")
    if payload.get("error"):
        raise InstallerError(f"MCP returned an error: {payload['error']}")
    result = payload.get("result")
    if not isinstance(result, Mapping):
        raise InstallerError("MCP response has no result object")
    return result


def _verification_from_results(
    transport: str,
    initialized: Mapping[str, Any],
    listed: Mapping[str, Any],
    health_call: Mapping[str, Any],
    expected_version: str | None,
) -> VerificationResult:
    server_info = initialized.get("serverInfo") or {}
    version = str(server_info.get("version") or "")
    if not version:
        raise InstallerError("MCP initialize did not report MarketCow version")
    if expected_version and version != expected_version:
        raise InstallerError(
            f"MarketCow version mismatch: expected {expected_version}, observed {version}"
        )
    definitions = listed.get("tools")
    if not isinstance(definitions, list):
        raise InstallerError("MCP tools/list did not return a tools array")
    names = tuple(sorted({
        str(item.get("name")) for item in definitions
        if isinstance(item, Mapping) and item.get("name")
    }))
    missing = tuple(sorted(REQUIRED_TOOLS - set(names)))
    extra = tuple(sorted(set(names) - REQUIRED_TOOLS))
    if missing:
        raise InstallerError(
            "MCP is missing required tools: " + ", ".join(missing)
        )
    if health_call.get("isError"):
        raise InstallerError("MCP service_health returned isError=true")
    structured = health_call.get("structuredContent")
    if not isinstance(structured, Mapping):
        raise InstallerError("MCP service_health returned no structuredContent")
    return VerificationResult(
        transport=transport,
        marketcow_version=version,
        tool_count=len(names),
        tools=names,
        missing_tools=missing,
        extra_tools=extra,
        service_health=structured,
    )


def verify_http(service_url: str, expected_version: str | None = None) -> VerificationResult:
    health = _request_json(_api_base(service_url) + "/v1/health")
    if str(health.get("status") or "").lower() not in {"ok", "healthy"}:
        raise InstallerError("MarketCow /v1/health is not healthy")
    initialize_payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {},
            "clientInfo": {"name": "marketcow-mcp-project", "version": INSTALLER_VERSION},
        },
    }
    initialized = _mcp_result(
        _request_json(service_url, method="POST", payload=initialize_payload), 1
    )
    protocol = str(initialized.get("protocolVersion") or PROTOCOL_VERSION)
    headers = {"MCP-Protocol-Version": protocol}
    listed = _mcp_result(_request_json(
        service_url,
        method="POST",
        payload={"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
        headers=headers,
    ), 2)
    health_call = _mcp_result(_request_json(
        service_url,
        method="POST",
        payload={
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {"name": "service_health", "arguments": {}},
        },
        headers=headers,
    ), 3)
    return _verification_from_results(
        "http", initialized, listed, health_call, expected_version
    )


class _StdioSession:
    def __init__(
        self,
        command: Path,
        service_url: str,
        cwd: Path,
        *,
        response_timeout: float = STDIO_RESPONSE_TIMEOUT_SECONDS,
    ) -> None:
        environment = dict(os.environ)
        environment["MARKETCOW_MCP_BASE_URL"] = _api_base(service_url)
        self.process = subprocess.Popen(
            [str(command)],
            cwd=cwd,
            env=environment,
            text=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.response_timeout = response_timeout

    def request(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        if self.process.stdin is None or self.process.stdout is None:
            raise InstallerError("stdio MCP pipes are unavailable")
        self.process.stdin.write(json.dumps(payload) + "\n")
        self.process.stdin.flush()
        with selectors.DefaultSelector() as selector:
            selector.register(self.process.stdout, selectors.EVENT_READ)
            if not selector.select(self.response_timeout):
                self.process.terminate()
                raise InstallerError(
                    "stdio MCP response timed out after "
                    f"{self.response_timeout:g} seconds"
                )
        line = self.process.stdout.readline()
        if not line:
            detail = self.process.stderr.read()[:1000] if self.process.stderr else ""
            raise InstallerError(f"stdio MCP exited without a response: {detail}")
        try:
            result = json.loads(line)
        except json.JSONDecodeError as exc:
            raise InstallerError("stdio MCP returned invalid JSON") from exc
        if not isinstance(result, Mapping):
            raise InstallerError("stdio MCP returned a non-object response")
        return result

    def close(self) -> None:
        if self.process.stdin:
            self.process.stdin.close()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.terminate()
            self.process.wait(timeout=5)
        if self.process.stdout:
            self.process.stdout.close()
        if self.process.stderr:
            self.process.stderr.close()


def verify_stdio(
    command: Path,
    service_url: str,
    workspace: Path,
    expected_version: str | None = None,
) -> VerificationResult:
    session = _StdioSession(command, service_url, workspace)
    try:
        initialized = _mcp_result(session.request({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {
                    "name": "marketcow-mcp-project",
                    "version": INSTALLER_VERSION,
                },
            },
        }), 1)
        listed = _mcp_result(session.request({
            "jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}
        }), 2)
        health_call = _mcp_result(session.request({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {"name": "service_health", "arguments": {}},
        }), 3)
    finally:
        session.close()
    return _verification_from_results(
        "stdio", initialized, listed, health_call, expected_version
    )


def verify_target(
    *,
    transport: str,
    service_url: str,
    workspace: Path,
    stdio_command: Path | None,
    expected_version: str | None,
) -> VerificationResult:
    if transport == "http":
        return verify_http(service_url, expected_version)
    if stdio_command is None:
        raise InstallerError("stdio transport requires --stdio-command")
    return verify_stdio(stdio_command, service_url, workspace, expected_version)


def _read_config(path: Path) -> tuple[str, bytes | None]:
    if not path.exists():
        return "", None
    if path.is_symlink() or not path.is_file():
        raise InstallerError("<workspace>/.codex/config.toml must be a regular file")
    raw = path.read_bytes()
    try:
        return raw.decode("utf-8"), raw
    except UnicodeDecodeError as exc:
        raise InstallerError("existing config.toml must be UTF-8") from exc


def install_or_upgrade(
    paths: WorkspacePaths,
    *,
    action: str,
    transport: str,
    service_url: str,
    stdio_command: Path | None,
    expected_version: str | None,
    dry_run: bool,
) -> dict[str, Any]:
    content, original = _read_config(paths.config)
    _, existing_block = _without_managed_block(content)
    _parse_unmanaged_config(content)
    if action == "upgrade" and existing_block is None:
        raise InstallerError("upgrade requires an existing MarketCow managed block")
    before = verify_target(
        transport=transport,
        service_url=service_url,
        workspace=paths.workspace,
        stdio_command=stdio_command,
        expected_version=expected_version,
    )
    block = managed_block(
        transport=transport,
        service_url=service_url,
        observed_version=before.marketcow_version,
        stdio_command=stdio_command,
    )
    merged = merge_managed_block(content, block)
    changed = merged.encode("utf-8") != (original or b"")
    backup: Path | None = None
    if changed and not dry_run:
        backup = _backup(paths, original)
        _atomic_write(paths.config, merged.encode("utf-8"))
    after = before if dry_run else verify_target(
        transport=transport,
        service_url=service_url,
        workspace=paths.workspace,
        stdio_command=stdio_command,
        expected_version=expected_version,
    )
    return {
        "action": action,
        "status": "dry_run" if dry_run else "success",
        "changed": changed,
        "workspace": str(paths.workspace),
        "config": str(paths.config),
        "backup": str(backup) if backup else None,
        "restore_command": (
            f"marketcow-mcp-project restore --workspace {_toml_string(str(paths.workspace))} "
            f"--backup {_toml_string(str(backup))}" if backup else None
        ),
        "verification_before": before.payload(),
        "verification_after": after.payload(),
    }


def uninstall(paths: WorkspacePaths, *, dry_run: bool) -> dict[str, Any]:
    content, original = _read_config(paths.config)
    unmanaged, existing = _without_managed_block(content)
    _parse_unmanaged_config(content)
    if existing is None:
        return {
            "action": "uninstall", "status": "dry_run" if dry_run else "success",
            "changed": False, "workspace": str(paths.workspace),
            "config": str(paths.config), "backup": None, "restore_command": None,
        }
    output = unmanaged.encode("utf-8")
    backup: Path | None = None
    if not dry_run:
        backup = _backup(paths, original)
        _atomic_write(paths.config, output)
    return {
        "action": "uninstall",
        "status": "dry_run" if dry_run else "success",
        "changed": True,
        "workspace": str(paths.workspace),
        "config": str(paths.config),
        "backup": str(backup) if backup else None,
        "restore_command": (
            f"marketcow-mcp-project restore --workspace {_toml_string(str(paths.workspace))} "
            f"--backup {_toml_string(str(backup))}" if backup else None
        ),
    }


def restore(paths: WorkspacePaths, backup_value: str, *, dry_run: bool) -> dict[str, Any]:
    backup = Path(backup_value).expanduser()
    if not backup.is_absolute():
        raise InstallerError("--backup must be an absolute path reported by this installer")
    try:
        resolved = backup.resolve(strict=True)
    except FileNotFoundError as exc:
        raise InstallerError("backup does not exist") from exc
    _ensure_backup_directory(paths, create=False)
    try:
        backup_root = paths.backups.resolve(strict=True)
    except FileNotFoundError as exc:
        raise InstallerError("no MarketCow backups exist for this workspace") from exc
    if resolved.parent != backup_root or not resolved.name.startswith("config."):
        raise InstallerError("backup must be a MarketCow backup for this workspace")
    current = paths.config.read_bytes() if paths.config.exists() else None
    rollback: Path | None = None
    removes_config = resolved.suffix == ".absent"
    if not dry_run:
        rollback = _backup(paths, current)
        if removes_config:
            paths.config.unlink(missing_ok=True)
        else:
            restored = resolved.read_bytes()
            try:
                tomllib.loads(restored.decode("utf-8"))
            except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
                raise InstallerError("backup is not valid UTF-8 TOML") from exc
            _atomic_write(paths.config, restored)
    return {
        "action": "restore",
        "status": "dry_run" if dry_run else "success",
        "changed": True,
        "workspace": str(paths.workspace),
        "config": str(paths.config),
        "restored_backup": str(resolved),
        "rollback_backup": str(rollback) if rollback else None,
        "config_removed": removes_config,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="marketcow-mcp-project",
        description=(
            "Install, verify, upgrade, uninstall, or restore the MarketCow MCP "
            "configuration for exactly one trusted Codex workspace."
        ),
    )
    subparsers = parser.add_subparsers(dest="action", required=True)

    def workspace_options(candidate: argparse.ArgumentParser) -> None:
        candidate.add_argument(
            "--workspace", required=True,
            help=(
                "Absolute path to the existing Codex workspace directory to manage; "
                "Git metadata is optional."
            ),
        )
        candidate.add_argument(
            "--dry-run", action="store_true",
            help="Validate and report the operation without writing files.",
        )

    def connection_options(candidate: argparse.ArgumentParser) -> None:
        candidate.add_argument(
            "--transport", choices=("http", "stdio"), default="http",
            help="Codex MCP transport; Streamable HTTP is the default.",
        )
        candidate.add_argument(
            "--service-url", default="http://127.0.0.1:8790/mcp",
            help="Streamable HTTP MCP URL; must end in /mcp (default: production 8790).",
        )
        candidate.add_argument(
            "--stdio-command",
            help="Absolute executable path for stdio transport; never resolved from cwd.",
        )
        candidate.add_argument(
            "--expected-version",
            help="Optional exact MarketCow version required during verification.",
        )

    for action in ("install", "upgrade", "verify"):
        candidate = subparsers.add_parser(action)
        workspace_options(candidate)
        connection_options(candidate)
    remove = subparsers.add_parser("uninstall")
    workspace_options(remove)
    recover = subparsers.add_parser("restore")
    workspace_options(recover)
    recover.add_argument(
        "--backup", required=True,
        help="Absolute backup path previously reported for this workspace.",
    )
    return parser


def run(arguments: argparse.Namespace) -> dict[str, Any]:
    paths = validate_workspace(arguments.workspace)
    if arguments.action == "uninstall":
        return uninstall(paths, dry_run=arguments.dry_run)
    if arguments.action == "restore":
        return restore(paths, arguments.backup, dry_run=arguments.dry_run)
    service_url = validate_service_url(arguments.service_url)
    stdio_command = (
        validate_stdio_command(arguments.stdio_command)
        if arguments.stdio_command else None
    )
    if arguments.transport == "stdio" and stdio_command is None:
        raise InstallerError("stdio transport requires --stdio-command")
    if arguments.transport == "http" and stdio_command is not None:
        raise InstallerError("--stdio-command is only valid with --transport stdio")
    if arguments.action == "verify":
        result = verify_target(
            transport=arguments.transport,
            service_url=service_url,
            workspace=paths.workspace,
            stdio_command=stdio_command,
            expected_version=arguments.expected_version,
        )
        return {
            "action": "verify", "status": "success", "changed": False,
            "workspace": str(paths.workspace), "verification": result.payload(),
        }
    return install_or_upgrade(
        paths,
        action=arguments.action,
        transport=arguments.transport,
        service_url=service_url,
        stdio_command=stdio_command,
        expected_version=arguments.expected_version,
        dry_run=arguments.dry_run,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        result = run(arguments)
    except InstallerError as exc:
        print(json.dumps({
            "status": "error", "error": str(exc), "action": arguments.action
        }, ensure_ascii=False), file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
