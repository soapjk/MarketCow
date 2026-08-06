from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
import threading
import tomllib
import unittest
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from marketcow.mcp_project_installer import (
    INSTALLER_VERSION,
    MANAGED_BEGIN,
    REQUIRED_TOOLS,
    _StdioSession,
    InstallerError,
    VerificationResult,
    build_parser,
    install_or_upgrade,
    restore,
    run,
    uninstall,
    validate_service_url,
    validate_stdio_command,
    validate_workspace,
    verify_http,
    verify_stdio,
)


def verified(*, transport: str = "http", version: str = "0.2.0") -> VerificationResult:
    tools = tuple(sorted(REQUIRED_TOOLS | {"future_read_tool"}))
    return VerificationResult(
        transport=transport,
        marketcow_version=version,
        tool_count=len(tools),
        tools=tools,
        missing_tools=(),
        extra_tools=("future_read_tool",),
        service_health={"status": "ok", "profile": "test"},
    )


class McpHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    tools = sorted(REQUIRED_TOOLS | {"future_read_tool"})

    def log_message(self, *_args) -> None:
        pass

    def _json(self, payload: dict) -> None:
        content = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def do_GET(self) -> None:
        if self.path == "/v1/health":
            self._json({"status": "ok", "profile": "test"})
        else:
            self.send_error(404)

    def do_POST(self) -> None:
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        message = json.loads(body)
        method = message["method"]
        if method == "initialize":
            result = {
                "protocolVersion": "2025-11-25",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "marketcow", "version": "0.2.0"},
            }
        elif method == "tools/list":
            result = {"tools": [{"name": name} for name in self.tools]}
        elif method == "tools/call":
            result = {
                "isError": False,
                "structuredContent": {"status": "ok", "profile": "test"},
                "content": [],
            }
        else:
            self.send_error(400)
            return
        self._json({"jsonrpc": "2.0", "id": message["id"], "result": result})


@contextmanager
def mcp_http_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), McpHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/mcp"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


class WorkspaceFixture(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        root = Path(self.temporary.name)
        self.workspace = root / "workspace"
        self.workspace.mkdir()
        subprocess.run(
            ["git", "init", "-q", str(self.workspace)], check=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        self.codex_home = root / "codex-home"
        self.codex_home.mkdir()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def paths(self):
        return validate_workspace(
            str(self.workspace), codex_home=str(self.codex_home)
        )


class WorkspaceSafetyTest(WorkspaceFixture):
    def test_workspace_must_be_explicit_git_root(self) -> None:
        with self.assertRaisesRegex(InstallerError, "absolute"):
            validate_workspace("workspace", codex_home=str(self.codex_home))
        child = self.workspace / "child"
        child.mkdir()
        with self.assertRaisesRegex(InstallerError, "exact Git root"):
            validate_workspace(str(child), codex_home=str(self.codex_home))

    def test_home_and_codex_home_fail_closed(self) -> None:
        with self.assertRaisesRegex(InstallerError, "HOME"):
            validate_workspace(str(Path.home()))
        subprocess.run(["git", "init", "-q", str(self.codex_home)], check=True)
        with self.assertRaisesRegex(InstallerError, "CODEX_HOME"):
            validate_workspace(
                str(self.codex_home), codex_home=str(self.codex_home)
            )

    def test_non_writable_workspace_and_symlink_codex_dir_are_rejected(self) -> None:
        original = self.workspace.stat().st_mode
        self.workspace.chmod(0o555)
        try:
            with self.assertRaisesRegex(InstallerError, "not writable"):
                self.paths()
        finally:
            self.workspace.chmod(stat.S_IMODE(original))
        target = Path(self.temporary.name) / "elsewhere"
        target.mkdir()
        (self.workspace / ".codex").symlink_to(target, target_is_directory=True)
        with self.assertRaisesRegex(InstallerError, "real directory"):
            self.paths()

    def test_url_and_stdio_entry_are_strict(self) -> None:
        self.assertEqual(
            validate_service_url("http://127.0.0.1:8790/mcp/"),
            "http://127.0.0.1:8790/mcp",
        )
        for invalid in (
            "file:///tmp/mcp", "http://user:pass@localhost/mcp",
            "http://localhost/v1/health", "http://localhost/mcp?q=1",
        ):
            with self.assertRaises(InstallerError):
                validate_service_url(invalid)
        with self.assertRaisesRegex(InstallerError, "absolute"):
            validate_stdio_command("marketcow-mcp")


class ManagedConfigTest(WorkspaceFixture):
    def install(self, **updates):
        values = {
            "action": "install",
            "transport": "http",
            "service_url": "http://127.0.0.1:8790/mcp",
            "stdio_command": None,
            "expected_version": "0.2.0",
            "dry_run": False,
        }
        values.update(updates)
        with patch(
            "marketcow.mcp_project_installer.verify_target",
            return_value=verified(transport=values["transport"]),
        ):
            return install_or_upgrade(self.paths(), **values)

    def test_empty_install_is_atomic_backed_up_and_valid_toml(self) -> None:
        result = self.install()
        config = self.workspace / ".codex/config.toml"
        content = config.read_text()
        parsed = tomllib.loads(content)
        self.assertEqual(parsed["mcp_servers"]["marketcow"]["url"],
                         "http://127.0.0.1:8790/mcp")
        self.assertIn(MANAGED_BEGIN, content)
        self.assertIn(f"installer_version = {INSTALLER_VERSION}", content)
        self.assertTrue(Path(result["backup"]).name.endswith(".absent"))
        self.assertFalse(any(config.parent.glob(".config.toml.*")))

    def test_existing_investrace_config_is_preserved(self) -> None:
        codex = self.workspace / ".codex"
        codex.mkdir()
        original = (
            "[mcp_servers.investrace]\n"
            'url = "http://127.0.0.1:8798/mcp"\n'
            "enabled = true\n"
        )
        (codex / "config.toml").write_text(original)
        self.install()
        content = (codex / "config.toml").read_text()
        parsed = tomllib.loads(content)
        self.assertEqual(
            parsed["mcp_servers"]["investrace"]["url"],
            "http://127.0.0.1:8798/mcp",
        )
        self.assertEqual(content[:len(original)], original)

    def test_reinstall_is_idempotent_without_second_backup(self) -> None:
        first = self.install()
        config = self.workspace / ".codex/config.toml"
        before = config.read_bytes()
        backups = list((self.workspace / ".codex/backups/marketcow-mcp").iterdir())
        second = self.install()
        self.assertFalse(second["changed"])
        self.assertIsNone(second["backup"])
        self.assertEqual(config.read_bytes(), before)
        self.assertEqual(
            list((self.workspace / ".codex/backups/marketcow-mcp").iterdir()),
            backups,
        )
        self.assertIsNotNone(first["restore_command"])

    def test_upgrade_uninstall_and_restore_preserve_other_servers(self) -> None:
        codex = self.workspace / ".codex"
        codex.mkdir()
        original = (
            "[mcp_servers.investrace]\n"
            'url = "http://127.0.0.1:8798/mcp"\n'
        )
        (codex / "config.toml").write_text(original)
        self.install()
        upgraded = self.install(
            action="upgrade", service_url="http://127.0.0.1:8792/mcp"
        )
        self.assertIn("8792/mcp", (codex / "config.toml").read_text())
        removed = uninstall(self.paths(), dry_run=False)
        self.assertNotIn(MANAGED_BEGIN, (codex / "config.toml").read_text())
        self.assertIn("investrace", (codex / "config.toml").read_text())
        restored = restore(self.paths(), removed["backup"], dry_run=False)
        self.assertIn(MANAGED_BEGIN, (codex / "config.toml").read_text())
        self.assertIn("8792/mcp", (codex / "config.toml").read_text())
        self.assertFalse(restored["config_removed"])
        self.assertTrue(Path(upgraded["backup"]).is_file())

    def test_dry_run_does_not_create_codex_directory(self) -> None:
        result = self.install(dry_run=True)
        self.assertEqual(result["status"], "dry_run")
        self.assertFalse((self.workspace / ".codex").exists())

    def test_unmanaged_same_name_and_invalid_toml_fail_before_verification(self) -> None:
        codex = self.workspace / ".codex"
        codex.mkdir()
        config = codex / "config.toml"
        config.write_text('[mcp_servers.marketcow]\nurl = "http://elsewhere/mcp"\n')
        with patch("marketcow.mcp_project_installer.verify_target") as verify:
            with self.assertRaisesRegex(InstallerError, "unmanaged"):
                install_or_upgrade(
                    self.paths(), action="install", transport="http",
                    service_url="http://127.0.0.1:8790/mcp", stdio_command=None,
                    expected_version=None, dry_run=False,
                )
        verify.assert_not_called()
        config.write_text("[broken\n")
        with self.assertRaisesRegex(InstallerError, "invalid TOML"):
            self.install()

    def test_restore_rejects_backup_from_another_location(self) -> None:
        foreign = Path(self.temporary.name) / "config.foreign.toml"
        foreign.write_text("")
        with self.assertRaisesRegex(InstallerError, "backups exist|this workspace"):
            restore(self.paths(), str(foreign), dry_run=False)

    def test_upgrade_requires_managed_install(self) -> None:
        with patch("marketcow.mcp_project_installer.verify_target"):
            with self.assertRaisesRegex(InstallerError, "requires"):
                install_or_upgrade(
                    self.paths(), action="upgrade", transport="http",
                    service_url="http://127.0.0.1:8790/mcp", stdio_command=None,
                    expected_version=None, dry_run=False,
                )

    def test_backup_symlink_escape_is_rejected(self) -> None:
        codex = self.workspace / ".codex"
        codex.mkdir()
        outside = Path(self.temporary.name) / "outside"
        outside.mkdir()
        (codex / "backups").symlink_to(outside, target_is_directory=True)
        with self.assertRaisesRegex(InstallerError, "real directory"):
            self.install()
        self.assertEqual(list(outside.iterdir()), [])
        self.assertFalse((codex / "config.toml").exists())


class ProtocolVerificationTest(WorkspaceFixture):
    def test_http_initialize_tools_and_health_are_verified(self) -> None:
        with mcp_http_server() as service_url:
            result = verify_http(service_url, expected_version="0.2.0")
            self.assertEqual(result.marketcow_version, "0.2.0")
            self.assertGreater(result.tool_count, len(REQUIRED_TOOLS))
            self.assertEqual(result.extra_tools, ("future_read_tool",))
            self.assertEqual(result.service_health["status"], "ok")

    def test_missing_tools_and_version_mismatch_fail_closed(self) -> None:
        original = McpHandler.tools
        try:
            McpHandler.tools = sorted(REQUIRED_TOOLS - {"get_dividends"})
            with mcp_http_server() as service_url:
                with self.assertRaisesRegex(InstallerError, "missing required"):
                    verify_http(service_url)
        finally:
            McpHandler.tools = original
        with mcp_http_server() as service_url:
            with self.assertRaisesRegex(InstallerError, "version mismatch"):
                verify_http(service_url, expected_version="9.9.9")

    def test_stdio_absolute_entry_starts_from_arbitrary_workspace(self) -> None:
        script = Path(self.temporary.name) / "fake-marketcow-mcp"
        cwd_record = Path(self.temporary.name) / "stdio-cwd.txt"
        script.write_text(
            f"#!{sys.executable}\n"
            "import json, os, sys\n"
            f"open({str(cwd_record)!r}, 'w').write(os.getcwd())\n"
            f"tools={sorted(REQUIRED_TOOLS | {'future_read_tool'})!r}\n"
            "for line in sys.stdin:\n"
            " m=json.loads(line); method=m['method']\n"
            " if method=='initialize': r={'protocolVersion':'2025-11-25','serverInfo':{'name':'marketcow','version':'0.2.0'}}\n"
            " elif method=='tools/list': r={'tools':[{'name': n} for n in tools]}\n"
            " else: r={'isError':False,'structuredContent':{'status':'ok'},'content':[]}\n"
            " print(json.dumps({'jsonrpc':'2.0','id':m['id'],'result':r}), flush=True)\n"
        )
        script.chmod(0o755)
        result = verify_stdio(
            validate_stdio_command(str(script)),
            "http://127.0.0.1:8790/mcp",
            self.workspace,
            expected_version="0.2.0",
        )
        self.assertEqual(result.transport, "stdio")
        self.assertEqual(Path(cwd_record.read_text()).resolve(), self.workspace.resolve())

    def test_stdio_response_timeout_is_bounded(self) -> None:
        script = Path(self.temporary.name) / "silent-mcp"
        script.write_text(
            f"#!{sys.executable}\n"
            "import sys, time\n"
            "sys.stdin.readline()\n"
            "time.sleep(30)\n"
        )
        script.chmod(0o755)
        session = _StdioSession(
            script,
            "http://127.0.0.1:8790/mcp",
            self.workspace,
            response_timeout=0.05,
        )
        try:
            with self.assertRaisesRegex(InstallerError, "timed out"):
                session.request({"jsonrpc": "2.0", "id": 1, "method": "initialize"})
        finally:
            session.close()

    def test_http_install_cli_path_writes_only_project_config_and_backup(self) -> None:
        parser = build_parser()
        with mcp_http_server() as service_url:
            arguments = parser.parse_args([
                "install", "--workspace", str(self.workspace),
                "--service-url", service_url, "--expected-version", "0.2.0",
            ])
            with patch.dict(os.environ, {"CODEX_HOME": str(self.codex_home)}):
                result = run(arguments)
        self.assertTrue(result["changed"])
        files = {
            path.relative_to(self.workspace).as_posix()
            for path in self.workspace.rglob("*") if path.is_file()
            and ".git/" not in path.relative_to(self.workspace).as_posix()
        }
        self.assertIn(".codex/config.toml", files)
        self.assertEqual(
            {path for path in files if not path.startswith(".codex/backups/")},
            {".codex/config.toml"},
        )
        self.assertFalse(any(self.codex_home.iterdir()))


if __name__ == "__main__":
    unittest.main()
