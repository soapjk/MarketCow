import os
import importlib.util
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LAUNCHD = ROOT / "ops" / "launchd"
RUNNER_SPEC = importlib.util.spec_from_file_location("marketcow_production_runner", LAUNCHD / "run-production.py")
assert RUNNER_SPEC is not None and RUNNER_SPEC.loader is not None
RUNNER = importlib.util.module_from_spec(RUNNER_SPEC)
sys.modules[RUNNER_SPEC.name] = RUNNER
RUNNER_SPEC.loader.exec_module(RUNNER)


class LaunchdStartupTest(unittest.TestCase):
    def test_required_executable_path_preserves_virtualenv_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            target = root / "base-python"
            link = root / "venv-python"
            self._write_executable(target, "#!/bin/sh\n")
            link.symlink_to(target)
            observed = RUNNER._required_path(
                {"PYTHON": str(link)},
                "PYTHON",
                preserve_executable_symlink=True,
            )
            self.assertEqual(observed, link.absolute())

    def test_shell_scripts_are_valid(self) -> None:
        for name in (
            "start-production.sh",
            "ensure-production-storage.sh",
            "install.sh",
            "uninstall.sh",
        ):
            subprocess.run(["/bin/sh", "-n", str(LAUNCHD / name)], check=True)
        compile(
            (LAUNCHD / "run-production.py").read_text(),
            str(LAUNCHD / "run-production.py"),
            "exec",
        )

    def test_startup_bootstraps_storage_before_api(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            support = root / "support"
            project = root / "project"
            python = project / ".venv" / "bin" / "python"
            log = root / "order.log"
            support.mkdir()
            python.parent.mkdir(parents=True)
            (support / "start-production.sh").write_bytes((LAUNCHD / "start-production.sh").read_bytes())
            self._write_executable(
                support / "ensure-production-storage.sh",
                f"#!/bin/sh\necho storage >> {log!s}\n",
            )
            self._write_executable(
                python,
                f"#!/bin/sh\necho api >> {log!s}\n",
            )

            subprocess.run(
                ["/bin/sh", str(support / "start-production.sh")],
                check=True,
                env={**os.environ, "MARKETCOW_PROJECT_DIR": str(project)},
            )

            self.assertEqual(log.read_text().splitlines(), ["storage", "api"])

    def test_storage_bootstrap_starts_missing_dependencies_idempotently(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            project = root / "project"
            postgres_bin = root / "postgres-bin"
            fake_bin = root / "bin"
            state = root / "state"
            runtime = root / "runtime"
            project.mkdir()
            postgres_bin.mkdir()
            fake_bin.mkdir()
            state.mkdir()
            # Application secrets may legally contain shell metacharacters. The
            # storage bootstrap validates this file exists but never sources it.
            (project / ".env.production").write_text(
                "MARKETCOW_CLICKHOUSE_USERNAME=marketcow\nMARKETCOW_CLICKHOUSE_PASSWORD='$1$literal'\n"
            )
            (root / "clickhouse.xml").write_text("<clickhouse/>\n")

            self._write_executable(
                postgres_bin / "initdb",
                '#!/bin/sh\nwhile [ "$1" != "-D" ]; do shift; done\nmkdir -p "$2"\necho 17 > "$2/PG_VERSION"\n',
            )
            self._write_executable(
                postgres_bin / "pg_isready",
                '#!/bin/sh\n[ -f "$FAKE_STATE/postgres-ready" ]\n',
            )
            self._write_executable(
                postgres_bin / "pg_ctl",
                '#!/bin/sh\ntouch "$FAKE_STATE/postgres-ready"\necho postgres-start >> "$FAKE_LOG"\n',
            )
            self._write_executable(
                postgres_bin / "psql",
                '#!/bin/sh\n[ -f "$FAKE_STATE/database-ready" ] && echo 1\n',
            )
            self._write_executable(
                postgres_bin / "createdb",
                '#!/bin/sh\ntouch "$FAKE_STATE/database-ready"\necho database-create >> "$FAKE_LOG"\n',
            )
            clickhouse = fake_bin / "clickhouse"
            self._write_executable(
                clickhouse,
                "#!/bin/sh\n"
                "[ \"$MARKETCOW_CLICKHOUSE_PASSWORD\" = '$1$literal' ]\n"
                'touch "$FAKE_STATE/clickhouse-ready"\n'
                'echo clickhouse-start >> "$FAKE_LOG"\n',
            )
            self._write_executable(
                fake_bin / "curl",
                '#!/bin/sh\n[ -f "$FAKE_STATE/clickhouse-ready" ]\n',
            )
            log = root / "actions.log"
            environment = {
                **os.environ,
                "PATH": f"{fake_bin}:{os.environ['PATH']}",
                "FAKE_STATE": str(state),
                "FAKE_LOG": str(log),
                "MARKETCOW_PROJECT_DIR": str(project),
                "MARKETCOW_RUNTIME_DIR": str(runtime),
                "MARKETCOW_POSTGRES_BIN": str(postgres_bin),
                "MARKETCOW_CLICKHOUSE_BIN": str(clickhouse),
                "MARKETCOW_CLICKHOUSE_CONFIG": str(root / "clickhouse.xml"),
                "MARKETCOW_STORAGE_READY_INTERVAL": "0",
            }

            for _ in range(2):
                subprocess.run(
                    ["/bin/sh", str(LAUNCHD / "ensure-production-storage.sh")],
                    check=True,
                    env=environment,
                    capture_output=True,
                    text=True,
                )

            self.assertEqual(
                log.read_text().splitlines(),
                ["postgres-start", "database-create", "clickhouse-start"],
            )

    def test_installer_copies_dependency_bootstrap_assets(self) -> None:
        installer = (LAUNCHD / "install.sh").read_text()
        self.assertIn('cp "$script_dir/ensure-production-storage.sh"', installer)
        self.assertIn('cp "$script_dir/clickhouse-production.xml"', installer)
        self.assertIn(
            '[ -f "$target_env" ] || cp "$project_dir/.env.production" "$target_env"',
            installer,
        )
        self.assertNotIn('cp "$script_dir/run-production.py"', installer)
        self.assertIn('com.marketcow.*.plist', installer)
        self.assertIn('retired-launch-agents', installer)
        self.assertIn('until launchctl bootstrap "$domain" "$target_plist"', installer)
        self.assertIn('cargo_bin=', installer)
        self.assertIn('rust_build_root/release/marketcow"', installer)
        self.assertIn('configure_polymarket_final_architecture.py', installer)

    def test_production_runner_builds_complete_polymarket_stack(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            project = root / "project"
            project.mkdir()
            rust_binary = root / "marketcow"
            rust_scope = root / "scope.json"
            fee_semantics = root / "fee-semantics.json"
            opportunity_config = root / "opportunity.yaml"
            refresh_config = root / "refresh.json"
            tradude_python = root / "python"
            registry = root / "registry"
            registry.mkdir()
            tradude = root / "tradude"
            tradude.mkdir()
            for path in (
                rust_binary, rust_scope, fee_semantics, opportunity_config,
                refresh_config, tradude_python,
            ):
                path.write_text("{}")
            tradude_python.chmod(0o700)
            environment = {
                "MARKETCOW_HOME": str(root / "data"),
                "MARKETCOW_RUST_BINARY": str(rust_binary),
                "MARKETCOW_RUST_SCOPE_ID": "a" * 64,
                "MARKETCOW_RUST_ADMIN_TOKEN": "test-admin-token",
                "MARKETCOW_POLYMARKET_RUST_SCOPE_FILE": str(rust_scope),
                "MARKETCOW_POLYMARKET_SCOPE_REGISTRY_ROOT": str(registry),
                "MARKETCOW_POLYMARKET_FEE_SEMANTICS_POLICY": str(fee_semantics),
                "MARKETCOW_POLYMARKET_TRADUDE_WORKTREE": str(tradude),
                "MARKETCOW_TRADUDE_PYTHON": str(tradude_python),
                "MARKETCOW_POLYMARKET_OPPORTUNITY_CONTROLLER_CONFIG": str(opportunity_config),
                "MARKETCOW_POLYMARKET_UNIVERSE_REFRESH_CONFIG": str(refresh_config),
                "MARKETCOW_POLYMARKET_DISCOVERY_DEPTH_NOTIONALS": "10,50,100,500",
            }

            services = RUNNER.build_services(
                project,
                environment,
                python="/production/python",
            )

            self.assertEqual(
                [service.name for service in services],
                [
                    "polymarket-discovery-collector",
                    "polymarket-rust-data-plane",
                    "unified-api",
                    "polymarket-opportunity-controller",
                    "polymarket-universe-activator",
                ],
            )
            commands = {service.name: service.command for service in services}
            self.assertEqual(
                commands["polymarket-rust-data-plane"],
                (str(rust_binary.resolve()), "serve"),
            )
            self.assertIn(
                "8795", commands["polymarket-discovery-collector"]
            )
            self.assertIn(
                str(fee_semantics.resolve()),
                commands["polymarket-discovery-collector"],
            )
            self.assertIn(
                "--realtime-market-limit",
                commands["polymarket-discovery-collector"],
            )
            self.assertIn(
                "1000", commands["polymarket-discovery-collector"]
            )
            self.assertIn(
                str(rust_scope.resolve()),
                commands["polymarket-discovery-collector"],
            )
            self.assertIn("8790", commands["unified-api"])
            self.assertNotIn("8791", " ".join(sum(commands.values(), ())))
            self.assertNotIn("18872", " ".join(sum(commands.values(), ())))
            self.assertNotIn("run_polymarket_live_read_api.py", " ".join(sum(commands.values(), ())))
            self.assertNotIn("run_polymarket_live_paper_scope.py", " ".join(sum(commands.values(), ())))
            self.assertNotIn("0.0.0.0", " ".join(sum(commands.values(), ())))
            rust_service = next(
                service for service in services
                if service.name == "polymarket-rust-data-plane"
            )
            self.assertEqual(
                rust_service.environment["MARKETCOW_RUST_BIND"],
                "127.0.0.1:8796",
            )
            self.assertEqual(
                rust_service.environment["MARKETCOW_RUST_SHADOW"], "false",
            )

    def test_production_runner_fails_closed_without_polymarket_scope(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            project = Path(folder)
            with self.assertRaisesRegex(ValueError, "MARKETCOW_RUST_BINARY is required"):
                RUNNER.build_services(project, {}, python="/production/python")

    def test_required_child_exit_stops_production_stack(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            project = Path(folder)
            services = (
                RUNNER.Service("long-running", ("/bin/sh", "-c", "sleep 60")),
                RUNNER.Service("failed", ("/bin/sh", "-c", "exit 7")),
            )

            result = RUNNER.supervise(
                services,
                project_dir=project,
                environment=os.environ,
                poll_seconds=0.01,
                shutdown_seconds=1,
            )

            self.assertEqual(result, 7)

    def test_production_storage_defaults_live_outside_source_checkout(self) -> None:
        storage_script = (LAUNCHD / "ensure-production-storage.sh").read_text()
        clickhouse_config = (LAUNCHD / "clickhouse-production.xml").read_text()
        expected_root = "/Volumes/T9/data/marketcow/production"

        self.assertIn(f"{expected_root}/runtime", storage_script)
        self.assertIn(f"{expected_root}/runtime/clickhouse/data/", clickhouse_config)
        self.assertIn(f"{expected_root}/runtime/clickhouse/access/", clickhouse_config)
        self.assertIn("<local_directory>", clickhouse_config)
        self.assertIn('"$clickhouse_dir/access"', storage_script)
        self.assertNotIn("/Volumes/T9/projects/marketcow/data-production", storage_script)
        self.assertNotIn("/Volumes/T9/projects/marketcow/data-production", clickhouse_config)

    def test_launchd_uses_installed_environment_and_stable_locale(self) -> None:
        plist = (LAUNCHD / "com.marketcow.production.plist").read_text()

        self.assertIn(
            "/Users/androidjk/Library/Application Support/MarketCow/production.env",
            plist,
        )
        self.assertIn("<key>LC_ALL</key>", plist)
        self.assertIn("<string>C</string>", plist)
        self.assertIn("<key>PYTHONPATH</key>", plist)
        self.assertIn("/Volumes/T9/projects/marketcow/src", plist)
        self.assertIn("MARKETCOW_PROJECT_DIR", plist)

    @staticmethod
    def _write_executable(path: Path, content: str) -> None:
        path.write_text(textwrap.dedent(content))
        path.chmod(0o700)


if __name__ == "__main__":
    unittest.main()
