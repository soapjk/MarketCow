from __future__ import annotations

import tempfile
import unittest
import os
import subprocess
import sys
from dataclasses import replace
from pathlib import Path
from urllib.parse import urlsplit
from unittest.mock import patch

from marketcow.config import Settings
from marketcow.clickhouse_writer import LocalClickHouseSpool
from marketcow.postgres_migrations import POSTGRES_TRANSACTION_DOMAINS
from marketcow.v2_factory import V2FactoryDependencies, create_v2_online_repositories


class V2FactoryTest(unittest.TestCase):
    def settings(self, root: Path, *, scheduler: bool = True) -> Settings:
        return Settings(
            database_path=None, raw_path=root / "raw", profile="v2-test",
            port=8793,
            metadata_backend="postgres",
            postgres_dsn="postgresql://user:test@127.0.0.1/marketcow_test",
            postgres_schema="marketcow_test", clickhouse_enabled=True,
            clickhouse_database="marketcow_test", clickhouse_password="test",
            storage_root=root, clickhouse_spool_path=root / "spool" / "clickhouse",
            market_bar_read_backend="clickhouse_canonical",
            raw_market_bar_read_backend="clickhouse_raw",
            runtime_architecture="postgres_clickhouse_v2",
            runtime_config_schema="marketcow.v2-runtime-config.v1",
            postgres_dsn_ref="TEST_POSTGRES_DSN",
            clickhouse_password_ref="TEST_CLICKHOUSE_PASSWORD",
            v2_allowed_root=root.parent,
            clickhouse_background_canonical=scheduler,
        )

    def dependencies(self, events, *, fail=""):
        class Resource:
            def __init__(self, name):
                self.name = name
                events.append("create:" + name)

            def open(self):
                events.append("open:" + self.name)
                if fail == self.name:
                    raise RuntimeError("startup failed")

            def close(self):
                events.append("close:" + self.name)

        class Spool:
            def __init__(self, *_args, **_kwargs):
                events.append("create:spool")
                if fail == "spool":
                    raise RuntimeError("startup failed")
                self.telemetry = None

        class Scheduler(Resource):
            def bind_writer(self, writer):
                events.append("bind:scheduler")
                writer.bound_scheduler = self

        class Writer:
            def __init__(self, name):
                self.name = name
                self.bound_scheduler = None

        def scheduler(enabled, **_kwargs):
            events.append("scheduler:" + str(enabled).lower())
            if not enabled:
                return None
            resource = Scheduler("scheduler")
            if fail == "scheduler":
                raise RuntimeError("startup failed")
            return resource

        clickhouse_count = 0

        def clickhouse_database(**_kwargs):
            nonlocal clickhouse_count
            clickhouse_count += 1
            name = "clickhouse" if clickhouse_count == 1 else "scheduler_clickhouse"
            return Resource(name)

        repository_count = 0

        def clickhouse_repository(database):
            nonlocal repository_count
            repository_count += 1
            name = "ch_repository" if repository_count == 1 else "scheduler_repository"
            events.append("create:" + name)
            if fail == name:
                raise RuntimeError("startup failed")
            return database

        writer_count = 0

        def writer(*_args, **_kwargs):
            nonlocal writer_count
            writer_count += 1
            name = "writer" if writer_count == 1 else "scheduler_writer"
            events.append("create:" + name)
            if fail == name:
                raise RuntimeError("startup failed")
            return Writer(name)

        def builder(*_args, **_kwargs):
            name = ("scheduler_builder"
                    if _args[0].name == "scheduler_clickhouse" else "builder")
            events.append("create:" + name)
            if fail == name:
                raise RuntimeError("startup failed")
            return {"name": name, "repository": _args[0], "writer": _args[1]}

        return V2FactoryDependencies(
            postgres_database=lambda *_a, **_k: Resource("postgres"),
            postgres_repository=lambda db: (events.append("create:pg_repository"), db)[1],
            clickhouse_database=clickhouse_database,
            clickhouse_repository=clickhouse_repository,
            telemetry=lambda **_k: (events.append("create:telemetry"), object())[1],
            spool=Spool,
            writer=writer,
            canonical_builder=builder,
            canonical_scheduler=scheduler,
        )

    def test_exact_routing_order_reverse_close_and_idempotence(self):
        with tempfile.TemporaryDirectory(suffix="-test") as folder:
            events = []
            resources = create_v2_online_repositories(
                self.settings(Path(folder)), self.dependencies(events)
            )
            self.assertEqual(set(resources.transaction_domains), set(POSTGRES_TRANSACTION_DOMAINS))
            self.assertTrue(all(value is resources.postgres
                                for value in resources.transaction_domains.values()))
            self.assertLess(events.index("open:postgres"), events.index("open:clickhouse"))
            self.assertLess(events.index("open:clickhouse"), events.index("create:spool"))
            self.assertLess(events.index("create:spool"),
                            events.index("open:scheduler_clickhouse"))
            self.assertNotIn("create:builder", events)
            self.assertLess(events.index("open:scheduler_clickhouse"),
                            events.index("create:scheduler_repository"))
            self.assertLess(events.index("create:scheduler_builder"),
                            events.index("create:scheduler"))
            self.assertIsNot(resources.clickhouse_database,
                             resources.scheduler_clickhouse_database)
            self.assertIsNot(resources.market_bars, resources.scheduler_market_bars)
            self.assertIsNot(resources.writer, resources.scheduler_writer)
            self.assertIs(resources.writer.bound_scheduler,
                          resources.canonical_scheduler)
            self.assertIsNone(resources.scheduler_writer.bound_scheduler)
            self.assertIs(resources.canonical_builder["repository"],
                          resources.scheduler_market_bars)
            resources.close()
            resources.close()
            self.assertEqual(events[-4:], [
                "close:scheduler", "close:scheduler_clickhouse",
                "close:clickhouse", "close:postgres"
            ])

    def test_postgres_probe_timeouts_are_explicitly_forwarded(self):
        with tempfile.TemporaryDirectory(suffix="-test") as folder:
            captured = {}
            dependencies = self.dependencies([])
            original = dependencies.postgres_database

            def postgres(*args, **kwargs):
                captured.update(kwargs)
                return original(*args, **kwargs)

            resources = create_v2_online_repositories(
                self.settings(Path(folder)),
                V2FactoryDependencies(**{
                    **dependencies.__dict__, "postgres_database": postgres,
                }),
            )
            resources.close()
            self.assertEqual(captured, {
                "connect_timeout": 2.0, "read_timeout": 5.0,
            })

    def test_partial_startup_failures_close_all_prior_connections(self):
        for failure, expected in (
            ("postgres", ["close:postgres"]),
            ("clickhouse", ["close:clickhouse", "close:postgres"]),
            ("spool", ["close:clickhouse", "close:postgres"]),
            ("scheduler_clickhouse", [
                "close:scheduler_clickhouse", "close:clickhouse", "close:postgres"
            ]),
            ("scheduler_repository", [
                "close:scheduler_clickhouse", "close:clickhouse", "close:postgres"
            ]),
            ("scheduler_writer", [
                "close:scheduler_clickhouse", "close:clickhouse", "close:postgres"
            ]),
            ("scheduler_builder", [
                "close:scheduler_clickhouse", "close:clickhouse", "close:postgres"
            ]),
            ("scheduler", [
                "close:scheduler_clickhouse", "close:clickhouse", "close:postgres"
            ]),
        ):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory(
                suffix="-test"
            ) as folder:
                events = []
                with self.assertRaises(RuntimeError):
                    create_v2_online_repositories(
                        self.settings(Path(folder)), self.dependencies(events, fail=failure)
                    )
                self.assertEqual([event for event in events if event.startswith("close:")], expected)

    def test_preflight_precedes_every_constructor_or_file_side_effect(self):
        with tempfile.TemporaryDirectory(suffix="-test") as folder:
            events = []
            invalid = replace(self.settings(Path(folder)), database_path=Path("forbidden.duckdb"))
            with patch("pathlib.Path.open", side_effect=AssertionError("file opened")), \
                    self.assertRaisesRegex(ValueError, "DuckDB"):
                create_v2_online_repositories(invalid, self.dependencies(events))
            self.assertEqual(events, [])

    def test_valid_injected_factory_never_opens_duckdb_or_constructs_warehouse(self):
        with tempfile.TemporaryDirectory(suffix="-test") as folder:
            events = []
            with patch("builtins.open", side_effect=AssertionError("file opened")), \
                    patch("pathlib.Path.open", side_effect=AssertionError("path opened")):
                resources = create_v2_online_repositories(
                    self.settings(Path(folder)), self.dependencies(events)
                )
            resources.close()

    def test_disabled_scheduler_has_no_scheduler_or_extra_path_side_effect(self):
        with tempfile.TemporaryDirectory(suffix="-test") as folder:
            events = []
            resources = create_v2_online_repositories(
                self.settings(Path(folder), scheduler=False), self.dependencies(events)
            )
            self.assertIsNone(resources.canonical_scheduler)
            self.assertIn("scheduler:false", events)
            self.assertNotIn("create:scheduler", events)
            resources.close()
            self.assertEqual(events[-2:], ["close:clickhouse", "close:postgres"])

    def test_disabled_scheduler_real_spool_has_no_scheduler_directory(self):
        with tempfile.TemporaryDirectory(suffix="-test") as folder:
            root = Path(folder)
            events = []
            deps = replace(self.dependencies(events), spool=LocalClickHouseSpool)
            resources = create_v2_online_repositories(
                self.settings(root, scheduler=False), deps
            )
            try:
                self.assertTrue((root / "spool" / "clickhouse" / "pending").is_dir())
                self.assertFalse((root / "spool" / "clickhouse" /
                                  "canonical-scheduler").exists())
            finally:
                resources.close()

    def test_factory_import_succeeds_with_duckdb_modules_trapped(self):
        script = """
import importlib.abc
import sys
class Blocked(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in {'duckdb', 'marketcow.storage', 'marketcow.duckdb_repositories'}:
            raise AssertionError('forbidden online import: ' + fullname)
        return None
sys.meta_path.insert(0, Blocked())
import marketcow.v2_factory
assert 'duckdb' not in sys.modules
assert 'marketcow.storage' not in sys.modules
assert 'marketcow.duckdb_repositories' not in sys.modules
"""
        subprocess.run([sys.executable, "-c", script], check=True)


@unittest.skipUnless(
    os.getenv("MARKETCOW_TEST_POSTGRES_DSN") and os.getenv("MARKETCOW_TEST_CLICKHOUSE_HOST"),
    "set disposable PostgreSQL and ClickHouse integration variables",
)
class V2FactoryIntegrationTest(unittest.TestCase):
    def test_real_postgres_clickhouse_factory_lifecycle(self):
        dsn = os.environ["MARKETCOW_TEST_POSTGRES_DSN"]
        parsed = urlsplit(dsn)
        host = os.environ["MARKETCOW_TEST_CLICKHOUSE_HOST"]
        port = int(os.environ.get("MARKETCOW_TEST_CLICKHOUSE_PORT", "8123"))
        password = os.environ["MARKETCOW_TEST_CLICKHOUSE_PASSWORD"]
        with tempfile.TemporaryDirectory(suffix="-test") as folder:
            root = Path(folder)
            settings = Settings(
                database_path=None, raw_path=root / "raw", profile="v2-test", port=8793,
                metadata_backend="postgres", postgres_dsn=dsn,
                postgres_schema="marketcow_test", clickhouse_enabled=True,
                clickhouse_host=host, clickhouse_port=port,
                clickhouse_database="marketcow_test",
                clickhouse_username=os.getenv(
                    "MARKETCOW_TEST_CLICKHOUSE_USERNAME", "marketcow"
                ),
                clickhouse_password=password, storage_root=root,
                clickhouse_spool_path=root / "spool" / "clickhouse",
                market_bar_read_backend="clickhouse_canonical",
                raw_market_bar_read_backend="clickhouse_raw",
                runtime_architecture="postgres_clickhouse_v2",
                runtime_config_schema="marketcow.v2-runtime-config.v1",
                postgres_dsn_ref="MARKETCOW_TEST_POSTGRES_DSN",
                clickhouse_password_ref="MARKETCOW_TEST_CLICKHOUSE_PASSWORD",
                v2_allowed_root=root.parent, clickhouse_background_canonical=True,
            )
            self.assertIn(parsed.hostname, {"127.0.0.1", "localhost", "::1"})
            resources = create_v2_online_repositories(settings)
            try:
                self.assertEqual(resources.clickhouse_database.diagnostics()["status"], "ok")
                with resources.postgres_database.connection() as connection:
                    tables = {row["tablename"] for row in connection.execute(
                        "SELECT tablename FROM pg_tables WHERE schemaname = %s",
                        (settings.postgres_schema,),
                    ).fetchall()}
                self.assertTrue(set(POSTGRES_TRANSACTION_DOMAINS).issubset(tables))
                self.assertIsNotNone(resources.canonical_scheduler)
                self.assertIsNot(resources.clickhouse_database.client,
                                 resources.scheduler_clickhouse_database.client)
                self.assertIsNot(resources.market_bars, resources.scheduler_market_bars)
                self.assertIsNot(resources.writer, resources.scheduler_writer)
                self.assertIs(resources.canonical_builder.repository,
                              resources.scheduler_market_bars)
            finally:
                resources.close()
                resources.close()


if __name__ == "__main__":
    unittest.main()
