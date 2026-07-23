from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.clickhouse_writer import AuthoritativeWriteError as WriterAuthoritativeWriteError
from marketcow.config import Settings
from marketcow.service import FundamentalService
from marketcow.health import HEALTH_THRESHOLDS
from marketcow.market_bars import (
    AuthoritativeMarketBarRepository,
    AuthoritativeWriteError,
)


def settings(root: Path) -> Settings:
    return Settings( raw_path=root / "raw", profile="test", port=8793,
        postgres_dsn="postgresql://user:test@127.0.0.1/marketcow_test",
        postgres_schema="marketcow_test",
        clickhouse_database="marketcow_test", clickhouse_password="test",
        storage_root=root, clickhouse_spool_path=root / "spool" / "clickhouse",
        postgres_dsn_ref="TEST_POSTGRES_DSN",
        clickhouse_password_ref="TEST_CLICKHOUSE_PASSWORD",
        allowed_root=root.parent,
    )


class AuthoritativeMarketBarAdapterTest(unittest.TestCase):
    def repository(self, outcome=None, error=None):
        direct = MagicMock()
        direct.prepare_raw_bars.return_value = [{"symbol": "A", "bar_time": "x"}]
        writer = MagicMock()
        if error:
            writer.write.side_effect = error
        else:
            writer.write.return_value = outcome
        return AuthoritativeMarketBarRepository(direct, writer), direct, writer

    def test_success_is_acknowledged_and_reads_delegate_directly(self):
        adapter, direct, writer = self.repository({
            "status": "success", "acknowledged": True, "verified": True,
            "written": 1,
        })
        self.assertEqual(adapter.upsert_price_bars(
            "A", "1d", "raw", "fixture", "2026-01-01T00:00:00Z", [{}]
        ), 1)
        writer.write.assert_called_once_with("raw", direct.prepare_raw_bars.return_value)
        direct.get_price_bars.return_value = [{"symbol": "A"}]
        self.assertEqual(adapter.get_price_bars("A", "1d", "raw", 1), [{"symbol": "A"}])

    def test_durable_pending_is_an_explicit_error_without_fallback(self):
        adapter, direct, _ = self.repository({
            "status": "durable_pending", "acknowledged": False, "verified": False,
        })
        with self.assertRaisesRegex(AuthoritativeWriteError, "durable_pending"):
            adapter.upsert_price_bars(
                "A", "1d", "raw", "fixture", "2026-01-01T00:00:00Z", [{}]
            )
        self.assertEqual([call[0] for call in direct.method_calls], ["prepare_raw_bars"])

    def test_terminal_authoritative_error_is_preserved(self):
        failure = WriterAuthoritativeWriteError({
            "status": "terminal_failure", "error": "bounded", "terminal": True,
        })
        adapter, _, _ = self.repository(error=failure)
        with self.assertRaises(WriterAuthoritativeWriteError) as raised:
            adapter.upsert_price_bars(
                "A", "1d", "raw", "fixture", "2026-01-01T00:00:00Z", [{}]
            )
        self.assertIs(raised.exception, failure)


class ServiceRoutingTest(unittest.TestCase):
    def test_service_and_api_route_pg_ch_and_close_factory_once(self):
        with tempfile.TemporaryDirectory(suffix="-test") as folder:
            root = Path(folder)
            pg = MagicMock()
            pg.get_economic_indicators.return_value = [{"indicator_id": "gdp"}]
            pg.query_fundamentals.return_value = [{"symbol": "000001"}]
            pg.list_artifacts.return_value = [{"artifact_id": "artifact-1"}]
            direct = MagicMock()
            direct.get_latest_quotes.return_value = [{"symbol": "AAPL", "close": 1.0}]
            direct.get_price_bars.return_value = []
            writer = MagicMock()
            resources = SimpleNamespace(
                postgres=pg, market_bars=direct, writer=writer,
                telemetry=None, canonical_scheduler=None, close=MagicMock(),
            )
            with patch(
                "marketcow.factory.create_online_repositories",
                return_value=resources,
            ):
                service = FundamentalService(settings(root))
            self.assertIs(service.metadata_repository, pg)
            self.assertIs(service.fundamental_repository, pg)
            self.assertIs(service.market_bar_repository.repository, direct)
            with TestClient(create_app(settings(root), service)) as client:
                self.assertEqual(client.get(
                    "/v1/economic-indicators"
                ).json()["indicators"][0]["indicator_id"], "gdp")
                self.assertEqual(client.get(
                    "/v1/fundamentals?limit=1"
                ).json()["items"][0]["symbol"], "000001")
                self.assertEqual(client.get(
                    "/v1/admin/artifacts?limit=1"
                ).json()["items"][0]["artifact_id"], "artifact-1")
                quote = client.get("/v1/quotes?symbols=AAPL&refresh=false")
                self.assertEqual(quote.status_code, 200)
                self.assertEqual(quote.json()["items"][0]["symbol"], "AAPL")
                health = client.get("/v1/health")
                self.assertEqual(
                    health.json()["database"],
                    "postgresql://marketcow_test+clickhouse://marketcow_test",
                )
                service.quote_provider = SimpleNamespace(
                    name="fixture", base_url="local://fixture",
                    fetch_history=lambda *_args: {
                        "symbol": "AAPL", "source": "fixture",
                        "source_url": "local://fixture",
                        "raw_response_locator": "payload", "_raw_payload": {"ok": True},
                        "bars": [{
                            "bar_at": "2026-07-20T00:00:00Z", "open": 1.0,
                            "high": 1.0, "low": 1.0, "close": 1.0,
                            "volume": 1.0,
                        }],
                    },
                )
                direct.prepare_raw_bars.return_value = [{"symbol": "AAPL"}]
                writer.write.return_value = {
                    "status": "success", "acknowledged": True,
                    "verified": True, "written": 1,
                }
                success = client.get(
                    "/v1/quotes/AAPL/history?refresh=true&interval=1d&adjustment=raw"
                )
                self.assertEqual(success.status_code, 200)
                writer.write.return_value = {
                    "status": "durable_pending", "acknowledged": False,
                    "verified": False,
                }
                direct.get_price_bars.return_value = []
                pending = client.get(
                    "/v1/quotes/AAPL/history?refresh=true&interval=1d&adjustment=raw"
                )
                self.assertEqual(pending.status_code, 502)
                self.assertIn("durable_pending", pending.json()["detail"])
                writer.write.side_effect = WriterAuthoritativeWriteError({
                    "status": "terminal_failure", "error": "bounded",
                    "terminal": True,
                })
                terminal = client.get(
                    "/v1/quotes/AAPL/history?refresh=true&interval=1d&adjustment=raw"
                )
                self.assertEqual(terminal.status_code, 502)
                self.assertLessEqual(len(terminal.json()["detail"]), 1000)
            resources.close.assert_called_once_with()

    def test_app_import_and_routes_under_duckdb_warehouse_open_traps(self):
        script = r'''
import importlib.abc, os, sys, tempfile
from pathlib import Path
root = Path(tempfile.mkdtemp(suffix='-test'))
os.environ.update({
 'MARKETCOW_PROFILE':'test', 'MARKETCOW_HOME':str(root),
 'MARKETCOW_ALLOWED_ROOT':str(root.parent),
 'MARKETCOW_POSTGRES_DSN_REF':'TEST_POSTGRES_DSN',
 'TEST_POSTGRES_DSN':'postgresql://u:p@127.0.0.1/marketcow_test',
 'MARKETCOW_CLICKHOUSE_PASSWORD_REF':'TEST_CLICKHOUSE_PASSWORD',
 'TEST_CLICKHOUSE_PASSWORD':'test', 'MARKETCOW_CLICKHOUSE_DATABASE':'marketcow_test',
})
class Blocked(importlib.abc.MetaPathFinder):
 def find_spec(self, fullname, path=None, target=None):
  if fullname in {'duckdb','marketcow.storage','marketcow.duckdb_repositories'}:
   raise AssertionError('forbidden: '+fullname)
sys.meta_path.insert(0, Blocked())
from marketcow import factory
class Repo:
 def get_economic_indicators(self,*a): return []
 def get_latest_quotes(self,*a): return []
 def __getattr__(self,name): return lambda *a,**k: []
class Writer: pass
class Resources:
 postgres=Repo(); market_bars=Repo(); writer=Writer(); telemetry=None
 canonical_scheduler=None
 def close(self): self.closed=True
factory.create_online_repositories=lambda settings: Resources()
from fastapi.testclient import TestClient
import marketcow.api as api
with TestClient(api.create_app()) as client:
 assert client.get('/v1/economic-indicators').status_code == 200
 assert client.get('/v1/quotes?symbols=AAPL&refresh=false').status_code == 200
 assert client.get('/v1/health').json()['database']=='postgresql://marketcow_test+clickhouse://marketcow_test'
assert 'duckdb' not in sys.modules
assert 'marketcow.storage' not in sys.modules
assert 'marketcow.duckdb_repositories' not in sys.modules
'''
        subprocess.run([sys.executable, "-c", script], check=True)


@unittest.skipUnless(
    os.getenv("MARKETCOW_TEST_POSTGRES_DSN") and
    os.getenv("MARKETCOW_TEST_CLICKHOUSE_HOST"),
    "set disposable PostgreSQL and ClickHouse integration variables",
)
class ServiceIntegrationTest(unittest.TestCase):
    def test_full_service_uses_restored_pg_ch_targets_and_shutdown(self):
        with tempfile.TemporaryDirectory(suffix="-test") as folder:
            root = Path(folder)
            settings = settings(root)
            settings = Settings(**{
                **settings.__dict__,
                "postgres_dsn": os.environ["MARKETCOW_TEST_POSTGRES_DSN"],
                "clickhouse_host": os.environ["MARKETCOW_TEST_CLICKHOUSE_HOST"],
                "clickhouse_port": int(os.environ.get(
                    "MARKETCOW_TEST_CLICKHOUSE_PORT", "8123"
                )),
                "clickhouse_username": os.environ.get(
                    "MARKETCOW_TEST_CLICKHOUSE_USERNAME", "marketcow"
                ),
                "clickhouse_password": os.environ[
                    "MARKETCOW_TEST_CLICKHOUSE_PASSWORD"
                ],
            })
            service = FundamentalService(settings)
            resources = service.online_resources
            service.market_bar_repository.upsert_price_bars(
                "AAPL", "1d", "raw", "fixture", "2026-07-21T00:00:01Z",
                [{
                    "bar_at": "2026-07-20T00:00:00Z", "open": 10.0,
                    "high": 12.0, "low": 9.0, "close": 11.0,
                    "volume": 100.0, "amount": None,
                }],
                {"market": "US", "observed_at": "2026-07-20T00:00:00Z"},
            )
            rebuilt = resources.canonical_builder.rebuild(
                "AAPL", "1d", "raw", "2026-07-20T00:00:00Z",
                "2026-07-20T00:00:00Z", 10,
            )
            self.assertEqual(rebuilt["status"], "ok")
            resources.market_bars.upsert_quote({
                "symbol": "AAPL", "source": "fixture", "close": 11.0,
                "observed_at": "2026-07-20T00:00:00Z",
                "ingested_at": "2026-07-21T00:00:01Z",
            })
            service.metadata_repository.record_provider_health(
                "fixture", True, "2026-07-21T00:00:01Z"
            )
            service.artifact_store.write_json(
                root / "raw" / "fixture", "fixture", {"ok": True},
                "fixture", "local://fixture", "payload",
                "2026-07-20T00:00:00Z", "2026-07-21T00:00:01Z",
            )
            with TestClient(create_app(settings, service)) as client:
                health = client.get("/v1/health")
                readiness = client.get("/v1/readiness")
                self.assertEqual(health.status_code, 200)
                self.assertEqual(readiness.status_code, 200)
                time.sleep(HEALTH_THRESHOLDS["degrade_after_seconds"] + 0.1)
                sustained_health = client.get("/v1/health")
                sustained_readiness = client.get("/v1/readiness")
                self.assertEqual(sustained_health.json()["storage_health"]["status"],
                                 "healthy")
                self.assertEqual(sustained_readiness.status_code, 200)
                self.assertTrue(sustained_readiness.json()["ready"])
                self.assertEqual(
                    health.json()["storage_health"]["components"]["postgresql"]["status"],
                    "healthy",
                )
                self.assertEqual(
                    health.json()["storage_health"]["components"]["clickhouse_main"]["status"],
                    "healthy",
                )
                self.assertNotIn("duckdb", health.text.lower())
                self.assertEqual(client.get(
                    "/v1/quotes?symbols=AAPL&refresh=false"
                ).json()["items"][0]["close"], 11.0)
                history = client.get(
                    "/v1/quotes/AAPL/history?refresh=false&interval=1d&adjustment=raw"
                )
                self.assertEqual(history.status_code, 200)
                self.assertEqual(history.json()["bars"][0]["close"], 11.0)
                raw = client.get(
                    "/v1/quotes/AAPL/raw-history?interval=1d&adjustment=raw&"
                    "start=2026-07-20T00:00:00Z&end=2026-07-20T00:00:00Z"
                )
                self.assertEqual(raw.status_code, 200)
                self.assertEqual(raw.json()["bars"][0]["source"], "fixture")
                self.assertEqual(client.get(
                    "/v1/sources/health"
                ).json()["items"][0]["provider"], "fixture")
                self.assertEqual(client.get(
                    "/v1/admin/artifacts"
                ).json()["items"][0]["dataset"], "fixture")
            self.assertIsNone(resources.clickhouse_database.client)
            self.assertTrue(resources.postgres_database.pool.closed)


if __name__ == "__main__":
    unittest.main()
