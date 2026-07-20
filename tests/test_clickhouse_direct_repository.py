import ast
from pathlib import Path
import unittest

from marketcow.clickhouse_repositories import (
    ClickHouseDatabase,
    ClickHouseMarketBarRepository,
    ClickHouseRepositoryError,
)
from marketcow.repositories import MarketBarRepository


SOURCE = Path(__file__).resolve().parents[1] / "src" / "marketcow"


def _module_name(path: Path) -> str:
    relative = path.relative_to(SOURCE).with_suffix("")
    parts = list(relative.parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(("marketcow", *parts))


def _imports_for(path: Path) -> set[str]:
    owner = _module_name(path)
    package = owner.split(".")[:-1]
    imports: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(), filename=str(path))):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                base = package[: len(package) - node.level + 1]
                prefix = ".".join((*base, node.module or ""))
            else:
                prefix = node.module or ""
            imports.add(prefix.rstrip("."))
    return imports


def _forbidden_paths(
    graph: dict[str, set[str]], entrypoint: str, forbidden: set[str]
) -> set[tuple[str, ...]]:
    violations: set[tuple[str, ...]] = set()
    pending = [(entrypoint,)]
    visited: set[str] = set()
    while pending:
        path = pending.pop()
        owner = path[-1]
        if owner in visited:
            continue
        visited.add(owner)
        for imported in graph.get(owner, set()):
            candidate = (*path, imported)
            if imported in forbidden or any(
                imported.startswith(f"{item}.") for item in forbidden
            ):
                violations.add(candidate)
            elif imported in graph and imported not in path:
                pending.append(candidate)
    return violations


class _FailingClient:
    def query(self, *_args, **_kwargs):
        raise RuntimeError("password=secret " + "x" * 10000)


class ClickHouseDirectRepositoryPolicyTest(unittest.TestCase):
    def test_direct_repository_satisfies_complete_contract(self):
        repository = ClickHouseMarketBarRepository(
            ClickHouseDatabase("127.0.0.1", 8123, "marketcow_test")
        )
        self.assertIsInstance(repository, MarketBarRepository)
        required = {
            name for name, value in MarketBarRepository.__dict__.items()
            if callable(value) and not name.startswith("_")
        }
        self.assertEqual(required - set(dir(repository)), set())

    def test_direct_call_chain_has_no_duckdb_or_offline_dependency(self):
        graph = {_module_name(path): _imports_for(path) for path in SOURCE.rglob("*.py")}
        forbidden = {
            "duckdb", "marketcow.storage", "marketcow.duckdb_repositories",
            "marketcow.clickhouse_shadow", "marketcow.local_backfill",
            "marketcow.local_restore",
        }
        violations = _forbidden_paths(
            graph, "marketcow.clickhouse_repositories", forbidden
        )
        self.assertEqual(violations, set(), sorted(violations))
        source = (SOURCE / "clickhouse_repositories.py").read_text()
        self.assertNotIn(" OFFSET ", source.upper())

    def test_transitive_gate_reports_complete_reachable_path(self):
        graph = {
            "marketcow.clickhouse_repositories": {"marketcow.bridge"},
            "marketcow.bridge": {"marketcow.second"},
            "marketcow.second": {"duckdb.engine"},
        }
        self.assertEqual(
            _forbidden_paths(
                graph, "marketcow.clickhouse_repositories", {"duckdb"}
            ),
            {(
                "marketcow.clickhouse_repositories", "marketcow.bridge",
                "marketcow.second", "duckdb.engine",
            )},
        )

    def test_backend_errors_are_bounded_and_do_not_fallback(self):
        database = ClickHouseDatabase("127.0.0.1", 8123, "marketcow_test")
        database.client = _FailingClient()
        repository = ClickHouseMarketBarRepository(database)
        with self.assertRaises(ClickHouseRepositoryError) as raised:
            repository.get_latest_quotes(["FAIL"])
        message = str(raised.exception)
        self.assertLessEqual(len(message), 100)
        self.assertNotIn("secret", message)
        self.assertNotIn("duckdb", message.lower())


if __name__ == "__main__":
    unittest.main()
