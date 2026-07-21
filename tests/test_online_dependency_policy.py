from __future__ import annotations

import ast
import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "src" / "marketcow"
POLICY_PATH = ROOT / "docs" / "architecture" / "storage-v2-online-dependency-policy.json"


def module_name(path: Path) -> str:
    relative = path.relative_to(SOURCE).with_suffix("")
    parts = list(relative.parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(("marketcow", *parts))


def imports_for(path: Path) -> set[str]:
    owner = module_name(path)
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


def forbidden_reachability_paths(
    graph: dict[str, set[str]], entrypoints: list[str], forbidden: set[str]
) -> set[tuple[str, ...]]:
    """Return every online path at the first boundary crossing into forbidden code."""
    violations: set[tuple[str, ...]] = set()
    for entrypoint in entrypoints:
        pending = [(entrypoint,)]
        visited_prefixes: set[tuple[str, ...]] = set()
        while pending:
            path = pending.pop()
            if path in visited_prefixes:
                continue
            visited_prefixes.add(path)
            for imported in sorted(graph.get(path[-1], set())):
                candidate = (*path, imported)
                if imported in forbidden:
                    violations.add(candidate)
                elif imported in graph and imported not in path:
                    pending.append(candidate)
    return violations


class OnlineDependencyPolicyTest(unittest.TestCase):
    def setUp(self):
        self.policy = json.loads(POLICY_PATH.read_text())
        self.graph = {module_name(path): imports_for(path) for path in SOURCE.rglob("*.py")}

    def test_policy_schema_and_modules_exist(self):
        self.assertEqual(self.policy["schema"], "marketcow.online-dependency-policy.v1")
        self.assertEqual(self.policy["decision"], "ADR-003")
        known = set(self.graph)
        declared = set(self.policy["online_entrypoints"])
        declared.update(self.policy["forbidden_internal_imports"])
        declared.update(self.policy["offline_only_modules"])
        self.assertEqual(declared - known, set())

    def test_online_transitive_duckdb_debt_is_exact_and_cannot_grow(self):
        forbidden = set(self.policy["forbidden_internal_imports"])
        forbidden.update(self.policy["offline_only_modules"])
        forbidden.update(self.policy["forbidden_external_imports"])
        actual = forbidden_reachability_paths(
            self.graph, self.policy["online_entrypoints"], forbidden
        )
        exceptions = {
            tuple(item["path"])
            for item in self.policy["temporary_reachability_exceptions"]
        }
        self.assertEqual(actual, exceptions)
        self.assertTrue(all(item["removal_item"].startswith("BG-")
                            for item in self.policy["temporary_reachability_exceptions"]))

    def test_exception_list_has_no_duplicates_or_unobserved_paths(self):
        items = self.policy["temporary_reachability_exceptions"]
        paths = [tuple(item["path"]) for item in items]
        self.assertEqual(len(paths), len(set(paths)))
        for path in paths:
            self.assertIn(path[0], self.policy["online_entrypoints"])
            for owner, imported in zip(path, path[1:]):
                self.assertIn(imported, self.graph[owner])

    def test_single_and_two_hop_indirect_forbidden_imports_are_reported(self):
        graph = {
            "online": {"bridge"},
            "bridge": {"second"},
            "second": {"duckdb"},
        }
        self.assertEqual(
            forbidden_reachability_paths(graph, ["online"], {"duckdb"}),
            {("online", "bridge", "second", "duckdb")},
        )
        graph["bridge"] = {"duckdb"}
        self.assertEqual(
            forbidden_reachability_paths(graph, ["online"], {"duckdb"}),
            {("online", "bridge", "duckdb")},
        )

    def test_offline_reverse_import_is_reported_but_unrelated_offline_is_legal(self):
        graph = {
            "online": {"bridge"},
            "bridge": {"offline.importer"},
            "unrelated": {"offline.importer"},
            "offline.importer": {"duckdb"},
        }
        self.assertEqual(
            forbidden_reachability_paths(
                graph, ["online"], {"offline.importer", "duckdb"}
            ),
            {("online", "bridge", "offline.importer")},
        )

    def test_unobserved_exception_cannot_hide_a_different_path(self):
        graph = {"online": {"bridge"}, "bridge": {"duckdb"}}
        actual = forbidden_reachability_paths(graph, ["online"], {"duckdb"})
        declared = {("online", "other_bridge", "duckdb")}
        self.assertNotEqual(actual, declared)


if __name__ == "__main__":
    unittest.main()
