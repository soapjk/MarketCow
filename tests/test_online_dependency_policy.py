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

    def test_online_direct_duckdb_debt_is_exact_and_cannot_grow(self):
        forbidden = set(self.policy["forbidden_internal_imports"])
        forbidden.update(self.policy["offline_only_modules"])
        forbidden.update(self.policy["forbidden_external_imports"])
        actual = {
            (owner, imported)
            for owner in self.policy["online_entrypoints"]
            for imported in self.graph[owner]
            if imported in forbidden
        }
        exceptions = {
            (item["from"], item["to"])
            for item in self.policy["temporary_direct_import_exceptions"]
        }
        self.assertEqual(actual, exceptions)
        self.assertTrue(all(item["removal_item"].startswith("BG-")
                            for item in self.policy["temporary_direct_import_exceptions"]))

    def test_exception_list_has_no_duplicates_or_unobserved_edges(self):
        items = self.policy["temporary_direct_import_exceptions"]
        pairs = [(item["from"], item["to"]) for item in items]
        self.assertEqual(len(pairs), len(set(pairs)))
        for owner, imported in pairs:
            self.assertIn(owner, self.policy["online_entrypoints"])
            self.assertIn(imported, self.graph[owner])


if __name__ == "__main__":
    unittest.main()
