from __future__ import annotations

import argparse
import ast
import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def assignment(path: Path, name: str) -> object:
    tree = ast.parse(path.read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == name for target in node.targets):
            return ast.literal_eval(node.value)
    raise KeyError(name)


def sha(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    postgres_path = ROOT / "src/marketcow/postgres_migrations.py"
    clickhouse_path = ROOT / "src/marketcow/clickhouse_repositories.py"
    postgres = assignment(postgres_path, "POSTGRES_MIGRATIONS")
    clickhouse = assignment(clickhouse_path, "CLICKHOUSE_MIGRATIONS")
    domains = assignment(postgres_path, "POSTGRES_TRANSACTION_DOMAINS")
    routes: list[dict[str, str]] = []
    route_pattern = re.compile(r'@app\.(get|post|put|delete|websocket)\(\s*["\']([^"\']+)')
    for relative in ("src/marketcow/api.py", "src/marketcow/polymarket_live_read_api.py"):
        for method, path in route_pattern.findall((ROOT / relative).read_text()):
            routes.append({"method": method.upper(), "path": path, "source": relative})
    result = {
        "schema_version": "marketcow.phase0-inventory.v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "source_commit": "6041e230ce1b325268ac25f2f00f47ad6c5bcd9c",
        "postgres": {"domains": list(domains), "migrations": [
            {"version": version, "description": description, "sha256": sha(sql.strip())}
            for version, description, sql in postgres]},
        "clickhouse": {"migrations": [
            {"version": version, "description": description,
             "statement_sha256": [sha(sql.strip()) for sql in statements]}
            for version, description, statements in clickhouse]},
        "public_contracts": {"routes": sorted(routes, key=lambda item: (item["path"], item["method"])),
                             "route_count": len(routes)},
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"output": str(args.output), "route_count": len(routes),
                      "postgres_migrations": len(postgres), "clickhouse_migrations": len(clickhouse)}, sort_keys=True))


if __name__ == "__main__":
    main()
