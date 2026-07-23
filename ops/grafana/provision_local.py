from __future__ import annotations

import argparse
import os
import plistlib
import re
import secrets
import shutil
from pathlib import Path
from urllib.parse import urlparse

import clickhouse_connect
import psycopg
from psycopg import sql


ROLE = "marketcow_grafana"
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _identifier(value: str, label: str) -> str:
    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f"{label} must be a simple identifier")
    return value


def _provision_postgres(dsn: str, schema: str, password: str) -> tuple[str, int, str]:
    parsed = urlparse(dsn)
    database = _identifier(parsed.path.removeprefix("/"), "PostgreSQL database")
    schema = _identifier(schema, "PostgreSQL schema")
    with psycopg.connect(dsn, autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (ROLE,))
            if cursor.fetchone() is None:
                cursor.execute(
                    sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                        sql.Identifier(ROLE), sql.Literal(password)
                    )
                )
            else:
                cursor.execute(
                    sql.SQL("ALTER ROLE {} PASSWORD {}").format(
                        sql.Identifier(ROLE), sql.Literal(password)
                    )
                )
            cursor.execute(f'ALTER ROLE "{ROLE}" SET default_transaction_read_only = on')
            cursor.execute(f'GRANT CONNECT ON DATABASE "{database}" TO "{ROLE}"')
            cursor.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{ROLE}"')
            cursor.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{ROLE}"')
            cursor.execute(
                f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{schema}" '
                f'GRANT SELECT ON TABLES TO "{ROLE}"'
            )
    return parsed.hostname or "127.0.0.1", parsed.port or 5432, database


def _provision_clickhouse(values: dict[str, str], password: str) -> tuple[str, int, str]:
    host = values["MARKETCOW_CLICKHOUSE_HOST"]
    port = int(values["MARKETCOW_CLICKHOUSE_PORT"])
    database = _identifier(values["MARKETCOW_CLICKHOUSE_DATABASE"], "ClickHouse database")
    client = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=values["MARKETCOW_CLICKHOUSE_USERNAME"],
        password=values[values["MARKETCOW_CLICKHOUSE_PASSWORD_REF"]],
        database=database,
        connect_timeout=3,
        send_receive_timeout=15,
    )
    try:
        escaped = password.replace("\\", "\\\\").replace("'", "\\'")
        client.command(
            f"CREATE USER IF NOT EXISTS {ROLE} IDENTIFIED WITH sha256_password BY '{escaped}'"
        )
        client.command(
            f"ALTER USER {ROLE} IDENTIFIED WITH sha256_password BY '{escaped}'"
        )
        client.command(f"GRANT SELECT ON {database}.* TO {ROLE}")
        client.command(f"GRANT SELECT ON system.parts TO {ROLE}")
        client.command(f"GRANT SELECT ON system.disks TO {ROLE}")
    finally:
        client.close()
    return host, port, database


def _write_provisioning(
    root: Path,
    dashboard_path: Path,
    pg: tuple[str, int, str],
    pg_schema: str,
    pg_password: str,
    ch: tuple[str, int, str],
    ch_password: str,
) -> None:
    for name in ("alerting", "dashboards", "datasources", "plugins", "notifiers"):
        (root / name).mkdir(parents=True, exist_ok=True)
    dashboard_files = root / "dashboard-files"
    dashboard_files.mkdir(parents=True, exist_ok=True)
    source_dashboard = dashboard_path / "marketcow-data-inventory.json"
    destination_dashboard = dashboard_files / source_dashboard.name
    temporary_dashboard = destination_dashboard.with_suffix(".json.tmp")
    shutil.copyfile(source_dashboard, temporary_dashboard)
    os.chmod(temporary_dashboard, 0o644)
    os.replace(temporary_dashboard, destination_dashboard)
    datasource = f"""apiVersion: 1
datasources:
  - name: MarketCow ClickHouse
    uid: marketcow-clickhouse
    type: grafana-clickhouse-datasource
    access: proxy
    isDefault: true
    jsonData:
      host: {ch[0]}
      port: {ch[1]}
      protocol: http
      username: {ROLE}
      defaultDatabase: {ch[2]}
      secure: false
    secureJsonData:
      password: {ch_password}
  - name: MarketCow PostgreSQL
    uid: marketcow-postgres
    type: postgres
    access: proxy
    url: {pg[0]}:{pg[1]}
    user: {ROLE}
    jsonData:
      database: {pg[2]}
      sslmode: disable
      postgresVersion: 1600
      timescaledb: false
      searchPath: {pg_schema}
    secureJsonData:
      password: {pg_password}
"""
    dashboard = f"""apiVersion: 1
providers:
  - name: MarketCow
    orgId: 1
    folder: ""
    type: file
    disableDeletion: false
    updateIntervalSeconds: 30
    allowUiUpdates: false
    options:
      path: {dashboard_files}
      foldersFromFilesStructure: false
"""
    for path, content in (
        (root / "datasources/marketcow.yaml", datasource),
        (root / "dashboards/marketcow.yaml", dashboard),
    ):
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(content)
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)


def _configure_launch_agent(plist_path: Path, provisioning_root: Path) -> None:
    data = plistlib.loads(plist_path.read_bytes())
    env = data.setdefault("EnvironmentVariables", {})
    env["GF_PATHS_PROVISIONING"] = str(provisioning_root)
    temporary = plist_path.with_suffix(".plist.tmp")
    temporary.write_bytes(plistlib.dumps(data, sort_keys=False))
    os.replace(temporary, plist_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Provision the local MarketCow Grafana dashboard")
    parser.add_argument("--env", type=Path, default=Path(".env.production"))
    parser.add_argument(
        "--provisioning-root",
        type=Path,
        default=Path("/Volumes/T9/monitoring-services/grafana-provisioning"),
    )
    parser.add_argument(
        "--launch-agent",
        type=Path,
        default=Path.home() / "Library/LaunchAgents/cn.llmay.monitoring.grafana.plist",
    )
    args = parser.parse_args()
    values = _env(args.env.resolve())
    pg_password = secrets.token_urlsafe(32)
    ch_password = secrets.token_urlsafe(32)
    pg = _provision_postgres(
        values[values["MARKETCOW_POSTGRES_DSN_REF"]],
        values["MARKETCOW_POSTGRES_SCHEMA"],
        pg_password,
    )
    ch = _provision_clickhouse(values, ch_password)
    repo_root = Path(__file__).resolve().parents[2]
    _write_provisioning(
        args.provisioning_root.resolve(),
        (repo_root / "ops/grafana/dashboards").resolve(),
        pg,
        values["MARKETCOW_POSTGRES_SCHEMA"],
        pg_password,
        ch,
        ch_password,
    )
    _configure_launch_agent(args.launch_agent.resolve(), args.provisioning_root.resolve())
    print("Grafana read-only database roles and local provisioning are ready.")
    print("No database credentials were written to the repository or printed.")


if __name__ == "__main__":
    main()
