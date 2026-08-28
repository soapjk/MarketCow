from __future__ import annotations

import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_realtime_core_has_no_database_or_http_dependency() -> None:
    manifest = (ROOT / "crates/marketcow-core/Cargo.toml").read_text()
    for forbidden in ("sqlite", "sqlx", "clickhouse", "reqwest", "axum", "tokio"):
        assert forbidden not in manifest.lower()


def test_python_worker_has_no_public_listener_or_database_client() -> None:
    source = (ROOT / "python/marketcow_workers/worker.py").read_text()
    for forbidden in ("start_server", "create_server", "listen(", "psycopg", "clickhouse"):
        assert forbidden not in source
    assert "open_unix_connection" in source


def test_cutover_and_rollback_are_guarded_and_non_mutating() -> None:
    clean_env = {"PATH": os.environ["PATH"]}
    for script in ("cutover.sh", "rollback.sh"):
        result = subprocess.run(
            [ROOT / "scripts/migration" / script], env=clean_env, text=True, capture_output=True, check=False
        )
        assert result.returncode == 64
        assert "refusing" in result.stderr


def test_domain_ownership_keeps_orders_and_tradude_disabled() -> None:
    registry = (ROOT / "docs/architecture/migration/domain-ownership.yaml").read_text()
    assert "real_order_submission_enabled: false" in registry
    assert "tradude_may_manage_marketcow: false" in registry
    assert "rust_write_enabled: false" in registry


def test_worker_artifact_registration_is_rust_owned_and_transactionally_fenced() -> None:
    worker = (ROOT / "python/marketcow_workers/worker.py").read_text()
    daemon = (ROOT / "crates/marketcowd/src/main.rs").read_text()
    storage = (ROOT / "crates/marketcow-storage/src/lib.rs").read_text()
    for forbidden in ("raw_artifact_manifest", "register_verified", "artifact_root"):
        assert forbidden not in worker
    assert "promote_worker_artifact" in daemon
    assert "succeed_with_artifact" in daemon
    assert "compare_and_swap_with_artifact" in storage
    transaction = storage.split("pub async fn compare_and_swap_with_artifact", 1)[1].split(
        "pub async fn compare_and_swap_many", 1
    )[0]
    assert "INSERT INTO raw_artifact_manifest" in transaction
    assert "UPDATE provider_job SET status" in transaction
    assert "transaction.commit()" in "".join(transaction.split())
