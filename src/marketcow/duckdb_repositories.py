from __future__ import annotations

import hashlib
import json
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from .repositories import ArtifactManifestRepository, Repositories
from .storage import Warehouse


class LocalArtifactStore:
    """Filesystem artifact bodies with manifests persisted by DuckDB."""

    def __init__(self, manifest_repository: ArtifactManifestRepository):
        self._manifest_repository = manifest_repository

    def write_json(
        self,
        folder: Path,
        dataset: str,
        payload: Any,
        source: str,
        source_url: str,
        raw_response_locator: str,
        observed_at: str,
        ingested_at: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        artifact_id = uuid.uuid4().hex
        folder.mkdir(parents=True, exist_ok=True)
        stamp = ingested_at.replace(":", "").replace("+", "_").replace(".", "")
        path = folder / f"{dataset}-{stamp}-{artifact_id[:8]}.json"
        body = {
            "artifact_id": artifact_id,
            "dataset": dataset,
            "source": source,
            "source_url": source_url,
            "observed_at": observed_at,
            "ingested_at": ingested_at,
            "raw_response_locator": raw_response_locator,
            "metadata": metadata or {},
            "payload": payload,
        }
        encoded = json.dumps(
            body, ensure_ascii=False, allow_nan=False, sort_keys=True
        ).encode("utf-8")
        path.write_bytes(encoded)
        manifest = {
            "artifact_id": artifact_id,
            "dataset": dataset,
            "source": source,
            "source_url": source_url,
            "observed_at": observed_at,
            "ingested_at": ingested_at,
            "raw_response_locator": raw_response_locator,
            "storage_path": str(path),
            "sha256": hashlib.sha256(encoded).hexdigest(),
            "byte_size": len(encoded),
            "metadata_json": json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True),
        }
        self.save_artifact(manifest)
        return manifest

    def save_artifact(self, row: Dict[str, Any]) -> None:
        self._manifest_repository.save_artifact(row)

    def save_artifacts(self, rows: List[Dict[str, Any]]) -> int:
        return self._manifest_repository.save_artifacts(rows)

    def artifact_paths(self) -> set[str]:
        return self._manifest_repository.artifact_paths()

    def list_artifacts(self, dataset: str = "", limit: int = 100) -> List[Dict[str, Any]]:
        return self._manifest_repository.list_artifacts(dataset, limit)

    def latest_artifact(
        self, dataset: str, metadata_key: str = "", metadata_value: str = ""
    ) -> Optional[Dict[str, Any]]:
        return self._manifest_repository.latest_artifact(dataset, metadata_key, metadata_value)


class Stage1FundamentalRepository:
    """Route migrated fundamentals to PostgreSQL and remaining datasets to DuckDB."""

    POSTGRES_METHODS = {
        "replace_fundamentals",
        "query_fundamentals",
        "replace_statement_rows",
        "get_statement_rows",
        "upsert_baostock",
        "get_baostock",
        "replace_tdx_period",
        "get_tdx",
        "tdx_coverage",
        "get_tdx_history",
    }

    def __init__(self, postgres_repository: Any, duckdb_repository: Warehouse):
        self._postgres_repository = postgres_repository
        self._duckdb_repository = duckdb_repository

    def __getattr__(self, name: str) -> Any:
        repository = (
            self._postgres_repository
            if name in self.POSTGRES_METHODS
            else self._duckdb_repository
        )
        return getattr(repository, name)


def create_duckdb_repositories(warehouse: Warehouse) -> Repositories:
    """Build the compatibility backend while each data domain is migrated separately."""

    return Repositories(
        metadata=warehouse,
        fundamentals=warehouse,
        market_bars=warehouse,
        artifacts=LocalArtifactStore(warehouse),
    )


def create_stage1_repositories(settings: Any, warehouse: Warehouse) -> tuple[Repositories, Any]:
    """Select the metadata backend while market bars and fundamentals remain on DuckDB."""

    settings.validate_runtime_isolation()
    if settings.metadata_backend == "duckdb":
        return create_duckdb_repositories(warehouse), None
    from .postgres_repositories import PostgresDatabase, PostgresMetadataRepository

    database = PostgresDatabase(settings.postgres_dsn, settings.postgres_schema)
    database.open()
    metadata = PostgresMetadataRepository(database)
    return Repositories(
        metadata=metadata,
        fundamentals=Stage1FundamentalRepository(metadata, warehouse),
        market_bars=warehouse,
        artifacts=LocalArtifactStore(metadata),
    ), database
