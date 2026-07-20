from __future__ import annotations

import hashlib
import json
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from .repositories import Repositories
from .storage import Warehouse


class LocalArtifactStore:
    """Filesystem artifact bodies with manifests persisted by DuckDB."""

    def __init__(self, warehouse: Warehouse):
        self._warehouse = warehouse

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
        self._warehouse.save_artifact(row)

    def save_artifacts(self, rows: List[Dict[str, Any]]) -> int:
        return self._warehouse.save_artifacts(rows)

    def artifact_paths(self) -> set[str]:
        return self._warehouse.artifact_paths()

    def list_artifacts(self, dataset: str = "", limit: int = 100) -> List[Dict[str, Any]]:
        return self._warehouse.list_artifacts(dataset, limit)

    def latest_artifact(
        self, dataset: str, metadata_key: str = "", metadata_value: str = ""
    ) -> Optional[Dict[str, Any]]:
        return self._warehouse.latest_artifact(dataset, metadata_key, metadata_value)


def create_duckdb_repositories(warehouse: Warehouse) -> Repositories:
    """Build the compatibility backend while each data domain is migrated separately."""

    return Repositories(
        metadata=warehouse,
        fundamentals=warehouse,
        market_bars=warehouse,
        artifacts=LocalArtifactStore(warehouse),
    )
