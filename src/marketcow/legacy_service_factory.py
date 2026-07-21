"""Legacy-only DuckDB assembly, dynamically loaded outside V2 profiles."""

from typing import Any

from .duckdb_repositories import create_stage1_repositories
from .storage import Warehouse


def create_legacy_service_repositories(
    settings: Any, warehouse: Any = None,
) -> tuple[Any, Any, Warehouse]:
    warehouse = warehouse or Warehouse(settings.database_path)
    repositories, resources = create_stage1_repositories(settings, warehouse)
    return repositories, resources, warehouse
