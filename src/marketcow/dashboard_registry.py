from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Iterable, Mapping
from urllib.parse import urlencode


REGISTRY_SCHEMA = "marketcow.dashboard-registry.v1"
_SAFE_ID = re.compile(r"^[A-Za-z0-9_.-]{1,120}$")
_THEMES = frozenset({"current", "dark", "light"})


@dataclass(frozen=True)
class DashboardRegistration:
    key: str
    project: str
    name: str
    dashboard_uid: str
    slug: str
    description: str = ""
    panel_id: int | None = None
    theme: str = "current"
    variables: tuple[tuple[str, str], ...] = ()
    enabled: bool = True
    sort_order: int = 100

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "DashboardRegistration":
        key = str(value.get("key", "")).strip()
        project = str(value.get("project", "")).strip()
        name = str(value.get("name", "")).strip()
        uid = str(value.get("dashboard_uid", "")).strip()
        slug = str(value.get("slug", "")).strip()
        if not _SAFE_ID.fullmatch(key):
            raise ValueError("dashboard key is invalid")
        if not project or len(project) > 80:
            raise ValueError("dashboard project is invalid")
        if not name or len(name) > 120:
            raise ValueError("dashboard name is invalid")
        if not _SAFE_ID.fullmatch(uid):
            raise ValueError("dashboard UID is invalid")
        if not _SAFE_ID.fullmatch(slug):
            raise ValueError("dashboard slug is invalid")
        panel = value.get("panel_id")
        if panel is not None and (not isinstance(panel, int) or panel < 1):
            raise ValueError("dashboard panel ID is invalid")
        theme = str(value.get("theme", "current")).strip().lower()
        if theme not in _THEMES:
            raise ValueError("dashboard theme is invalid")
        variables = value.get("variables", {})
        if not isinstance(variables, Mapping) or len(variables) > 20:
            raise ValueError("dashboard variables are invalid")
        normalized_variables = []
        for variable, item in variables.items():
            variable = str(variable).strip()
            item = str(item).strip()
            if not _SAFE_ID.fullmatch(variable) or len(item) > 200:
                raise ValueError("dashboard variable is invalid")
            normalized_variables.append((variable, item))
        sort_order = value.get("sort_order", 100)
        if not isinstance(sort_order, int) or not 0 <= sort_order <= 10000:
            raise ValueError("dashboard sort order is invalid")
        return cls(
            key=key,
            project=project,
            name=name,
            dashboard_uid=uid,
            slug=slug,
            description=str(value.get("description", "")).strip()[:500],
            panel_id=panel,
            theme=theme,
            variables=tuple(sorted(normalized_variables)),
            enabled=bool(value.get("enabled", True)),
            sort_order=sort_order,
        )

    def path(self) -> str:
        prefix = "d-solo" if self.panel_id is not None else "d"
        parameters: list[tuple[str, str | int]] = [
            ("orgId", 1),
            ("kiosk", "tv"),
        ]
        if self.theme != "current":
            parameters.append(("theme", self.theme))
        if self.panel_id is not None:
            parameters.append(("panelId", self.panel_id))
        parameters.extend((f"var-{key}", value) for key, value in self.variables)
        return f"/{prefix}/{self.dashboard_uid}/{self.slug}?{urlencode(parameters)}"

    def public(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "project": self.project,
            "name": self.name,
            "description": self.description,
            "dashboard_uid": self.dashboard_uid,
            "panel_id": self.panel_id,
            "theme": self.theme,
            "sort_order": self.sort_order,
            "path": self.path(),
        }


DEFAULT_DASHBOARDS = (
    DashboardRegistration(
        key="marketcow-inventory",
        project="MarketCow",
        name="数据库存与质量",
        description="PostgreSQL、ClickHouse 数据库存、覆盖率和质量检查。",
        dashboard_uid="marketcow-data-inventory",
        slug="marketcow-data-inventory-quality",
        sort_order=10,
    ),
    DashboardRegistration(
        key="marketcow-api-observability",
        project="MarketCow",
        name="API 请求聚合监控",
        description="Prometheus 请求速率、错误率、延迟分位数和在途请求。",
        dashboard_uid="marketcow-api-observability",
        slug="marketcow-api-observability",
        sort_order=20,
    ),
)


def load_dashboard_registry(raw: str = "") -> tuple[DashboardRegistration, ...]:
    registrations = list(DEFAULT_DASHBOARDS)
    if raw.strip():
        try:
            values = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError("dashboard registry must be valid JSON") from exc
        if not isinstance(values, list) or len(values) > 100:
            raise ValueError("dashboard registry must be a list of at most 100 items")
        registrations.extend(DashboardRegistration.from_mapping(value) for value in values)
    return validate_dashboard_registry(registrations)


def validate_dashboard_registry(
    values: Iterable[DashboardRegistration],
) -> tuple[DashboardRegistration, ...]:
    enabled = [value for value in values if value.enabled]
    keys = [value.key for value in enabled]
    if len(keys) != len(set(keys)):
        raise ValueError("dashboard keys must be unique")
    return tuple(sorted(enabled, key=lambda item: (item.sort_order, item.project, item.name)))


def registry_document(values: Iterable[DashboardRegistration]) -> dict[str, Any]:
    items = validate_dashboard_registry(values)
    return {"schema": REGISTRY_SCHEMA, "items": [item.public() for item in items]}
