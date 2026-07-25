from __future__ import annotations

import threading
import uuid
import re
from collections import deque
from datetime import datetime, timezone
from typing import Any, Mapping

from .telemetry import sanitize_text


AUDIT_SCHEMA = "marketcow.admin-audit.v1"
MAX_MEMORY_AUDIT_EVENTS = 500
_SECRET_KEY = re.compile(r"(?i)(authorization|cookie|password|passwd|secret|token|api[_-]?key|dsn)")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sanitize_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    result = {}
    for key, item in list(value.items())[:32]:
        clean_key = sanitize_text(key)[:64]
        result[clean_key] = "[REDACTED]" if _SECRET_KEY.search(clean_key) else _sanitize_value(item)
    return result


def _sanitize_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return _sanitize_mapping(value)
    if isinstance(value, (list, tuple)):
        return [_sanitize_value(item) for item in value[:100]]
    if isinstance(value, str):
        return sanitize_text(value)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return sanitize_text(value)


class AdminAuditService:
    """Append-only administration audit with a bounded test/runtime fallback."""

    def __init__(self, repository: Any) -> None:
        self.repository = repository
        self._memory: deque[dict[str, Any]] = deque(maxlen=MAX_MEMORY_AUDIT_EVENTS)
        self._lock = threading.Lock()

    @property
    def durable(self) -> bool:
        return all(hasattr(self.repository, name) for name in (
            "append_admin_audit", "list_admin_audit",
        ))

    def append(
        self, *, actor: str, action: str, target: str, outcome: str,
        parameters: Mapping[str, Any] | None = None, request_id: str = "",
        detail: str = "",
    ) -> dict[str, Any]:
        if outcome not in {"accepted", "succeeded", "rejected", "failed"}:
            raise ValueError("audit outcome is invalid")
        event = {
            "audit_id": uuid.uuid4().hex,
            "schema_version": AUDIT_SCHEMA,
            "occurred_at": _now(),
            "actor": sanitize_text(actor or "local")[:120],
            "action": sanitize_text(action)[:120],
            "target": sanitize_text(target)[:240],
            "outcome": outcome,
            "request_id": sanitize_text(request_id)[:120],
            "parameters_json": _sanitize_mapping(parameters or {}),
            "detail": sanitize_text(detail)[:1000],
        }
        if self.durable:
            saved = self.repository.append_admin_audit(event)
            return dict(saved or event)
        with self._lock:
            self._memory.appendleft(event)
        return event

    def list(
        self, *, limit: int = 50, offset: int = 0,
        action: str = "", outcome: str = "",
    ) -> dict[str, Any]:
        if not 1 <= limit <= 200 or not 0 <= offset <= 10000:
            raise ValueError("audit pagination is invalid")
        if outcome and outcome not in {"accepted", "succeeded", "rejected", "failed"}:
            raise ValueError("audit outcome is invalid")
        if self.durable:
            items = self.repository.list_admin_audit(
                limit=limit, offset=offset, action=action, outcome=outcome
            )
        else:
            with self._lock:
                values = list(self._memory)
            if action:
                values = [item for item in values if item["action"] == action]
            if outcome:
                values = [item for item in values if item["outcome"] == outcome]
            items = values[offset:offset + limit]
        clean = [_sanitize_mapping(dict(item)) for item in items]
        return {
            "schema": AUDIT_SCHEMA,
            "items": clean,
            "page": {"limit": limit, "offset": offset, "returned": len(clean)},
            "durable": self.durable,
        }
