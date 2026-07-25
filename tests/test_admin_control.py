from __future__ import annotations

import unittest

from marketcow.admin_control import AUDIT_SCHEMA, AdminAuditService


class MemoryRepository:
    pass


class DurableRepository:
    def __init__(self):
        self.rows = []

    def append_admin_audit(self, row):
        self.rows.append(row)
        return row

    def list_admin_audit(self, limit=50, offset=0, action="", outcome=""):
        values = list(reversed(self.rows))
        if action:
            values = [row for row in values if row["action"] == action]
        if outcome:
            values = [row for row in values if row["outcome"] == outcome]
        return values[offset:offset + limit]


class AdminAuditTest(unittest.TestCase):
    def test_memory_fallback_is_bounded_and_redacted(self):
        audit = AdminAuditService(MemoryRepository())
        result = audit.append(
            actor="operator", action="job.create", target="job-1",
            outcome="accepted", parameters={"token": "secret-value"},
        )
        self.assertEqual(result["schema_version"], AUDIT_SCHEMA)
        page = audit.list()
        self.assertFalse(page["durable"])
        self.assertNotIn("secret-value", str(page))

    def test_durable_repository_supports_filter_and_pagination(self):
        repository = DurableRepository()
        audit = AdminAuditService(repository)
        audit.append(actor="a", action="job.create", target="1", outcome="accepted")
        audit.append(actor="a", action="job.cancel", target="1", outcome="succeeded")
        page = audit.list(limit=1, action="job.cancel")
        self.assertTrue(page["durable"])
        self.assertEqual(page["page"]["returned"], 1)
        self.assertEqual(page["items"][0]["action"], "job.cancel")

    def test_invalid_pagination_and_outcome_are_rejected(self):
        audit = AdminAuditService(MemoryRepository())
        with self.assertRaisesRegex(ValueError, "pagination"):
            audit.list(limit=0)
        with self.assertRaisesRegex(ValueError, "outcome"):
            audit.append(actor="a", action="a", target="a", outcome="unknown")


if __name__ == "__main__":
    unittest.main()
