# MarketCow administration API

The administration frontend uses versioned read models and explicit command
endpoints. Browser code never accesses PostgreSQL, ClickHouse, Grafana credentials,
or provider credentials directly.

## Read models

- `GET /v1/admin/overview` returns service, storage, provider, and recent history-job
  summaries as `marketcow.admin-overview.v1`.
- `GET /v1/admin/providers` supports `status`, `limit`, and `offset` and returns
  `marketcow.admin-providers.v1`. Credential values are never part of the response.
- `GET /v1/admin/dashboards` returns the validated dashboard registry.
- `GET /v1/admin/history-jobs` supports `status`, `limit`, and `offset`.
- `GET /v1/admin/audit` supports `action`, `outcome`, `limit`, and `offset`.

All collection responses contain explicit page metadata. The current bounded
history-job implementation can page through the 200 most recently updated jobs.

## History commands

- `POST /v1/admin/history-jobs` creates a job. Its body includes the required
  `idempotency_key`; replaying the same key returns the existing job.
- `POST /v1/admin/history-jobs/{job_id}/cancel` requests cancellation.
- `POST /v1/admin/history-jobs/{job_id}/retry-failed` retries eligible failures and
  returns `409 Conflict` when the current state does not permit a retry.

Clients may send `X-Request-ID` to correlate a command. The audit actor always comes
from the authenticated server-side identity; actor headers supplied by a client are
ignored.

## Audit

Every history-job command appends a sanitized
`marketcow.admin-audit.v1` record. PostgreSQL runtimes use the append-only
`admin_audit_event` domain. Test doubles without the repository extension use a
bounded in-memory fallback and explicitly report `"durable": false`.

Audit parameters are allow-listed by each command and sanitized again by the audit
service. Keys that indicate authorization, cookies, passwords, secrets, tokens,
API keys, or DSNs are replaced with `[REDACTED]`.
