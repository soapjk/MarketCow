# MarketCow historical backfill contract for Tradude

Verified against MarketCow `main` at commit `5b1b423` on 2026-07-25. This
document describes the current application contract, including known limitations;
it does not authorize or require a service change.

## 1. Authentication and endpoints

Base URL in the current local production runtime is
`http://127.0.0.1:8790`.

### Canonical bars

```text
GET /v1/canonical-bars/{instrument_id}
```

Required query parameters:

- `start`: timezone-aware ISO 8601 timestamp.
- `end`: timezone-aware ISO 8601 timestamp. The canonical query is inclusive at
  both ends for stored bar `window_start`.
- `interval`: exactly one of `1-MINUTE`, `5-MINUTE`, `15-MINUTE`,
  `30-MINUTE`, `1-HOUR`, `1-DAY`.
- `adjustment`: exactly `raw`, `qfq`, or `hfq`.
- `page_size`: integer `1..5000`.
- `cursor`: optional opaque cursor returned by the previous page.

The current application middleware does **not** authenticate this route because
it is outside `/v1/admin/*`. An `Authorization` header is therefore neither
required nor evaluated by the application for canonical reads. A reverse proxy
may impose an additional policy. This is an important limitation if Tradude
requires authenticated reads; the history-job route below is authenticated.

Success is HTTP 200:

```json
{
  "schema_version": 1,
  "manifest": {
    "schema_version": 1,
    "dataset_id": "...",
    "snapshot_id": "...",
    "canonical_version": "...",
    "instruments": ["600519.XSHG"],
    "interval": "5-MINUTE",
    "adjustment": "raw",
    "start": "2026-06-01T00:00:00Z",
    "end": "2026-06-30T23:59:59Z",
    "end_inclusive": true,
    "row_count": 1234,
    "content_hash": "sha256:..."
  },
  "count": 500,
  "bars": [{
    "schema_version": 1,
    "instrument_id": "600519.XSHG",
    "interval": "5-MINUTE",
    "adjustment": "raw",
    "price_type": "LAST",
    "aggregation_source": "EXTERNAL",
    "window_start": "...Z",
    "window_end": "...Z",
    "ts_event": "...Z",
    "ts_init": "...Z",
    "open": "1.23",
    "high": "1.25",
    "low": "1.22",
    "close": "1.24",
    "volume": "100",
    "selected_source": "...",
    "quality_status": "...",
    "row_version": "..."
  }],
  "page_size": 500,
  "next_cursor": "...",
  "truncated": true,
  "provenance": {"layer": "canonical", "backend": "clickhouse"}
}
```

`manifest.row_count` is the total number of rows in the requested snapshot, not
the current page count. Follow `next_cursor` until it is null. Every page must
have the identical manifest, and the assembled row count must equal
`manifest.row_count`. A write during a page read returns HTTP 409 with
`detail.code=canonical_snapshot_changed`. A snapshot change between page
requests or an expired cursor may instead invalidate the bound cursor and
return HTTP 400. In either case discard every page already collected and
restart from page one without a cursor. HTTP 404 with
`detail.code=instrument_not_found` means the instrument master record is absent.
An empty canonical result is HTTP 200 with `row_count=0`, `count=0`, and
`bars=[]`; it is not a 404.

### Create and inspect a history job

```text
POST /v1/admin/history-jobs
GET  /v1/admin/history-jobs/{job_id}
GET  /v1/admin/history-jobs?limit=50&offset=0&status=running
POST /v1/admin/history-jobs/{job_id}/retry-failed
POST /v1/admin/history-jobs/{job_id}/cancel
```

These routes require authentication in production:

```http
Authorization: Bearer mcsa.<account-id>.<secret>
```

A service account needs role `viewer` plus scope `history:read` for GET. POST
requires role `operator` plus scope `history:write`. Bearer requests do not need
CSRF. Missing/invalid credentials return 401; insufficient role/scope returns
403. Tradude must receive its own service account through the local credential
provisioning process; it must not copy a browser session or another service's
secret.

All create fields are required:

```json
{
  "symbols": ["600519.XSHG"],
  "provider": "tushare",
  "range": "custom",
  "range_start": "2026-06-01T00:00:00Z",
  "range_end": "2026-07-01T00:00:00Z",
  "interval": "5m",
  "adjustment": "raw",
  "allow_fallback": false,
  "max_concurrency": 2,
  "max_attempts": 5,
  "retry_backoff_seconds": 1,
  "retry_max_backoff_seconds": 60,
  "retry_jitter_seconds": 1,
  "retry_budget_seconds": 1800,
  "canonical_wait_seconds": 10,
  "idempotency_key": "tradude-600519-xshg-5m-20260601-20260701-raw-v1"
}
```

- `symbols`: `1..100` unique provider-neutral `SYMBOL.MIC` IDs.
- `provider`: `yahoo`, `tushare`, or `hyperliquid`
  (`yahoo_chart` is normalized to `yahoo`).
- `range`: still required. Use `custom` with explicit boundaries. If explicit
  boundaries are absent, supported named ranges are `1d`, `5d`, `1mo`, `3mo`,
  `6mo`, `1y`, `2y`, `5y`, `10y`, `ytd`, `max`; these are frozen relative to
  server time and should not be used for certified backfills.
- `range_start` and `range_end`: provide both or neither; both must include a
  timezone and start must be strictly before end. They are normalized to UTC
  and frozen into `request_json`. For backfills, treat `range_end` as the fetch
  window boundary and choose it explicitly from the target interval/calendar.
- `interval`: storage form, not the canonical schema form. Tradude mapping is
  `1-MINUTE -> 1m`, `5-MINUTE -> 5m`, `15-MINUTE -> 15m`,
  `30-MINUTE -> 30m`, `1-HOUR -> 1h`, `1-DAY -> 1d`. Do not use `60m` for a
  Tradude `1-HOUR` dataset because canonical reads map `1-HOUR` to storage
  interval `1h`.
- `adjustment`: explicit `raw`, `qfq`, or `hfq`.
- `allow_fallback`: explicit boolean. Use `false` for a deterministic certified
  workflow unless fallback provenance is expressly accepted.
- `max_concurrency`: `1..16`; `max_attempts`: `1..10`.
- retry backoff/jitter/max/budget and `canonical_wait_seconds` are required;
  their numeric bounds are respectively `0..60`, `0..600`, `0..60`,
  `0..86400`, and `0..60` seconds.
- `idempotency_key`: `8..200` characters. It identifies one complete logical
  request. A duplicate key returns the existing job even if the new payload is
  different, so derive/record the key from all immutable request semantics and
  verify the returned `job.request_json` before trusting a replay.

Provider constraints used by the current job/shard path:

- `tushare`: CN instruments; `1m`, `5m`, `15m`, `30m`, `60m`, `1h`; `raw`
  only. Tradude should use `1h`, not `60m`.
- `yahoo`: CN/HK/US instruments; `1m`, `2m`, `5m`, `15m`, `30m`, `60m`,
  `90m`, `1h`, `1d`, `5d`, `1wk`, `1mo`, `3mo`; `raw` or `qfq`.
- `hyperliquid`: `.HYPL` instruments; the current shard planner supports `1m`,
  `5m`, `15m`, `30m`, `1h`, `1d`; `raw` only.

Create returns HTTP 202:

```json
{"job_id": "...", "created": true, "job": { "...detail fields..." }}
```

An idempotent replay also returns 202 with the original `job_id` and
`created=false`. Job detail includes persisted job fields (`job_id`,
`idempotency_key`, `status`, `request_json`, timestamps, `error_json`),
aggregate counts/progress/row counts/canonical counts, and `items`. Each item
contains `item_id`, `symbol`, `status`, `provider`, selected `source`,
`attempt`, `rows_fetched`, `rows_persisted`, `canonical_status`,
`error_code`, sanitized `error_message`, timestamps, and `shards`. Shards
contain their absolute range, status, attempt, row counts, and write receipt;
lease secrets are not returned.

## 2. Lifecycle and retry rules

Job states:

- non-terminal: `queued`, `running`, `cancel_requested`;
- terminal: `succeeded`, `partially_failed`, `failed`, `canceled`.

Item/shard states are `queued`, `running`, `succeeded`, `failed`, `canceled`.
Item `canonical_status` is independent: `pending`, `completed`, or `failed`.

Poll `GET /v1/admin/history-jobs/{job_id}`. The administration UI polls every
2 seconds. Tradude may use 1–2 seconds initially and cap at 5 seconds with
jitter. A client/network timeout does not imply task failure: keep the
`job_id`, repeat GET, or repeat POST with exactly the same payload and
idempotency key if creation outcome is unknown. Jobs, shards, leases, and
checkpoints survive service restart.

Transient provider/storage failures are retried automatically: connection
errors, timeouts, HTTP 429, HTTP 5xx, recognized rate limits, and selected
storage failures. Delay is exponential
`retry_backoff_seconds * 2^(attempt-1)`, capped by
`retry_max_backoff_seconds`, plus bounded jitter. `Retry-After` can raise the
delay up to that cap. A retry that exceeds `retry_budget_seconds` fails with
`retry_budget_exhausted`. Authentication/authorization failures, unsupported
provider/range/interval/adjustment, ordinary HTTP 4xx, invalid requests and
unknown errors fail immediately. After fixing a non-transient cause,
`retry-failed` requeues failed items/shards; it returns 409 if nothing is
eligible or the job is still finalizing.

`partially_failed` means at least one symbol succeeded and at least one failed.
For an all-instruments-certified dataset it remains a failed backfill.
Provider success with zero bars can produce a succeeded item with
`rows_fetched=0`, `rows_persisted=0`; the canonical verifier also treats an
empty provider result as completed. Therefore neither job `succeeded` nor
`canonical_status=completed` alone proves coverage.

Safe canonical re-read sequence:

1. Wait for a terminal job state.
2. Require every target item to be `succeeded`, with no item error, and wait
   until every target item has `canonical_status=completed`
   (`canonical_pending=0`, `canonical_failed=0`). If canonical is still
   pending, continue polling the same terminal job because the background
   verifier can update it later.
3. Re-read canonical bars from page one for the original explicit target
   range. Retry the entire snapshot on snapshot/cursor invalidation.
4. Require identical manifests across pages, total collected rows equal
   `manifest.row_count`, correct instrument/interval/adjustment, strictly
   increasing unique `ts_event`, valid OHLC/volume, and then compare the
   returned event set to the exact event set expected from Tradude's explicit
   `calendar_ids`, target range and session policy.
5. Only if the expected event set is a subset/equal to the canonical event set
   may Tradude say “MarketCow has filled the target range”, repair Parquet, run
   full validation again, and issue a new attestation.

The canonical v1 endpoint has no `session` query parameter and `HistoricalBar`
has no session field. It must not be assumed that returned bars are
`regular`, `extended`, or `all`. MarketCow cannot currently prove
session-specific completeness. Tradude's `session` and `calendar_ids` remain
mandatory inputs to Tradude validation; if session discrimination is required
and cannot be proved from the requested provider/calendar policy, fail closed
rather than infer it.

## 3. End-to-end Python example

```python
import os, time, requests

BASE = os.environ.get("MARKETCOW_API_URL", "http://127.0.0.1:8790")
KEY = os.environ["MARKETCOW_API_KEY"]
AUTH = {"Authorization": f"Bearer {KEY}"}

instrument = "600519.XSHG"
start = "2026-06-01T00:00:00Z"
fetch_end = "2026-07-01T00:00:00Z"
canonical_end = "2026-06-30T23:59:59Z"
payload = {
    "symbols": [instrument],
    "provider": "tushare",
    "range": "custom",
    "range_start": start,
    "range_end": fetch_end,
    "interval": "5m",
    "adjustment": "raw",
    "allow_fallback": False,
    "max_concurrency": 1,
    "max_attempts": 5,
    "retry_backoff_seconds": 1,
    "retry_max_backoff_seconds": 60,
    "retry_jitter_seconds": 1,
    "retry_budget_seconds": 1800,
    "canonical_wait_seconds": 10,
    "idempotency_key": "tradude-600519-xshg-5m-20260601-20260701-raw-v1",
}

created = requests.post(
    f"{BASE}/v1/admin/history-jobs",
    headers=AUTH,
    json=payload,
    timeout=30,
)
created.raise_for_status()
created_body = created.json()
job_id = created_body["job_id"]
assert created_body["job"]["request_json"]["idempotency_key"] == payload["idempotency_key"]

deadline = time.monotonic() + 3600
while True:
    response = requests.get(
        f"{BASE}/v1/admin/history-jobs/{job_id}",
        headers=AUTH,
        timeout=30,
    )
    response.raise_for_status()
    job = response.json()
    if job["status"] in {"failed", "partially_failed", "canceled"}:
        raise RuntimeError(
            [(item["symbol"], item["error_code"], item["error_message"])
             for item in job["items"] if item["status"] != "succeeded"]
        )
    if (
        job["status"] == "succeeded"
        and job["canonical_pending"] == 0
        and job["canonical_failed"] == 0
        and all(item["canonical_status"] == "completed" for item in job["items"])
    ):
        break
    if time.monotonic() >= deadline:
        raise TimeoutError(f"history job still not ready: {job_id}")
    time.sleep(2)

query = {
    "start": start,
    "end": canonical_end,
    "interval": "5-MINUTE",
    "adjustment": "raw",
    "page_size": 5000,
}
bars, manifest, cursor = [], None, None
while True:
    if cursor:
        query["cursor"] = cursor
    page = requests.get(
        f"{BASE}/v1/canonical-bars/{instrument}",
        params=query,
        timeout=30,
    )
    page.raise_for_status()
    body = page.json()
    manifest = body["manifest"] if manifest is None else manifest
    if body["manifest"] != manifest:
        raise RuntimeError("canonical manifest changed; discard and restart")
    bars.extend(body["bars"])
    cursor = body["next_cursor"]
    if not cursor:
        assert body["truncated"] is False
        break

assert len(bars) == manifest["row_count"]
if not bars:
    raise RuntimeError("MarketCow still has no canonical bars for target range")
# Next: compare {bar["ts_event"]} with Tradude's explicit calendar/session
# expected set. Only then repair Parquet and re-run the full validator.
```

## 4. Local verification locations

- `src/marketcow/api.py:118-170`: history request model and validation.
- `src/marketcow/api.py:922-1043`: canonical v1 endpoint and response building.
- `src/marketcow/api.py:2003-2105`: history create/get/retry/cancel endpoints.
- `src/marketcow/admin_auth.py:285-345,363+`: bearer service accounts,
  roles, and scopes.
- `src/marketcow/history_jobs.py`: durable lifecycle, status aggregation,
  retry classification, row/canonical counters.
- `src/marketcow/history_shards.py:9-125`: named-range freezing, explicit UTC
  windows, interval/provider shard constraints.
- `src/marketcow/history_canonical.py`: asynchronous canonical verification.
- `src/marketcow/market_data_contracts.py:478-568`: manifest, historical bar,
  canonical page schema.
- `web/src/features/operations/OperationsPage.tsx:76-155`: current
  visualization client; 2-second polling, explicit date range creation,
  provider-specific interval choices, retry/cancel commands.
- `docs/history-jobs.md` and `docs/admin-security.md`: operator examples and
  service-account configuration.
- Tradude's existing reader:
  `/Volumes/T9/projects/trade/tradude/backtest/data/marketcow_client.py`.
- Tradude repair flow:
  `/Volumes/T9/projects/trade/tradude/backtest/data/manager.py`.
