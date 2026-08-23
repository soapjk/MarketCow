# MarketCow atomic freshness verification

Date: 2026-08-23 (Asia/Shanghai)

## Automated verification

```sh
/tmp/marketcow-ff65fa86-venv/bin/ruff check src tests scripts
git diff --check
/tmp/marketcow-ff65fa86-venv/bin/pytest -q
uv build --out-dir /tmp/marketcow-ff65fa86-final-dist
```

Results:

- Ruff and `git diff --check`: passed.
- Test suite: 681 passed, 21 skipped, 91 subtests passed in 43.65s.
- Build: source distribution and wheel built successfully.

## One-hour Tradude concurrency-model acceptance

```sh
PYTHONPATH=scripts /tmp/marketcow-ff65fa86-venv/bin/python \
  scripts/verify_polymarket_shared_events_soak.py \
  --v48-config artifacts/polymarket-full-sync-acceptance-20260823/unified-shadow.yaml \
  --output artifacts/polymarket-full-sync-acceptance-20260823/one-hour-soak.json \
  --duration-seconds 3600 \
  --base-url http://127.0.0.1:8795 \
  --api-pid 78043 --collector-pid 99524
```

The report's top-level `passed` value and all criteria are `true`.

- Duration: 3600.319616 seconds.
- 100-market monitor samples: health 2,977; bootstrap 2,977;
  snapshot 2,978; full-sync 2,977; rejected-candidate probe 2,977.
- Continuous `/events` samples: 5,950; cursor advanced from 19,183,238 to
  19,456,226; cursor gaps and duplicate events: 0.
- Successful-response maximum book age after consumer JSON parsing/projection:
  4.685540 seconds (< 5 seconds).
- Integrity failures and fail-closed frames among successful responses: 0.
- Client timeouts: 0 on every route; events p99 0.219881 seconds.
- Unresolved gaps: 0; WebSocket disconnect delta and disconnected health
  samples: 0.
- Real-time SQLite query maximum: 0 ms; observed read source only
  `memory_projection`.
- Maximum asynchronous persistence queue depth 4,293 and lag 2,197 events,
  both below the configured bounded threshold of 10,000.
- Server-Timing completeness failures: 0. Recorded maximum response sizes:
  health 1,882 B; bootstrap 1,054,428 B; snapshot 984,441 B; full-sync
  2,042,241 B; events 1,048,892 B; probe 64,576 B.
- All 100 retained non-200 diagnostic records were retryable HTTP 503 budget
  rejections with code `polymarket_snapshot_freshness_budget_exhausted`;
  no other error code was observed.

Reproduce the compact report summary with:

```sh
jq '{passed,duration_seconds,criteria,route_sample_counts,route_failure_counts,client_timeout_counts,integrity_failure_count,fail_closed_frame_count,maximum_book_age_seconds,maximum_unresolved_gap_count,maximum_persistence_queue_depth,maximum_persistence_lag_events,live_stream_disconnect_count_delta,disconnected_health_samples,realtime_sqlite_query_ms_max,read_sources,server_timing_failure_count,response_body_bytes_maximum,events}' artifacts/polymarket-full-sync-acceptance-20260823/one-hour-soak.json
```

## Contract evidence

- Atomic projection capture and immutable copying under one lock:
  `src/marketcow/polymarket_live_stream.py`, `_capture_scope`.
- Shared-boundary response model and validator:
  `src/marketcow/polymarket_live.py`, `LiveFullSyncResponse`.
- Strict measured construction/serialization budget and retryable 503 gate:
  `src/marketcow/polymarket_live_stream.py`, `_serialize_scoped_response`.
- Exact scoped health includes selected-market and relation token books:
  `tests/test_polymarket_live_stream.py`,
  `test_full_sync_scoped_health_covers_relation_books_without_duplicates`.
- Deduplicated `snapshot.books` with frame `token_ids` and
  `relation_token_ids`, and explicit absence of the old embedded fields:
  `tests/test_polymarket_live_read_api.py`,
  `test_atomic_full_sync_contract_and_server_timing`.
- Retryable budget exhaustion response:
  `tests/test_polymarket_live_read_api.py`,
  `test_full_sync_rejects_insufficient_delivery_headroom`.
- ASGI response write time and actual wire response size:
  `src/marketcow/polymarket_events_observability.py`,
  `PolymarketReadTraceMiddleware`.
