# Catalog capture page-boundary resume

The existing Rust `catalog_capture` executable now accepts `--resume`. No new
collector, HTTP endpoint or scheduler is introduced. Omitting the flag still
refuses an existing capture directory.

Use the same capture root and explicit filter/identity selection. The maintained
worker exposes the complete capture → normalize → publish workflow:

```sh
PYTHONPATH=src python3 -m marketcow.catalog_refresh_worker \
  --config /absolute/path/worker.json \
  --resume-capture /absolute/configured/root/capture-ID
```

This command makes upstream requests. It is not a status check. There is no
automatic process restart or infinite retry. The binary referenced by the worker config
must first contain this change. A normal worker invocation does not auto-select
an old failed capture.

## Recovery guarantees

- A SQLite writer lock excludes concurrent new-version capture processes in the
  same root and is released on process death. Stop any older capture binary
  before using it; older versions do not participate in this lock.
- Complete pages are read within byte limits and checked against recorded SHA,
  size, page number, request URL/filter, input cursor, output cursor and unique
  market identities. The identity database is rebuilt from those pages.
- An HTTP failure, truncated response, torn final ledger line or unlogged page
  is retried as a whole page. Bytes are never appended to the partial response.
- The old ledger/report and incomplete next page are retained in a
  `resume-attempt-*` directory before replacing the active ledger. Successful
  prefix bodies are not downloaded again or overwritten.
- Corrupt successful evidence, duplicate identities, changed filters and cursor
  loops stop recovery. An upstream rejection of a saved cursor stops capture;
  it never silently switches to a fresh traversal.
- A complete terminal ledger needs zero further requests, including when the
  process died before writing its final report. Preparation still requires a
  complete report. Previously started preparation is explicitly refused by the
  resume worker rather than overwritten.

## Budgets and time semantics

Page and retained-body budgets include the verified prefix and this attempt's
new responses. Archived failed attempts remain on disk separately; the worker's
artifact/free-space checks still apply before each explicit invocation. Direct
binary callers must account for archived disk usage. `maximum_seconds` is a new
monotonic budget for each invocation, including prefix verification. It is not a
lifetime limit or hard-real-time guarantee. Each request also retains its timeout.

`capture_started_at` stays at the original start; `attempt_started_at` and
`resumed_verified_pages` disclose the resumed run. Per-page source receipt times
are retained. This is a multi-time traversal, **not an atomic upstream snapshot**.
Markets moving while traversal is interrupted can still affect source coverage;
resume does not establish completeness beyond the source protocol's guarantees.

Old failed captures containing `report.json` and `pages.jsonl` can be recovered
without `capture.json`. New captures persist that initial metadata before the
first request. Body errors now retain the reqwest debug/source chain so timeout
and premature EOF can be distinguished in new evidence.

Local verification: standalone Cargo tests cover successful-prefix recovery,
partial bodies, torn ledgers, hash corruption, request mismatch, duplicate IDs,
terminal boundaries and byte budgets. Worker tests verify same-root execution,
successful ingestion, and no ingestion/new traversal after upstream failure.
These are offline tests, not evidence that an old Gamma cursor is still accepted.

## Automatic page retries

Default `--maximum-retries 3 --retry-delay-seconds 2` allows at most four
requests for the same page, with 2/4/8 second waits. `Retry-After` seconds or an
HTTP date can lengthen that wait, but cannot extend the total deadline. The CLI
permits zero retries and caps configured retries at 10, base delay at 60 seconds.
The SDK's implicit retry remains disabled; there is only this one retry layer.

Timeout/connect/body failures and HTTP 408/429/500/502/503/504 are retried.
Authentication, invalid cursor and other permanent status errors are not.
Malformed JSON, identity/hash errors and size-limit truncation are not retried.
Each new attempt uses the identical request and cursor. Partial bodies are
saved as separate `retry-*` files and indexed by `retries.jsonl`; they are not
included in the successful page ledger. All retained retry bodies consume the
invocation's total byte budget alongside the verified pages. Budget/deadline
exhaustion stops before another request. Reports expose retry retained bytes
separately so existing successful-page verification remains compatible.

This is bounded recovery, not a guarantee that the upstream will eventually
respond. After exhaustion a later explicit resume can reuse the complete prefix.
Monitoring must distinguish capture completion from normalization/publication
and inspect the actual service exit status, not just a timer notification.
