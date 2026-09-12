# BTC hourly implementation evidence — 2026-09-10

## Paired real hour and isolated Rust research input — 2026-09-12

`btc_hour_pairing` now constructs an immutable, content-addressed bundle from
one exact reviewed Gamma rule, official Binance 1m/1h archives and a two-provider
CTF finality quorum. It requires all 60 final minute bars, requires their OHLC,
four volume fields and trade count to equal the official final 1h bar, and then
requires the Binance close/open result to equal the bound Up/Down CTF payouts.
It copies both raw RPC observation receipts into the bundle. Standard CTF
collateral finality remains distinct from adapter redemption and the Paper pUSD
mapping; both are explicitly false rather than inferred.

Actual bundle:
`/Volumes/T9/data/marketcow/research/btc-hourly/datasets/4358214-20260910T0600Z-r3`.
Dataset ID `ac0d827e3bd389c9aa864bfd87fbd7b8b05a394f9947fe87ea4af1dd21e72adc`;
manifest file SHA-256
`b218a93e03c8825981a3c95b1cb78ce7010234bb84c55614c75a0d524f541ed9`.
The 2026-09-10 06:00 UTC Binance candle opened at 78536.71 and closed at
78416.01, so the independently derived result is Down. The finality receipt is
Up=0, Down=1. Historical first-receipt and L2 coverage remain absent.

The original 2026-09-07 archive and prior bounded live export were copied from
temporary storage without byte changes into
`/Volumes/T9/data/marketcow/research/btc-hourly/sources/binance/` and
`/Volumes/T9/data/marketcow/research/btc-hourly/datasets/` respectively. Their
existing manifest/report hashes are unchanged.
The matching Gamma and RPC originals are retained under
`/Volumes/T9/data/marketcow/research/btc-hourly/sources/polymarket/4358214-r1`;
the r3 bundle was rebuilt from these persistent paths rather than `/private/tmp`.

The Rust example `btc_research_stream` supplies the missing out-of-pool research
input primitive locally. It accepts at most three reviewed markets/six exact
tokens and subscribes directly to the official market WebSocket without changing
Discovery or Live. Transport input and persistence use separate queues; the
persistence queue has explicit item and encoded-byte limits. Time, batches,
frames, archive bytes and individual batch bytes are independently bounded.
Connection boundaries remain explicit `source_gap` facts. See
`docs/btc-hour-research-stream.md`. This example has not been deployed or used to
open a new production subscription.

## Current delivery summary

The local Binance SPOT service is installed as persistent launchd component
`com.marketcow.binance-btc`, bound only to `127.0.0.1:8792`. Release
`btc-hour-r2-20260910` uses Nautilus 1.231.0 with no execution client and keeps
the existing durable root `/Volumes/T9/data/marketcow/production/binance-btc`.
An actual restart recovered the prior sequence (20389), advanced beyond 20999,
and reported published=durable, pending=0, error=null. The first 100 records read
after restart all passed strict observed-as-of filtering. The raw callback now
records its application-owned receive boundary as both UTC and monotonic time;
`socket_kernel_receive_time_unknown` states the remaining clock-boundary gap.

Implemented Polymarket pieces are exact BTC-hour Gamma rule/identity review,
reviewed binding, bounded direct-Rust full-sync/WS raw capture, durable hour
lifecycle registry, staged Discovery-parent/Live-child rotation, and a bounded
settlement monitor. A real ended BTC-hour contract was resolved from two
independent Polygon RPC providers at finalized blocks, with exact CTF condition,
collateral, collection and token-position binding. The verified quorum is usable
by the lifecycle registry but does not itself mutate an account.

The current formal Discovery1000 did not contain the observed current BTC-hour
market, and every incumbent member reported active/accepting. Consequently no
MarketCow component silently chose an eviction. Formal end-to-end D8 remains
blocked on a strategy-owned revised Discovery1000 list, followed by the already
built admit/prepare/activate/baseline sequence and formal Rust source-route
deployment. MarketCow does not invent that strategic selection.

Local regression after the receive-boundary fix: 113 tests passed; Ruff and diff
checks passed. This does not claim sustained profitability or Binance L2, which
the user explicitly excluded.

## CTF outcome token binding

The existing Rust `source_finality_reader` now supports `read_bound` with explicit
collateral and two 32-byte hexadecimal token IDs in payout-slot order. It calls
`getCollectionId(bytes32,bytes32,uint256)` for zero-parent index sets 1/2, then
`getPositionId(address,bytes32)`; all four calls use the same canonical finalized
block hash already used for code and payout reads. Full-width token identities are
compared without u128 conversion. ABI selectors were independently computed with
Keccak-256: 856296f7 and 39dd7530.

Source contract reference:
https://github.com/gnosis/conditional-tokens-contracts/blob/master/contracts/ConditionalTokens.sol
This source reference is not proof of the actual deployed bytecode or adapter.

`finality_read` accepts optional `collateral` and `token_ids_hex` fields and uses
11 calls instead of 7 when present; original total body/time bounds remain.
No default RPC is installed. Bound tokens reduce one evidence gap but do not
prove adapter redemption or independent finality; settlement_import_allowed stays
false and real observation is still unresolved/resolved_unverified as applicable.

Executed `cargo test --offline -p marketcowd --example finality_reader_offline
--no-default-features`: 9 passed. Wrong token rejection and same-block binding are
tested, including token IDs beyond u128. The actual finality CLI compile is checked
separately; no live RPC query or production deployment was performed.

## Fixed-prefix research export

`btc_dataset_export` now exports an explicit durable prefix, source and UTC window
under record/scanned-byte limits, preserving canonical fact rows and raw evidence
inside them. Manifest records prefix/output hashes, config, missing timestamp count,
selected count and dataset identity. Observed mode never substitutes processing time
for unknown receipt time; event-time mode remains explicitly research-only. No claim
that matching event time proves historical availability or whole-hour coverage.
Append-after-watermark reproducibility, missing timestamps, future watermarks and
byte exhaustion have local tests. Failed export retains an incomplete manifest.

Actual previously captured Binance r2 export:
`/private/tmp/marketcow-btc-export-RgiywN/dataset`, through 726,
2026-09-10T00:00:00Z inclusive to 2026-09-11T00:00:00Z exclusive,
source `binance_global_spot`, event-time research, 1000-record/1048576-byte bounds.
Selected/scanned 726, 594481 bytes, output SHA
`414ee869d5c279aefcc7adf7612c6ba17dc49c4b1d2735957014b66e0bb704b0`
matches the original fact-log file. Dataset ID
`e8b92fb717f3f60203eaa0118346f9a16303894bfd78432bc0f91de6fd83b97e`.
This run made no new network requests. Per-market/hour joins and settlement finality
still require their distinct evidence; this export does not create missing inputs.

## Measured blocked-disk capacity evidence

Run `PYTHONPATH=src python3 -m marketcow.btc_fact_benchmark --output
/absolute/new-directory --count 10000` (2–10000 records; synthetic 1024-byte payload,
no network). Writer is held behind a bounded gate while a fast subscriber reads each
record; a one-record slow subscriber is independently rejected. One extra publish
must hit capacity, then the gate opens, writer exits and committed pages are verified.

2026-09-10 actual results:

| Records | Pending raw bytes | Publish→consume/parse P95 | Peak process RSS | Drain wall |
|---:|---:|---:|---:|---:|
| 1000 | 1117783 | 32.958 µs | 28016640 B | 0.324733 s |
| 10000 | 11197784 | 34.125 µs | 44875776 B | 3.215424 s |

Both held durable=0 throughout blocked publication, delivered all records to fast
consumer, rejected slow consumer, then drained to published=durable=count, pending=0,
error=null and verified all committed records. Tracemalloc growth was 1281726 and
12852967 bytes respectively; it is Python-allocation evidence, not total RSS growth.
Latency includes same-thread JSON decoding and instrumentation; not pure CPU,
network latency, full Nautilus memory or a sustained-service guarantee.

Reports: `/private/tmp/marketcow-btc-qa-kt29zG/run/report.json` and
`/private/tmp/marketcow-btc-qa-H8ntpT/run/report.json`. Executed benchmark source SHA:
`500f8c7f759a13e77ffd64722466987d79d3a931cbae0ca113d59de8307434b5`.

## Polymarket capture entry point

Local entry: `PYTHONPATH=src python3 -m marketcow.btc_polymarket_worker
--config /absolute/config.json --output /absolute/new-capture-directory`.
Configuration schema `marketcow.btc-hour.capture-config.v1` requires exactly:
`endpoint`, `scope_id`, `reviewed_markets` (1–3 inline evidence/review pairs),
`seconds` (1–1800), `maximum_frames`, `maximum_total_bytes`, `maximum_frame_bytes`,
`maximum_fullsync_bytes`, `maximum_pending`, `maximum_pending_bytes`,
`maximum_disk_bytes`, and `schema_version`. All budgets are explicit positive integers.
Pending/archive budgets must accommodate escaped JSON raw evidence, not just wire size.
No existing scope is changed. No ready book, current rule approval, or settlement
authority is inferred from an input file's presence.

`btc_polymarket_capture` reads the existing direct Rust full-sync/stream routes,
checks the full-sync atomic model, explicit reviewed identity coverage, source
cursor ordering and event integrity via the existing decoder. It preserves raw
application messages through FactLog's independent writer. This is research
capture only: confirmation shape checks are not the complete executable-book
projection's causal validation. Startup outside the requested scope fails;
it does not silently subscribe or activate another scope.

The entry writes immutable per-capture config/code hashes, binding identities,
published/durable counters and final report. Failed captures retain prior facts,
report a sanitized exception type and exit nonzero. Deadline expiry is reported
as failure, not a successful uninterrupted capture. The application receive
boundary is recorded immediately after each HTTP body or WebSocket frame returns,
with wall and monotonic clocks. Kernel/socket receipt time remains unknown;
capture processing time is separate.

Executed BTC local suite: 59 passed, one dependency deprecation warning;
Ruff/diff checks passed. Entry tests inject synthetic transport into the actual
entry and disk writer; network access and formal service deployment are not claimed.
The separate snapshot test uses the real in-process projection with an explicitly
synthetic configured scope, not a captured production full-sync.

## Binance continuity and repair implementation

`btc_continuity` now tracks fixed-cardinality trade/aggTrade/1m/1h state, validates
source fields, rejects superseded updates from the Nautilus live handler while
retaining raw evidence, and exposes unresolved trade-ID/final-bar gaps.
`btc_backfill` is wired to the existing Nautilus `query_klines` transport (not its
unbounded multi-page convenience method). Per interval there is at most one active
request and one coalesced pending range, with 1000-bar, 15-second and 1MiB decoded
payload bounds. This byte check is after adapter decoding, not a network-body cap;
original HTTP bytes are not yet captured by this path and are explicitly marked absent.

Repairs validate complete ordered final bars before publishing a supplemental
record; they never overwrite the current bar or invent historical receipt time.
A completion clears the observed Kline gap only if it covers the entire currently
accumulated range. Older receipts cannot clear a newer gap. Missing final bars at
the beginning of skipped windows are included. Trade gaps remain unresolved.
Repeated equivalent in-flight requests are suppressed; newer ranges are coalesced.
Shutdown prevents queued requests from starting. No automatic retry of failed calls.

Offline BTC regression: 49 passed, one dependency deprecation warning, Ruff/diff
checks passed. Command uses `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1` because the installed
pytest rerun plugin attempts a socket bind denied by the current sandbox; tests
themselves were not weakened. Current network/production deployment remain unverified.

## Current delivery scope (supersedes the earlier deferral)

The user clarified that Binance trades/Klines, Polymarket BTC hourly realtime
books/trades/identity/rotation, and historical/ongoing actual settlement are all
required. Only Binance L2 is excluded. Complete development, QA and formal upgrade
are requested; no trading/account mutations. Do not treat settlement as optional.

The local worker now optionally mounts `/v1/btc-research/status`, `/facts`, and
`/stream` on loopback via `--read-port`. Streaming uses the same in-memory FactLog,
not the SQLite read path; independent bounded subscriptions are removed on exit.
Its start message explicitly says future-only, no replay, unverified continuity.
This is raw evidence delivery, not a ready book protocol. Bind failure aborts startup.

Actual 30-second local run: `/private/tmp/marketcow-btc-read-LzTXT9`, port 18901,
epoch `4bb2e92e9197418a8c41c75a440a5f60`; independent WebSocket consumer received
sequences 652–656 (trade, kline, kline, trade, trade). Worker exited 0 with
published=durable=992, pending=0, error=null. No formal service modified.
Regression command below now returns 53 passed, one dependency deprecation warning.
The new test receives live frames while disk writing is deliberately blocked and
reuses a one-subscriber slot after disconnect. This is not reconnect/backfill QA.

## QA scope revision and executed regression

User priority: audit existing implementation and fix its bugs; defer new backfill,
durable hour rotation, authoritative settlement and historical L2 capabilities.
Deferred features are not counted as passing acceptance.

2026-09-10 local QA fixed two reproducible correctness defects:

- Startup checked individual raw hashes but failed to reject missing index rows.
  Startup now checks contiguous sequence/offset and positive bounded lengths;
  paginated reads reject missing sequences and invalid offsets/lengths. Constructor
  database-open failure also releases ownership. Existing preserved-tail files are
  size-checked before being loaded for verification.
- Hour planning used same-zone wall-clock subtraction across DST. Duration and
  selection comparisons now use UTC, accepting the repeated one-hour interval and
  rejecting a two-real-hour interval that looks like one local-clock hour.

Executed a real child process exiting with code 73 after raw fsync and before
index commit. Startup rejected the unindexed tail; explicit recovery preserved its
exact bytes; durable remained zero, and a fresh record could subsequently commit.
Also verified blocked-writer close timeout retains the owner lock, corrupted index
startup releases its lock, HTTP failure stops further download requests, expired
deadline opens zero requests, future dates create no output, and redirects are
not followed. Checksum overflow reads 4097 bytes to detect a 4096-byte limit;
that detection byte is counted, not presented as a strict 4096-byte network bound.

Reproduction (no network calls in this suite):

```
PYTHONPATH=src python3 -m pytest tests/test_btc_archive_download.py tests/test_btc_hourly_dataset.py tests/test_btc_fact_log.py tests/test_btc_nautilus_identity.py tests/test_btc_research_read.py tests/test_launchd_startup.py -q
python3 -m ruff check src/marketcow/btc_*.py tests/test_btc_*.py
git diff --check
```

Result: 52 passed (37 BTC + 15 startup), one Starlette/httpx deprecation warning;
Ruff and diff checks passed. This is not a full-workspace or sustained resource
acceptance. Remaining QA includes measured latency/RSS under sustained bounded
load and broader corruption/failure coverage. `maximum_disk_bytes` currently caps
the raw fact log, not SQLite/index or preserved-tail total directory consumption;
do not describe it as a total disk quota. Raw callback capture is unverified source
evidence, not a validated/tradable book or a continuity guarantee.

## Implemented locally

- `btc_hourly_dataset`: bounded local ZIP/CHECKSUM importer, exact raw offsets,
  source/config/code hashes, missing-bar ranges, immutable output and final manifest.
- `btc_archive_download`: four fixed official archive/checksum GETs for one UTC day,
  no redirect/retry, per-body and total deadlines/budgets; failed report retained.
- `btc_fact_log`: independent writer thread, bounded pending count/bytes including
  in-flight IO, immediate in-memory subscriber delivery, independent slow subscriber
  rejection, separate published/durable watermarks, single-writer lock, indexed reads.
  Explicit recovery preserves uncommitted tail before truncation, requires source resync.
- `btc_nautilus_worker`: fixed-version SPOT data-only node, raw callback wrapper without
  editing Nautilus, captures unfinished and finished Klines, process-lifetime bounded run.
  It does not yet implement IPC/API delivery or complete source-gap detection/backfill.
- `btc_hourly_identity`: DST roundtrip/ambiguity validation, pure bounded subscription
  proposal and independent pending-settlement planning. Not durable rotation orchestration.

Nautilus actual imported version 1.231.0; checkout HEAD
`b4cba96c3f2c628559e9d59073ce97576adb297c`; path
`/Volumes/T9/projects/trade/nautilus_trader/nautilus_trader/__init__.py`.
No execution client; no source tree edits. The runtime callback is a version-dependent
hook, not an upstream guaranteed API. Full dependency/license audit remains pending.

## Actual official Binance archives

Evidence root `/private/tmp/marketcow-btc-archive-r1`.
Fixed day 2026-09-07 UTC; four GETs; report error=null; two imports.

| Interval | Records | Gaps | ZIP SHA256 |
|---|---:|---:|---|
| 1m | 1440 | 0 | ccf135fcadf6a18361e1967a1fc2dab961e62f73e7cd9e01848c1ee758e84a44 |
| 1h | 24 | 0 | 6a93e517460a2550f52ad942db13ebc4684501ecaf7172cb0189d5f91789ae0f |

Compared minute-aggregated open/high/low/close/volume against all 24 hourly bars:
zero mismatches. This does not certify historical first availability or Polymarket payout.
No Polymarket market identity is yet joined to this day.

Reproduce with a new output directory:

```
PYTHONPATH=src python3 -m marketcow.btc_archive_download --day 2026-09-07 --output /absolute/new-directory
```

## Actual Nautilus bounded live tests

First `/private/tmp/marketcow-btc-nautilus-r1`: 480 published/durable messages,
but empty-string credentials caused the old adapter to attempt a fee authentication
request (-2014). This is a failed read-only configuration test, not accepted evidence.
Fixed to `api_key=None, api_secret=None` (data factory passes these without env fallback).

Second `/private/tmp/marketcow-btc-nautilus-r2`: 30-second scheduled run, 8MiB
archive bound, 726 published/durable, pending zero, error=null, stopped=true.
Observed 690 trade records, 35 unfinished Klines, 1 finished Kline.
`facts.jsonl`: 594481 bytes, SHA256
`414ee869d5c279aefcc7adf7612c6ba17dc49c4b1d2735957014b66e0bb704b0`.
No authentication error printed in second run. No explicit reconnect test yet.
`first_received_at` is the Nautilus raw-callback entry time and is paired with
`received_monotonic_ns`; it is not represented as the earlier kernel/socket time.

```
PYTHONPATH=src /Volumes/T9/projects/trade/nautilus_trader/.venv/bin/python -m marketcow.btc_nautilus_worker \
  --output /absolute/new-live-root --seconds 30 --maximum-disk-bytes 8388608
```

## Tests and outstanding delivery

27 focused synthetic tests pass: importer, as-of boundary, blocked disk, capacity,
slow consumer isolation, write failure, restart, tail preservation, DST and raw callbacks.
Ruff checks pass. One existing Starlette/httpx deprecation warning occurred.
`btc_research_read.create_read_app` additionally supplies a local read-only page
facade with a fixed committed upper watermark and observed/event-time modes;
unobserved historical rows remain excluded in observed mode. It is tested via
TestClient, not installed as a listener or integrated into the production API.
These are not full-workspace tests or complete failure coverage.

Remaining: strict shared typed envelopes beyond raw events; bounded download offline
failure tests; source sequence/gap and backfill; actual IPC/HTTP/read export composition;
startup component wiring; durable hour rotation; Polymarket hour identity/rule pairing;
authoritative settlement bindings and configured RPC; target-specific historical L2
sample/license/coverage audit; real cross-source D8 report; longer boundary/reconnect
and CPU/RSS measurements. Existing source_finality_reader remains resolved_unverified.

No production service, account, or Polymarket scope was changed. Both test workers exited.

## Continuity restart integration — 2026-09-10

The Binance worker now attaches a fixed-four-entity continuity checkpoint to
raw observations and explicit repair receipts. FactLog restores only checkpoints
from its hash-verified committed prefix during its existing startup traversal.
Publication remains memory-first; no disk operation was added to the callback.
Startup remains O(committed history) in time and bounded per record in memory;
this is not a constant-time startup index. Legacy captures without checkpoints
cannot establish historical continuity and are not labelled complete.

Restart preserves unresolved gaps and duplicate/final-bar fences. Subsequent
ordered updates expose existing gaps to the bounded repair scheduler, rather
than requiring a second gap to trigger repair. The scheduler suppresses repeated
attempts for the same range per interval, including failures and oversized ranges;
new websocket messages do not cause unlimited retries of that range.

Verification: `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python3 -m pytest
tests/test_btc_*.py tests/test_launchd_startup.py -q` produced 82 passed and one
dependency deprecation warning. Changed-module Ruff and git diff checks passed.
New tests cover checkpoint roundtrip, atomic rejection, committed versus pending
repair state, and 100 repeated submissions after a failed repair without another
fetch. These are local tests, not an actual network reconnect or deployed result.
Polymarket rotation, authoritative live finality evidence and formal service
upgrade are still outstanding; this section does not declare the overall goal
complete or supersede the requested end-to-end scope.

## Persistent hourly lifecycle — 2026-09-10

`btc_polymarket_worker` accepts explicit `--lifecycle-db`,
`--maximum-lifecycle-markets` and `--maximum-lifecycle-payload-bytes` together.
This dedicated control-path SQLite registry preserves reviewed hour identities
across separate captures. It is not invoked by realtime callbacks. Expired
markets leave the subscription proposal but remain pending settlement after
restart. Capacity rejects new rows atomically without deleting old evidence.
The byte limit covers stored payloads, not SQLite/index/journal overhead.

`HourRegistry.observe_settlement` binds and preserves exact supplied Rust CTF
observation bytes/hash to a registered condition. It only accepts unresolved or
resolved-unverified observations with settlement import disabled; it is not a
new cryptographic finality verifier. It does not remove those markets from the
pending list, infer winners or write an account. Rule/token binding changes
require review rather than overwriting a previous identity. No automatic pool
activation or RPC request has been introduced by this registry.

Local tests exercise expiry/restart, pending retention after reported resolution,
exact raw evidence retention, capacity atomicity, identity conflict and the
capture entry point integration. Actual discovery/prepare/activate orchestration
and authoritative finality evidence are still required for the complete service.

## Existing-control rotation orchestration — 2026-09-10

`btc_rotation.rotate_and_capture` now calls the existing live prepare/activate
operations and then the direct Rust capture path, requiring matching requested
identities, preparation CAS, applied candidate/selection, advancing revision,
new scope and ready. A durable bounded attempt receipt precedes each mutation;
lost mutation responses require reconciliation, and reopening the same attempt
does not replay publication. Failed baseline capture retains the actual applied
receipt without attempting version rollback. Execution eligibility stays false.

`HotHttpOperations` uses the already defined authenticated hot-scopes routes,
TLS or loopback only, explicit timeout/byte bounds, no redirects or retries.
Caller identity comes from server bearer authentication, not an arbitrary body.
This is callable orchestration, not yet the automatic discovery/service loop:
the caller must supply a reviewed request with valid parent Discovery membership.
No real management POST or production activation was executed in this work.
Four orchestration scenarios passed within an 89-test combined run; an additional
HTTP redirect/no-retry test then passed in the five-test rotation module run.

## Rust-backed hour identity discovery — 2026-09-10

The existing Rust market-evidence router now includes GET
`/v1/prediction-markets/polymarket/research/btc-hour-evidence?slug=...`.
It uses the official Gamma `/markets/slug/{slug}` route, shared single concurrency
and one-second rate limiter, 15-second timeout and 256KiB source body cap,
without redirects/retries. Only bounded bitcoin-up-or-down slug strings are
accepted; the returned slug, market and condition identities are checked.
Raw original evidence uses the existing market-evidence.v1 schema and the exact
requested source URL. No hour semantics or execution approval is inferred.
Official source: https://docs.polymarket.com/api-reference/markets/get-market-by-slug

`python -m marketcow.btc_discovery --endpoint URL --output PATH
--maximum-response-bytes N --maximum-disk-bytes N --seconds N` performs one
bounded three-hour lookup through that Rust route and archives responses using
FactLog. It does not invoke Gamma directly. UTC planning uses America/New_York
only to construct lookup hypotheses; ambiguous fall-back slugs are rejected.
Identity duplicates/hash mismatch/rejections stop the round. Valid response
discovery still requires full rule review before bind_hour/activation.

Six Rust evidence tests and three Python discovery tests passed locally; CLI
help works. The added route has NOT been deployed or tested against current
hourly source responses. Continuous scheduling, review policy and automatic
parent-pool admission remain necessary before claiming autonomous rotation.

## Continuous discovery and startup prerequisite fix — 2026-09-10

Discovery now supports `--continuous --interval-seconds N
--maximum-consecutive-failures N`, requiring interval >=60 seconds and a failure
streak budget of 1..10. There is one in-flight round, bounded by its existing
time/request/byte budgets. SIGTERM/SIGINT allow the bounded in-flight round to
finish, prevent the next round, and drain the independent writer. Discovery
does not approve rules or mutate scope. Rules/admission are still not connected
to an autonomous end-to-end rotation service.

The production shell now asks the same Python component selector whether stock
storage is required before starting it. Binance-only and Rust-only selection
do not start PostgreSQL/ClickHouse; unified-api retains that prerequisite.
The actual subprocess selector and existing shell ordering tests pass.

Deployment check: a read-only `ssh -o BatchMode=yes -o ConnectTimeout=5 czx@u1
'uname -s'` failed immediately with `connect ... port 22: Operation not permitted`.
No U1 build, upload, service change or new endpoint availability is inferred.
This runtime network restriction currently blocks external deployment validation,
but does not stop remaining local implementation.

## Settlement runner and real service compile — 2026-09-10

`python -m marketcow.btc_settlement_worker --help` exposes a finite sweep of
explicit ended-market IDs using the existing Rust finality_read binary. Binary
SHA, condition and both uint256 token identities are checked before any child
starts. Operator RPC profile paths stay local; reports contain hashes, not RPC
configuration contents. Child timeouts terminate/reap the owned process, failed
reads retain available output and stop the batch. A successful sweep means raw
observations stored, NOT verified finality or account settlement. Actual RPC
endpoint/deployment policy and live evidence remain unavailable here.

Capture now distinguishes its own normal observation timer after ready from a
source timeout or missing-ready deadline. All socket context managers are exited;
the completion receipt records the stop reason. Scope coverage additionally
checks exact Up/Down token labels, not just the unordered two-token set.

Combined Python tests: 102 passed with one dependency deprecation warning before
the final outcome-label adjustment; 11 focused binding/capture/duration tests
passed after it. `cargo check --offline -p marketcowd --bin
marketcow-discovery-collector --no-default-features` exited 0, including the new
Rust research route in the actual service target; existing unused-code warnings
remain. This is local compilation, not a release build or production deployment.
