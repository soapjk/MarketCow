# Rust historical-price read-through v1

Deployed on U1 as `history-readthrough-r1` on 2026-09-08. Python calls MarketCow, never the
official host. Rust uses the existing reqwest/rustls system-proxy configuration.
This is a research endpoint, not a market selection or trading operation.

`GET /v1/prediction-markets/polymarket/history/prices`

Required query: `token_id` (decimal token identity), `start_ts`, `end_ts`
(Unix UTC seconds), `fidelity_minutes`. The interval must be positive, entirely
in the past and at most seven days; fidelity is 1–10080 minutes. Unknown or
duplicate query fields are rejected. No arbitrary upstream URL is accepted.

Fixed upstream: `https://clob.polymarket.com/prices-history` with official query
keys `market`, `startTs`, `endTs`, `fidelity`. No retries or redirects. Independent
two-request concurrency ceiling, global one-start-per-second admission, 15-second
request deadline and 1 MiB retained raw-body cap. These are research budgets,
not changes to Live stream queues. Transport may allocate a chunk before its
length is checked; the cap is not a claim about total RSS or hard real time.
There is no disk write, persistent cache, publication lock, or scope mutation.

HTTP 200 from MarketCow means a complete upstream HTTP response was captured,
not necessarily upstream success. Check `upstream_status`. The evidence envelope
contains request identity, fixed source URL, start/receipt UTC timestamps, exact
body length, SHA-256 and base64. A 403 is preserved, never interpreted as empty
history. Transport/size/deadline failures return typed 502/504 errors without
pretending to preserve a complete body. Invalid requests are 400; saturation
and rate admission are 429. No sensitive upstream headers are returned.

This route has the same LAN visibility as the existing read-only Live service;
it is not an internet-facing management endpoint. No credentials are added.
History prices are not executable bid/ask depth, historical rule versions or
proof of settlement finality. Gamma success alone does not prove history access.

Python usage (new output directory required):

```
python3 scripts/probe_marketcow_history.py \
  --base-url http://192.168.124.3:8793 \
  --token-id TOKEN --start-ts START --end-ts END --fidelity-minutes 60 \
  --output /absolute/new-evidence-directory
```

The Python client retains the API envelope and verified upstream bytes; a
non-200 upstream status fails the command without retrying. Its 2 MiB response
limit permits one additional detection byte. The previous direct-official
diagnostic script remains for historical reproduction, not the new access path.

## Local verification

```
TMPDIR=/tmp cargo test --offline -p marketcowd --bin marketcow-discovery-collector -- --test-threads=2
python3 -m pytest tests/test_official_history_probe.py tests/test_marketcow_history_probe.py -q
```

Observed: 90 Rust passed, 3 ignored; 28 Python passed. Rust HTTP test uses a
local synthetic upstream, not official-history access. Initial full Rust run
failed one pre-existing socket test because the macOS temporary path exceeded
SUN_LEN; short TMPDIR resolved it without changing product code.

## Formal deployment scope

Destination: U1 `/mnt/p44pro/marketcow-shadow-v3-runtime/linux`.
Transfer only this feature's source delta: new `source_price_history.rs`, module
declaration in `discovery_collector.rs`, route merge in `source_public_api.rs`.
Do not copy unrelated dirty worktree changes. Build the Linux collector on U1;
freeze the binary, source delta, hashes and old unit into a private release
directory `releases/history-readthrough-r1` for rollback/audit. No public upload.
Coordinate Paper pause, switch only `marketcow-polymarket-collector.service`,
then verify existing Live full-sync/WS and one bounded history request through
the Python client. Discovery, proxy, control service, roots and accounts stay
unchanged. Keep the old binary and do not delete runtime data. Confirm actual
running binary hash and upstream outcome before reporting deployment success.

## Actual deployment evidence

User explicitly confirmed the disclosed upload and deployment. U1 release tests:
90 passed, 3 ignored. Release build completed successfully. Only Live unit binary
path changed; old unit saved as `rollback.service` and old release retained.
Binary SHA-256: `5ee452d7c8965be7264544e923a34dcafea47571f363f6bf39e8ecb903a3acc8`.
Actual Rust PID 344367 (wrapper 344364); Discovery 334944/control 334964 unchanged.
Local runner/process and LAN connection checks were empty before restart; no
account operation or process kill was performed locally.

Live full-sync/WS: instance `a9ab6b55491d44efb5e05b066f9542ff`, original scope
`54b2ad555edfd47bd1863fc71c7bb6443ca57a50c7433644b78ef3a78a4e4a12`, 250 identities,
cursor 74435448 → 74436070, 622 events + 1 ready, no error in bounded sample.
Full-sync 39488795 bytes, SHA-256
`a01418d95a2a2df5252a577c982c5743be15cbcd4e55f8fe8048f9349aa0f829`.
Local report `/private/tmp/marketcow-history-live-r1.json`.

Actual Python → formal MarketCow → Rust → official history: HTTP 200,
96 price points/2678 raw bytes, received `2026-09-08T13:58:43.874Z`.
Token `1461054050624993235896212005078254719114110436454300529113204898615019158205`,
start 1788220800/end 1788825600/fidelity 60. Raw SHA-256
`da3f90d99f20985f35af37a092a15d3e360912482f73c182db1cf92402f70d88`.
Envelope and raw response retained at `/private/tmp/marketcow-history-formal-r1/`.
This proves this history request worked, not full seven-day coverage, paired
finality labels, executable depth, or long-term streaming stability.
