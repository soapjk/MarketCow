# U1 direct Rust formal migration — incomplete runtime verification

Observed 2026-09-06 UTC. This is a progress record, not Task acceptance.

## Current topology

- Live: `http://192.168.124.3:8793`, Rust collector public HTTP/WS directly.
- Discovery: `http://192.168.124.3:8795`, independent Rust collector public HTTP/WS.
- Former Python read API unit is masked/inactive. Diagnostic Python log wrapper
  is not in the market-data path.
- Original bounded scoped/Discovery roots remain in use; probe roots were not
  substituted for formal state. No account reset or trading service enabled.

## Latest Live-only diagnostic activation

Release: `linux/releases/direct-rust-send-diag-6b23ab9` under U1 runtime.
Binary SHA256: `c55462ae4ebcabb1b39e02d8fe02f45f685421d60d1bb9419f4e028fbd8a4a88`.
Manifest SHA256: `8bf85052d60b1f8f1384722cb7177042b9cf07a29558769ee0860e789a14f7b0`.
Unit `marketcow-send-diag-activation-r1`: Result success, ExecMainStatus 0.
Release tests: 63 passed, 2 ignored; not the whole workspace suite.
Installer's three synthetic tests cover Live-only mutation, failed baseline
rollback, and rejecting a durable cursor below the Paper account cursor.

Pause receipt `channel_message:64472e0f-abb0-494a-bf5f-9cfd768c19ed` binds
Paper checkpoint SHA256 `866d39a3eb61cb78a578d3920359925ea190aa4142f6d24cb0f470b2bb3bbde0`,
durable account cursor 10029374. Status cursor 10047408 was a later synchronization
baseline, NOT the account cursor. Never reuse this hash after resumed writes.

Live clean-stop durable cursor 10116380, gap count 0, recent history 67106698
bytes <= 67108864. Discovery wrapper PID 264102 and proxy PID 239477/configuration
hashes were unchanged. New Live wrapper 265319 / Rust 265320; PIDs are observations,
not permanent identities. Previous Live release retained for rollback.

Actual LAN sample `/private/tmp/marketcow-send-diag-lan-r1/`:

- full-sync cursor 10124336, exactly 250 configured markets;
- instance `04144f457d6840f89ec965477c10093c`;
- 1559 events + 1 ready + 1 book_confirmations, ending cursor 10125895;
- full-sync SHA256 `061f68691086a9925508951c6aa736fad093843e04fa479c00500b3e89516434`;
- wire SHA256 `e3bda027f3bb5f5ccf9f839342f805a89ec6743cb4204fc70c5e73db6dd6970f`.

The combined script subsequently timed out during Discovery's 15s receive wait.
Therefore its overall exit was failure even though its Live assertions passed.
Discovery is configured for 30s REST polling; a subsequent audit uses the same
45s total window rather than treating 15s without an event as definitive failure.
This does not alter source freshness or Paper's 5s policy.

Separate Discovery recheck passed: 100 deltas, 2800499 -> 2800599, unchanged
projection `10fa860d9a5eda9978616ac5a59bbafdcbed372d913aed5e00d961572f86eb59`.
Files `discovery-recheck.full-sync.json` and `discovery-recheck.ws.jsonl` in the
same directory have SHA256 `981ea5ec9c465e26275423ce16fa542905dc9e6d0761ca73373ea0a60414a02d`
and `fa14c8d3b17bdb7c42dfa554893315e7b9f35f0b4d5fa2b95c0ce8a8d9b7f90c`.
This is a separate successful sample, not a retroactive pass for the first run.

Both captured fixtures were subsequently replayed through the unchanged Tradude
consumer using `scripts/replay_public_rust_fixture.py` and
`scripts/replay_rust_discovery_fixture.py`, with the SHA arguments above and
Tradude's interpreter/PYTHONPATH. Live: 250 identities, 1559 events, one ready,
one confirmation, zero global protocol errors or local confirmation rejects;
Discovery: 1000 markets, 241 relations, ready true/gaps zero and all 100 strict
deltas accepted. This replay performs no Paper execution or account mutation.

Resource sample at Live elapsed 7m43s: Rust PID265320 RSS3108920KiB, unit
MemoryCurrent3197571072 bytes/MemoryPeak3738206208 bytes. Discovery PID264123
elapsed20m57s RSS950984KiB, MemoryCurrent981798912/MemoryPeak982749184 bytes.
Both NRestarts zero. Cgroup memory includes more than RSS; this is neither a
steady-state proof nor a long-term memory bound measurement. Repeated Paper
full-sync/reconnect remains part of this actual load and must not be hidden.

## Remaining actual failure

Prior direct-Rust release logged five `deadline has elapsed` stream terminations.
These did not identify data vs Pong sends or associate each peer failure with a
specific connection. They are not evidence for the old Python apply bottleneck.
Candidate 6b23ab9 adds error-only UTC/connection/frame/cursor/bytes/send-duration
context; it changes neither queue caps nor 5s send timeout. Socket await wall time
is not CPU time or network RTT. No payload or unbounded frame history is logged.

Paper's actual resumed account retained cash and six positions, but its run had
11 failures before the coordinated pause. Full-sync/short WS smoke is thus not
proof of stable Paper operation. Next evidence must correlate these new Rust
send errors with the peer's bounded processor/checkpoint timing. Do not restart
Paper automatically or claim stability based on active units.

## Actual send diagnostics, 16:23–16:25 UTC

All three following failures are `socket_send_timeout kind=data`, not Pong or
replay expiry. They share instance `04144f457d6840f89ec965477c10093c`.

| Rust connection | Open UTC | Failed frame/cursor | Bytes | Timeout UTC / elapsed |
|---|---|---|---:|---|
| 436c060fa2dc4200b54fb3df25415061 | 16:23:44.525249637 | event / 10190800 | 3117 | 16:23:51.756761369 / 5.000811s |
| 31705d8646b6453386e28410164e6958 | 16:24:23.852692966 | book_confirmations / 10210577 | 6903 | 16:24:30.873866953 / 5.001774s |
| 519fa3f0fda94a79af44b58767a8740d | 16:25:05.442145665 | book_confirmations / 10225960 | 7975 | 16:25:12.961127103 / 5.000688s |

Source: U1 `linux/logs/direct-rust-send-diag-6b23ab9-marketcow-polymarket-collector.log`,
filter `public_live_stream_closed`. First connection initial cursor 10188874
matches the peer's actual full-sync, unlike coincidental adjacent connections.

Peer's rotating `paper-runtime.log` independently records checkpoint intervals:

- cursor10190114: 16:23:44.992596–16:23:54.947031, monotonic elapsed9.938551625s;
- cursor10209699: 16:24:24.274180–16:24:35.267414, monotonic10.991881333s;
- cursor10225378: 16:25:05.890431–16:25:18.073395, monotonic12.182869041s.

Each Rust timeout is inside the corresponding reported checkpoint wall-time
interval. Approximate send-start UTC obtained by subtracting elapsed is not an
independently recorded timestamp. Cross-host clock offsets and wall/monotonic
differences remain uncorrected. This is strong aligned evidence of serial
checkpoint-induced application receive stalls alongside data-send backpressure;
it is not a TCP receive-window trace or proof that every historical error shares
this cause. A controlled consumer-side fix/comparison remains necessary.

Do not enlarge source buffers/timeout to mask the serial checkpoint path. Peer
must preserve durable account ordering while removing full historical-state
serialization from normal receive progress; source diagnostics remain available
without restarting the service.

## Local object-assembly comparison (not deployed)

At source22816d1, `cargo test --locked --workspace -q` exited0 locally:
285 passed across test binaries, 9 ignored, no failures. Ignored tests are not
counted as passing; the explicit real assembly benchmark above was run separately.
This local debug regression does not replace U1 release/load verification.

Candidate 3a5dd6f removes four deep clones of completed JSON object fields.
Ignored test `real_fixture_object_assembly_benchmark` explicitly requires the
same captured 40270232-byte Live full-sync and its SHA; it asserts exactly250
scope identities and equal canonical output SHA. Two separate local debug
processes (old clone first, then move), one sample each:

| Mode | Object merge including old-field disposal | `/usr/bin/time -l` maximum RSS bytes |
|---|---:|---:|
| clone | 194316 us | 660652032 |
| move | 136 us | 582844416 |

Both output hashes equal input `061f68691086a9925508951c6aa736fad093843e04fa479c00500b3e89516434`.
Parsing and two full canonical hash passes are outside the merge timer but
inside process-memory measurement. Cargo/test overhead is included in the
external command measurement. This is not repeated randomized release A/B,
full-sync construction as a whole, or U1 live RSS evidence. No formal restart.

Reproduce from this worktree for each mode `clone` and `move`:

```sh
MARKETCOW_ASSEMBLY_FIXTURE=/private/tmp/marketcow-send-diag-lan-r1/live.full-sync.json \
MARKETCOW_ASSEMBLY_SHA256=061f68691086a9925508951c6aa736fad093843e04fa479c00500b3e89516434 \
MARKETCOW_ASSEMBLY_MODE=clone /usr/bin/time -l cargo test -q -p marketcowd \
  --bin marketcow-discovery-collector real_fixture_object_assembly_benchmark \
  --locked -- --ignored --nocapture
```
