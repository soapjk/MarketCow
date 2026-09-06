# Direct Rust public API — first isolated real-data probe

Date: 2026-09-06. This is partial migration evidence, not Task acceptance.

U1 release binary SHA256:
`8ad46042cd084263357925342f046eae89f4166387711b976ee9ea7c72b3196b`.
Source commit: `5562be7`. The subsequent stale-confirmation guard is not in
this binary or this capture.

`scripts/check_u1_public_rust_r1.py` started only an isolated Rust collector
with direct HTTP/WS on loopback 8794, using a separate bounded SQLite root.
It did not start a Python API, change formal units, or execute Paper.
The finite runner and collector both exited 0; child PID became 0.
Formal collector/API PIDs remained 251219/254235 and active.

Artifacts under U1 `/mnt/p44pro/marketcow-shadow-v3-runtime/linux/logs/`:

- `public-rust-smoke-r1.report.json`
- `public-rust-smoke-r1.full-sync.json`: 39,999,320 bytes;
  SHA256 `8b94b1a665878b1f10490b27c93bab22eca427c822d93c054995f2e9a0f6bda9`.
- `public-rust-smoke-r1.ws.jsonl`: SHA256
  `8656075bf5562be0e7efa9271ffddd77bd2ded4cd260fdc5596be77b95145c16`.
  This contains original application messages with a newline delimiter added,
  not TCP or WebSocket control frames.

Results: exactly 250 subscribed identities; baseline cursor 7962829;
1 ready, 1,978 events and 1,012 confirmations; final consumed cursor 7964807.
The capture reached its 16 MiB byte budget in about 10.13 seconds including
close, so it is not a 20-second or sustained-load result. Full-sync initially
retried connection-refused while the isolated process started.

The unmodified Tradude `ConfiguredLiveState` replay accepted the baseline and
all frames with no global protocol exception. It locally rejected 42
confirmations: 40 `confirmation precedes original book`, 2
`confirmation time moved backwards`. These are real defects, not a reason to
discard healthy markets or relax consumer checks. A subsequent source guard
rejects a confirmation whose receipt does not advance the current/original/
confirmed timestamp fence; 53 daemon tests pass, one fixture test is ignored
by default. Real retest of that guard remains pending.

Reproduce decoding with Tradude's existing Python environment, its workspace
on PYTHONPATH, and `scripts/replay_public_rust_fixture.py`. Supply the two
artifact paths and the exact hashes above using required `--full-sync`,
`--full-sync-sha256`, `--wire`, `--wire-sha256` arguments. This performs no
network access or Paper/account processing.

Final isolated durable cursor 7967593, gap count 0, history floor 7952234,
recent event bytes 67,106,642 within 64 MiB; no new JSONL. This does not replace
a complete independent durability checksum audit. MemoryPeak was unavailable
after transient-unit cleanup; no RSS/latency or slow-consumer claim is made.

Remaining: strict full metadata/projector parity, resource/slow-client bounds,
longer causal/retention/recovery tests, Rust Discovery online migration, and a
coordinated formal switch with Paper account continuation. No formal switch
or acceptance follows from this probe.
