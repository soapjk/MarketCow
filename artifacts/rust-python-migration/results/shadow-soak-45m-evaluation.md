# MarketCow 45-minute shadow soak evaluation

Status: **failed performance gates; retained as negative evidence**

This report evaluates the independently scheduled headless and HTTP/network shadow soaks. It does
not mark Phase 3 or Phase 5 complete, and the headless run is not a substitute for the HTTP/network
run. Real order submission remained disabled and Tradude did not manage the MarketCow lifecycle.

## Headless shadow soak

- ProcessExit Automation: `scheduled_task:e0d1dc26-2366-4184-b1a9-9c6027a8e0a9`
- launch commit: `b9dcb9301a975db6b9f9fb03793e703bc345930a`
- launch binary SHA-256: `6c6c242097a028eee6ad3f8e1e60287e9b27783484b7096c9c5c38dac8b6637f`
- isolated storage: `/tmp/marketcow-headless-current-45m.hyMyVf`
- requested/actual duration: `2700 s` / `2701.173748792 s`
- result: `passed=false`
- result SHA-256: `0d46b288c8ea4b83f23949a6eefb27f1b9b0f3dc2cf16346da513e76ce944962`
- 4,994 canonical events; 0 rejected; 0 gaps; 0 disconnects; queue depth 0
- published/persisted cursor: `4994/4994`; maximum book age `1,144 ms`
- apply p99: `57,868 µs`; maximum persistence latency: `1,128,231 µs`
- failed gates: apply p99 must be at most `50,000 µs`, and maximum persistence latency must be at
  most `50,000 µs`

Recovery was verified against an isolated copy using the exact launch binary. The WAL contained
4,994 contiguous records through cursor 4,994; daemon restart reached readiness; full-sync returned
published/persisted cursor `4994/4994`, two books, and zero unresolved gaps. Recovery result SHA-256:
`23a98af9c8ce5b558e29bfbc65eca482bb2c918b4b6497d96932b9dcb23a78fc`.

Reproduce recovery verification:

```sh
python3 scripts/migration/verify_soak_recovery.py \
  --binary target/debug/marketcow \
  --storage-root /tmp/marketcow-headless-current-45m.hyMyVf \
  --expected-binary-sha256 6c6c242097a028eee6ad3f8e1e60287e9b27783484b7096c9c5c38dac8b6637f \
  --output artifacts/rust-python-migration/results/headless-shadow-soak-45m-recovery.json
```

## HTTP/network shadow soak

- ProcessExit Automation: `scheduled_task:74e5ead3-9581-4d3d-b9f9-84d3155dc863`
- requested/actual duration: `2700 s` / `2700.406594 s`
- result: `passed=false`
- result SHA-256: `968e085caa9e9c3a82781c574e0e3d06d9d602b48fa9d495107f904faeadf0e4`
- 8,287 samples; 2,245 ingests; 18,863 HTTP requests; no recorded runner failure
- 0 gaps; 0 disconnects; queue depth 0; cursor lag 0; maximum book age `1,829 ms`
- readiness p99: `5.905583 ms`; maximum persistence latency: `767,399 µs`
- failed gate: maximum persistence latency must be at most `50,000 µs`

The v2 HTTP runner used `TemporaryDirectory`, which deleted its storage when the process exited, and
the result schema did not record the launch binary SHA/commit or storage root. Consequently, an
artifact-specific restart recovery check is impossible for this run. This is a second evidence
failure independent of the latency failure; the runner must retain an explicit isolated storage
root and embed build identity before the HTTP/network soak is rerun.

## Required follow-up

1. Separate and measure WAL `sync_data` latency from checkpoint contention; preserve both maximum
   and percentile distributions without relaxing the fail-closed gate.
2. Make both soak schemas embed commit, binary SHA-256, storage root, gate-by-gate verdicts, and
   failure reasons.
3. Retain HTTP storage and run the same isolated-copy WAL/checkpoint restart verifier.
4. Rebuild from the then-current source and rerun both 45-minute soaks through processExit
   Automations. Neither current failed run is acceptance evidence for criterion 5.
