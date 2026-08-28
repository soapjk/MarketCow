# Phase 5 worker resource isolation milestone

Date: 2026-08-28

Status: partial Phase 5 evidence. This does not claim Phase 5 or the migration is complete.

## Implemented controls

- Worker memory configuration is bounded to 128–16384 MiB; default 2048 MiB.
- Worker accumulated CPU configuration is bounded to 1–86400 seconds; default 900 seconds.
- Before `exec`, Rust installs CPU, 256-file-descriptor and zero-core-dump limits. Linux also
  installs an address-space limit.
- The supervisor independently samples resident memory every 250 milliseconds. An
  over-limit worker is killed and reaped; an unavailable or malformed RSS observation also
  kills the worker fail-closed.
- Memory-limit kills and monitor failures are exported in health and Prometheus metrics.
- Resource enforcement applies only to Python provider workers and cannot stop or restart
  the MarketCow HTTP/WAL platform.
- Worker credentials remain cleared, real orders remain disabled, and Tradude has no
  MarketCow lifecycle role.

## Reproducible verification

```sh
cargo test -p marketcowd worker_ -- --nocapture
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

The focused tests execute real child processes and prove that:

1. PostgreSQL, ClickHouse and admin credentials are absent.
2. CPU, file-descriptor and core-dump limits are visible inside the child.
3. An RSS-over-limit process is killed and reaped.
4. Restart attempts remain bounded by the sliding window.

## Remaining Phase 5 gates

- Per-provider rate-limit and concurrency policies.
- Provider-specific secret reference/FD delivery where a handler requires a credential.
- Remaining Provider handlers and removal of corresponding Python public API/database
  ownership after differential tests pass.
