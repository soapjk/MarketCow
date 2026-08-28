# Phase 2 Rust MCP transport milestone

Date: 2026-08-28

Status: partial Phase 2 evidence. This does not claim Phase 2 or the migration is complete.

## Implemented boundary

- Axum owns `POST /mcp` on the same Rust public listener as HTTP and WebSocket.
- JSON-RPC 2.0 supports initialize negotiation for the four frozen protocol versions, ping,
  notifications, bounded batches, tools/list and tools/call.
- Any browser Origin is rejected; Content-Type must be application/json; unsupported protocol
  headers, bodies over 1 MiB and batches over 100 messages fail closed.
- `service_health` is the first native read-only tool and returns Rust-owned health state with
  real-order submission disabled.
- Its complete definition is shared by a checked-in golden fixture and independently compared
  by Rust and the legacy Python MCP suite.
- Only the proven tool is advertised. The other 13 legacy tools are intentionally not claimed
  as migrated and remain a Phase 2 gate.

## Reproducible verification

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
uv run --isolated --frozen python -m unittest -v tests.test_mcp_server
python3 -m ruff check tests/test_mcp_server.py
```

Rust tests cover the contract fixture, protocol fallback, tool execution, notification 202,
origin rejection, media-type rejection, unsupported protocol, parse error, empty batch and
oversized body. A separate test starts an actual TCP listener and performs HTTP initialize.

Observed result: 76 Rust tests passed, 0 failed and 2 explicitly environment-gated storage
tests were ignored; Clippy passed with warnings denied after correcting the warning it surfaced;
all 17 legacy Python MCP tests passed in a lockfile-pinned isolated environment; Ruff passed.
The active soak binary retained SHA-256
`a211af3d01471f813ac3b2b35ac6a2181fd4c916711850844b48ac19380f849d`.

Real-order submission remains disabled. Tradude does not manage MarketCow lifecycle.
