# Phase 2 MCP staged proxy milestone

Date: 2026-08-28

Status: partial Phase 2 evidence. This is a shadow compatibility bridge, not final Python
public API retirement and not migration completion.

Implementation commit: `85d7e3e52119c30c999fef76959c18d8ac04b049`.

## Boundary and safety

- Clients connect only to Rust `/mcp`; Python's legacy MCP endpoint is an explicit loopback-only
  staged upstream.
- The configured URL must be uncredentialed `http://<loopback-ip>:<distinct-port>/mcp` with no
  query or fragment. Rust rejects public hosts and a self-loop to its own listener.
- The reusable client disables environment proxies and redirects, bounds connect/total timeout,
  streams at most 1 MiB and requires HTTP 200 plus JSON media type.
- Rust accepts tools/list only if the set is exactly the frozen 14 names and every definition is
  read-only and non-destructive. Native `service_health` replaces the legacy copy.
- Each proxied call is restricted to a frozen name and its JSON-RPC id/result/error envelope is
  checked. An unavailable, malformed, oversized or divergent upstream fails closed.
- Health reports only whether a proxy is configured, never its URL.

## Reproducible differential

```sh
cargo build -p marketcowd --target-dir /tmp/<isolated-target>/target
sha256sum /tmp/<isolated-target>/target/debug/marketcow
uv run --isolated --frozen python scripts/migration/verify_mcp_staged_proxy.py \
  --binary /tmp/<isolated-target>/target/debug/marketcow \
  --expected-binary-sha256 <sha256> \
  --source-commit <full-git-sha> \
  --output artifacts/rust-python-migration/results/phase2-mcp-staged-proxy-differential.json
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
uv run --isolated --frozen python -m unittest -v tests.test_mcp_server
python3 -m ruff check tests/test_mcp_server.py \
  scripts/migration/verify_mcp_staged_proxy.py
```

The checked result used binary SHA-256
`56b469ef56d6cd8dd76b68fe38ef8ad725d7e3af27dc75f5c257b90280b9652a`.
It proves all 14 definitions and one `get_quotes` result match the Python server across real
process and TCP boundaries. `passed` is true and every individual check is true.

Observed suite result: 77 Rust tests passed, 0 failed and 2 explicitly environment-gated
storage tests were ignored; Clippy passed with warnings denied; all 17 legacy Python MCP tests
passed in a lockfile-pinned isolated environment; Ruff passed. The pre-existing v3 soak binary
remained unchanged at SHA-256
`a211af3d01471f813ac3b2b35ac6a2181fd4c916711850844b48ac19380f849d`.

The remaining release gate is to replace all 13 proxied tools and remove
`MARKETCOW_LEGACY_MCP_URL`. Real-order submission remains disabled. Tradude does not manage
MarketCow lifecycle.
