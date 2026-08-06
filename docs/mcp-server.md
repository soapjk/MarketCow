# MarketCow MCP Server

MarketCow starts a read-only Streamable HTTP MCP endpoint with the API by default. Agents
can use it for financial-data analysis without a second service process. It is an adapter
over the public HTTP API: it does not connect directly to PostgreSQL or ClickHouse, and it
does not expose admin, refresh, import, or other mutating operations.

## Start and ports

Start MarketCow first:

```bash
uv run marketcow --profile development start --host 127.0.0.1 --port 8792
```

The same process now serves MCP at `http://127.0.0.1:8792/mcp`. Configure an MCP client:

```toml
[mcp_servers.marketcow]
url = "http://127.0.0.1:8792/mcp"
enabled = true
```

Port 8792 is the development example. The local production convention is 8790, so a
production endpoint is `http://127.0.0.1:8790/mcp`. Always configure the instance that is
actually running; do not silently substitute one profile's port for the other.

For a project-scoped setup that does not modify user-level or system-level MCP
configuration, follow the [project installation guide](mcp-project-installation.md).

The HTTP transport is sessionless and returns JSON responses directly. Requests with an
`Origin` header are rejected to prevent browser-based DNS rebinding, and request bodies are
limited to 1 MiB.

For clients that only support subprocess transport, use a versioned absolute executable and
an explicit API base. This works from any client cwd:

```bash
MARKETCOW_MCP_BASE_URL=http://127.0.0.1:8792 \
  /absolute/path/to/marketcow/.venv/bin/marketcow-mcp
```

The stdio process uses newline-delimited JSON-RPC. Logs and startup errors use stderr so they
cannot corrupt the protocol stream.

Optional environment variables:

- `MARKETCOW_MCP_BASE_URL`: MarketCow HTTP base URL; default
  `http://127.0.0.1:8790`.
- `MARKETCOW_MCP_TIMEOUT_SECONDS`: per-request timeout from `0` to `120`; default `20`.
- `MARKETCOW_MCP_BEARER_TOKEN`: optional API bearer token. Keep it in the MCP client's
  environment rather than command-line arguments or configuration committed to Git.
- `MARKETCOW_MCP_ENABLED`: controls the in-process `/mcp` endpoint; enabled by default.

The adapter does not inherit `HTTP_PROXY`, `HTTPS_PROXY`, or `ALL_PROXY`. Its MarketCow
connection is direct, which prevents local requests and an optional bearer token from being
sent through a proxy.

## Tools

- `service_health`
- `search_instruments`
- `get_instrument`
- `get_quotes`
- `get_market_bars`
- `get_canonical_bars`
- `get_fundamental`
- `get_financial_statements`
- `get_dividends`
- `get_fund_dividend_history`
- `get_exposure_facts`
- `search_convertible_bonds`
- `get_convertible_bond`
- `get_convertible_bond_market`

Every MCP tool is declared read-only: no tool exposes admin, import, deployment, or database
mutation. Read-only does not mean every implementation is cache-only. Quote and bar tools force
cached reads; the fund dividend history tool exposes an explicit `refresh` switch and defaults
to refreshing missing or stale official evidence through MarketCow. Some other dividend and
fundamental reads may also refresh MarketCow's internal cache through their public read
contract. Batch sizes and history page sizes are bounded to protect the agent context window.
For reproducible analysis, prefer `get_canonical_bars`, follow `next_cursor`, and retain the
returned manifest, provenance, quality, and adjustment fields.

The server implements the MCP stdio lifecycle and tools protocol without adding another
runtime dependency. This preserves MarketCow's tested `httpx` constraint.

Use the bundled `marketcow-mcp-project` CLI for Codex project-only install, verification,
upgrade, uninstall, and restore. The authoritative workflow is the
[Codex project installation guide](mcp-project-installation.md).

Convertible-bond field contracts, scorer mapping and missing-value semantics are
documented in [Convertible-Bond MCP v1](mcp-convertible-bonds.md).

Fund and ETF cash-distribution fields, source precedence, date-window semantics, and the
distinction from index dividend yield are documented in
[Fund Dividend History v1](fund-dividend-history.md).
