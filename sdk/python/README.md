# marketcow-client

Typed Python client for MarketCow's stable market-data surface. The SDK preserves
instrument identity, decimal values, UTC timestamps, provenance, quality, stale status,
and per-item failures instead of flattening them into lossy convenience values.

## Install locally

Python 3.11 or newer is required.

```bash
pip install ./sdk/python
```

The package distribution is `marketcow-client`; the import name is `marketcow_sdk`, so it
does not collide with the MarketCow server package.

## Five-minute quickstart

For a trusted loopback or private-network instance:

```python
from marketcow_sdk import MarketCowClient

with MarketCowClient("http://127.0.0.1:8790") as client:
    health = client.health.check()
    print(health.status, health.version)

    result = client.quotes.get(
        ["AAPL.XNAS", "700.XHKG", "600519.XSHG"],
        refresh=False,
    )
    for quote in result.items:
        print(quote.instrument_id, quote.price, quote.source, quote.stale)
    for error in result.errors:
        print("unavailable", error)
```

`refresh=False` is deliberately the default and asks MarketCow to prefer its cache. The
server may still acquire missing or stale data according to the endpoint's documented
freshness policy. Set `refresh=True` only when a provider refresh is explicitly intended.
Provider fallback is also explicit:

```python
quotes = client.quotes.get(
    ["600519.XSHG"],
    refresh=True,
    provider="longport",
    allow_fallback=False,
)
```

## Public dual-JWT authentication

Token issuance is outside MarketCow and this SDK. Pass callbacks that obtain valid tokens;
the SDK never logs or includes them in exception representations.

```python
from marketcow_sdk import DualJwtAuth, MarketCowClient

auth = DualJwtAuth(
    access_token_provider=get_investrace_access_token,
    marketcow_token_provider=get_marketcow_secondary_token,
)

# A public deployment can use a base URL ending in /public so SDK /v1 paths become
# https://data.example.com/public/v1/...
with MarketCowClient("https://data.example.com/public", auth=auth) as client:
    print(client.health.check())
```

Use `BearerTokenAuth` for a gateway that accepts one bearer token, `CallbackAuth` for a
custom header contract, and `NoAuth` only for a trusted endpoint.

## Canonical bars

The iterator follows signed cursors but refuses to combine different snapshot IDs.

```python
series = client.bars.iter_canonical(
    "AAPL.XNAS",
    start="2026-01-01T00:00:00Z",
    end="2026-06-30T23:59:59Z",
    interval="1-DAY",
    adjustment="raw",
    page_size=1000,
)

print(series.manifest.snapshot_id, series.manifest.content_hash)
for bar in series:
    print(bar.window_start, bar.close, bar.selected_source)
```

## Async client

```python
from marketcow_sdk import AsyncMarketCowClient

async with AsyncMarketCowClient("http://127.0.0.1:8790") as client:
    instruments = await client.instruments.search("AAPL")
    quotes = await client.quotes.get(["AAPL.XNAS"])
```

## Errors and retries

Stable server codes are mapped to typed exceptions such as
`AuthenticationError`, `RateLimitError`, `ProviderUnavailableError`,
`InstrumentNotFoundError`, and `SnapshotChangedError`. Exceptions retain `code`,
`status_code`, `request_id`, `provider`, `instrument_id`, `retry_after`, and bounded
structured details.

Only operations known not to trigger upstream acquisition retry transient network failures
and HTTP 429/502/503/504. Market-data reads that may fill or refresh a cache do not retry
automatically, even when `refresh=False`. The SDK never silently changes a provider;
fallback occurs only when the caller passes `allow_fallback=True`.

## Data rights

The SDK licenses client code, not market data. A MarketCow operator and consumer must verify
the current terms, redistribution rights, market-data entitlements, and rate limits of every
configured provider before commercial or public use.

## Current scope

Version 0.1 covers HTTP health, instruments, quotes, market bars, canonical bars, FX,
dividends, fund distributions, and exposure facts. Realtime WebSocket support will follow
after the externally authenticated stream contract is frozen.
