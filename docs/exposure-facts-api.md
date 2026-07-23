# Exposure facts API for Investrace

`GET /v1/exposure-facts/{symbol}?refresh=false` returns auditable issuer and
fund facts. It never returns dynamic themes, concepts, factors, inferred risk
weights, or LLM classifications.

## Symbol and market contract

- A shares: six digits or `.SH`, `.SZ`, `.BJ`; response is canonical exchange suffix.
- Hong Kong: one to five digits plus `.HK`; response is zero-padded `.HK`.
- US: ticker, with dots normalized to dashes.
- Covered listing markets: `CN`, `HK`, `US`. Unsupported or malformed symbols
  return HTTP 400 with `code=invalid_symbol`.

`status=unavailable` is a successful, explicit no-facts response. Per-source
failures appear in `degradations` with a stable `source_unavailable` code and
redacted exception type. ETF/fund holdings independently use
`available|unavailable`; absence never falls back to the manager's industry.

## Provenance and freshness

Each fact or material carries `source_id`, a `source_url` or stable
`source_record_id`, `fetched_at`, `effective_at`, `source_tier`, and
`confidence`. Top-level `as_of` is the newest effective time used.

The reusable `ExposureFactsService` defaults to a 24-hour fresh TTL and a
7-day bounded stale fallback. Responses use `cache_status`:
`refreshed`, `fresh`, `stale`, or `empty`. A stale response is only served
inside `stale_max_seconds` and includes source degradations.

Basic classifications are an open list and may be empty or contain multiple
schemes. They are facts, not a complete exposure/risk model.

## Example

```bash
curl 'http://127.0.0.1:8790/v1/exposure-facts/MU?refresh=true'
```

```python
import requests

facts = requests.get(
    "http://127.0.0.1:8790/v1/exposure-facts/SOXX",
    timeout=10,
).json()
if facts["status"] == "available":
    holdings = facts["holdings"]
    if holdings["status"] == "available":
        weighted_constituents = holdings["constituents"]
    else:
        weighted_constituents = None  # explicit degradation; do not infer manager industry
```

The production repository adapter currently exposes persisted quote identity
facts and A-share fundamental industry facts. ETF constituent and issuer
business-document adapters can be bound to `ExposureFactsService` as those
verified datasets become available; until then those sections are explicitly
`unavailable`.
