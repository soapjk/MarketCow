# Corptie result: MarketCow Instrument ID unification

Task: `f080d27c-2c85-4c3c-95a9-772f0acdacfc`

## Result

MarketCow's market-data-facing identity is now `SYMBOL.MIC`.

- Canonical examples: `AAPL.XNAS`, `700.XHKG`, `600519.XSHG`,
  `000001.XSHE`.
- Legacy internal forms (`CN:SSE:...`, `CN:SZSE:...`, `HK:HKEX:...`,
  `US:US:...`, and venue-first dotted IDs) are rejected.
- Provider and broker symbols are resolved only through explicit namespaces.
- `AAPL.US` and bare US tickers require an explicit MIC; MarketCow does not
  infer `XNAS` or `XNYS`.
- `CanonicalInstrument` exposes `mic` directly and has no deprecated
  `exchange` property alias. Existing response/database fields named
  `exchange` are populated from `instrument.mic`.
- Quote, history, dividend, exposure, search, repository, and realtime-related
  call sites and tests use the canonical identity.
- Yahoo, LongPort, Tushare, SEC, HKEX, Eastmoney, and Sina representations stay
  at their provider boundary and do not become canonical IDs.

## Primary implementation

- `src/marketcow/instruments.py`
- `src/marketcow/normalize.py`
- `src/marketcow/api.py`
- `src/marketcow/service.py`
- `src/marketcow/dividends.py`
- `src/marketcow/exposure_facts.py`
- `src/marketcow/providers/instrument_search.py`
- `src/marketcow/providers/longport_quote.py`
- `src/marketcow/providers/yahoo_quote.py`
- `src/marketcow/providers/cn_dividends.py`
- `src/marketcow/providers/hkex_dividends.py`
- `src/marketcow/providers/sec_dividends.py`
- `src/marketcow/providers/structured_dividends.py`

## Contract documentation

- `docs/market-data-v1.md`
- `docs/history-jobs.md`
- `docs/provider-development.md`

## Verification

Executed locally:

```text
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p 'test_*.py'
Ran 327 tests in 14.613s
OK (skipped=19)
```

Focused identity/history/provider/dividend/realtime/repository suite:

```text
Ran 147 tests in 7.940s
OK (skipped=17)
```

Ruff correctness checks over the changed implementation modules:

```text
All checks passed!
```

Repository scans found no source generation of the retired colon or
venue-first identity formats. The remaining legacy strings occur only in
negative tests and contract documentation that explicitly states they are
rejected.

No push or deployment was performed. No local commit was created because the
shared worktree contained unrelated pre-existing changes; this artifact points
to the verified local worktree state without claiming ownership of those
unrelated changes.
