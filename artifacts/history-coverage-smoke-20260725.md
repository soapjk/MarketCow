# History coverage adaptive-sharding smoke evidence

Date: 2026-07-25  
Mode: read-only provider probes; no history job created and no bars written.

## Scope

- Provider: configured `tushare_via_stockai888`
- Instrument: `600519.XSHG` (`600519.SH` in provider namespace)
- Interval: `1m`
- Requested window: `[2026-06-25T00:00:00Z, 2026-07-25T23:59:59Z)`
- Credential values were neither printed nor stored in this artifact.

## Planning result

The capability-aware planner generated two initial shards:

| Shard | Planned maximum | Observed rows | Coverage result |
| --- | ---: | ---: | --- |
| 2026-06-25 → 2026-07-13 | 2,904 | 1,205 | split required |
| 2026-07-13 → 2026-07-25 | 2,178 | 2,169 | split required |

The first response only contained bars for July 1–7 even though the XSHG
calendar contained additional sessions in the requested shard. The second
response omitted the July 20 session. Both responses were below the configured
3,000-row planning budget, proving that row-count threshold detection alone is
insufficient; calendar coverage detected both silent gaps.

Exact one-day `stk_mins` probes returned zero rows for June 25, June 26,
June 29, June 30, July 8, July 9, July 10, and July 20. They returned 241 rows
for July 24. Exact `adj_factor(trade_date=...)` probes returned one valid factor
for every listed date.

## Adjustment factor boundary result

A multi-day `adj_factor(start_date,end_date)` request still omitted July 24,
while `adj_factor(trade_date=20260724)` returned factor `8.6463`. This validates
the implemented exact-date supplementation after the widened range query.

## Acceptance conclusion

- The real provider can silently omit complete exchange sessions without
  reaching a simple row limit.
- The new XSHG calendar gate correctly marks both initial responses as requiring
  subdivision.
- At minimum shard size, an unexplained missing or incomplete session must end
  as `upstream_coverage_unproven`; it must not be reported as successful.
- Exact-date factor supplementation is required for the configured compatible
  endpoint.
