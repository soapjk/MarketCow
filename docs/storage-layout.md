# MarketCow local storage layout

MarketCow source checkouts and runtime data are intentionally separated on the T9 volume.

```text
/Volumes/T9/projects/marketcow/           # source checkout only
/Volumes/T9/data/marketcow/
├── production/                           # production storage root
├── development/                          # development storage root
├── experiments/                          # dated probes and one-off collections
└── artifacts/                            # local contract and verification artifacts
```

Production uses `MARKETCOW_HOME=/Volumes/T9/data/marketcow/production` and
`MARKETCOW_ALLOWED_ROOT=/Volumes/T9/data/marketcow`. PostgreSQL, ClickHouse,
raw inputs, spools, imports, and prediction-market datasets therefore live under
the production data root rather than beside the Git checkout.

The launchd dependency bootstrap uses
`/Volumes/T9/data/marketcow/production/runtime` by default. Override it only with
an explicit `MARKETCOW_RUNTIME_DIR` when running an isolated test instance.
