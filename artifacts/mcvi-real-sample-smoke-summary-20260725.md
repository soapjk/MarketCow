# MCVI authorized real-sample smoke summary — 2026-07-25

The purchased CSV contents and source path are intentionally excluded.

- Dataset: operator-authorized US 1-minute bars.
- Canonical Instrument: `AAPL.XNAS` via explicit
  `provider:purchased_vendor_2025` mapping.
- Profile: `purchased-us-minute-bars@2025.1`.
- Adjustment: `raw`.
- File SHA-256:
  `7cda49593be55c41ebcfa0edc7c27ee5dfe7b7d69954812ac8dd8c58848a838a`.
- File size: 3,570,886 bytes.
- Range: `2025-01-02T09:00:00Z` through `2025-05-02T23:59:00Z`.

## Dry-run

- Status: `valid`.
- Rows: 64,836 valid; 0 invalid; 0 duplicate; 0 unordered.
- OHLC/non-finite/negative failures: 0.
- Abnormal-price warnings: 0.
- Intraday gaps: 7,006, retained as an explicit warning because extended-hours
  trade bars are event-sparse.

## Formal import and recovery

- Manifest:
  `dbd7d089af06b7c14365612e8491a06e48ec2e594f2faf80ced162cc0d0f07fb`.
- Successful job: `c1bd8be06261413b81090ec01e3b0960`.
- Shards: 3/3 succeeded; rows read/written: 64,836/64,836.
- Raw receipts: 3; raw coverage: 64,836.
- Canonical coverage: 64,836.
- Direct `FINAL` audit: 64,836 raw rows / 64,836 unique raw keys and
  64,836 canonical rows / 64,836 unique canonical keys.
- Canonical invalid OHLC rows: 0.
- Canonical abnormal-price rows: 0.
- Quality schema: `marketcow.csv-import-quality.v1`.
- Quality status: `passed`; failures: 0.

The first formal attempt exposed two production-only compatibility defects:
PostgreSQL TEXT values returned as bytes on the local SQL-ASCII cluster, and a
ClickHouse `FINAL` alias syntax difference. The attempt safely reached
`failed` after all raw receipts were durable. After fixing both boundaries and
the canonical range-splitting gate, the retry reused the same three stable raw
ingestion IDs and completed without duplicate raw or canonical keys.

Full create-only evidence files remain local and untracked. They contain no CSV
rows, configuration body, source path, archive path, or `request_json`.
