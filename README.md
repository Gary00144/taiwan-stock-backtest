# Taiwan Stock Backtest Data Updater

This repository maintains the automatic Yahoo adjusted-price feed used by the
Taiwan Stock Backtest site.

## Schedule

- Monday-Friday at 14:20 `Asia/Taipei`.
- A manual run is available through `workflow_dispatch`.
- The workflow also runs when the updater or workflow definition changes, which
  gives setup changes an immediate validation run.

## Safety rules

- `0050` and `2330` must both have the requested trading date before a build can
  start.
- The official TWSE/TPEx universe must pass minimum-size validation.
- Every instrument must return a usable Yahoo history.
- At least 90% of the instrument universe must contain the current trading-day
  row. Suspended/non-trading securities may legitimately be absent that day.
- If any required validation fails, the previous `data` branch is left intact.
- Holidays produce no snapshot and leave the previous `data` branch intact.

## Published data

Successful runs replace the single snapshot commit on the `data` branch. The
generated payloads live under `generated-data/` and include:

- `market_manifest.json`
- `market_monthly.bin`
- `market_daily_YYYY.bin`
- `market_data_status.json`

The `.bin` files are deterministic gzip-compressed JSON.  The site reads the
manifest first and uses the bundled deployment data as a fallback if GitHub is
temporarily unavailable.
