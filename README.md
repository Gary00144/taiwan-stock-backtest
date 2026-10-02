# Taiwan Stock Backtest Data Updater

This repository maintains the automatic Yahoo adjusted-price feed used by the
Taiwan Stock Backtest site.

## Schedule

- Monday-Friday at 14:20/14:40, 15:20/15:40, 16:20/16:40 and 20:20 `Asia/Taipei`.
- A manual run is available through `workflow_dispatch`, with an optional `date`
  input (`YYYY-MM-DD`) for recovering a closed trading day.
- Midnight, weekend and pre-close runs target the latest closed weekday instead
  of publishing intraday prices. Holidays still leave the previous snapshot intact.
- The workflow also runs when the updater or workflow definition changes, which
  gives setup changes an immediate validation run.

## Recovery

- Incomplete HTTP responses are discarded and retried; partial content is never
  accepted as a successful download.
- If the MOPS CSV host is unavailable or its company list is unusable, the updater
  uses TWSE/TPEx official company OpenAPI feeds, with per-market size checks.
- A failed build is retried up to three times. Freshness checks skip any date that
  is already covered by the published snapshot, including manual older dates.
- When the global Yahoo chart has delayed/null recent closes, the updater repairs
  its tail from Yahoo Taiwan. Shared raw prices must match, and dividend changes
  rebase the earlier adjusted history; a price-basis mismatch stops publication.
- Recovery downloads full history, so a snapshot for a later day also restores
  missing earlier trading-day rows.

## Safety rules

- `0050` and `2330` must both have the requested trading date before a build can
  start.
- The official TWSE/TPEx universe must pass minimum-size validation.
- Every instrument must return a usable Yahoo history.
- At least 90% of the instrument universe must contain the current trading-day
  row. Suspended/non-trading securities may legitimately be absent that day.
- If any required validation fails, the previous `data` branch is left intact.
- A no-price response is treated as a holiday only when the TWSE calendar confirms
  closure. Missing anchor prices on an open day fail and trigger recovery retries.
- Verified holidays produce no snapshot and leave the previous `data` branch intact.

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

