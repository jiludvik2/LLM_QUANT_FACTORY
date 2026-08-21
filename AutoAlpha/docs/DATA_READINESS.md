# Current Data Readiness

Assessment target: `../data/processed/daily_panel`, inspected on 2026-07-15.

The online service may be configured with `../data` directly. `DataWorkspaceReport` resolves the
processed panel, source directory, catalog, quality report, and panel metadata; it binds their
metadata fingerprint to every iteration and delivery artifact.

## Available

The partitioned panel contains 17 annual parquet files, 9,482,111 rows, and 3,192 symbols.
It provides raw and adjusted OHLC prices, volume, amount, returns, observation history, OHLC
validity, activity, and a generic tradable-observation flag. This is sufficient for provisional
price/volume factor development when signals are delayed to the next trading session.

## Platform Capability Levels

The service exposes the same module-level decision through `/ready` and `/api/data-center` under
`AUTOALPHA_DATA_CAPABILITY_MATRIX_V1`. Operators should treat these labels as policy, not as
decorative UI state:

| Level | Allowed use | Production meaning |
|---|---|---|
| `RESEARCH_READY` | AutoAlpha factor research and close-of-day screening | Research only; no execution claim |
| `PROXY_BACKTEST_READY` | Manual and batch A-share long-only proxy backtests | Non-PIT evidence only; reconcile vector and event engines |
| `PROXY_PAPER_READY` | Paper trading with next-session open proxy, T+1 and fees | Operational rehearsal only; still blocked from production |
| `PRODUCTION_BLOCKED` | Strict capital ledger and production candidate promotion | Missing PIT market state or source lineage |
| `STRICT_PIT_READY` | Strict capital ledger and production promotion gates | Requires versioned PIT state, eligibility, limits and classifications |

The current local data basis is expected to be `RESEARCH_READY` plus non-PIT proxy levels where raw
execution prices are available. It must remain `PRODUCTION_BLOCKED` until the blockers below are
removed from versioned source tables.

## Production blockers

- No source `knowledge_time`, ingestion batch, or revision history.
- No point-in-time listing, delisting, ST, name, board, or eligibility history.
- No explicit suspension state/reason or side-specific open-time limit tradability.
- No historical industry classification, index membership, or free-float capitalization.
- No source lineage proving when each record became visible to the research system.

The platform therefore does not label the current panel as institutionally point-in-time ready.
`autoalpha inspect-data ../data/processed/daily_panel` reports these blockers and strict workflows
must call `require_institutional_pit()` before production admission. Missing fields must arrive from
versioned source tables; they must not be synthesized from present-day state or filled with defaults.

## Yahoo Finance research source (US / UK / EU)

Besides the Tushare/A-share pipeline, the platform registers a second pluggable market-data
source: `yahoo` (Yahoo Finance daily OHLCV). Sources are resolved through
`autoalpha.data.sources` (`get_source` / `available_sources`); adding a future vendor such as
EODHD means registering one new descriptor module without touching consumer code.

Ingestion command (requires the optional dependency `pip install 'autoalpha-research[yahoo]'`):

```bash
cd AutoAlpha
uv run python -m autoalpha.data.sources.yahoo_cli \
    --root ../data-yahoo \
    --universe AAPL --universe MSFT --universe XOM --universe KO --universe JPM \
    --universe LSE:SHEL.L --universe LSE:AZN.L --universe LSE:HSBA.L --universe LSE:BP.L \
    --universe LSE:ULVR.L \
    --universe ASML.AS --universe SAP.DE --universe MC.PA --universe NESN.SW --universe ADYEN.AS \
    --start 2020-01-01
```

Universe entries are plain Yahoo tickers or `EXCHANGE:TICKER` scoped entries; no full-market
index scraping is performed. The command writes the same partitioned-panel workspace layout the
existing tooling consumes (`processed/daily_panel/trade_year=*`, `_metadata.json`,
`catalog/data_quality.json`, `catalog/daily_catalog.csv`, raw per-ticker lineage under
`data/downloads/yahoo_eod/`) and can be inspected unchanged via
`uv run autoalpha inspect-data <root>/processed/daily_panel` or the data-center workspace report.

Ingestion is resumable and incremental, mirroring the operational shape of the Tushare sync: a
raw download store under `data/downloads/yahoo_eod/` keeps one vendor-shaped parquet file per
ticker; durable state in `data/state/yahoo_eod.json` records each ticker's covered date range and
recent failures. Re-running the command only fetches each ticker's missing dates (paced and
bounded-retried) and then regenerates the processed panel from the complete raw store, so already-
fetched history is never refetched, rebuilt, or replaced - growing a universe or catching up a
schedule accumulates instead of overwriting. The processed panel is a pure derivative of the raw
store, so no destructive step remains and no overwrite flag exists.

**Capability ceiling: `RESEARCH_READY` only.** Yahoo Finance provides no point-in-time listing,
delisting, suspension, price-limit, or free-float history, so these panels must never be used for
proxy-execution backtests, paper trading, or production admission; panel metadata sets
`capital_ledger_ready=false`, `capital_ledger_proxy_ready=false`, and an explicit non-PIT marker,
and the standard capability matrix reports strict-PIT modules as `PRODUCTION_BLOCKED`. Column
semantics are documented in `YAHOO_COLUMN_NOTES` (`autoalpha.data.sources.yahoo_source`). Fields
without a Yahoo equivalent (`name`, exchanged `amount`) are approximated or substituted with an
explicit note rather than bent to fit A-share assumptions: identifiers stay Yahoo tickers,
volume stays shares (no board lots), amounts are close × volume in local listing currency, and
LSE prices remain in pence. Cross-currency comparison requires explicit normalization.

## Required next ingestion

Ingest security master revisions, exchange trading status and price limits, index/industry history,
point-in-time shares and free float, and vendor ingestion timestamps. Preserve raw source batches,
then produce standard and PIT feature tables through `TableContract` and immutable snapshots.
