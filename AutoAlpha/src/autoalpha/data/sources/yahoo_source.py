"""Yahoo Finance EOD ingestion for US, UK, and EU ticker universes.

Research-grade only. Yahoo Finance provides no point-in-time listing,
delisting, suspension, or price-limit history, so panels produced here carry
the platform's existing ``RESEARCH_READY`` ceiling and are explicitly blocked
from capital-ledger/proxy-execution and strict PIT use in their metadata.

This adapter is deliberately self-contained: it shares the *panel shape* with
the A-share daily panel but imports nothing from the Tushare modules and makes
no A-share assumption (no board lots, CNY units, T+1, or price-limit
semantics). Every field without a Yahoo equivalent is documented in
``YAHOO_COLUMN_NOTES`` and the panel metadata rather than silently bent to fit.

The network boundary is a single injectable ``fetcher`` so unit tests run
fully offline with recorded fixtures; the default fetcher lazily imports the
optional ``yfinance`` dependency.
"""

from __future__ import annotations

import json
import shutil
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from autoalpha.data.sources.base import (
    SourceCapabilityProfile,
    SourceDescriptor,
    register_source,
)

SOURCE_ID = "yahoo"
SOURCE_PROTOCOL = "AUTOALPHA_YAHOO_EOD_PANEL_V1"
CAPABILITY_CEILING = "RESEARCH_READY"
PIT_READINESS = "NOT_POINT_IN_TIME_NO_LISTING_DELISTING_SUSPENSION_HISTORY"

FetchResult = dict[str, pd.DataFrame]
Fetcher = Callable[[tuple[str, ...], str | None, str | None], FetchResult]

#: Columns the shared daily-panel consumers expect, and their Yahoo semantics.
YAHOO_COLUMN_NOTES: tuple[tuple[str, str], ...] = (
    ("ts_code", "Yahoo ticker (for example AAPL, SHEL.L, ASML.AS); the security key column."),
    ("name", "No Yahoo equivalent in the price API; the ticker itself is stored."),
    ("trade_date", "Session date in the listing exchange's local calendar (as returned by Yahoo)."),
    ("open", "Unadjusted open (yfinance auto_adjust=False)."),
    ("high", "Unadjusted high (yfinance auto_adjust=False)."),
    ("low", "Unadjusted low (yfinance auto_adjust=False)."),
    ("close", "Unadjusted close (yfinance auto_adjust=False)."),
    ("adj_close", "Yahoo 'Adj Close': split- and dividend-adjusted close."),
    (
        "pre_close",
        "Derived: previous available session's unadjusted close per ticker; on corporate-action "
        "dates it differs economically from the officially quoted previous close.",
    ),
    (
        "change",
        "Derived: close - pre_close on unadjusted prices; jumps on ex-dividend/split dates are "
        "corporate actions, not losses.",
    ),
    (
        "pct_chg",
        "Derived: percent change of unadjusted close; not corporate-action adjusted (use "
        "adj_close-derived returns for that).",
    ),
    ("vol", "Yahoo Volume in shares (no board-lot conversion is applied)."),
    (
        "amount",
        "No Yahoo equivalent. Approximated as close * vol in local listing currency (dollar/"
        "pound/euro volume proxy). Mixed across currencies; never a CNY amount.",
    ),
    ("ret_1d", "Derived: close / pre_close - 1 on unadjusted prices."),
    ("close_to_close_ret", "Derived: same as ret_1d (no suspension gaps are represented)."),
    ("observation_gap_days", "Derived: calendar days since the ticker's previous available row."),
    ("history_observations", "Derived: 1-based running count of available rows per ticker."),
    ("is_valid_ohlc", "Derived: positive and internally consistent OHLC relationship."),
    ("has_activity", "Derived: positive volume (and therefore positive approximated amount)."),
    (
        "is_tradable_observation",
        "Derived: is_valid_ohlc AND has_activity. Yahoo cannot confirm "
        "the security actually traded or was buyable that session.",
    ),
)

KNOWN_LIMITATIONS: tuple[str, ...] = (
    "Yahoo Finance provides no point-in-time listing, delisting, suspension, ST-style status, "
    "price-limit, or free-float history; the panel must not be used for production admission.",
    "Survivorship bias: only currently listed tickers requested in the universe config appear; "
    "delisted tickers and their history are absent.",
    "Prices are in each listing's local currency; LSE tickers are quoted in pence (GBp) while US "
    "tickers are in USD and many EU tickers in EUR. Cross-sectional price-level and amount "
    "comparisons mix currencies unless explicitly normalized.",
    "'amount' is an approximated notional (close * volume), not an exchanged value.",
    "Rows with missing OHLC or volume are dropped; the gap cannot be attributed to suspension, "
    "holiday, or data error.",
)


@dataclass(frozen=True)
class YahooUniverseEntry:
    """One configured ticker, optionally scoped to an exchange tag."""

    ticker: str
    exchange: str | None = None

    def to_dict(self) -> dict[str, str | None]:
        return {"ticker": self.ticker, "exchange": self.exchange}


def parse_yahoo_universe(specs: list[str] | tuple[str, ...]) -> tuple[YahooUniverseEntry, ...]:
    """Parse plain tickers and ``EXCHANGE:TICKER`` scoped entries.

    Plain tickers keep ``exchange=None``. Duplicate tickers keep their first
    declaration. Exchange tags are normalized to upper case; tickers keep their
    original case because Yahoo suffixes are case-sensitive.
    """
    entries: dict[str, YahooUniverseEntry] = {}
    for raw in specs:
        text = str(raw).strip()
        if not text:
            raise ValueError("Universe entries must not be empty")
        if ":" in text:
            exchange, _, ticker = text.partition(":")
            exchange = exchange.strip().upper()
            ticker = ticker.strip()
            if not exchange or not ticker:
                raise ValueError(
                    f"Invalid exchange-scoped universe entry {raw!r}; expected EXCHANGE:TICKER"
                )
        else:
            exchange, ticker = None, text
        if any(character.isspace() for character in ticker):
            raise ValueError(f"Invalid ticker {raw!r}; tickers must not contain whitespace")
        if "/" in ticker or "\\" in ticker or ".." in ticker:
            raise ValueError(
                f"Invalid ticker {raw!r}; tickers must not contain path separators or '..'"
            )
        if ticker not in entries:
            entries[ticker] = YahooUniverseEntry(ticker=ticker, exchange=exchange)
    if not entries:
        raise ValueError("Universe configuration is empty; provide at least one ticker")
    return tuple(entries[ticker] for ticker in sorted(entries))


def default_yahoo_fetcher(
    tickers: tuple[str, ...], start: str | None, end: str | None
) -> FetchResult:
    """Fetch daily bars through yfinance; requires the optional dependency."""
    try:
        import yfinance
    except ImportError as error:  # pragma: no cover - exercised only without the extra
        raise RuntimeError(
            "The yfinance package is not installed. Install the optional dependency with: "
            "pip install 'autoalpha-research[yahoo]'"
        ) from error
    raw = yfinance.download(
        tickers=list(tickers),
        start=start,
        end=end,
        auto_adjust=False,
        actions=False,
        group_by="ticker",
        threads=True,
        progress=False,
    )
    if raw is None or len(raw) == 0:
        return {}
    return _split_yahoo_frame(raw, tickers)


def _split_yahoo_frame(raw: pd.DataFrame, tickers: tuple[str, ...]) -> FetchResult:
    frames: FetchResult = {}
    columns = raw.columns
    multi = isinstance(columns, pd.MultiIndex)
    levels = columns.nlevels if multi else 1
    for ticker in tickers:
        if multi and levels == 2:
            if ticker not in columns.get_level_values(0):
                continue
            frame = raw[ticker].copy()
        else:
            frame = raw.copy()
        frame = frame.rename(
            columns={
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Adj Close": "adj_close",
                "Volume": "volume",
            }
        )
        required = {"open", "high", "low", "close", "adj_close", "volume"}
        if not required <= set(frame.columns):
            continue
        frames[ticker] = frame[list(required)]
    return frames


def run_yahoo_ingestion(
    *,
    root: Path,
    universe: list[str] | tuple[str, ...],
    start: str | None = None,
    end: str | None = None,
    fetcher: Fetcher | None = None,
    retries: int = 2,
    retry_backoff_seconds: float = 1.0,
    workers: int = 4,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Ingest a configured ticker universe into the shared daily-panel layout.

    Layout produced under ``root``::

        processed/daily_panel/trade_year=YYYY/part-0000.parquet
        processed/daily_panel/_metadata.json
        catalog/data_quality.json
        catalog/daily_catalog.csv
        data/downloads/yahoo_eod/<TICKER>.parquet  (raw per-ticker lineage)
        data/downloads/yahoo_eod/_manifest.json

    Fresh runs (no existing panel under ``root``) always succeed. When a panel
    already exists at ``root``, the call refuses to replace it unless
    ``overwrite=True`` is passed explicitly, so re-running the ingestion
    command with a different or smaller universe cannot silently drop
    previously ingested tickers or date ranges.

    Returns a summary dict; raises ``RuntimeError`` when no ticker produced
    data, or ``FileExistsError`` when a panel already exists and
    ``overwrite`` is not set.
    """
    resolved_root = root.expanduser().resolve()
    panel_path = _panel_path(resolved_root)
    if panel_path.exists() and not overwrite:
        raise FileExistsError(
            f"Yahoo daily panel already exists at {panel_path}; re-running ingestion would "
            "silently drop tickers/date ranges from the existing panel. Pass overwrite=True "
            "(or --overwrite on the CLI) to replace it deliberately."
        )
    entries = parse_yahoo_universe(universe)
    fetch = fetcher or default_yahoo_fetcher
    tickers = tuple(entry.ticker for entry in entries)
    frames, failures = _fetch_all(
        fetch,
        tickers,
        start=start,
        end=end,
        retries=retries,
        backoff=retry_backoff_seconds,
        workers=workers,
    )
    if not frames:
        detail = ", ".join(sorted(failures)) or "no data returned"
        raise RuntimeError(f"Yahoo ingestion produced no data for any ticker: {detail}")
    normalized = {ticker: _normalize_ticker(frame) for ticker, frame in frames.items()}
    normalized = {ticker: frame for ticker, frame in normalized.items() if not frame.empty}
    if not normalized:
        raise RuntimeError(
            "Yahoo ingestion produced no usable rows after normalization: "
            + ", ".join(sorted(failures or normalized))
        )
    panel_frame = _build_panel_frame(normalized)
    summary = _write_workspace(
        resolved_root, panel_frame, normalized, entries, start=start, end=end
    )
    return {
        "source_id": SOURCE_ID,
        "ok": not failures,
        "capability_ceiling": CAPABILITY_CEILING,
        "point_in_time_ready": False,
        "root": str(resolved_root),
        "rows": summary["rows"],
        "symbols": summary["symbols"],
        "first_trade_date": summary["first_trade_date"],
        "last_trade_date": summary["last_trade_date"],
        "tickers_ingested": sorted(normalized),
        "failed_tickers": dict(sorted(failures.items())),
    }


def _fetch_all(
    fetch: Fetcher,
    tickers: tuple[str, ...],
    *,
    start: str | None,
    end: str | None,
    retries: int,
    backoff: float,
    workers: int,
) -> tuple[FetchResult, dict[str, str]]:
    """Fetch per-ticker frames; per-ticker failures are reported, not raised."""
    results: FetchResult = {}
    failures: dict[str, str] = {}
    # One batched call when possible keeps request counts low; fall back to a
    # single-ticker call when the batch fails so one bad ticker cannot poison
    # the whole universe.
    for attempt in range(retries + 1):
        try:
            results.update(fetch(tickers, start, end))
            break
        except Exception as error:  # noqa: BLE001 - vendor exceptions are untyped
            if attempt == retries:
                failures["__batch__"] = f"{type(error).__name__}: {error}"
            else:
                time.sleep(backoff * (attempt + 1))
    missing = [ticker for ticker in tickers if ticker not in results]
    if missing:
        with ThreadPoolExecutor(max_workers=max(1, min(workers, len(missing)))) as pool:
            outcomes = list(
                pool.map(lambda t: _fetch_one(fetch, t, start, end, retries, backoff), missing)
            )
        for ticker, outcome in zip(missing, outcomes, strict=True):
            if isinstance(outcome, pd.DataFrame):
                results[ticker] = outcome
            else:
                failures[ticker] = outcome
    return {t: f for t, f in results.items() if t in tickers}, {
        k: v for k, v in failures.items() if k != "__batch__"
    }


def _fetch_one(
    fetch: Fetcher,
    ticker: str,
    start: str | None,
    end: str | None,
    retries: int,
    backoff: float,
) -> pd.DataFrame | str:
    last: Exception | None = None
    for attempt in range(retries + 1):
        try:
            frames = fetch((ticker,), start, end)
            if ticker in frames:
                return frames[ticker]
            return "no data returned for ticker"
        except Exception as error:  # noqa: BLE001 - vendor exceptions are untyped
            last = error
            if attempt < retries:
                time.sleep(backoff * (attempt + 1))
    return f"{type(last).__name__}: {last}"


def _normalize_ticker(frame: pd.DataFrame) -> pd.DataFrame:
    """Clean one ticker's raw frame and derive the shared panel columns."""
    data = frame.copy()
    data.index = pd.to_datetime(data.index, errors="coerce")
    data = data[~data.index.isna()]
    data = data.sort_index()
    data = data.dropna(subset=["open", "high", "low", "close", "volume"], how="any")
    data = data[data["volume"] >= 0]
    if data.empty:
        return data
    close = data["close"].astype("float64")
    pre_close = close.shift(1)
    data = data.assign(
        close=close,
        pre_close=pre_close,
        change=close - pre_close,
        pct_chg=(close / pre_close - 1.0) * 100.0,
        ret_1d=close / pre_close - 1.0,
        close_to_close_ret=close / pre_close - 1.0,
        observation_gap_days=data.index.to_series().diff().dt.days.astype("float64"),
        history_observations=range(1, len(data) + 1),
        is_valid_ohlc=(
            (data["open"] > 0)
            & (data["high"] > 0)
            & (data["low"] > 0)
            & (close > 0)
            & (data["high"] >= data[["open", "close", "low"]].max(axis=1))
            & (data["low"] <= data[["open", "close", "high"]].min(axis=1))
        ),
        has_activity=data["volume"].astype("float64") > 0,
    )
    data["amount"] = close * data["volume"].astype("float64")
    data["is_tradable_observation"] = data["is_valid_ohlc"] & data["has_activity"]
    return data


def _build_panel_frame(frames: FetchResult) -> pd.DataFrame:
    parts = []
    for ticker, frame in frames.items():
        data = pd.DataFrame(
            {
                "ts_code": ticker,
                "name": ticker,
                "trade_date": frame.index,
                "open": frame["open"].to_numpy(),
                "high": frame["high"].to_numpy(),
                "low": frame["low"].to_numpy(),
                "close": frame["close"].to_numpy(),
                "adj_close": frame["adj_close"].to_numpy(),
                "pre_close": frame["pre_close"].to_numpy(),
                "change": frame["change"].to_numpy(),
                "pct_chg": frame["pct_chg"].to_numpy(),
                "vol": frame["volume"].astype("float64").to_numpy(),
                "amount": frame["amount"].to_numpy(),
                "ret_1d": frame["ret_1d"].to_numpy(),
                "close_to_close_ret": frame["close_to_close_ret"].to_numpy(),
                "observation_gap_days": frame["observation_gap_days"].to_numpy(),
                "history_observations": frame["history_observations"].to_numpy(),
                "is_valid_ohlc": frame["is_valid_ohlc"].to_numpy(),
                "has_activity": frame["has_activity"].to_numpy(),
                "is_tradable_observation": frame["is_tradable_observation"].to_numpy(),
            }
        )
        parts.append(data)
    panel = pd.concat(parts, ignore_index=True)
    panel["trade_date"] = pd.to_datetime(panel["trade_date"]).dt.normalize()
    panel["trade_year"] = panel["trade_date"].dt.year
    panel["ts_code"] = panel["ts_code"].astype("string")
    panel["name"] = panel["name"].astype("string")
    return panel.sort_values(["trade_date", "ts_code"]).reset_index(drop=True)


def _panel_path(root: Path) -> Path:
    return root / "processed" / "daily_panel"


def _write_workspace(
    root: Path,
    panel: pd.DataFrame,
    frames: FetchResult,
    entries: tuple[YahooUniverseEntry, ...],
    *,
    start: str | None,
    end: str | None,
) -> dict[str, Any]:
    panel_path = _panel_path(root)
    catalog_path = root / "catalog"
    download_path = root / "data" / "downloads" / "yahoo_eod"
    staging = panel_path.with_name(f".{panel_path.name}.staging")
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)
    catalog_path.mkdir(parents=True, exist_ok=True)
    download_path.mkdir(parents=True, exist_ok=True)
    try:
        rows_written = 0
        for year, group in panel.groupby("trade_year", sort=True):
            partition = staging / f"trade_year={int(year)}"
            partition.mkdir(parents=True, exist_ok=True)
            group.drop(columns=["trade_year"]).to_parquet(
                partition / "part-0000.parquet", index=False
            )
            rows_written += len(group)
        summary = {
            "rows": int(rows_written),
            "symbols": int(panel["ts_code"].nunique()),
            "first_trade_date": panel["trade_date"].min().date().isoformat(),
            "last_trade_date": panel["trade_date"].max().date().isoformat(),
        }
        metadata = {
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "source": "data/downloads/yahoo_eod",
            "source_id": SOURCE_ID,
            "source_protocol": SOURCE_PROTOCOL,
            "format": "parquet",
            "partitioning": ["trade_year"],
            "price_adjustment": "unadjusted",
            "execution_price_adjustment": "unadjusted",
            "volume_unit": "shares",
            "amount_unit": "approximated_local_currency_notional",
            "currency_policy": "MIXED_LOCAL_CURRENCY_SEE_KNOWN_LIMITATIONS",
            "capital_ledger_ready": False,
            "capital_ledger_proxy_ready": False,
            "data_capability_ceiling": CAPABILITY_CEILING,
            "point_in_time_readiness": PIT_READINESS,
            "known_limitations": list(KNOWN_LIMITATIONS),
            "column_notes": {name: note for name, note in YAHOO_COLUMN_NOTES},
            "universe": [entry.to_dict() for entry in entries],
            "requested_start": start,
            "requested_end": end,
            **summary,
        }
        (staging / "_metadata.json").write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        _write_quality_report(catalog_path, panel, summary)
        _write_catalog(catalog_path, panel)
        _write_raw_frames(download_path, frames, entries, start=start, end=end)
        _atomic_replace(staging, panel_path)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return summary


def _write_quality_report(catalog_path: Path, panel: pd.DataFrame, summary: dict[str, Any]) -> None:
    keys = ["ts_code", "trade_date"]
    null_keys = int(panel[keys].isna().any(axis=1).sum())
    duplicate_keys = int(panel.duplicated(keys).sum())
    null_market_values = int(
        panel[["open", "high", "low", "close", "vol", "amount"]].isna().any(axis=1).sum()
    )
    invalid_ohlc = int((~panel["is_valid_ohlc"]).sum())
    negative_activity = int(((panel["vol"] < 0) | (panel["amount"] < 0)).sum())
    return_difference = (
        (panel["close"] / panel["pre_close"] - 1.0) - panel["pct_chg"] / 100.0
    ).abs()
    mismatch = panel["pre_close"].notna() & (panel["pre_close"] != 0) & (return_difference > 0.0005)
    checks = {
        "duplicate_keys": duplicate_keys,
        "null_keys": null_keys,
        "null_market_values": null_market_values,
        "invalid_ohlc_rows": invalid_ohlc,
        "negative_activity_rows": negative_activity,
        "return_mismatch_rows": int(mismatch.sum()),
    }
    report = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "source": "data/downloads/yahoo_eod",
        "source_id": SOURCE_ID,
        "source_protocol": SOURCE_PROTOCOL,
        "summary": summary,
        "checks": checks,
        "passed": all(value == 0 for value in checks.values()),
        "capability_ceiling": CAPABILITY_CEILING,
        "point_in_time_readiness": PIT_READINESS,
        "known_limitations": list(KNOWN_LIMITATIONS),
    }
    (catalog_path / "data_quality.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def _write_catalog(catalog_path: Path, panel: pd.DataFrame) -> None:
    grouped = panel.groupby("ts_code", sort=True)
    catalog = pd.DataFrame(
        {
            "ts_code": grouped.size().index,
            "rows": grouped.size().to_numpy(),
            "first_trade_date": grouped["trade_date"].min().astype(str).to_numpy(),
            "last_trade_date": grouped["trade_date"].max().astype(str).to_numpy(),
            "name": grouped["name"].agg(lambda value: str(value.iloc[0])).to_numpy(),
        }
    )
    catalog.to_csv(catalog_path / "daily_catalog.csv", index=False)


def _write_raw_frames(
    download_path: Path,
    frames: FetchResult,
    entries: tuple[YahooUniverseEntry, ...],
    *,
    start: str | None,
    end: str | None,
) -> None:
    exchange_by_ticker = {entry.ticker: entry.exchange for entry in entries}
    manifest: dict[str, Any] = {
        "source_id": SOURCE_ID,
        "source_protocol": SOURCE_PROTOCOL,
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "requested_start": start,
        "requested_end": end,
        "capability_ceiling": CAPABILITY_CEILING,
        "point_in_time_readiness": PIT_READINESS,
        "tickers": {},
    }
    for ticker in sorted(frames):
        frame = frames[ticker]
        path = download_path / f"{ticker}.parquet"
        frame.to_parquet(path, index=True)
        manifest["tickers"][ticker] = {
            "exchange": exchange_by_ticker.get(ticker),
            "rows": int(len(frame)),
            "first_date": frame.index.min().date().isoformat() if len(frame) else None,
            "last_date": frame.index.max().date().isoformat() if len(frame) else None,
        }
    temporary = download_path / "_manifest.json.tmp"
    temporary.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    temporary.replace(download_path / "_manifest.json")


def _atomic_replace(staging: Path, target: Path) -> None:
    backup = target.with_name(f".{target.name}.backup")
    if backup.exists():
        shutil.rmtree(backup)
    if target.exists():
        target.rename(backup)
    try:
        staging.rename(target)
    except Exception:
        if backup.exists():
            backup.rename(target)
        raise
    shutil.rmtree(backup, ignore_errors=True)


YAHOO_SOURCE = SourceDescriptor(
    source_id=SOURCE_ID,
    label="Yahoo Finance EOD (US/UK/EU)",
    markets=("US", "GB", "EU"),
    capability=SourceCapabilityProfile(
        capability_ceiling=CAPABILITY_CEILING,
        point_in_time_ready=False,
        rationale=(
            "Yahoo Finance supplies daily OHLCV with adjusted closes but no point-in-time "
            "listing, delisting, suspension, or price-limit history; the panel is research-grade "
            "only and stays blocked from proxy execution and production admission."
        ),
    ),
    ingestion_command=(
        "uv run python -m autoalpha.data.sources.yahoo_cli --root <data-root> --universe AAPL "
        "--universe LSE:SHEL.L [--start YYYY-MM-DD] [--end YYYY-MM-DD]"
    ),
    ingest=run_yahoo_ingestion,
    notes=tuple(note for _, note in YAHOO_COLUMN_NOTES),
)
register_source(YAHOO_SOURCE)
