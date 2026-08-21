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

Ingestion mirrors the Tushare pipeline's operational shape, adapted to a
per-symbol range vendor:

1. A raw download store under ``data/downloads/yahoo_eod/`` holds one
   vendor-shaped parquet file per symbol (unadjusted OHLC, adjusted close,
   volume) as pulled from Yahoo.
2. Durable state in ``data/state/yahoo_eod.json`` records each symbol's
   covered date range and recent failures, so a re-run fetches only what is
   missing: already-covered ranges are never refetched, and fetched history
   is never rebuilt or replaced - runs are resumable and incremental.
3. Requests are paced (``RequestPacer``) and retried with bounded backoff.
4. The processed daily panel is always regenerated from the complete raw
   store. Because it is a pure derivative of the accumulating raw store,
   rebuilding it cannot silently destroy prior ingested history; there is no
   destructive step and therefore no overwrite flag.

The network boundary is a single injectable ``fetcher`` so unit tests run
fully offline with recorded fixtures; the default fetcher lazily imports the
optional ``yfinance`` dependency.
"""

from __future__ import annotations

import json
import shutil
import threading
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
DEFAULT_REQUESTS_PER_MINUTE = 60
RAW_COLUMNS = ("open", "high", "low", "close", "adj_close", "volume")

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
    (
        "is_valid_ohlc",
        "Derived: always True in the panel; rows whose OHLC relationship is impossible are "
        "excluded at build time and counted as unusable vendor rows.",
    ),
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
    (
        "Rows with missing OHLC or volume are dropped; the gap cannot be attributed to suspension, "
        "holiday, or data error.",
    ) + (
        "Rows with impossible OHLC geometry (vendor anomalies) are excluded from the processed "
        "panel, preserved in the raw store, and counted under 'unusable_vendor_rows_dropped'.",
    )
)


class RequestPacer:
    """Serialize outbound requests to at most ``requests_per_minute``."""

    def __init__(self, requests_per_minute: int | None) -> None:
        self.interval = 60.0 / max(1, requests_per_minute) if requests_per_minute else 0.0
        self.next_request_at = 0.0
        self.lock = threading.Lock()

    def wait(self) -> None:
        if self.interval <= 0.0:
            return
        with self.lock:
            now = time.monotonic()
            delay = max(0.0, self.next_request_at - now)
            self.next_request_at = max(now, self.next_request_at) + self.interval
        if delay:
            time.sleep(delay)


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
        required = set(RAW_COLUMNS)
        if not required <= set(frame.columns):
            continue
        frames[ticker] = frame[list(RAW_COLUMNS)]
    return frames


class YahooRawStore:
    """Raw per-symbol download store plus durable resume state.

    Layout under the workspace root::

        data/downloads/yahoo_eod/<TICKER>.parquet  (vendor-shaped daily bars)
        data/downloads/yahoo_eod/_manifest.json    (derived store summary)
        data/state/yahoo_eod.json                  (durable resume state)
    """

    def __init__(self, root: Path) -> None:
        self.download_path = root / "data" / "downloads" / "yahoo_eod"
        self.state_path = root / "data" / "state" / "yahoo_eod.json"

    def ensure_dirs(self) -> None:
        self.download_path.mkdir(parents=True, exist_ok=True)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)

    def load_state(self) -> dict[str, Any]:
        try:
            value = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return value if isinstance(value, dict) else {}

    def save_state(self, state: dict[str, Any]) -> None:
        self.ensure_dirs()
        temporary = self.state_path.with_suffix(".json.tmp")
        temporary.write_text(
            json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.replace(self.state_path)

    def read_raw(self, ticker: str) -> pd.DataFrame | None:
        path = self.download_path / f"{ticker}.parquet"
        if not path.exists():
            return None
        frame = pd.read_parquet(path)
        return _canonicalize_raw(frame)

    def write_raw(self, ticker: str, frame: pd.DataFrame) -> None:
        self.ensure_dirs()
        path = self.download_path / f"{ticker}.parquet"
        temporary = path.with_suffix(".tmp.parquet")
        frame.to_parquet(temporary, index=True)
        temporary.replace(path)

    def tickers_with_files(self) -> list[str]:
        return sorted(path.stem for path in self.download_path.glob("*.parquet"))

    def read_all(self) -> FetchResult:
        return {
            ticker: frame
            for ticker in self.tickers_with_files()
            if not (frame := self.read_raw(ticker)).empty
        }

    def write_manifest(
        self,
        *,
        entries: tuple[YahooUniverseEntry, ...],
        state: dict[str, Any],
        start: str | None,
        end: str | None,
    ) -> None:
        self.ensure_dirs()
        exchange_by_ticker = {entry.ticker: entry.exchange for entry in entries}
        symbols = state.get("symbols", {}) if isinstance(state.get("symbols"), dict) else {}
        manifest: dict[str, Any] = {
            "source_id": SOURCE_ID,
            "source_protocol": SOURCE_PROTOCOL,
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "requested_start": start,
            "requested_end": end,
            "capability_ceiling": CAPABILITY_CEILING,
            "point_in_time_readiness": PIT_READINESS,
            "ingestion_mode": "RESUMABLE_INCREMENTAL_RAW_STORE",
            "tickers": {
                ticker: {
                    "exchange": exchange_by_ticker.get(ticker)
                    or symbols.get(ticker, {}).get("exchange"),
                    **{key: value for key, value in info.items() if key != "updated_at"},
                }
                for ticker, info in sorted(symbols.items())
                if isinstance(info, dict)
            },
        }
        temporary = self.download_path / "_manifest.json.tmp"
        temporary.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        temporary.replace(self.download_path / "_manifest.json")


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
    requests_per_minute: int | None = DEFAULT_REQUESTS_PER_MINUTE,
) -> dict[str, Any]:
    """Resumably ingest a configured ticker universe into the shared layout.

    Layout produced under ``root``::

        processed/daily_panel/trade_year=YYYY/part-0000.parquet
        processed/daily_panel/_metadata.json
        catalog/data_quality.json
        catalog/daily_catalog.csv
        data/downloads/yahoo_eod/<TICKER>.parquet   (accumulating raw store)
        data/downloads/yahoo_eod/_manifest.json
        data/state/yahoo_eod.json                   (resume state)

    Re-running against an existing workspace only fetches each symbol's missing
    date range and then regenerates the processed panel from the complete raw
    store, so prior history is preserved and accumulated - never replaced.
    Returns a summary dict; raises ``RuntimeError`` when the raw store holds no
    data at all after the run.
    """
    resolved_root = root.expanduser().resolve()
    entries = parse_yahoo_universe(universe)
    fetch = fetcher or default_yahoo_fetcher
    store = YahooRawStore(resolved_root)
    state = store.load_state()
    symbols_state = _dict_section(state, "symbols")
    pacer = RequestPacer(requests_per_minute)

    plans, up_to_date = _plan_fetch_windows(entries, symbols_state, start=start, end=end)
    fetched, failures, no_new_rows = _fetch_planned(
        fetch,
        plans,
        covered=set(symbols_state),
        retries=retries,
        backoff=retry_backoff_seconds,
        workers=workers,
        pacer=pacer,
    )
    up_to_date = up_to_date | no_new_rows

    rows_added = 0
    merged_tickers: list[str] = []
    for ticker in sorted(plans):
        new_frame = fetched.get(ticker)
        if new_frame is None or new_frame.empty:
            continue
        existing = store.read_raw(ticker)
        combined = _merge_raw(existing, new_frame)
        if combined.empty:
            continue
        rows_added += max(0, len(combined) - (len(existing) if existing is not None else 0))
        store.write_raw(ticker, combined)
        merged_tickers.append(ticker)

    _update_state(state, entries, store, merged_tickers, fetched, failures, up_to_date)
    store.save_state(state)
    store.write_manifest(entries=entries, state=state, start=start, end=end)

    raw_frames = store.read_all()
    if not raw_frames:
        detail = ", ".join(sorted(failures)) or "no data returned"
        raise RuntimeError(f"Yahoo ingestion produced no data for any ticker: {detail}")
    normalized: FetchResult = {}
    dropped_vendor_rows = 0
    for ticker, raw_frame in raw_frames.items():
        usable = _usable_raw(raw_frame)
        if usable is None:
            continue
        clean = _normalize_ticker(usable)
        dropped_vendor_rows += max(0, len(usable) - len(clean))
        if not clean.empty:
            normalized[ticker] = clean
    if not normalized:
        raise RuntimeError("Yahoo raw store contains no usable rows after normalization")
    panel_frame = _build_panel_frame(normalized)
    summary = _write_workspace(
        resolved_root,
        panel_frame,
        entries,
        state=state,
        start=start,
        end=end,
        rows_added=rows_added,
        up_to_date=up_to_date,
        dropped_vendor_rows=dropped_vendor_rows,
    )
    return {
        "source_id": SOURCE_ID,
        "ok": not failures,
        "capability_ceiling": CAPABILITY_CEILING,
        "point_in_time_ready": False,
        "root": str(resolved_root),
        "mode": "incremental" if len(symbols_state) else "fresh",
        "rows": summary["rows"],
        "symbols": summary["symbols"],
        "first_trade_date": summary["first_trade_date"],
        "last_trade_date": summary["last_trade_date"],
        "tickers_ingested": merged_tickers,
        "tickers_up_to_date": sorted(up_to_date),
        "failed_tickers": dict(sorted(failures.items())),
    }


def _plan_fetch_windows(
    entries: tuple[YahooUniverseEntry, ...],
    symbols_state: dict[str, Any],
    *,
    start: str | None,
    end: str | None,
) -> tuple[dict[str, tuple[str | None, str | None]], set[str]]:
    """Compute per-symbol fetch windows from durable state.

    Returns ``(plans, up_to_date)`` where plans map ticker to
    ``(window_start, window_end)``. Symbols whose stored coverage already
    reaches the requested ``end`` are skipped entirely and reported as
    up to date; covered symbols otherwise resume the day after their last
    stored session so fetched history is never refetched or rewritten.
    """
    plans: dict[str, tuple[str | None, str | None]] = {}
    up_to_date: set[str] = set()
    for entry in entries:
        info = symbols_state.get(entry.ticker)
        info = info if isinstance(info, dict) else {}
        last_date = info.get("last_date")
        if last_date:
            # Fetcher windows are end-exclusive, so an end of last_date + one
            # day is already fully covered by stored history.
            if end and pd.Timestamp(end) <= pd.Timestamp(last_date) + pd.Timedelta(days=1):
                up_to_date.add(entry.ticker)
                continue
            next_day = (pd.Timestamp(last_date) + pd.Timedelta(days=1)).date().isoformat()
            plans[entry.ticker] = (next_day, end)
        else:
            plans[entry.ticker] = (start, end)
    return plans, up_to_date


def _fetch_planned(
    fetch: Fetcher,
    plans: dict[str, tuple[str | None, str | None]],
    *,
    covered: set[str],
    retries: int,
    backoff: float,
    workers: int,
    pacer: RequestPacer,
) -> tuple[FetchResult, dict[str, str], set[str]]:
    """Fetch every planned window; per-ticker failures are reported, not raised.

    Returns ``(fetched, failures, no_new_rows)``. ``no_new_rows`` holds covered
    tickers whose fetch returned nothing newer - not a failure, simply no new
    sessions beyond stored coverage.
    """
    results: FetchResult = {}
    failures: dict[str, str] = {}
    no_new_rows: set[str] = set()
    groups: dict[tuple[str | None, str | None], list[str]] = {}
    for ticker, window in plans.items():
        groups.setdefault(window, []).append(ticker)

    def request(window: tuple[str | None, str | None], tickers: tuple[str, ...]) -> FetchResult:
        pacer.wait()
        return fetch(tickers, window[0], window[1])

    for window, tickers in sorted(groups.items(), key=lambda item: str(item)):
        remaining = list(tickers)
        for attempt in range(retries + 1):
            try:
                batch = request(window, tuple(remaining))
                break
            except Exception as error:  # noqa: BLE001 - vendor exceptions are untyped
                if attempt == retries:
                    for ticker in remaining:
                        failures[ticker] = f"{type(error).__name__}: {error}"
                    remaining = []
                else:
                    time.sleep(backoff * (attempt + 1))
        else:
            continue
        # Vendors can silently return all-NaN frames for some tickers in a
        # batch; treat those as missing and retry them per symbol below.
        for ticker in list(remaining):
            frame = batch.get(ticker)
            usable = _usable_raw(frame) if frame is not None else None
            if usable is None:
                batch.pop(ticker, None)
            else:
                batch[ticker] = usable
        results.update(batch)
        remaining = [ticker for ticker in remaining if ticker not in batch]
    missing = [ticker for ticker in plans if ticker not in results and ticker not in failures]
    if missing:
        with ThreadPoolExecutor(max_workers=max(1, min(workers, len(missing)))) as pool:
            outcomes = list(
                pool.map(
                    lambda t: _fetch_one(
                        fetch, t, plans[t][0], plans[t][1], retries, backoff, pacer
                    ),
                    missing,
                )
            )
        for ticker, outcome in zip(missing, outcomes, strict=True):
            if isinstance(outcome, pd.DataFrame):
                results[ticker] = outcome
            elif ticker in covered and outcome == "no data returned for ticker":
                no_new_rows.add(ticker)  # covered ticker has no newer session yet
            else:
                failures[ticker] = outcome
    # An unusable response for an already-covered ticker means nothing new;
    # for a never-covered ticker it is indistinguishable from a typo or delist.
    for ticker in plans:
        if ticker in results and ticker not in failures:
            usable = _usable_raw(results[ticker])
            if usable is None:
                results.pop(ticker)
                if ticker in covered:
                    no_new_rows.add(ticker)
                else:
                    failures[ticker] = "no data returned for ticker"
            else:
                results[ticker] = usable
    return (
        {ticker: frame for ticker, frame in results.items() if ticker in plans},
        failures,
        no_new_rows,
    )


def _fetch_one(
    fetch: Fetcher,
    ticker: str,
    start: str | None,
    end: str | None,
    retries: int,
    backoff: float,
    pacer: RequestPacer,
) -> pd.DataFrame | str:
    last: Exception | None = None
    for attempt in range(retries + 1):
        try:
            pacer.wait()
            frames = fetch((ticker,), start, end)
            frame = frames.get(ticker)
            usable = _usable_raw(frame) if frame is not None else None
            if usable is not None:
                return usable
            return "no data returned for ticker"
        except Exception as error:  # noqa: BLE001 - vendor exceptions are untyped
            last = error
            if attempt < retries:
                time.sleep(backoff * (attempt + 1))
    return f"{type(last).__name__}: {last}"


def _merge_raw(existing: pd.DataFrame | None, new_frame: pd.DataFrame) -> pd.DataFrame:
    """Merge a fresh pull into stored raw history without dropping any row."""
    incoming = _usable_raw(new_frame)
    if incoming is None:
        return existing if existing is not None else _canonicalize_raw(new_frame).iloc[0:0]
    if existing is None or existing.empty:
        return incoming
    combined = pd.concat([existing, incoming])
    combined = combined[~combined.index.duplicated(keep="last")]
    return combined.sort_index()


def _canonicalize_raw(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize one raw pull to the store's vendor-shaped column contract."""
    data = frame.copy()
    data.index = pd.to_datetime(data.index, errors="coerce")
    data = data[~data.index.isna()]
    data = data.sort_index()
    missing = [column for column in RAW_COLUMNS if column not in data.columns]
    for column in missing:
        data[column] = float("nan")
    return data[list(RAW_COLUMNS)]


def _usable_raw(frame: pd.DataFrame) -> pd.DataFrame | None:
    """Return the frame's usable rows, or ``None`` when the pull is empty.

    Vendors can silently return all-NaN frames (rate limits, partial batch
    failures). Such pulls must never reach the raw store or advance resume
    coverage, so they are treated as missing data instead.
    """
    data = _canonicalize_raw(frame).dropna(
        subset=["open", "high", "low", "close", "volume"], how="any"
    )
    return data if not data.empty else None


def _update_state(
    state: dict[str, Any],
    entries: tuple[YahooUniverseEntry, ...],
    store: YahooRawStore,
    merged_tickers: list[str],
    fetched: FetchResult,
    failures: dict[str, str],
    up_to_date: set[str],
) -> None:
    now = datetime.now(UTC).isoformat()
    exchange_by_ticker = {entry.ticker: entry.exchange for entry in entries}
    symbols_state = state.setdefault("symbols", {})
    failed_state = state.setdefault("failed", {})
    state.update(
        {
            "source_id": SOURCE_ID,
            "source_protocol": SOURCE_PROTOCOL,
            "updated_at": now,
        }
    )
    for ticker in merged_tickers:
        frame = fetched[ticker]
        raw_previous = symbols_state.get(ticker)
        previous = raw_previous if isinstance(raw_previous, dict) else {}
        first_date = previous.get("first_date") or frame.index.min().date().isoformat()
        symbols_state[ticker] = {
            "exchange": exchange_by_ticker.get(ticker) or previous.get("exchange"),
            "first_date": min(str(first_date), frame.index.min().date().isoformat()),
            "last_date": frame.index.max().date().isoformat(),
            "updated_at": now,
        }
        failed_state.pop(ticker, None)
    for ticker, message in failures.items():
        failed_state[ticker] = {"error": message, "updated_at": now}
    for ticker in up_to_date:
        info = symbols_state.get(ticker)
        if isinstance(info, dict):
            info["confirmed_up_to_date_at"] = now


def _normalize_ticker(frame: pd.DataFrame) -> pd.DataFrame:
    """Clean one ticker's raw frame and derive the shared panel columns.

    Rows with missing prices or impossible OHLC geometry are excluded from the
    panel as unusable vendor anomalies; they remain in the raw store and are
    counted in the quality report.
    """
    data = _canonicalize_raw(frame)
    data = data.dropna(subset=["open", "high", "low", "close", "volume"], how="any")
    data = data[data["volume"] >= 0]
    valid_geometry = (
        (data["open"] > 0)
        & (data["high"] > 0)
        & (data["low"] > 0)
        & (data["close"] > 0)
        & (data["high"] >= data[["open", "close", "low"]].max(axis=1))
        & (data["low"] <= data[["open", "close", "high"]].min(axis=1))
    )
    data = data[valid_geometry]
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
        is_valid_ohlc=True,
        has_activity=data["volume"].astype("float64") > 0,
    )
    data["amount"] = close * data["volume"].astype("float64")
    data["is_tradable_observation"] = data["has_activity"]
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
    entries: tuple[YahooUniverseEntry, ...],
    *,
    state: dict[str, Any],
    start: str | None,
    end: str | None,
    rows_added: int,
    up_to_date: set[str],
    dropped_vendor_rows: int,
) -> dict[str, Any]:
    """Regenerate the derived processed panel from the complete raw store.

    The panel is a pure function of the accumulating raw store, so this atomic
    swap cannot silently destroy ingested history: everything it contains is
    re-derivable from ``data/downloads/yahoo_eod/``.
    """
    panel_path = _panel_path(root)
    catalog_path = root / "catalog"
    staging = panel_path.with_name(f".{panel_path.name}.staging")
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)
    catalog_path.mkdir(parents=True, exist_ok=True)
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
            "ingestion_mode": "RESUMABLE_INCREMENTAL_RAW_STORE",
            "resume_state_path": "data/state/yahoo_eod.json",
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
            "covered_symbols": sorted(
                ticker
                for ticker, info in _dict_section(state, "symbols").items()
                if isinstance(info, dict)
            ),
            "last_run_rows_added": rows_added,
            "last_run_up_to_date": sorted(up_to_date),
            "unusable_vendor_rows_dropped": dropped_vendor_rows,
            "requested_start": start,
            "requested_end": end,
            **summary,
        }
        (staging / "_metadata.json").write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        _write_quality_report(
            catalog_path, panel, summary, dropped_vendor_rows=dropped_vendor_rows
        )
        _write_catalog(catalog_path, panel)
        _atomic_replace(staging, panel_path)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return summary


def _write_quality_report(
    catalog_path: Path,
    panel: pd.DataFrame,
    summary: dict[str, Any],
    *,
    dropped_vendor_rows: int,
) -> None:
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
    # Impossible-OHLC vendor rows never reach the panel (they are excluded at
    # normalization and preserved in the raw store); the count is surfaced
    # informationally without blocking research readiness.
    informational = {
        "unusable_vendor_rows_dropped_from_panel": dropped_vendor_rows,
    }
    report = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "source": "data/downloads/yahoo_eod",
        "source_id": SOURCE_ID,
        "source_protocol": SOURCE_PROTOCOL,
        "summary": summary,
        "checks": checks,
        "informational": informational,
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


def _dict_section(state: dict[str, Any], key: str) -> dict[str, Any]:
    value = state.get(key)
    return value if isinstance(value, dict) else {}


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
