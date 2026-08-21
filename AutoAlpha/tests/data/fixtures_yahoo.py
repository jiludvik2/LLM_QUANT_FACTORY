"""Recorded Yahoo-shaped fixture frames for offline source-adapter tests.

Shapes mirror what ``yfinance.download(..., auto_adjust=False,
group_by="ticker")`` yields after ``_split_yahoo_frame`` normalization: one
DataFrame per ticker indexed by session date with columns
``open/high/low/close/adj_close/volume``. Values are synthetic but structurally
faithful (LSE prices in pence, EU prices in EUR) so no network is needed.
"""

from __future__ import annotations

import pandas as pd


def _sessions(start: str, periods: int) -> pd.DatetimeIndex:
    return pd.bdate_range(start, periods=periods)


def _frame(
    index: pd.DatetimeIndex,
    *,
    base_price: float,
    drift: float = 0.0,
    volume: int = 1_000_000,
) -> pd.DataFrame:
    step = pd.Series(range(len(index)), dtype="float64", index=index)
    open_ = base_price * (1 + drift) ** step
    close = open_ * 1.01
    high = open_ * 1.02
    low = open_ * 0.99
    return pd.DataFrame(
        {
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "adj_close": close * 0.98,
            "volume": pd.Series([volume] * len(index), dtype="float64", index=index),
        },
        index=index,
    )


def yahoo_fixture_frames(periods: int = 10) -> dict[str, pd.DataFrame]:
    """Three tickers spanning US, LSE (pence), and EU (EUR) listings."""
    sessions = _sessions("2024-01-02", periods)
    return {
        "AAPL": _frame(sessions, base_price=185.0, drift=0.002),
        "SHEL.L": _frame(sessions, base_price=2850.0, drift=-0.001),
        "ASML.AS": _frame(sessions, base_price=680.0, drift=0.001),
    }


def yahoo_fixture_fetcher(periods: int = 10) -> object:
    """A fetcher callable with the production signature but no network."""

    def fetch(tickers, start, end):  # noqa: ANN001 - test double
        frames = yahoo_fixture_frames(periods)
        selected: dict[str, pd.DataFrame] = {}
        for ticker in tickers:
            if ticker in frames:
                frame = frames[ticker]
                if start:
                    frame = frame[frame.index >= pd.Timestamp(start)]
                if end:
                    frame = frame[frame.index < pd.Timestamp(end)]
                if not frame.empty:
                    selected[ticker] = frame
        return selected

    return fetch
