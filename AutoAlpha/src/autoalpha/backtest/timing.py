from __future__ import annotations

import pandas as pd

from autoalpha.backtest.conventions import MarketConventions, resolve_optional

EOD_NEXT_OPEN_RETURN_CONVENTION = "EOD_T__OPEN_T1_TO_OPEN_T2"

# Timing conventions this engine currently implements. Anything else must
# fail closed rather than silently reusing the CN A-share lag structure.
SUPPORTED_SIGNAL_TIMINGS = frozenset({"EOD_T"})
SUPPORTED_EXECUTION_TIMINGS = frozenset({"OPEN_T1"})


def supported_timing(conventions: MarketConventions | str | None = None) -> str:
    """Validate that ``conventions`` uses a timing structure we implement.

    Returns the canonical convention label (``EOD_NEXT_OPEN_RETURN_CONVENTION``)
    so runs can record which timing governed them. Raises ``ValueError``
    for unsupported timings instead of guessing.
    """
    resolved = (
        resolve_optional(conventions)
        if isinstance(conventions, str) or conventions is None
        else conventions
    )
    if resolved.signal_timing not in SUPPORTED_SIGNAL_TIMINGS:
        raise ValueError(
            f"unsupported signal_timing {resolved.signal_timing!r} for market "
            f"{resolved.market!r}; supported: {sorted(SUPPORTED_SIGNAL_TIMINGS)}"
        )
    if resolved.execution_timing not in SUPPORTED_EXECUTION_TIMINGS:
        raise ValueError(
            f"unsupported execution_timing {resolved.execution_timing!r} for market "
            f"{resolved.market!r}; supported: {sorted(SUPPORTED_EXECUTION_TIMINGS)}"
        )
    return EOD_NEXT_OPEN_RETURN_CONVENTION


def next_open_return_for_eod_signal(
    open_prices: pd.DataFrame,
    conventions: MarketConventions | None = None,
) -> pd.DataFrame:
    """Return earned after an EOD signal is executed at the next session open."""
    if conventions is not None:
        supported_timing(conventions)
    return open_prices.shift(-2).div(open_prices.shift(-1)).sub(1.0)


def entry_aligned_open_return(open_prices: pd.DataFrame) -> pd.DataFrame:
    """Open-to-next-open return indexed by the entry session."""
    return open_prices.pct_change(fill_method=None).shift(-1)
