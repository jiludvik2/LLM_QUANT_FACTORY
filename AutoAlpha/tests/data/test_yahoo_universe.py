from __future__ import annotations

import pytest

from autoalpha.data.sources.yahoo_source import parse_yahoo_universe


def test_plain_and_scoped_entries() -> None:
    entries = parse_yahoo_universe(["AAPL", "lse:SHEL.L", "EURONEXT:ASML.AS"])
    assert [(entry.ticker, entry.exchange) for entry in entries] == [
        ("AAPL", None),
        ("ASML.AS", "EURONEXT"),
        ("SHEL.L", "LSE"),
    ]


def test_duplicate_ticker_keeps_first_declaration() -> None:
    entries = parse_yahoo_universe(["US:MSFT", "MSFT"])
    assert len(entries) == 1
    assert entries[0].exchange == "US"


def test_ticker_case_is_preserved() -> None:
    entries = parse_yahoo_universe(["BRK-B"])
    assert entries[0].ticker == "BRK-B"


@pytest.mark.parametrize("bad", ["", "  ", "LSE:", ":AAPL", "BAD TICKER"])
def test_invalid_entries_rejected(bad: str) -> None:
    with pytest.raises(ValueError):
        parse_yahoo_universe([bad])


@pytest.mark.parametrize(
    "bad", ["../etc/passwd", "AAPL/../MSFT", "a/b", "a\\b", "..", "LSE:../AAPL"]
)
def test_path_traversal_tickers_rejected(bad: str) -> None:
    with pytest.raises(ValueError):
        parse_yahoo_universe([bad])


def test_empty_universe_rejected() -> None:
    with pytest.raises(ValueError, match="empty"):
        parse_yahoo_universe([])
