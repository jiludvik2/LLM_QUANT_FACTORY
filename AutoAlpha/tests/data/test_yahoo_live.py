"""Opt-in live-network smoke test for the Yahoo Finance adapter.

Skipped by default so CI never touches the network. Run locally with:

    AUTOALPHA_YAHOO_LIVE_TEST=1 uv run pytest -q tests/data/test_yahoo_live.py
"""

from __future__ import annotations

import os

import pytest

from autoalpha.data.current_panel import inspect_current_panel
from autoalpha.data.sources.yahoo_source import run_yahoo_ingestion
from autoalpha.data.workspace import inspect_data_workspace

pytestmark = pytest.mark.skipif(
    os.environ.get("AUTOALPHA_YAHOO_LIVE_TEST", "").casefold() != "1",
    reason="live Yahoo Finance network test; enable with AUTOALPHA_YAHOO_LIVE_TEST=1",
)

SAMPLE_UNIVERSE = (
    "AAPL",
    "MSFT",
    "XOM",
    "KO",
    "JPM",
    "LSE:SHEL.L",
    "LSE:AZN.L",
    "LSE:HSBA.L",
    "LSE:BP.L",
    "LSE:ULVR.L",
    "ASML.AS",
    "SAP.DE",
    "MC.PA",
    "NESN.SW",
    "ADYEN.AS",
)


def test_sample_universe_ingestion(tmp_path) -> None:  # type: ignore[no-untyped-def]
    root = tmp_path / "yahoo-live"
    result = run_yahoo_ingestion(
        root=root,
        universe=SAMPLE_UNIVERSE,
        start="2024-01-01",
        end="2024-03-31",
    )
    assert result["symbols"] >= 10
    readiness = inspect_current_panel(root / "processed" / "daily_panel")
    assert readiness.price_research_ready is True
    assert readiness.institutional_pit_ready is False
    report = inspect_data_workspace(root)
    assert report.price_research_ready is True
    assert report.institutional_pit_ready is False
