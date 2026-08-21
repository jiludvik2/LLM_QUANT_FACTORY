from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from fixtures_yahoo import yahoo_fixture_fetcher

from autoalpha.data.current_panel import inspect_current_panel
from autoalpha.data.sources.yahoo_source import (
    CAPABILITY_CEILING,
    PIT_READINESS,
    SOURCE_PROTOCOL,
    parse_yahoo_universe,
    run_yahoo_ingestion,
)
from autoalpha.data.workspace import inspect_data_workspace


@pytest.fixture
def ingestion_root(tmp_path: Path) -> Path:
    root = tmp_path / "yahoo-workspace"
    result = run_yahoo_ingestion(
        root=root,
        universe=["AAPL", "LSE:SHEL.L", "ASML.AS"],
        start="2024-01-01",
        end="2024-12-31",
        fetcher=yahoo_fixture_fetcher(),
    )
    assert result["ok"], result["failed_tickers"]
    return root


def test_ingestion_builds_shared_panel_layout(ingestion_root: Path) -> None:
    panel = ingestion_root / "processed" / "daily_panel"
    partitions = sorted(path.name for path in panel.glob("trade_year=*"))
    assert partitions == ["trade_year=2024"]
    assert (panel / "_metadata.json").is_file()
    assert (ingestion_root / "catalog" / "data_quality.json").is_file()
    assert (ingestion_root / "catalog" / "daily_catalog.csv").is_file()
    raw = ingestion_root / "data" / "downloads" / "yahoo_eod"
    assert {path.stem for path in raw.glob("*.parquet")} == {"AAPL", "SHEL.L", "ASML.AS"}
    manifest = json.loads((raw / "_manifest.json").read_text(encoding="utf-8"))
    assert manifest["tickers"]["SHEL.L"]["exchange"] == "LSE"


def test_panel_loads_through_workspace_tooling(ingestion_root: Path) -> None:
    readiness = inspect_current_panel(ingestion_root / "processed" / "daily_panel")
    assert readiness.price_research_ready is True
    # Honest capability labeling: research-level only, never institutional PIT.
    assert readiness.institutional_pit_ready is False
    assert readiness.blockers

    report = inspect_data_workspace(ingestion_root)
    assert report.quality_passed is True
    assert report.source_integrity_passed is True
    assert report.price_research_ready is True
    assert report.institutional_pit_ready is False
    assert report.symbols == 3
    assert report.first_trade_date == "2024-01-02"
    for base_field in ("open", "high", "low", "close", "adj_close", "vol", "amount"):
        assert base_field in report.factor_fields


def test_capability_matrix_reports_research_level_for_yahoo_workspace(
    ingestion_root: Path,
) -> None:
    from autoalpha.data.execution_basis import inspect_execution_data_basis
    from autoalpha.service.data_center import build_data_capability_matrix

    report = inspect_data_workspace(ingestion_root)
    basis = inspect_execution_data_basis(Path(report.panel_path))
    # Proxy/ledger execution stays blocked on approximated mixed-currency amounts.
    assert basis.capital_ledger_ready is False
    assert basis.capital_ledger_proxy_ready is False
    matrix = build_data_capability_matrix(
        workspace=report.to_dict(), execution_basis=basis.to_dict()
    )
    summary = matrix["summary"]
    assert summary["research_ready"] is True
    assert summary["strict_pit_ready"] is False
    assert summary["production_allowed"] is False
    assert summary["levels"].get("RESEARCH_READY", 0) >= 2
    assert summary["levels"].get("PRODUCTION_BLOCKED", 0) >= 1


def test_metadata_declares_honest_capability_ceiling(ingestion_root: Path) -> None:
    metadata = json.loads(
        (ingestion_root / "processed" / "daily_panel" / "_metadata.json").read_text(
            encoding="utf-8"
        )
    )
    assert metadata["source_protocol"] == SOURCE_PROTOCOL
    assert metadata["data_capability_ceiling"] == CAPABILITY_CEILING == "RESEARCH_READY"
    assert metadata["point_in_time_readiness"] == PIT_READINESS
    assert metadata["capital_ledger_ready"] is False
    assert metadata["capital_ledger_proxy_ready"] is False
    assert metadata["amount_unit"] == "approximated_local_currency_notional"
    assert metadata["volume_unit"] == "shares"
    assert metadata["currency_policy"].startswith("MIXED_LOCAL_CURRENCY")
    assert metadata["known_limitations"]


def test_panel_column_semantics(ingestion_root: Path) -> None:
    frame = pd.read_parquet(ingestion_root / "processed" / "daily_panel")
    aapl = frame[frame["ts_code"] == "AAPL"].sort_values("trade_date").reset_index(drop=True)
    # First session per ticker has no previous close.
    assert pd.isna(aapl.loc[0, "pre_close"])
    assert pd.notna(aapl.loc[1, "pre_close"])
    derived_pct = (aapl["close"] / aapl["pre_close"] - 1.0) * 100.0
    assert (derived_pct.dropna() - aapl["pct_chg"].dropna()).abs().max() < 1e-9
    # amount is the documented close * shares approximation.
    assert (aapl["amount"] - aapl["close"] * aapl["vol"]).abs().max() < 1e-6
    assert aapl["is_tradable_observation"].dtype == bool
    assert bool(aapl["is_tradable_observation"].all())
    # Unadjusted prices plus an adjusted close, matching the shared panel split.
    assert (aapl["adj_close"] != aapl["close"]).all()


def test_rows_with_missing_prices_are_dropped(tmp_path: Path) -> None:
    base_fetcher = yahoo_fixture_fetcher()
    gap_start = pd.Timestamp("2024-01-02")

    def gappy_fetcher(tickers, start, end):  # noqa: ANN001
        frames = base_fetcher(tickers, start, end)
        frame = frames.get("AAPL")
        if frame is None:
            return frames
        broken = frame.copy()
        broken.loc[gap_start, "close"] = float("nan")
        frames["AAPL"] = broken
        return frames

    root = tmp_path / "gappy"
    result = run_yahoo_ingestion(
        root=root,
        universe=["AAPL"],
        fetcher=gappy_fetcher,
    )
    assert result["ok"]
    frame = pd.read_parquet(root / "processed" / "daily_panel")
    assert gap_start not in set(frame["trade_date"])
    assert len(frame[frame["ts_code"] == "AAPL"]) == 9


def test_partial_universe_failure_is_reported_but_panel_is_written(tmp_path: Path) -> None:
    root = tmp_path / "partial"
    result = run_yahoo_ingestion(
        root=root,
        universe=["AAPL", "MISSING.TICKER"],
        fetcher=yahoo_fixture_fetcher(),
    )
    assert result["ok"] is False
    assert "MISSING.TICKER" in result["failed_tickers"]
    assert result["tickers_ingested"] == ["AAPL"]
    assert inspect_current_panel(root / "processed" / "daily_panel").rows > 0


def test_total_failure_raises(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="no data"):
        run_yahoo_ingestion(
            root=tmp_path / "empty",
            universe=["MISSING.TICKER"],
            fetcher=yahoo_fixture_fetcher(),
        )


def test_rerun_without_overwrite_refuses_and_leaves_panel_intact(tmp_path: Path) -> None:
    root = tmp_path / "guarded"
    run_yahoo_ingestion(root=root, universe=["AAPL"], fetcher=yahoo_fixture_fetcher())
    original_metadata = (root / "processed" / "daily_panel" / "_metadata.json").read_text(
        encoding="utf-8"
    )
    with pytest.raises(FileExistsError, match="already exists"):
        run_yahoo_ingestion(
            root=root, universe=["AAPL", "LSE:SHEL.L"], fetcher=yahoo_fixture_fetcher()
        )
    assert inspect_current_panel(root / "processed" / "daily_panel").rows == 10
    assert (root / "processed" / "daily_panel" / "_metadata.json").read_text(
        encoding="utf-8"
    ) == original_metadata


def test_overwrite_flag_replaces_existing_panel(tmp_path: Path) -> None:
    root = tmp_path / "overwritten"
    run_yahoo_ingestion(root=root, universe=["AAPL"], fetcher=yahoo_fixture_fetcher())
    result = run_yahoo_ingestion(
        root=root,
        universe=["AAPL", "LSE:SHEL.L"],
        fetcher=yahoo_fixture_fetcher(),
        overwrite=True,
    )
    assert result["ok"]
    report = inspect_data_workspace(root)
    assert report.symbols == 2


def test_lineage_write_failure_does_not_swap_panel_in(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import autoalpha.data.sources.yahoo_source as yahoo_source

    def _boom(*args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        raise OSError("disk full")

    monkeypatch.setattr(yahoo_source, "_write_raw_frames", _boom)
    root = tmp_path / "lineage-failure"
    with pytest.raises(OSError, match="disk full"):
        yahoo_source.run_yahoo_ingestion(
            root=root, universe=["AAPL"], fetcher=yahoo_fixture_fetcher()
        )
    assert not (root / "processed" / "daily_panel").exists()


def test_universe_roundtrip_into_metadata(tmp_path: Path) -> None:
    root = tmp_path / "universe-meta"
    universe = ["AAPL", "LSE:SHEL.L"]
    run_yahoo_ingestion(root=root, universe=universe, fetcher=yahoo_fixture_fetcher())
    metadata = json.loads(
        (root / "processed" / "daily_panel" / "_metadata.json").read_text(encoding="utf-8")
    )
    expected = [entry.to_dict() for entry in parse_yahoo_universe(universe)]
    assert metadata["universe"] == expected
