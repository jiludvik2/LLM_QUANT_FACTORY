from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from fixtures_yahoo import RecordingFetcher, yahoo_fixture_fetcher, yahoo_fixture_frames

from autoalpha.data.current_panel import inspect_current_panel
from autoalpha.data.sources.yahoo_source import (
    CAPABILITY_CEILING,
    PIT_READINESS,
    RAW_COLUMNS,
    SOURCE_PROTOCOL,
    parse_yahoo_universe,
    run_yahoo_ingestion,
)
from autoalpha.data.workspace import inspect_data_workspace

UNPACED = {"requests_per_minute": 0}


@pytest.fixture
def ingestion_root(tmp_path: Path) -> Path:
    root = tmp_path / "yahoo-workspace"
    result = run_yahoo_ingestion(
        root=root,
        universe=["AAPL", "LSE:SHEL.L", "ASML.AS"],
        start="2024-01-01",
        end="2024-12-31",
        fetcher=yahoo_fixture_fetcher(),
        **UNPACED,
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
    assert manifest["ingestion_mode"] == "RESUMABLE_INCREMENTAL_RAW_STORE"


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
    assert metadata["ingestion_mode"] == "RESUMABLE_INCREMENTAL_RAW_STORE"
    assert metadata["data_capability_ceiling"] == CAPABILITY_CEILING == "RESEARCH_READY"
    assert metadata["point_in_time_readiness"] == PIT_READINESS
    assert metadata["capital_ledger_ready"] is False
    assert metadata["capital_ledger_proxy_ready"] is False
    assert metadata["amount_unit"] == "approximated_local_currency_notional"
    assert metadata["volume_unit"] == "shares"
    assert metadata["currency_policy"].startswith("MIXED_LOCAL_CURRENCY")
    assert metadata["known_limitations"]
    assert all(isinstance(note, str) for note in metadata["known_limitations"])


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
        **UNPACED,
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
        **UNPACED,
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
            **UNPACED,
        )


def test_all_nan_batch_pull_is_retried_per_symbol(tmp_path: Path) -> None:
    """Vendors can silently return all-NaN frames in batch pulls; the adapter
    must retry those tickers per symbol instead of storing unusable rows."""
    root = tmp_path / "nan-batch"
    good = yahoo_fixture_fetcher(periods=10)

    def nan_for_shel_batch(tickers, start, end):  # noqa: ANN001
        frames = good(tickers, start, end)
        if len(tickers) > 1 and "SHEL.L" in frames:
            shel = frames["SHEL.L"]
            frames["SHEL.L"] = shel.assign(
                **{column: float("nan") for column in shel.columns}
            )
        return frames

    def per_symbol_ok(tickers, start, end):  # noqa: ANN001
        return good(tickers, start, end)

    def fetcher(tickers, start, end):  # noqa: ANN001
        return nan_for_shel_batch(tickers, start, end) if len(tickers) > 1 else per_symbol_ok(
            tickers, start, end
        )

    result = run_yahoo_ingestion(
        root=root,
        universe=["AAPL", "LSE:SHEL.L", "MISSING.TICKER"],
        end="2024-12-31",
        fetcher=fetcher,
        **UNPACED,
    )
    # SHEL.L was recovered through the per-symbol retry path.
    assert "SHEL.L" not in result["failed_tickers"]
    report = inspect_data_workspace(root)
    assert report.symbols == 2
    shel = pd.read_parquet(root / "data" / "downloads" / "yahoo_eod" / "SHEL.L.parquet")
    assert shel["close"].notna().all()


def test_all_nan_pull_never_advances_resume_coverage(tmp_path: Path) -> None:
    root = tmp_path / "nan-coverage"

    def all_nan(tickers, start, end):  # noqa: ANN001
        frame = yahoo_fixture_frames(10)["AAPL"].assign(
            **{column: float("nan") for column in (*RAW_COLUMNS,)}
        )
        return {"AAPL": frame} if "AAPL" in tickers else {}

    with pytest.raises(RuntimeError, match="no data"):
        run_yahoo_ingestion(
            root=root,
            universe=["AAPL"],
            end="2024-12-31",
            fetcher=all_nan,
            retries=0,
            **UNPACED,
        )
    state = json.loads((root / "data" / "state" / "yahoo_eod.json").read_text(encoding="utf-8"))
    assert "AAPL" not in state["symbols"]
    assert "AAPL" in state["failed"]
    assert not (root / "processed" / "daily_panel").exists()


def test_impossible_ohlc_row_is_excluded_from_panel_but_kept_in_raw_store(
    tmp_path: Path,
) -> None:
    base = yahoo_fixture_fetcher(periods=10)

    def anomalous(tickers, start, end):  # noqa: ANN001
        frames = base(tickers, start, end)
        frame = frames.get("AAPL")
        if frame is not None:
            # Vendor glitch: high below open is impossible geometry.
            broken = frame.copy()
            broken.iloc[3, broken.columns.get_loc("high")] = broken.iloc[3]["low"] - 1.0
            frames["AAPL"] = broken
        return frames

    root = tmp_path / "anomaly"
    result = run_yahoo_ingestion(
        root=root,
        universe=["AAPL"],
        fetcher=anomalous,
        **UNPACED,
    )
    assert result["ok"]
    panel = pd.read_parquet(root / "processed" / "daily_panel")
    assert len(panel) == 9  # the impossible row never reaches the panel
    raw = pd.read_parquet(root / "data" / "downloads" / "yahoo_eod" / "AAPL.parquet")
    assert len(raw) == 10  # the original row is preserved in the raw store
    quality = json.loads((root / "catalog" / "data_quality.json").read_text(encoding="utf-8"))
    assert quality["passed"] is True
    assert quality["informational"]["unusable_vendor_rows_dropped_from_panel"] == 1
    metadata = json.loads(
        (root / "processed" / "daily_panel" / "_metadata.json").read_text(encoding="utf-8")
    )
    assert metadata["unusable_vendor_rows_dropped"] == 1
    report = inspect_data_workspace(root)
    assert report.quality_passed is True and report.price_research_ready is True


def test_universe_roundtrip_into_metadata(tmp_path: Path) -> None:
    root = tmp_path / "universe-meta"
    universe = ["AAPL", "LSE:SHEL.L"]
    run_yahoo_ingestion(
        root=root, universe=universe, fetcher=yahoo_fixture_fetcher(), **UNPACED
    )
    metadata = json.loads(
        (root / "processed" / "daily_panel" / "_metadata.json").read_text(encoding="utf-8")
    )
    expected = [entry.to_dict() for entry in parse_yahoo_universe(universe)]
    assert metadata["universe"] == expected


def test_rerun_fetches_only_missing_range_and_preserves_history(tmp_path: Path) -> None:
    root = tmp_path / "resumable"
    first = run_yahoo_ingestion(
        root=root,
        universe=["AAPL"],
        end="2024-12-31",
        fetcher=yahoo_fixture_fetcher(periods=10, start="2024-01-02"),
        **UNPACED,
    )
    assert first["mode"] == "fresh"
    assert first["rows"] == 10

    # Sessions continuing exactly where the first pull's coverage ended.
    later = yahoo_fixture_fetcher(periods=10, start="2024-01-16")
    recorder = RecordingFetcher(later)
    second = run_yahoo_ingestion(
        root=root,
        universe=["AAPL"],
        end="2024-12-31",
        fetcher=recorder,
        **UNPACED,
    )
    assert second["mode"] == "incremental"
    assert second["ok"]
    # Only the missing window was requested: the day after stored coverage.
    assert recorder.requested_starts == {"2024-01-16"}
    assert second["rows"] == 20
    assert second["tickers_ingested"] == ["AAPL"]

    frame = pd.read_parquet(root / "processed" / "daily_panel")
    aapl = frame[frame["ts_code"] == "AAPL"].sort_values("trade_date")
    # History from the first run is preserved, not rebuilt or replaced.
    assert len(aapl) == 20
    assert aapl["trade_date"].min() == pd.Timestamp("2024-01-02")
    assert aapl["trade_date"].max() == pd.Timestamp("2024-01-29")
    assert not aapl["trade_date"].duplicated().any()


def test_state_file_tracks_coverage_for_resume(tmp_path: Path) -> None:
    root = tmp_path / "stateful"
    run_yahoo_ingestion(
        root=root,
        universe=["AAPL"],
        end="2024-12-31",
        fetcher=yahoo_fixture_fetcher(periods=10),
        **UNPACED,
    )
    state = json.loads((root / "data" / "state" / "yahoo_eod.json").read_text(encoding="utf-8"))
    assert state["source_id"] == "yahoo"
    assert state["symbols"]["AAPL"]["first_date"] == "2024-01-02"
    assert state["symbols"]["AAPL"]["last_date"] == "2024-01-15"


def test_orphaned_temp_download_does_not_leak_into_panel_or_state(tmp_path: Path) -> None:
    root = tmp_path / "crash-recovery"
    download_dir = root / "data" / "downloads" / "yahoo_eod"
    download_dir.mkdir(parents=True)
    # Simulate a crash between the temp-file write and the atomic rename in
    # YahooRawStore.write_raw: a stray temp file left in the download store.
    (download_dir / "AAPL.parquet.tmp").write_bytes(b"not a real parquet file")

    result = run_yahoo_ingestion(
        root=root,
        universe=["AAPL"],
        end="2024-12-31",
        fetcher=yahoo_fixture_fetcher(periods=5),
        **UNPACED,
    )
    assert result["ok"], result["failed_tickers"]

    # The orphaned temp file must not survive as a phantom ticker anywhere.
    assert not (download_dir / "AAPL.parquet.tmp").exists()
    frame = pd.read_parquet(root / "processed" / "daily_panel")
    assert set(frame["ts_code"]) == {"AAPL"}
    state = json.loads((root / "data" / "state" / "yahoo_eod.json").read_text(encoding="utf-8"))
    assert set(state["symbols"]) == {"AAPL"}
    assert state["failed"] == {}


def test_fully_covered_rerun_skips_network_and_refreshes_panel(tmp_path: Path) -> None:
    root = tmp_path / "covered"
    run_yahoo_ingestion(
        root=root,
        universe=["AAPL", "LSE:SHEL.L"],
        end="2024-01-16",
        fetcher=yahoo_fixture_fetcher(periods=10),
        **UNPACED,
    )
    recorder = RecordingFetcher(yahoo_fixture_fetcher(periods=10))
    result = run_yahoo_ingestion(
        root=root,
        universe=["AAPL", "LSE:SHEL.L"],
        end="2024-01-16",
        fetcher=recorder,
        **UNPACED,
    )
    assert result["ok"]
    assert recorder.calls == []  # nothing was fetched at all
    assert set(result["tickers_up_to_date"]) == {"AAPL", "SHEL.L"}
    # The panel is regenerated from the raw store and stays intact.
    report = inspect_data_workspace(root)
    assert report.symbols == 2
    assert report.rows == 20
    assert report.quality_passed is True


def test_empty_incremental_response_is_not_a_failure(tmp_path: Path) -> None:
    root = tmp_path / "quiet"
    run_yahoo_ingestion(
        root=root,
        universe=["AAPL"],
        end="2024-12-31",
        fetcher=yahoo_fixture_fetcher(periods=10),
        **UNPACED,
    )

    def no_new_data(tickers, start, end):  # noqa: ANN001
        return {}

    result = run_yahoo_ingestion(
        root=root,
        universe=["AAPL"],
        end="2024-12-31",
        fetcher=no_new_data,
        **UNPACED,
    )
    assert result["ok"], result["failed_tickers"]
    assert result["tickers_up_to_date"] == ["AAPL"]
    frame = pd.read_parquet(root / "processed" / "daily_panel")
    assert len(frame[frame["ts_code"] == "AAPL"]) == 10


def test_failed_symbol_is_recorded_in_state_and_cleared_on_success(tmp_path: Path) -> None:
    root = tmp_path / "flaky"
    first = run_yahoo_ingestion(
        root=root,
        universe=["AAPL", "MISSING.TICKER"],
        fetcher=yahoo_fixture_fetcher(),
        **UNPACED,
    )
    assert first["ok"] is False
    state = json.loads((root / "data" / "state" / "yahoo_eod.json").read_text(encoding="utf-8"))
    assert "MISSING.TICKER" in state["failed"]

    second = run_yahoo_ingestion(
        root=root,
        universe=["AAPL", "MISSING.TICKER"],
        fetcher=yahoo_fixture_fetcher(),
        **UNPACED,
    )
    assert second["ok"] is False  # still missing from the fixture
    assert "MISSING.TICKER" in second["failed_tickers"]
    # AAPL was up to date on the second run and stays healthy in the panel.
    assert "AAPL" in second["tickers_up_to_date"]
    report = inspect_data_workspace(root)
    assert report.symbols == 1
