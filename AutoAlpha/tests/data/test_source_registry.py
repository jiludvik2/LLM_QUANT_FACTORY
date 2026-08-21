from __future__ import annotations

import pytest

from autoalpha.data.sources import available_sources, get_source, register_source
from autoalpha.data.sources.base import (
    SourceCapabilityProfile,
    SourceDescriptor,
    UnknownDataSourceError,
)


def test_builtin_sources_are_registered() -> None:
    source_ids = {source.source_id for source in available_sources()}
    assert {"tushare", "yahoo"} <= source_ids


def test_capability_labels_reuse_platform_levels() -> None:
    allowed_levels = {
        "RESEARCH_READY",
        "PROXY_BACKTEST_READY",
        "PROXY_PAPER_READY",
        "STRICT_PIT_READY",
        "PRODUCTION_BLOCKED",
        "BLOCKED",
    }
    for source in available_sources():
        assert source.capability.capability_ceiling in allowed_levels


def test_yahoo_ceiling_is_research_only_without_pit() -> None:
    yahoo = get_source("yahoo")
    assert yahoo.capability.capability_ceiling == "RESEARCH_READY"
    assert yahoo.capability.point_in_time_ready is False
    assert yahoo.ingest is not None
    assert {"US", "GB", "EU"} <= set(yahoo.markets)


def test_tushare_is_not_pit_and_documents_external_pipeline() -> None:
    tushare = get_source("tushare")
    assert tushare.capability.point_in_time_ready is False
    assert tushare.ingest is None
    with pytest.raises(NotImplementedError):
        tushare.require_ingestion_callable()


def test_yahoo_ingestion_callable_is_resolvable() -> None:
    assert callable(get_source("yahoo").require_ingestion_callable())


def test_unknown_source_raises_typed_error() -> None:
    with pytest.raises(UnknownDataSourceError):
        get_source("eodhd")


def test_register_source_rejects_conflicting_descriptor() -> None:
    conflicting = SourceDescriptor(
        source_id="tushare",
        label="different",
        markets=("CN",),
        capability=SourceCapabilityProfile(
            capability_ceiling="RESEARCH_READY",
            point_in_time_ready=False,
            rationale="conflict probe",
        ),
        ingestion_command="noop",
    )
    with pytest.raises(ValueError, match="already registered"):
        register_source(conflicting)
