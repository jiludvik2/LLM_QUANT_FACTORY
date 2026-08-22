"""Registry entry for the existing Tushare/A-share ingestion pipeline.

This module does not re-implement Tushare ingestion. It registers the
already-shipped pipeline under the shared source registry so consumers can
enumerate and dispatch sources uniformly:

- Raw downloads and resumable feature sync: ``autoalpha.data.tushare_feature_sync``
- Panel build/audit CLI: the repository-root ``mf-data`` command
  (``uv run mf-data all``, see ``src/multifactor_ashare``)
- Service-side scheduled sync: ``autoalpha.service.data_sync``

The Tushare basis shares the A-share non-PIT-proxy caveats documented in
``AutoAlpha/docs/DATA_READINESS.md``; its ceiling is the proxy paper level, and
production admission remains blocked pending point-in-time market state.
"""

from __future__ import annotations

from autoalpha.data.sources.base import (
    SourceCapabilityProfile,
    SourceDescriptor,
    register_source,
)

TUSHARE_SOURCE_ID = "tushare"

TUSHARE_SOURCE = SourceDescriptor(
    source_id=TUSHARE_SOURCE_ID,
    label="Tushare A-share (CN)",
    markets=("CN",),
    capability=SourceCapabilityProfile(
        capability_ceiling="PROXY_PAPER_READY",
        point_in_time_ready=False,
        rationale=(
            "Research and non-PIT proxy backtests/paper trading are supported on the forward-"
            "adjusted panel with raw execution prices; strict production admission stays blocked "
            "until versioned PIT listing/delisting/suspension/limit state exists."
        ),
    ),
    ingestion_command=(
        "uv run mf-data all  (raw download + audit + standard panel build); "
        "uv run python -m autoalpha.data.tushare_feature_sync  (resumable feature sync)"
    ),
    ingest=None,
    notes=(
        "Panel identifiers follow Tushare ts_code conventions (for example 000001.SZ).",
        "Volume is board lots (100 shares) and amount is thousands of CNY per panel metadata.",
    ),
)

register_source(TUSHARE_SOURCE)
