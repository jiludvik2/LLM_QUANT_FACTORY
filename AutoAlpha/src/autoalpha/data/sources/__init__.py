"""Pluggable market-data sources for the shared daily-panel contract.

Importing this package registers the built-in sources. Adding a future source
(for example EODHD) means adding one descriptor module that calls
:func:`autoalpha.data.sources.base.register_source` and importing it here;
consumers only ever use :func:`get_source` / :func:`available_sources`.
"""

from __future__ import annotations

from autoalpha.data.sources import tushare_source, yahoo_source
from autoalpha.data.sources.base import (
    SourceCapabilityProfile,
    SourceDescriptor,
    UnknownDataSourceError,
    available_sources,
    get_source,
    register_source,
)

__all__ = [
    "SourceCapabilityProfile",
    "SourceDescriptor",
    "UnknownDataSourceError",
    "available_sources",
    "get_source",
    "register_source",
    "tushare_source",
    "yahoo_source",
]
