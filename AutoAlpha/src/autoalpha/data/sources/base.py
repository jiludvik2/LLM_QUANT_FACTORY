"""Pluggable market-data source registry.

Consumers resolve sources through :func:`get_source` instead of importing vendor
adapters directly, so adding a future source (for example EODHD) requires only a
new descriptor module; consumer code stays untouched.

Capability labels reused here are the platform's existing
``AUTOALPHA_DATA_CAPABILITY_MATRIX_V1`` levels (see
``autoalpha.service.data_center``). No new labels are introduced by sources.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any


class UnknownDataSourceError(KeyError):
    """A source id was requested that is not registered."""


@dataclass(frozen=True)
class SourceCapabilityProfile:
    """Honest readiness ceiling for one source, using existing platform labels.

    ``capability_ceiling`` MUST be one of the levels documented in
    ``AutoAlpha/docs/DATA_READINESS.md``. ``point_in_time_ready`` claims strict
    PIT readiness and must stay ``False`` unless versioned listing, delisting,
    suspension, and limit state history exist for every panel row.
    """

    capability_ceiling: str
    point_in_time_ready: bool
    rationale: str


@dataclass(frozen=True)
class SourceDescriptor:
    """Registration record for one market-data source."""

    source_id: str
    label: str
    markets: tuple[str, ...]
    capability: SourceCapabilityProfile
    ingestion_command: str
    # Callable entry point for programmatic ingestion. ``None`` means ingestion
    # is delegated to an external pipeline; the command string documents it.
    ingest: Callable[..., dict[str, Any]] | None = None
    notes: tuple[str, ...] = field(default=())

    def require_ingestion_callable(self) -> Callable[..., dict[str, Any]]:
        if self.ingest is None:
            raise NotImplementedError(
                f"Source {self.source_id} delegates ingestion to an external pipeline; "
                f"use: {self.ingestion_command}"
            )
        return self.ingest


_REGISTRY: dict[str, SourceDescriptor] = {}


def register_source(descriptor: SourceDescriptor, *, replace: bool = False) -> None:
    """Register a source descriptor once per id."""
    existing = _REGISTRY.get(descriptor.source_id)
    if existing is not None and existing != descriptor and not replace:
        raise ValueError(
            f"Source id {descriptor.source_id!r} is already registered with a different "
            "descriptor; pass replace=True to override it deliberately"
        )
    _REGISTRY[descriptor.source_id] = descriptor


def get_source(source_id: str) -> SourceDescriptor:
    try:
        return _REGISTRY[source_id]
    except KeyError as error:
        known = ", ".join(sorted(_REGISTRY)) or "<none>"
        raise UnknownDataSourceError(
            f"Unknown market-data source {source_id!r}; registered sources: {known}"
        ) from error


def available_sources() -> tuple[SourceDescriptor, ...]:
    """All registered sources ordered by id for deterministic display."""
    return tuple(_REGISTRY[source_id] for source_id in sorted(_REGISTRY))
