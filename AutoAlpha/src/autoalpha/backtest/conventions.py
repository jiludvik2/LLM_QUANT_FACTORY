"""Market trading conventions: versioned, market-parameterized inputs.

Vendored mirror of the ``market_data.conventions`` schema (pure stdlib,
frozen dataclasses, dated fee resolution, canonical-JSON sha256
fingerprints). AutoAlpha deliberately carries its own copy so it never
gains a runtime dependency on the market-data repository; keep the two
shapes in sync and record ``schema_version`` with any change.

Conventions are read-only inputs to research, backtest and execution
code. Nothing here grants authority to mutate gates or protocols at
runtime.

Built-ins register the current hard-coded CN A-share values as
``CN_ASHARE`` (so default behavior is unchanged) plus structural US / GB /
EU sets mirroring the market-data builtins. Non-CN markets resolve but do
not yet change any ingestion or research-protocol behavior.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import date
from typing import Any

__all__ = [
    "DEFAULT_MARKET",
    "FeeSchedule",
    "MarketConventions",
    "PriceLimitBand",
    "UnknownMarketError",
    "available_markets",
    "conventions_identity",
    "register",
    "resolve",
    "resolve_optional",
]

DEFAULT_MARKET = "CN_ASHARE"


class UnknownMarketError(KeyError):
    """Raised when no registered conventions match a market id."""


def _require_non_negative(name: str, value: float) -> None:
    if value < 0:
        raise ValueError(f"{name} must be non-negative, got {value!r}")


@dataclass(frozen=True)
class FeeSchedule:
    """Explicit fees effective from ``effective_from`` (inclusive).

    Rates are in basis points of traded notional; minima are in quote
    currency. Spread and impact remain execution-layer concerns.
    """

    effective_from: date
    commission_bps_each_side: float = 0.0
    minimum_commission: float = 0.0
    stamp_duty_bps_sell: float = 0.0
    transfer_fee_bps_each_side: float = 0.0

    def __post_init__(self) -> None:
        for name in (
            "commission_bps_each_side",
            "minimum_commission",
            "stamp_duty_bps_sell",
            "transfer_fee_bps_each_side",
        ):
            _require_non_negative(name, getattr(self, name))

    def to_dict(self) -> dict[str, Any]:
        return {
            "effective_from": self.effective_from.isoformat(),
            "commission_bps_each_side": self.commission_bps_each_side,
            "minimum_commission": self.minimum_commission,
            "stamp_duty_bps_sell": self.stamp_duty_bps_sell,
            "transfer_fee_bps_each_side": self.transfer_fee_bps_each_side,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> FeeSchedule:
        payload = dict(data)
        payload["effective_from"] = date.fromisoformat(payload["effective_from"])
        return cls(**payload)


@dataclass(frozen=True)
class PriceLimitBand:
    """Daily price-limit band for one board/segment, in percent."""

    board: str
    limit_up_pct: float | None = None
    limit_down_pct: float | None = None

    def __post_init__(self) -> None:
        if self.limit_up_pct is not None:
            _require_non_negative("limit_up_pct", self.limit_up_pct)
        if self.limit_down_pct is not None:
            _require_non_negative("limit_down_pct", self.limit_down_pct)

    def to_dict(self) -> dict[str, Any]:
        return {
            "board": self.board,
            "limit_up_pct": self.limit_up_pct,
            "limit_down_pct": self.limit_down_pct,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> PriceLimitBand:
        return cls(**dict(data))


@dataclass(frozen=True)
class MarketConventions:
    """A complete, versioned set of market conventions for one market."""

    market: str
    schema_version: str = "1"
    conventions_version: str = "1"

    # --- Session & timing -------------------------------------------------
    signal_timing: str = "EOD_T"
    execution_timing: str = "OPEN_T1"
    settlement: str = "T1"
    calendar_id: str = ""

    # --- Trading rules ----------------------------------------------------
    lot_size: int = 1
    t_plus_1_sellable: bool = False
    short_selling_allowed: bool = True
    price_limit_bands: tuple[PriceLimitBand, ...] = field(default_factory=tuple)

    # --- Costs ------------------------------------------------------------
    fee_schedules: tuple[FeeSchedule, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if not self.market:
            raise ValueError("market must be a non-empty identifier")
        if self.lot_size < 1:
            raise ValueError(f"lot_size must be >= 1, got {self.lot_size!r}")
        dates = [s.effective_from for s in self.fee_schedules]
        if len(dates) != len(set(dates)):
            raise ValueError("fee_schedules contain duplicate effective_from dates")
        boards = [b.board for b in self.price_limit_bands]
        if len(boards) != len(set(boards)):
            raise ValueError("price_limit_bands contain duplicate boards")

    def fee_schedule_for(self, as_of: date) -> FeeSchedule:
        """Return the schedule effective on ``as_of`` (inclusive start).

        Raises ``LookupError`` when ``as_of`` precedes every schedule rather
        than guessing with the earliest one (fail closed).
        """
        eligible = [s for s in self.fee_schedules if s.effective_from <= as_of]
        if not eligible:
            raise LookupError(
                f"No fee schedule for {self.market} effective on or before {as_of}"
            )
        return max(eligible, key=lambda s: s.effective_from)

    def latest_fee_schedule(self) -> FeeSchedule:
        """Return the schedule with the most recent ``effective_from``."""
        if not self.fee_schedules:
            raise LookupError(f"No fee schedules defined for {self.market}")
        return max(self.fee_schedules, key=lambda s: s.effective_from)

    def fingerprint(self) -> str:
        """Stable sha256 over canonical JSON of this convention set."""
        payload = json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {
            "market": self.market,
            "schema_version": self.schema_version,
            "conventions_version": self.conventions_version,
            "signal_timing": self.signal_timing,
            "execution_timing": self.execution_timing,
            "settlement": self.settlement,
            "calendar_id": self.calendar_id,
            "lot_size": self.lot_size,
            "t_plus_1_sellable": self.t_plus_1_sellable,
            "short_selling_allowed": self.short_selling_allowed,
            "price_limit_bands": [b.to_dict() for b in self.price_limit_bands],
            "fee_schedules": [s.to_dict() for s in self.fee_schedules],
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> MarketConventions:
        payload = dict(data)
        payload["price_limit_bands"] = tuple(
            PriceLimitBand.from_dict(b) for b in payload.get("price_limit_bands", ())
        )
        payload["fee_schedules"] = tuple(
            FeeSchedule.from_dict(s) for s in payload.get("fee_schedules", ())
        )
        return cls(**payload)

    def with_overrides(self, **overrides: Any) -> MarketConventions:
        """Derive an adjusted copy; use sparingly and record the result."""
        return replace(self, **overrides)


def conventions_identity(conventions: MarketConventions | None) -> dict[str, str]:
    """Evidence identity for the conventions that governed a run."""
    resolved = conventions or resolve(DEFAULT_MARKET)
    return {
        "market": resolved.market,
        "conventions_version": resolved.conventions_version,
        "conventions_fingerprint": resolved.fingerprint(),
    }


# --- Registry --------------------------------------------------------------


def _cn_ashare() -> MarketConventions:
    """Current CN A-share defaults exactly as previously hard-coded.

    The dated schedules encode the historical breakpoints that
    ``use_historical_fee_schedule`` used to switch on by hand:
    transfer fee halved from 2022-04-29 and stamp duty halved from
    2023-08-28. Commission defaults stay caller-overridable because the
    vector proxy (2.5 bps) and cash ledger (1.5 bps) legitimately differ.
    """
    schedules = (
        FeeSchedule(
            effective_from=date(1990, 12, 19),
            commission_bps_each_side=1.5,
            minimum_commission=5.0,
            transfer_fee_bps_each_side=0.2,
            stamp_duty_bps_sell=10.0,
        ),
        FeeSchedule(
            effective_from=date(2022, 4, 29),
            commission_bps_each_side=1.5,
            minimum_commission=5.0,
            transfer_fee_bps_each_side=0.1,
            stamp_duty_bps_sell=10.0,
        ),
        FeeSchedule(
            effective_from=date(2023, 8, 28),
            commission_bps_each_side=1.5,
            minimum_commission=5.0,
            transfer_fee_bps_each_side=0.1,
            stamp_duty_bps_sell=5.0,
        ),
    )
    return MarketConventions(
        market="CN_ASHARE",
        calendar_id="XSHG",
        lot_size=100,
        settlement="T1",
        t_plus_1_sellable=True,
        short_selling_allowed=False,
        price_limit_bands=(
            PriceLimitBand(board="MAIN_BOARD", limit_up_pct=10.0, limit_down_pct=10.0),
            PriceLimitBand(board="CHINEXT_STAR", limit_up_pct=20.0, limit_down_pct=20.0),
            PriceLimitBand(board="BSE", limit_up_pct=30.0, limit_down_pct=30.0),
        ),
        fee_schedules=schedules,
    )


_BUILTIN_MARKETS: dict[str, MarketConventions] = {
    "CN_ASHARE": _cn_ashare(),
    # Structural non-CN defaults mirroring the market-data builtins;
    # research-grade only until per-market protocol config lands.
    "US": MarketConventions(market="US", calendar_id="XNYS", lot_size=1, settlement="T1"),
    "GB": MarketConventions(
        market="GB",
        calendar_id="XLON",
        lot_size=1,
        settlement="T2",
        fee_schedules=(
            FeeSchedule(
                effective_from=date(1986, 10, 27),
                stamp_duty_bps_sell=0.0,
                transfer_fee_bps_each_side=50.0,
            ),
        ),
    ),
    "EU": MarketConventions(market="EU", calendar_id="XPAR", lot_size=1, settlement="T2"),
}

_REGISTRY: dict[str, MarketConventions] = dict(_BUILTIN_MARKETS)


def register(conventions: MarketConventions, *, overwrite: bool = False) -> None:
    """Register a convention set under its own market id (process-local)."""
    if not overwrite and conventions.market in _BUILTIN_MARKETS:
        raise ValueError(f"{conventions.market!r} is a built-in market")
    _REGISTRY[conventions.market] = conventions


def available_markets() -> tuple[str, ...]:
    return tuple(sorted(_REGISTRY))


def resolve(market: str) -> MarketConventions:
    try:
        return _REGISTRY[market]
    except KeyError:
        raise UnknownMarketError(
            f"unknown market {market!r}; available: {', '.join(available_markets())}"
        ) from None


def resolve_optional(market: str | None) -> MarketConventions:
    """Resolve ``market`` or fall back to the default CN A-share set."""
    return resolve(market) if market else resolve(DEFAULT_MARKET)
