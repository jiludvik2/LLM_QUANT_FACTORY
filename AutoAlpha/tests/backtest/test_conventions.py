from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from autoalpha.backtest import conventions as conv
from autoalpha.backtest.costs import ChinaAExecutionCosts
from autoalpha.backtest.timing import supported_timing
from autoalpha.execution.simulator import ExecutionSimulator, Order

# --- Registry & built-ins ---------------------------------------------------


def test_builtin_markets_resolve() -> None:
    assert "CN_ASHARE" in conv.available_markets()
    for market in ("US", "GB", "EU"):
        resolved = conv.resolve(market)
        assert resolved.market == market
        assert resolved.lot_size == 1


def test_unknown_market_fails_closed() -> None:
    with pytest.raises(conv.UnknownMarketError):
        conv.resolve("JP")


def test_default_market_is_cn_ashare() -> None:
    assert conv.DEFAULT_MARKET == "CN_ASHARE"
    assert conv.resolve_optional(None).market == "CN_ASHARE"


def test_cn_ashare_structure_matches_legacy_hardcoding() -> None:
    cn = conv.resolve("CN_ASHARE")
    assert cn.lot_size == 100
    assert cn.t_plus_1_sellable is True
    assert cn.short_selling_allowed is False
    boards = {b.board: b for b in cn.price_limit_bands}
    assert boards["MAIN_BOARD"].limit_up_pct == 10.0


# --- Dated fee resolution ----------------------------------------------------


def test_cn_dated_fee_breakpoints_match_legacy_values() -> None:
    cn = conv.resolve("CN_ASHARE")
    # Before 2022-04-29: transfer 0.2 bps, stamp 10 bps.
    early = cn.fee_schedule_for(date(2022, 4, 28))
    assert early.transfer_fee_bps_each_side == pytest.approx(0.2)
    assert early.stamp_duty_bps_sell == pytest.approx(10.0)
    # Between breakpoints: transfer 0.1 bps, stamp still 10 bps.
    middle = cn.fee_schedule_for(date(2022, 4, 29))
    assert middle.transfer_fee_bps_each_side == pytest.approx(0.1)
    assert middle.stamp_duty_bps_sell == pytest.approx(10.0)
    # From 2023-08-28: stamp halved to 5 bps.
    current = cn.fee_schedule_for(date(2023, 8, 28))
    assert current.stamp_duty_bps_sell == pytest.approx(5.0)


def test_fee_lookup_before_all_schedules_raises() -> None:
    with pytest.raises(LookupError):
        conv.resolve("CN_ASHARE").fee_schedule_for(date(1989, 1, 1))


def test_costs_from_conventions_reproduce_legacy_dated_fees() -> None:
    costs = ChinaAExecutionCosts.from_conventions("CN_ASHARE")
    sell_2021 = costs.fee_breakdown("SELL", 100_000.0, date(2021, 6, 1))
    sell_now = costs.fee_breakdown("SELL", 100_000.0, date(2024, 1, 1))
    assert sell_2021["transfer_fee"] == pytest.approx(100_000 * 0.2 / 10_000)
    assert sell_2021["stamp_duty"] == pytest.approx(100_000 * 10.0 / 10_000)
    assert sell_now["transfer_fee"] == pytest.approx(100_000 * 0.1 / 10_000)
    assert sell_now["stamp_duty"] == pytest.approx(100_000 * 5.0 / 10_000)


def test_costs_default_path_unchanged() -> None:
    costs = ChinaAExecutionCosts()
    assert costs.conventions_market == "CN_ASHARE"
    assert costs.historical_fee_schedules == ()
    breakdown = costs.fee_breakdown("SELL", 100_000.0, date(2015, 1, 1))
    assert breakdown["commission"] == pytest.approx(max(5.0, 15.0))
    assert breakdown["stamp_duty"] == pytest.approx(50.0)
    assert breakdown["transfer_fee"] == pytest.approx(1.0)


# --- Fingerprints ------------------------------------------------------------


def test_fingerprint_is_stable_and_sensitive() -> None:
    us_a = conv.resolve("US")
    us_b = conv.MarketConventions.from_dict(us_a.to_dict())
    assert us_a.fingerprint() == us_b.fingerprint()
    tweaked = us_a.with_overrides(lot_size=10)
    assert tweaked.fingerprint() != us_a.fingerprint()
    gb = conv.resolve("GB")
    assert gb.fingerprint() != us_a.fingerprint()


# --- Timing ------------------------------------------------------------------


def test_supported_timing_validates_markets() -> None:
    label = supported_timing("US")
    assert label == "EOD_T__OPEN_T1_TO_OPEN_T2"
    odd = conv.resolve("US").with_overrides(execution_timing="CLOSE_T0")
    with pytest.raises(ValueError):
        supported_timing(odd)


# --- Simulator lot handling --------------------------------------------------


def _market_slices(price: float = 10.0) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "price": [price, price],
            "volume": [1_000_000.0, 1_000_000.0],
            "can_trade": [True, True],
        },
        index=pd.DatetimeIndex(["2024-01-02", "2024-01-03"]),
    )


def _order(quantity: int = 250) -> Order:
    return Order(
        order_id="o1",
        symbol="TEST",
        side="BUY",
        quantity=quantity,
        decision_price=10.0,
        style="VWAP",
    )


def test_simulator_lot_rounding_under_us_conventions() -> None:
    sim = ExecutionSimulator(conventions="US")
    report = sim.execute(
        _order(250), _market_slices(), adv_shares=10_000_000.0, daily_volatility=0.02
    )
    assert report.filled_quantity == 250  # lot size 1 fills every share
    assert report.unfilled_quantity == 0


def test_simulator_default_keeps_cn_lot_of_100() -> None:
    sim = ExecutionSimulator(conventions="CN_ASHARE")
    report = sim.execute(
        _order(300), _market_slices(), adv_shares=10_000_000.0, daily_volatility=0.02
    )
    assert report.filled_quantity == 200  # VWAP halves round down to whole 100-share lots
    assert report.unfilled_quantity == 100


def test_simulator_explicit_order_lot_wins_over_conventions() -> None:
    order = Order(
        order_id="o2",
        symbol="TEST",
        side="BUY",
        quantity=20,
        decision_price=10.0,
        style="VWAP",
        lot_size=10,
    )
    sim = ExecutionSimulator(conventions="US")
    report = sim.execute(order, _market_slices(), adv_shares=10_000_000.0, daily_volatility=0.02)
    assert report.filled_quantity == 20


def test_simulator_records_conventions_identity() -> None:
    sim = ExecutionSimulator(conventions="EU")
    assert sim.conventions is not None
    identity = conv.conventions_identity(sim.conventions)
    assert identity["market"] == "EU"
    assert len(identity["conventions_fingerprint"]) == 64


# --- Evidence helpers --------------------------------------------------------


def test_conventions_identity_defaults_to_cn() -> None:
    identity = conv.conventions_identity(None)
    assert identity["market"] == "CN_ASHARE"
    assert identity["conventions_fingerprint"] == conv.resolve("CN_ASHARE").fingerprint()


def test_register_custom_market() -> None:
    custom = conv.MarketConventions(market="XX_TEST", lot_size=7)
    conv.register(custom)
    try:
        assert conv.resolve("XX_TEST").lot_size == 7
        with pytest.raises(ValueError):
            conv.register(conv.resolve("US"))
    finally:
        conv._REGISTRY.pop("XX_TEST", None)
