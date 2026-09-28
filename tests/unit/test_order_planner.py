from __future__ import annotations

from dataclasses import replace

import pytest

from core.models import ReplenishmentState, StateDecision, TransitionReason
from core.order_planner import plan_buy_orders
from tests.helpers import D, make_filters, make_market, make_strategy_config


def make_state_decision(state: ReplenishmentState, delta_bnb: str = "7") -> StateDecision:
    return StateDecision(
        current_state=state,
        raw_candidate_state=state,
        candidate_state=state,
        confirmed_state=state,
        transitioned=False,
        reason=TransitionReason.HIGH_BALANCE,
        candidate_reason=TransitionReason.HIGH_BALANCE,
        t_depletion=None,
        effective_supply=D("0"),
        delta_bnb=D(delta_bnb),
        candidate_streak=0,
    )


def test_watch_orders_are_three_layer_maker_bids_below_ref_price() -> None:
    cfg = make_strategy_config()
    plan = plan_buy_orders(
        make_state_decision(ReplenishmentState.WATCH),
        make_market(),
        make_filters(),
        cfg,
        quote_budget=D("10000"),
    )
    assert len(plan.orders) == 3
    assert all(order.order_type == "LIMIT_MAKER" for order in plan.orders)
    assert all(order.post_only is True for order in plan.orders)
    ref_price = D("599.8995")
    assert [order.price for order in plan.orders] == [
        D("596.90"),
        D("592.70"),
        D("584.90"),
    ]
    assert all(order.price < ref_price for order in plan.orders)


def test_accumulate_orders_are_three_tighter_limit_bids() -> None:
    cfg = make_strategy_config()
    plan = plan_buy_orders(
        make_state_decision(ReplenishmentState.ACCUMULATE),
        make_market(),
        make_filters(),
        cfg,
        quote_budget=D("10000"),
    )
    assert len(plan.orders) == 3
    assert all(order.order_type == "LIMIT" for order in plan.orders)
    assert all(order.time_in_force == "GTC" for order in plan.orders)


def test_urgent_plan_is_ioc_style_and_not_long_lived() -> None:
    cfg = make_strategy_config()
    plan = plan_buy_orders(
        make_state_decision(ReplenishmentState.URGENT, delta_bnb="2"),
        make_market(),
        make_filters(),
        cfg,
        quote_budget=D("2000"),
    )
    assert len(plan.orders) == 1
    assert plan.orders[0].time_in_force == "IOC"
    assert plan.orders[0].post_only is False


@pytest.mark.parametrize(
    ("quote_budget", "delta_bnb", "price_tick", "min_notional", "expected_qty"),
    [
        ("3000", "8", "0.01", "5", "4.98"),
        ("6", "8", "0.01", "5", "0"),
        ("6.0171", "8", "0.01", "6", "0.01"),
        ("12.0341", "8", "0.01", "5", "0.01"),
        ("12.0342", "8", "0.01", "5", "0.02"),
        ("12.039", "8", "1", "5", "0.01"),
        ("3000", "1.009", "0.01", "5", "1.00"),
        ("3000", "0.019", "0.01", "8", "0"),
        ("0", "8", "0.01", "5", "0"),
    ],
)
def test_urgent_quantity_respects_final_limit_price_and_filters(
    quote_budget, delta_bnb, price_tick, min_notional, expected_qty
) -> None:
    filters = replace(
        make_filters(), price_tick=D(price_tick), min_notional=D(min_notional)
    )
    plan = plan_buy_orders(
        make_state_decision(ReplenishmentState.URGENT, delta_bnb=delta_bnb),
        make_market(),
        filters,
        make_strategy_config(),
        quote_budget=D(quote_budget),
    )
    if D(expected_qty) == D("0"):
        assert plan.orders == ()
        return

    assert len(plan.orders) == 1
    order = plan.orders[0]
    assert order.qty == D(expected_qty)
    assert order.qty <= D(delta_bnb)
    assert order.qty >= filters.min_qty
    assert order.qty % filters.qty_step == D("0")
    assert order.price is not None
    assert order.price % filters.price_tick == D("0")
    assert filters.min_notional <= order.qty * order.price <= D(quote_budget)


def test_invalid_small_layers_are_dropped_or_merged() -> None:
    cfg = make_strategy_config()
    filters = make_filters()
    plan = plan_buy_orders(
        make_state_decision(ReplenishmentState.WATCH, delta_bnb="0.02"),
        make_market(),
        filters,
        cfg,
        quote_budget=D("20"),
    )
    assert len(plan.orders) <= 1
    assert all(order.qty >= filters.min_qty for order in plan.orders)
    assert all(order.qty * order.price >= filters.min_notional for order in plan.orders)
