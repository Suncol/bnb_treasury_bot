from __future__ import annotations

from dataclasses import replace

import pytest

from core.models import OrderView, ReplenishmentState
from core.order_planner import plan_buy_orders
from tests.helpers import D, make_filters, make_market, make_strategy_config, make_state_decision


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


@pytest.mark.parametrize('state,budget,delta,filter_changes,occupied,expected', [
    ('WATCH', '5.85', '1', {}, (), ('.01', '584.90')),
    ('WATCH', '5.849', '1', {}, (), ('.01', '584.90')),
    ('WATCH', '5.8489999', '1', {}, (), None),
    ('ACCUMULATE', '11.818', '1', {'min_notional': '6'}, (), ('.02', '590.90')),
    ('ACCUMULATE', '11.8179999', '1', {'min_notional': '6'}, (), None),
    ('ACCUMULATE', '11.818', '1', {'min_qty': '.015'}, (), ('.02', '590.90')),
    ('ACCUMULATE', '5.99', '1', {'max_notional': '5.9'}, (), None),
    ('ACCUMULATE', '5.99', '.009', {}, (), None),
    ('ACCUMULATE', '5.92', '1', {'min_price': '595'}, (), None),
    ('ACCUMULATE', '5.99', '1', {'max_price': '596'}, (), ('.01', '595.70')),
    ('ACCUMULATE', '5.92', '1', {}, ('590.90',), None),
    ('ACCUMULATE', '6', '1', {}, ('598.09', '595.70', '590.90'), None),
    ('ACCUMULATE', '5', '1', {'price_tick': '100'}, (), ('.01', '500')),
])
def test_minimum_bid_respects_actual_available_prices_and_order_filters(
    state, budget, delta, filter_changes, occupied, expected,
):
    cfg, market = make_strategy_config(), make_market()
    state = ReplenishmentState(state)
    filters = replace(make_filters(), **{key: D(value) for key, value in filter_changes.items()})
    orders = tuple(OrderView(cfg.symbol, str(i), 'owned-' + str(i), cfg.strategy_id,
                             D(price), D('.01'), D('0'), market.ts, state=state)
                   for i, price in enumerate(occupied))
    plan = plan_buy_orders(make_state_decision(state, delta), market, filters, cfg,
                           D(budget), existing_orders=orders)
    assert [(order.qty, order.price) for order in plan.orders] == (
        [] if expected is None else [(D(expected[0]), D(expected[1]))]
    )
