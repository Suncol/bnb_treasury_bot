from dataclasses import replace
from datetime import timedelta

import pytest

from core.models import OrderPlan, OrderView, ReplenishmentState, RunMode
from core.order_planner import plan_buy_orders
from core.replenishment_engine import build_cycle_plan
from tests.helpers import D, make_account, make_engine_inputs, make_strategy_config
from tests.unit.test_order_planner import make_state_decision


def layer_inputs(*, age=0, contract="30.98", spot_usd="10000"):
    inputs = make_engine_inputs(account=make_account(
        contract_bnb=D(contract), spot_usd=D(spot_usd)
    ), previous_state=ReplenishmentState.WATCH)
    cfg = make_strategy_config()
    orders = tuple(OrderView(cfg.symbol, str(i), f"client-{i}", cfg.strategy_id,
                             D(price), D(qty), D("0"), inputs.now-timedelta(hours=age),
                             state=ReplenishmentState.WATCH)
                   for i, (price, qty) in enumerate([
                       ("596.90", "0.30"), ("592.70", "0.40"), ("584.90", "0.30")]))
    return replace(inputs, orders=orders, pending=replace(inputs.pending, open_buy_remaining_qty=D("1")))


def test_occupied_layers_block_small_orders_and_unnecessary_funding():
    plan = build_cycle_plan(layer_inputs(spot_usd="0"), make_strategy_config())
    assert not plan.buy_plan.orders and not plan.transfer_decision.allow


def test_filled_layer_can_be_replenished_while_other_layers_remain():
    inputs = layer_inputs()
    inputs = replace(inputs, orders=inputs.orders[1:],
                     account=replace(inputs.account, spot_bnb=D("0.8")),
                     pending=replace(inputs.pending, open_buy_remaining_qty=D("0.7")))
    plan = build_cycle_plan(inputs, make_strategy_config())
    assert [(o.price, o.qty) for o in plan.buy_plan.orders] == [(D("596.90"), D("0.02"))]


def test_external_orders_do_not_occupy_strategy_layer_positions():
    inputs = layer_inputs(contract="27")
    inputs = replace(inputs, orders=(replace(inputs.orders[0], strategy_id=None),),
                     pending=replace(inputs.pending, open_buy_remaining_qty=D("0.3")))
    plan = build_cycle_plan(inputs, make_strategy_config())
    assert len(plan.buy_plan.orders) == 3


def test_rounded_identical_prices_merge_into_one_layer():
    inputs = make_engine_inputs()
    plan = plan_buy_orders(make_state_decision(ReplenishmentState.WATCH, "1"),
                           inputs.market, replace(inputs.filters, price_tick=D("100")),
                           make_strategy_config(), D("10000"))
    assert [(o.price, o.qty) for o in plan.orders] == [(D("500"), D("1"))]


def test_queued_layer_checks_its_actual_cost_instead_of_top_layer_price():
    inputs = make_engine_inputs()
    candidate = OrderPlan("BNBUSDT", "BUY", "LIMIT_MAKER", D("1.12"), D("584.90"), "GTC", True)
    plan = plan_buy_orders(make_state_decision(ReplenishmentState.WATCH, "1.13"),
                           inputs.market, inputs.filters, make_strategy_config(),
                           candidate.qty*candidate.price, queued_order=candidate)
    assert plan.orders == (candidate,)


@pytest.mark.parametrize("trigger", ["unchanged", "price", "ttl", "quantity"])
def test_periodic_price_comparison_preserves_other_cancellation_checks(trigger):
    inputs = layer_inputs(age=6, contract="31")
    if trigger == "price":
        inputs = replace(inputs, market=replace(inputs.market, mid_price=D("601"),
                         best_bid=D("600.5"), best_ask=D("601.5")))
    elif trigger == "ttl":
        inputs = replace(inputs, orders=tuple(replace(o, created_at=o.created_at-timedelta(hours=18))
                                              for o in inputs.orders))
    elif trigger == "quantity":
        inputs = replace(inputs, account=replace(inputs.account, contract_bnb=D("31.5")))
    plan = build_cycle_plan(inputs, make_strategy_config())
    assert bool(plan.cancellations) == (trigger != "unchanged")
    if trigger == "quantity":
        assert all(c.reason == "Remaining quantity exceeds inventory or episode allowance"
                   for c in plan.cancellations)


@pytest.mark.parametrize("blocker", [None, "position", "slots", "money", "pending", "paused", "chase", "price"])
def test_queued_layer_reuses_current_quantity_and_execution_gates(blocker):
    inputs = make_engine_inputs(account=make_account(contract_bnb=D("28.26"), spot_usd=D("3000")),
                                previous_state=ReplenishmentState.WATCH)
    inputs = replace(inputs, pending=replace(inputs.pending, open_buy_remaining_qty=D("2.61")))
    candidate = OrderPlan("BNBUSDT", "BUY", "LIMIT_MAKER", D("1.12"), D("584.90"), "GTC", True)
    if blocker == "position":
        inputs = replace(inputs, filters=replace(inputs.filters, max_position=D("3.2")))
    elif blocker == "slots":
        inputs = replace(inputs, filters=replace(inputs.filters, exchange_order_slots=0))
    elif blocker == "money":
        inputs = replace(inputs, account=replace(inputs.account, spot_usd=D("500")))
    elif blocker == "pending":
        inputs = replace(inputs, pending=replace(inputs.pending, has_unknown_transfers=True))
    elif blocker == "paused":
        inputs = replace(inputs, run_mode=RunMode.PAUSED)
    elif blocker == "chase":
        inputs = replace(inputs, market=replace(inputs.market, return_1h=D("0.04")))
    elif blocker == "price":
        inputs = replace(inputs, market=replace(inputs.market, mid_price=D("570"), best_bid=D("569.5"), best_ask=D("570.5")))
    plan = build_cycle_plan(inputs, make_strategy_config(), queued_order=candidate)
    assert plan.buy_plan.orders == ((candidate,) if blocker is None else ())


def test_queued_urgent_order_preserves_crash_episode_quantity_cap():
    inputs = make_engine_inputs(account=make_account(contract_bnb=D("24"), spot_usd=D("3000")),
                                previous_state=ReplenishmentState.URGENT)
    inputs = replace(inputs, crash_guard=replace(inputs.crash_guard, active=True,
                     started_at=inputs.now, last_trigger_at=inputs.now, filled_bnb=D("2.5")))
    candidate = OrderPlan("BNBUSDT", "BUY", "LIMIT", D("1"), D("601.71"), "IOC", False)
    plan = build_cycle_plan(inputs, make_strategy_config(), queued_order=candidate)
    assert plan.crash_guard.active
    assert not plan.buy_plan.orders
