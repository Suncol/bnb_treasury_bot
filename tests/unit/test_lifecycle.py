from dataclasses import replace
from datetime import timedelta

import pytest

from core.models import GateStatus, OrderView, ReplenishmentState, RunMode, SliceState
from core.replenishment_engine import build_cycle_plan
from tests.helpers import D, make_account, make_engine_inputs, make_strategy_config


def inputs_with_orders(*orders, **changes):
    inputs = make_engine_inputs(
        account=make_account(contract_bnb=D("27"), spot_usd=D("10000")),
        previous_state=ReplenishmentState.ACCUMULATE,
    )
    return replace(
        inputs,
        orders=orders,
        pending=replace(
            inputs.pending,
            open_buy_remaining_qty=sum((o.remaining_qty for o in orders), D("0")),
        ),
        **changes,
    )


def order(key, price="580", qty="1", **changes):
    now = make_engine_inputs().now
    return replace(
        OrderView(
            "BNBUSDT",
            key,
            "client-" + key,
            "bnb-treasury",
            D(price),
            D(qty),
            D("0"),
            now,
        ),
        **changes,
    )


@pytest.mark.parametrize(
    "pending",
    [
        {"pending_bnb_to_contract": D("1")},
        {"pending_usd_to_spot": D("500")},
        {"pending_usd_to_contract": D("500")},
        {"has_pending_orders": True},
        {"has_pending_cancels": True},
        {"has_unknown_orders": True},
        {"has_unknown_transfers": True},
    ],
)
def test_every_unresolved_operation_blocks_new_actions(pending):
    inputs = make_engine_inputs(
        account=make_account(
            contract_bnb=D("24"), spot_bnb=D("5.5"), spot_usd=D("8000")
        )
    )
    plan = build_cycle_plan(
        replace(inputs, pending=replace(inputs.pending, **pending)),
        make_strategy_config(),
    )
    assert plan.gates.reconciliation_gate == GateStatus.WAIT_RECONCILE
    assert not plan.buy_plan.orders and not plan.transfer_decision.allow
    assert plan.bnb_transfer_plan is None and plan.spot_usd_sweep_plan is None


def test_watch_uses_batch_minimum_and_accumulate_does_not():
    inputs = make_engine_inputs(
        account=make_account(contract_bnb=D("30"), spot_bnb=D("1.5")),
        previous_state=ReplenishmentState.WATCH,
    )
    assert build_cycle_plan(inputs, make_strategy_config()).bnb_transfer_plan is None
    plan = build_cycle_plan(
        replace(inputs, previous_state=ReplenishmentState.ACCUMULATE),
        make_strategy_config(),
    )
    assert plan.bnb_transfer_plan.amount == D("1")


def test_exchange_transfer_minimum_applies_after_budget_clipping():
    inputs = make_engine_inputs(
        account=make_account(
            contract_bnb=D("30"), contract_max_withdraw_amount=D("50000")
        ),
        previous_state=ReplenishmentState.WATCH,
    )
    inputs = replace(
        inputs, filters=replace(inputs.filters, min_transfer_usd=D("2000"))
    )
    assert build_cycle_plan(
        inputs, make_strategy_config()
    ).transfer_decision.adjusted_amount == D("2000")
    plan = build_cycle_plan(
        replace(inputs, budget_used_24h=D("1500")), make_strategy_config()
    )
    assert not plan.transfer_decision.allow


def test_chase_filter_keeps_lower_bids_without_funding_or_new_orders():
    old = order("low", created_at=make_engine_inputs().now - timedelta(hours=7))
    inputs = inputs_with_orders(old)
    inputs = replace(inputs, market=replace(inputs.market, return_1h=D("0.10")))
    plan = build_cycle_plan(inputs, make_strategy_config())
    assert (
        not plan.cancellations
        and not plan.buy_plan.orders
        and not plan.transfer_decision.allow
    )


@pytest.mark.parametrize("mode", [RunMode.AUTO, RunMode.PAUSED])
def test_zero_deficit_and_paused_still_cancel_only_strategy_orders(mode):
    own, external = order("own", qty="20"), order("manual", strategy_id=None)
    inputs = inputs_with_orders(own, external, run_mode=mode)
    plan = build_cycle_plan(inputs, make_strategy_config())
    assert plan.state_decision.delta_bnb == 0
    assert [c.order_id for c in plan.cancellations] == ["own"]
    assert not plan.buy_plan.orders


def test_ttl_and_periodic_reprice_require_reconciliation_first():
    old = order("expired", created_at=make_engine_inputs().now - timedelta(hours=24))
    plan = build_cycle_plan(inputs_with_orders(old), make_strategy_config())
    assert [c.order_id for c in plan.cancellations] == ["expired"]
    assert not plan.buy_plan.orders and not plan.transfer_decision.allow


def test_idle_cancels_old_bids_even_when_zero_deficit():
    inputs = inputs_with_orders(
        order("own"),
        account=make_account(contract_bnb=D("42")),
        previous_state=ReplenishmentState.IDLE,
    )
    assert build_cycle_plan(inputs, make_strategy_config()).cancellations


def test_margin_deterioration_reduces_target_and_cancels_excess():
    plan = build_cycle_plan(
        inputs_with_orders(order("large", qty="5"), margin_change_24h=D("-501")),
        make_strategy_config(),
    )
    assert plan.active_buy_target == D("29") and plan.cancellations


def test_slice_persists_below_initial_threshold_and_waits():
    inputs = make_engine_inputs(
        account=make_account(contract_bnb=D("19"), spot_usd=D("30000"))
    )
    first = build_cycle_plan(inputs, make_strategy_config())
    assert first.slice_state.active and sum(o.qty for o in first.buy_plan.orders) <= D(
        "3"
    )
    waiting = replace(
        inputs, slice_state=SliceState(True, inputs.now + timedelta(minutes=30))
    )
    assert not build_cycle_plan(waiting, make_strategy_config()).buy_plan.orders
    later = replace(
        inputs,
        account=replace(inputs.account, contract_bnb=D("24")),
        slice_state=SliceState(True),
    )
    assert sum(
        o.qty for o in build_cycle_plan(later, make_strategy_config()).buy_plan.orders
    ) <= D("3")


@pytest.mark.parametrize(
    "invalid", ["account", "market", "future", "missing_window", "crossed"]
)
def test_invalid_or_incomplete_data_never_funds_buys(invalid):
    inputs = make_engine_inputs(
        account=make_account(contract_bnb=D("24"), spot_usd=D("10000"))
    )
    if invalid == "account":
        inputs = replace(
            inputs,
            account=replace(inputs.account, ts=inputs.now - timedelta(seconds=11)),
        )
    else:
        changes = {
            "market": {"ts": inputs.now - timedelta(seconds=6)},
            "future": {"ts": inputs.now + timedelta(seconds=1)},
            "missing_window": {"drawdown_15m": None},
            "crossed": {"best_bid": D("601")},
        }[invalid]
        inputs = replace(inputs, market=replace(inputs.market, **changes))
    plan = build_cycle_plan(inputs, make_strategy_config())
    assert plan.gates.data_gate == GateStatus.BLOCKED
    assert not plan.buy_plan.orders and not plan.transfer_decision.allow


def test_sweep_outside_idle_keeps_next_demand_buffer_and_honors_switch():
    cfg = make_strategy_config()
    inputs = make_engine_inputs(
        account=make_account(contract_bnb=D("27"), spot_usd=D("5000")),
        previous_state=ReplenishmentState.ACCUMULATE,
    )
    inputs = replace(inputs, market=replace(inputs.market, return_1h=D("0.10")))
    plan = build_cycle_plan(inputs, cfg)
    assert plan.spot_usd_sweep_plan.amount == D("3000")
    assert (
        build_cycle_plan(
            inputs, replace(cfg, risk=replace(cfg.risk, sweep_enabled=False))
        ).spot_usd_sweep_plan
        is None
    )


def test_fee_reserve_is_included_in_urgent_budget():
    inputs = make_engine_inputs(
        account=make_account(
            contract_bnb=D("24"),
            spot_usd=D("3000"),
            contract_max_withdraw_amount=D("4000"),
        )
    )
    cfg = make_strategy_config()
    order_plan = build_cycle_plan(inputs, cfg).buy_plan.orders[0]
    assert order_plan.qty * order_plan.price * (1 + cfg.risk.fee_reserve_rate) <= D(
        "3000"
    )


@pytest.mark.parametrize("state", [
    ReplenishmentState.WATCH,
    ReplenishmentState.ACCUMULATE,
    ReplenishmentState.URGENT,
])
@pytest.mark.parametrize("gap,minimum", [("0.001", "5"), ("0.01", "10"), ("0.01", "6")])
def test_funding_requires_an_order_valid_at_its_final_price(state, gap, minimum):
    inputs = make_engine_inputs(
        account=make_account(
            contract_bnb=D("32") - D(gap), contract_max_withdraw_amount=D("50000"),
        ),
        previous_state=state,
    )
    inputs = replace(inputs, filters=replace(inputs.filters, min_notional=D(minimum)))
    plan = build_cycle_plan(inputs, make_strategy_config())
    # At notional 6 the IOC qualifies at its rounded limit, discounted bids do not.
    eligible = gap == "0.01" and minimum == "6" and state == ReplenishmentState.URGENT
    assert plan.transfer_decision.allow == eligible
    assert not plan.buy_plan.orders


def test_funding_estimate_uses_only_the_executable_quantity():
    inputs = make_engine_inputs(
        account=make_account(contract_bnb=D("24"), contract_max_withdraw_amount=D("50000")),
    )
    inputs = replace(inputs, filters=replace(inputs.filters, max_qty=D("1")))
    plan = build_cycle_plan(inputs, make_strategy_config())
    assert plan.transfer_decision.adjusted_amount == D("1000")
    assert not plan.buy_plan.orders
