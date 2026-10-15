from dataclasses import replace
from datetime import timedelta

import pytest

from core.models import GateStatus, ReplenishmentState, RunMode, SliceState
from core.replenishment_engine import build_cycle_plan
from tests.helpers import D, make_account, make_engine_inputs, make_strategy_config, inputs_with_orders, order


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


@pytest.mark.parametrize("balance,state,spot,reserved,rate,amount", [
    ("28", ReplenishmentState.ACCUMULATE, "0.8", "0", "0.04", None),
    ("30", ReplenishmentState.WATCH, "1.5", "0", "0.04", None),
    ("30", ReplenishmentState.ACCUMULATE, "1.5", "0", "0.04", None),
    ("28", ReplenishmentState.ACCUMULATE, "2.5", "0", "0.04", "2"),
    ("27.99", ReplenishmentState.WATCH, "0.8", "0", "0.04", "0.3"),
    ("27.99", ReplenishmentState.ACCUMULATE, "0.8", "0", "0.04", "0.3"),
    ("30", ReplenishmentState.URGENT, "0.8", "0", "2", "0.3"),
    ("25", ReplenishmentState.WATCH, "0.8", "0", "0.04", "0.3"),
    ("27.5", ReplenishmentState.ACCUMULATE, "0.59", "0", "0.04", None),
    ("27.5", ReplenishmentState.ACCUMULATE, "1.0", "0.3", "0.04", "0.2"),
    ("28", ReplenishmentState.ACCUMULATE, "2.6", "0.2", "0.04", None),
    ("31.89", ReplenishmentState.ACCUMULATE, "0.69", "0", "0.04", "0.11"),
    ("31.9", ReplenishmentState.ACCUMULATE, "0.60", "0", "0.04", "0.10"),
    ("40", ReplenishmentState.ACCUMULATE, "3.5", "0", "0.04", None),
])
def test_bnb_return_batches_by_confirmed_inventory_and_urgency(
    balance, state, spot, reserved, rate, amount
):
    inputs = make_engine_inputs(
        account=replace(make_account(contract_bnb=D(balance), spot_bnb=D(spot)),
                        reserved_spot_bnb=D(reserved)),
        previous_state=state, consumption_rate=D(rate),
    )
    plan = build_cycle_plan(inputs, make_strategy_config())
    if amount is None:
        assert plan.bnb_transfer_plan is None
    else:
        assert plan.bnb_transfer_plan is not None
        assert plan.bnb_transfer_plan.amount == D(amount)


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


@pytest.mark.parametrize("margin_change", [None, D("-500"), D("-501"), D("-10000")])
def test_margin_loss_keeps_target_and_valid_existing_bids(margin_change):
    plan = build_cycle_plan(
        inputs_with_orders(order("large", qty="5"), margin_change_24h=margin_change),
        make_strategy_config(),
    )
    assert plan.active_buy_target == D("32")
    assert plan.state_decision.delta_bnb == 0
    assert not plan.cancellations and not plan.buy_plan.orders


def test_margin_loss_keeps_full_bnb_transfer_target():
    inputs = make_engine_inputs(
        account=make_account(contract_bnb=D("24"), spot_bnb=D("12.5"))
    )
    plan = build_cycle_plan(
        replace(inputs, margin_change_24h=D("-10000")), make_strategy_config()
    )
    assert plan.active_buy_target == D("32")
    assert plan.bnb_transfer_plan.amount == D("8")


@pytest.mark.parametrize(
    "withdraw,budget_used,expected_gate,expected_level",
    [
        ("4000", "0", GateStatus.HARD_VETO, "WARNING"),
        ("100000", "3000", GateStatus.BLOCKED, "BUDGET_EXHAUSTED"),
    ],
)
def test_margin_loss_does_not_bypass_funding_limits(
    withdraw, budget_used, expected_gate, expected_level
):
    inputs = make_engine_inputs(
        account=make_account(
            contract_bnb=D("27"), contract_max_withdraw_amount=D(withdraw)
        ),
        previous_state=ReplenishmentState.ACCUMULATE,
        budget_used_24h=D(budget_used),
    )
    plan = build_cycle_plan(
        replace(inputs, margin_change_24h=D("-10000")), make_strategy_config()
    )
    assert plan.active_buy_target == D("32")
    assert plan.transfer_decision.gate_status == expected_gate
    assert plan.transfer_decision.level == expected_level
    assert not plan.transfer_decision.allow and not plan.buy_plan.orders


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
    inputs = replace(inputs, filters=replace(
        inputs.filters, min_notional=D(minimum), min_transfer_bnb=D("0.01")
    ))
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
