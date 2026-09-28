from __future__ import annotations

import pytest
from dataclasses import replace

from core.models import GateStatus, ReplenishmentState
from core.replenishment_engine import build_cycle_plan
from tests.helpers import (
    D,
    make_account,
    make_engine_inputs,
    make_pending,
    make_strategy_config,
)


def test_idle_without_deficit_emits_no_buy_or_usd_transfer_and_may_sweep() -> None:
    cfg = make_strategy_config()
    inputs = make_engine_inputs(
        account=make_account(contract_bnb=D("42"), spot_usd=D("2000")),
        pending=make_pending(),
        previous_state=ReplenishmentState.IDLE,
    )
    plan = build_cycle_plan(inputs, cfg)
    assert plan.state_decision.confirmed_state == ReplenishmentState.IDLE
    assert plan.buy_plan.orders == ()
    assert plan.transfer_decision.allow is False
    assert plan.spot_usd_sweep_plan is not None


@pytest.mark.parametrize(
    ("contract_bnb", "consumption_rate"),
    [("30", "0"), ("30", "0.1"), ("27", "0")],
)
def test_confirmed_idle_with_deficit_does_not_fund_buys(
    contract_bnb, consumption_rate
) -> None:
    inputs = make_engine_inputs(
        account=make_account(
            contract_bnb=D(contract_bnb), contract_max_withdraw_amount=D("50000")
        ),
        consumption_rate=D(consumption_rate),
        previous_state=ReplenishmentState.IDLE,
    )
    plan = build_cycle_plan(inputs, make_strategy_config())
    assert plan.state_decision.confirmed_state == ReplenishmentState.IDLE
    assert plan.state_decision.delta_bnb > D("0")
    assert plan.buy_plan.orders == ()
    assert plan.transfer_decision.allow is False
    assert plan.transfer_decision.adjusted_amount == D("0")
    assert plan.transfer_decision.level == "NO_ACTION"
    assert all(alert.code != "TRANSFER_GATE" for alert in plan.alerts)


def test_idle_with_deficit_can_still_return_spot_assets_to_futures() -> None:
    inputs = make_engine_inputs(
        account=make_account(
            contract_bnb=D("30"), spot_bnb=D("1.5"), spot_usd=D("2000")
        ),
        previous_state=ReplenishmentState.IDLE,
    )
    plan = build_cycle_plan(inputs, make_strategy_config())
    assert plan.state_decision.confirmed_state == ReplenishmentState.IDLE
    assert plan.state_decision.delta_bnb == D("1")
    assert plan.buy_plan.orders == ()
    assert plan.transfer_decision.allow is False
    assert plan.bnb_transfer_plan is not None
    assert plan.bnb_transfer_plan.amount == D("1")
    assert plan.bnb_transfer_plan.to_account == "USDⓈ-M Futures"
    assert plan.spot_usd_sweep_plan is not None
    assert plan.spot_usd_sweep_plan.amount == D("1500")
    assert plan.spot_usd_sweep_plan.to_account == "USDⓈ-M Futures"


def test_watch_waits_for_funding_then_plans_three_orders() -> None:
    cfg = make_strategy_config()
    inputs = make_engine_inputs(
        account=make_account(
            contract_bnb=D("30"),
            spot_usd=D("0"),
            contract_max_withdraw_amount=D("20000"),
        ),
        pending=make_pending(),
        consumption_rate=D("0.1"),
        previous_state=ReplenishmentState.WATCH,
    )
    plan = build_cycle_plan(inputs, cfg)
    assert plan.state_decision.confirmed_state == ReplenishmentState.WATCH
    assert plan.transfer_decision.allow is True
    assert plan.buy_plan.orders == ()
    assert plan.gates.transfer_gate == GateStatus.OPEN
    funded = replace(
        inputs,
        account=replace(
            inputs.account, spot_usd=plan.transfer_decision.adjusted_amount
        ),
        budget_used_24h=plan.transfer_decision.adjusted_amount,
    )
    assert len(build_cycle_plan(funded, cfg).buy_plan.orders) == 3


def test_accumulate_with_existing_spot_bnb_only_plans_bnb_transfer() -> None:
    cfg = make_strategy_config()
    inputs = make_engine_inputs(
        account=make_account(contract_bnb=D("26"), spot_bnb=D("7.5"), spot_usd=D("0")),
        pending=make_pending(),
        previous_state=ReplenishmentState.ACCUMULATE,
    )
    plan = build_cycle_plan(inputs, cfg)
    assert plan.state_decision.confirmed_state == ReplenishmentState.ACCUMULATE
    assert plan.transfer_decision.allow is False
    assert plan.buy_plan.orders == ()
    assert plan.bnb_transfer_plan is not None
    assert plan.bnb_transfer_plan.asset == "BNB"


def test_urgent_with_spot_usd_and_hard_veto_can_still_plan_urgent_buy() -> None:
    cfg = make_strategy_config()
    inputs = make_engine_inputs(
        account=make_account(
            contract_bnb=D("24"),
            contract_max_withdraw_amount=D("4000"),
            spot_usd=D("3000"),
        ),
        pending=make_pending(),
        consumption_rate=D("1"),
        previous_state=ReplenishmentState.URGENT,
    )
    plan = build_cycle_plan(inputs, cfg)
    assert plan.state_decision.confirmed_state == ReplenishmentState.URGENT
    assert plan.transfer_decision.allow is False
    assert plan.gates.transfer_gate == GateStatus.HARD_VETO
    assert len(plan.buy_plan.orders) == 1
    assert plan.buy_plan.orders[0].time_in_force == "IOC"
    order = plan.buy_plan.orders[0]
    assert order.price is not None
    assert order.qty * order.price <= inputs.account.spot_usd


def test_urgent_buy_stays_within_free_spot_funds_and_allowed_transfer() -> None:
    inputs = make_engine_inputs(
        account=make_account(
            contract_bnb=D("24"),
            contract_max_withdraw_amount=D("50000"),
            spot_usd=D("500"),
            reserved_spot_usd=D("500"),
        ),
        previous_state=ReplenishmentState.URGENT,
    )
    plan = build_cycle_plan(inputs, make_strategy_config())
    assert plan.transfer_decision.allow is True
    assert plan.transfer_decision.adjusted_amount == D("3000")
    assert plan.buy_plan.orders == ()
    inputs = replace(
        inputs,
        account=replace(inputs.account, spot_usd=D("3500")),
        budget_used_24h=D("3000"),
    )
    plan = build_cycle_plan(inputs, make_strategy_config())
    assert len(plan.buy_plan.orders) == 1
    order = plan.buy_plan.orders[0]
    assert order.price is not None
    quote_budget = (
        inputs.account.spot_usd
        - inputs.account.reserved_spot_usd
        + plan.transfer_decision.adjusted_amount
    )
    assert order.qty * order.price <= quote_budget


def test_cycle_plan_respects_single_action_group_constraints() -> None:
    cfg = make_strategy_config()
    inputs = make_engine_inputs(
        account=make_account(contract_bnb=D("24"), spot_usd=D("0")),
        pending=make_pending(),
        previous_state=ReplenishmentState.URGENT,
    )
    plan = build_cycle_plan(inputs, cfg)
    assert len(plan.buy_plan.orders) <= 3
    assert plan.bnb_transfer_plan is None or plan.bnb_transfer_plan.asset == "BNB"
    assert plan.transfer_decision.adjusted_amount >= D("0")
