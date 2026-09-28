from __future__ import annotations

import pytest

from core.models import GateStatus, ReplenishmentState, RunMode
from core.replenishment_engine import build_cycle_plan
from tests.helpers import D, make_account, make_engine_inputs, make_pending, make_strategy_config


def test_paused_mode_preserves_state_but_blocks_all_actions() -> None:
    cfg = make_strategy_config()
    inputs = make_engine_inputs(
        account=make_account(contract_bnb=D("24"), spot_usd=D("3000")),
        pending=make_pending(),
        consumption_rate=D("1"),
        previous_state=ReplenishmentState.URGENT,
        run_mode=RunMode.PAUSED,
    )
    plan = build_cycle_plan(inputs, cfg)
    assert plan.state_decision.confirmed_state == ReplenishmentState.URGENT
    assert plan.gates.run_mode_gate == GateStatus.BLOCKED
    assert plan.buy_plan.orders == ()
    assert plan.transfer_decision.allow is False
    assert plan.bnb_transfer_plan is None


def test_urgent_only_mode_blocks_non_urgent_actions_but_keeps_state_decision() -> None:
    cfg = make_strategy_config()
    inputs = make_engine_inputs(
        account=make_account(contract_bnb=D("30"), contract_max_withdraw_amount=D("20000")),
        pending=make_pending(),
        consumption_rate=(D("30") - D("25")) / D("72"),
        previous_state=ReplenishmentState.WATCH,
        run_mode=RunMode.URGENT_ONLY,
    )
    plan = build_cycle_plan(inputs, cfg)
    assert plan.state_decision.confirmed_state == ReplenishmentState.WATCH
    assert plan.gates.run_mode_gate == GateStatus.BLOCKED
    assert plan.buy_plan.orders == ()
    assert plan.transfer_decision.allow is False


@pytest.mark.parametrize(
    ("has_unknown_orders", "has_unknown_transfers"),
    [
        (True, False),
        (False, True),
        (True, True),
    ],
)
def test_unknown_operations_force_wait_reconcile_gate(
    has_unknown_orders, has_unknown_transfers
) -> None:
    cfg = make_strategy_config()
    inputs = make_engine_inputs(
        account=make_account(contract_bnb=D("24"), spot_usd=D("3000")),
        pending=make_pending(
            has_unknown_orders=has_unknown_orders,
            has_unknown_transfers=has_unknown_transfers,
        ),
        consumption_rate=D("1"),
        previous_state=ReplenishmentState.URGENT,
    )
    plan = build_cycle_plan(inputs, cfg)
    assert plan.state_decision.confirmed_state == ReplenishmentState.URGENT
    assert plan.gates.reconciliation_gate == GateStatus.WAIT_RECONCILE
    assert plan.buy_plan.orders == ()
    assert plan.transfer_decision.allow is False
    assert plan.bnb_transfer_plan is None


def test_urgent_hard_veto_blocks_futures_transfer_but_allows_spot_funded_ioc_buy() -> None:
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
    assert plan.gates.transfer_gate == GateStatus.HARD_VETO
    assert plan.transfer_decision.allow is False
    assert len(plan.buy_plan.orders) == 1
    assert plan.buy_plan.orders[0].time_in_force == "IOC"
