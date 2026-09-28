from __future__ import annotations

from core.models import GateStatus, ReplenishmentState
from core.replenishment_engine import build_cycle_plan
from tests.helpers import D, make_account, make_engine_inputs, make_pending, make_strategy_config


def test_unknown_operations_pause_new_transfer_and_buy_actions() -> None:
    cfg = make_strategy_config()
    inputs = make_engine_inputs(
        account=make_account(contract_bnb=D("24")),
        pending=make_pending(has_unknown_orders=True, has_unknown_transfers=True),
        consumption_rate=D("1"),
        previous_state=ReplenishmentState.URGENT,
    )
    plan = build_cycle_plan(inputs, cfg)
    assert plan.gates.reconciliation_gate == GateStatus.WAIT_RECONCILE
    assert plan.buy_plan.orders == ()
    assert plan.transfer_decision.allow is False
    assert plan.bnb_transfer_plan is None
