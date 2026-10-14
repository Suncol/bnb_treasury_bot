from dataclasses import replace

import pytest

from core.models import GateStatus, ReplenishmentState
from core.replenishment_engine import build_cycle_plan
from core.state_machine import evaluate_state
from tests.helpers import D, make_account, make_engine_inputs, make_state_inputs, make_strategy_config
from tests.unit.test_lifecycle import inputs_with_orders, order


@pytest.mark.parametrize("balance", ["28", "30", "31.9", "32", "40"])
@pytest.mark.parametrize("rate", ["0", "0.04", "1"])
def test_idle_does_not_start_a_round_above_alert(balance, rate):
    decision = evaluate_state(make_state_inputs(
        contract_bnb=D(balance), consumption_rate=D(rate),
        previous_state=ReplenishmentState.IDLE,
    ), make_strategy_config())
    assert decision.confirmed_state == ReplenishmentState.IDLE
    assert decision.raw_candidate_state == ReplenishmentState.IDLE


@pytest.mark.parametrize("previous", [ReplenishmentState.ACCUMULATE, ReplenishmentState.WATCH,
                                      ReplenishmentState.URGENT])
@pytest.mark.parametrize("balance", ["28", "30", "31.9"])
@pytest.mark.parametrize("rate", ["0", "0.04"])
def test_active_round_keeps_accumulating_after_recovery(previous, balance, rate):
    cfg = make_strategy_config()
    inputs = make_state_inputs(
        contract_bnb=D(balance), consumption_rate=D(rate),
        previous_state=previous,
    )
    decision = evaluate_state(inputs, cfg)
    assert decision.raw_candidate_state == ReplenishmentState.ACCUMULATE
    assert decision.confirmed_state != ReplenishmentState.IDLE
    confirmed = evaluate_state(replace(inputs,
        previous_state=decision.confirmed_state,
        previous_candidate_state=decision.candidate_state,
        candidate_streak=decision.candidate_streak,
    ), cfg)
    assert confirmed.confirmed_state == ReplenishmentState.ACCUMULATE
    assert confirmed.candidate_streak == 0


def test_recovery_above_alert_keeps_the_existing_accumulation_reprice_cycle():
    from datetime import timedelta

    inputs = make_engine_inputs(account=make_account(contract_bnb=D("28.1")),
                                consumption_rate=D("0.04"),
                                previous_state=ReplenishmentState.ACCUMULATE,
                                previous_candidate_state=ReplenishmentState.WATCH,
                                candidate_streak=1)
    bid = order("old", price="580", state=ReplenishmentState.ACCUMULATE,
                created_at=inputs.now-timedelta(hours=2))
    plan = build_cycle_plan(replace(inputs, orders=(bid,),
        pending=replace(inputs.pending, open_buy_remaining_qty=bid.remaining_qty),
    ), make_strategy_config())
    assert plan.state_decision.confirmed_state == ReplenishmentState.ACCUMULATE
    assert [c.order_id for c in plan.cancellations] == [bid.order_id]


@pytest.mark.parametrize("balance,complete", [("31.9", False), ("31.900001", True), ("31.99", True), ("32", True)])
def test_round_completion_uses_confirmed_contract_balance_and_transfer_minimum(balance, complete):
    cfg = make_strategy_config()
    inputs = make_engine_inputs(account=make_account(contract_bnb=D(balance)),
                                previous_state=ReplenishmentState.WATCH)
    first = build_cycle_plan(inputs, cfg)
    second = build_cycle_plan(replace(inputs,
        previous_state=first.state_decision.confirmed_state,
        previous_candidate_state=first.state_decision.candidate_state,
        candidate_streak=first.state_decision.candidate_streak,
    ), cfg)
    assert (second.state_decision.confirmed_state == ReplenishmentState.IDLE) == complete
    reconciled = build_cycle_plan(replace(inputs, advance_state=False), cfg)
    assert (reconciled.state_decision.confirmed_state == ReplenishmentState.IDLE) == complete
    unsettled = build_cycle_plan(replace(inputs,
        previous_candidate_state=ReplenishmentState.IDLE, candidate_streak=1,
        pending=replace(inputs.pending, pending_bnb_to_contract=D("0.2")),
    ), cfg)
    assert not unsettled.inventory_valid
    assert unsettled.state_decision.confirmed_state == ReplenishmentState.WATCH


def test_watch_returns_the_final_small_batch_to_finish_the_round():
    inputs = make_engine_inputs(
        account=make_account(contract_bnb=D("30.5"), spot_bnb=D("1.99")),
        previous_state=ReplenishmentState.WATCH,
    )
    plan = build_cycle_plan(inputs, make_strategy_config())
    assert plan.state_decision.confirmed_state == ReplenishmentState.WATCH
    assert plan.bnb_transfer_plan.amount == D("1.49")


@pytest.mark.parametrize("protection", ["none", "price", "ttl", "crash", "inconsistent", "unknown_order"])
def test_pending_transfer_freezes_inventory_but_preserves_real_order_protection(protection):
    from datetime import timedelta

    bid = order("safe", price="580", qty="1", state=ReplenishmentState.ACCUMULATE)
    inputs = inputs_with_orders(bid)
    inputs = replace(inputs, pending=replace(inputs.pending, pending_bnb_to_contract=D("0.3")))
    if protection == "price":
        inputs = replace(inputs, orders=(replace(bid, price=D("610")),))
    elif protection == "ttl":
        inputs = replace(inputs, orders=(replace(bid, created_at=bid.created_at-timedelta(hours=8)),))
    elif protection == "crash":
        inputs = replace(inputs, orders=(replace(bid, price=D("598")),),
                         market=replace(inputs.market, drawdown_1m=D("0.02")))
    elif protection == "inconsistent":
        inputs = replace(inputs, snapshot_consistent=False)
    elif protection == "unknown_order":
        inputs = replace(inputs, pending=replace(inputs.pending, has_unknown_orders=True))
    plan = build_cycle_plan(inputs, make_strategy_config())
    assert not plan.inventory_valid
    assert plan.state_decision.confirmed_state == inputs.previous_state
    assert plan.gates.reconciliation_gate == GateStatus.WAIT_RECONCILE
    assert not plan.buy_plan.orders and not plan.transfer_decision.allow
    assert plan.bnb_transfer_plan is None and plan.spot_usd_sweep_plan is None
    assert bool(plan.cancellations) == (protection != "none")
