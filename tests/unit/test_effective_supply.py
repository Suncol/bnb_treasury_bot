from __future__ import annotations

from core.models import ReplenishmentState, RunMode, StateInputs
from core.state_machine import compute_delta_bnb, compute_effective_supply
from tests.helpers import D


def make_state_inputs(**overrides) -> StateInputs:
    return StateInputs(
        contract_bnb=overrides.get("contract_bnb", D("24")),
        contract_max_withdraw_amount=overrides.get("contract_max_withdraw_amount", D("6000")),
        contract_available_balance=overrides.get("contract_available_balance", D("6200")),
        spot_bnb=overrides.get("spot_bnb", D("2.5")),
        spot_usd=overrides.get("spot_usd", D("0")),
        reserved_spot_usd=overrides.get("reserved_spot_usd", D("0")),
        pending_bnb_to_contract=overrides.get("pending_bnb_to_contract", D("1")),
        pending_usd_to_spot=overrides.get("pending_usd_to_spot", D("0")),
        open_buy_remaining_qty=overrides.get("open_buy_remaining_qty", D("3")),
        filled_untransferred_bnb=overrides.get("filled_untransferred_bnb", D("2")),
        consumption_rate=overrides.get("consumption_rate", D("0")),
        previous_state=overrides.get("previous_state", ReplenishmentState.IDLE),
        previous_candidate_state=overrides.get("previous_candidate_state", None),
        candidate_streak=overrides.get("candidate_streak", 0),
        run_mode=overrides.get("run_mode", RunMode.AUTO),
        has_unknown_orders=overrides.get("has_unknown_orders", False),
        has_unknown_transfers=overrides.get("has_unknown_transfers", False),
    )


def test_effective_supply_does_not_double_count_pending_bnb_to_contract() -> None:
    inputs = make_state_inputs()
    effective = compute_effective_supply(inputs, D("0.5"))
    assert effective == D("31")


def test_filled_untransferred_bnb_reduces_delta_bnb() -> None:
    base_inputs = make_state_inputs(filled_untransferred_bnb=D("0"))
    richer_inputs = make_state_inputs(filled_untransferred_bnb=D("2"))
    base_delta = compute_delta_bnb(compute_effective_supply(base_inputs, D("0.5")), D("32"))
    richer_delta = compute_delta_bnb(compute_effective_supply(richer_inputs, D("0.5")), D("32"))
    assert richer_delta < base_delta


def test_open_buy_remaining_qty_can_only_reduce_delta() -> None:
    no_orders = make_state_inputs(open_buy_remaining_qty=D("0"))
    with_orders = make_state_inputs(open_buy_remaining_qty=D("3"))
    no_orders_delta = compute_delta_bnb(compute_effective_supply(no_orders, D("0.5")), D("32"))
    with_orders_delta = compute_delta_bnb(compute_effective_supply(with_orders, D("0.5")), D("32"))
    assert with_orders_delta <= no_orders_delta


def test_spot_transferable_bnb_zero_when_spot_below_reserve() -> None:
    inputs = make_state_inputs(spot_bnb=D("0.4"), pending_bnb_to_contract=D("0"))
    effective = compute_effective_supply(inputs, D("0.5"))
    assert effective == D("29")
