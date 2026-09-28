from __future__ import annotations

from core.models import ReplenishmentState, RunMode, StateInputs, TransitionReason
from core.state_machine import (
    classify_candidate_state,
    compute_t_depletion,
    evaluate_state,
)
from tests.helpers import D, make_strategy_config


def make_state_inputs(**overrides) -> StateInputs:
    return StateInputs(
        contract_bnb=overrides.get("contract_bnb", D("30")),
        contract_max_withdraw_amount=overrides.get("contract_max_withdraw_amount", D("6000")),
        contract_available_balance=overrides.get("contract_available_balance", D("6200")),
        spot_bnb=overrides.get("spot_bnb", D("0.5")),
        spot_usd=overrides.get("spot_usd", D("0")),
        reserved_spot_usd=overrides.get("reserved_spot_usd", D("0")),
        pending_bnb_to_contract=overrides.get("pending_bnb_to_contract", D("0")),
        pending_usd_to_spot=overrides.get("pending_usd_to_spot", D("0")),
        open_buy_remaining_qty=overrides.get("open_buy_remaining_qty", D("0")),
        filled_untransferred_bnb=overrides.get("filled_untransferred_bnb", D("0")),
        consumption_rate=overrides.get("consumption_rate", D("0")),
        previous_state=overrides.get("previous_state", ReplenishmentState.IDLE),
        previous_candidate_state=overrides.get("previous_candidate_state", None),
        candidate_streak=overrides.get("candidate_streak", 0),
        run_mode=overrides.get("run_mode", RunMode.AUTO),
        has_unknown_orders=overrides.get("has_unknown_orders", False),
        has_unknown_transfers=overrides.get("has_unknown_transfers", False),
    )


def test_classify_idle_when_balance_high_and_depletion_far() -> None:
    cfg = make_strategy_config()
    t_depletion = D("100")
    state = classify_candidate_state(D("40"), t_depletion, cfg.thresholds)
    assert state == ReplenishmentState.IDLE


def test_classify_watch_on_72h_boundary() -> None:
    cfg = make_strategy_config()
    state = classify_candidate_state(D("30"), D("72"), cfg.thresholds)
    assert state == ReplenishmentState.WATCH


def test_classify_accumulate_when_below_alert_even_if_depletion_far() -> None:
    cfg = make_strategy_config()
    state = classify_candidate_state(D("27.99"), D("100"), cfg.thresholds)
    assert state == ReplenishmentState.ACCUMULATE


def test_urgent_immediate_when_bnb_hits_low_boundary() -> None:
    cfg = make_strategy_config()
    inputs = make_state_inputs(
        contract_bnb=D("25"),
        previous_state=ReplenishmentState.WATCH,
        consumption_rate=D("0.1"),
    )
    decision = evaluate_state(inputs, cfg)
    assert decision.confirmed_state == ReplenishmentState.URGENT
    assert decision.transitioned is True
    assert decision.candidate_reason == TransitionReason.LOW_BALANCE


def test_t_depletion_none_when_rate_is_zero() -> None:
    assert compute_t_depletion(D("30"), D("25"), D("0")) is None


def test_state_uses_balance_only_when_rate_non_positive() -> None:
    cfg = make_strategy_config()
    inputs = make_state_inputs(contract_bnb=D("29"), consumption_rate=D("0"))
    decision = evaluate_state(inputs, cfg)
    assert decision.t_depletion is None
    assert decision.raw_candidate_state == ReplenishmentState.IDLE


def test_non_urgent_transition_requires_two_confirmations() -> None:
    cfg = make_strategy_config()
    inputs_first = make_state_inputs(
        contract_bnb=D("27.5"),
        previous_state=ReplenishmentState.WATCH,
        previous_candidate_state=ReplenishmentState.IDLE,
        candidate_streak=0,
    )
    first = evaluate_state(inputs_first, cfg)
    assert first.confirmed_state == ReplenishmentState.WATCH
    assert first.candidate_state == ReplenishmentState.ACCUMULATE
    assert first.candidate_streak == 1
    assert first.transitioned is False

    inputs_second = make_state_inputs(
        contract_bnb=D("27.5"),
        previous_state=ReplenishmentState.WATCH,
        previous_candidate_state=first.candidate_state,
        candidate_streak=first.candidate_streak,
    )
    second = evaluate_state(inputs_second, cfg)
    assert second.confirmed_state == ReplenishmentState.ACCUMULATE
    assert second.transitioned is True
    assert second.reason == TransitionReason.HYSTERESIS_CONFIRMED


def test_urgent_exit_requires_low_plus_one_buffer() -> None:
    cfg = make_strategy_config()
    inputs = make_state_inputs(
        contract_bnb=D("25.8"),
        previous_state=ReplenishmentState.URGENT,
        previous_candidate_state=ReplenishmentState.ACCUMULATE,
        candidate_streak=1,
        consumption_rate=D("0"),
    )
    decision = evaluate_state(inputs, cfg)
    assert decision.raw_candidate_state == ReplenishmentState.ACCUMULATE
    assert decision.candidate_state == ReplenishmentState.URGENT
    assert decision.confirmed_state == ReplenishmentState.URGENT
    assert decision.reason == TransitionReason.EXIT_URGENT_BUFFER
