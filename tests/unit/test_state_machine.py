from __future__ import annotations

from core.models import ReplenishmentState, TransitionReason
from core.state_machine import (
    classify_candidate_state,
    evaluate_state,
)
from tests.helpers import D, make_state_inputs, make_strategy_config


def test_classify_idle_when_balance_high_and_depletion_far() -> None:
    cfg = make_strategy_config()
    t_depletion = D("100")
    state = classify_candidate_state(D("40"), t_depletion, cfg.thresholds)
    assert state == ReplenishmentState.IDLE


def test_active_round_keeps_accumulating_on_72h_boundary() -> None:
    cfg = make_strategy_config()
    state = classify_candidate_state(
        D("30"), D("72"), cfg.thresholds, ReplenishmentState.WATCH
    )
    assert state == ReplenishmentState.ACCUMULATE


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


def test_state_uses_balance_only_when_rate_non_positive() -> None:
    cfg = make_strategy_config()
    inputs = make_state_inputs(contract_bnb=D("29"), consumption_rate=D("0"))
    decision = evaluate_state(inputs, cfg)
    assert decision.t_depletion is None
    assert decision.raw_candidate_state == ReplenishmentState.IDLE
