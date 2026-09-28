from __future__ import annotations

import pytest

from core.models import ReplenishmentState, TransitionReason
from core.state_machine import classify_candidate_state, evaluate_state
from tests.helpers import D, make_state_inputs, make_strategy_config


@pytest.mark.parametrize(
    ("contract_bnb", "consumption_rate", "previous_state", "expected_candidate", "expected_candidate_reason"),
    [
        (
            D("25"),
            D("1"),
            ReplenishmentState.WATCH,
            ReplenishmentState.URGENT,
            TransitionReason.LOW_BALANCE,
        ),
        (
            D("25.6"),
            D("0.1"),
            ReplenishmentState.WATCH,
            ReplenishmentState.URGENT,
            TransitionReason.DEPLETION_6H,
        ),
        (
            D("29.8"),
            D("0.2"),
            ReplenishmentState.WATCH,
            ReplenishmentState.ACCUMULATE,
            TransitionReason.DEPLETION_24H,
        ),
        (
            D("32.2"),
            D("0.1"),
            ReplenishmentState.IDLE,
            ReplenishmentState.WATCH,
            TransitionReason.DEPLETION_72H,
        ),
        (
            D("40"),
            D("1"),
            ReplenishmentState.WATCH,
            ReplenishmentState.IDLE,
            TransitionReason.HIGH_BALANCE,
        ),
    ],
)
def test_candidate_reason_matches_threshold_path(
    contract_bnb, consumption_rate, previous_state, expected_candidate, expected_candidate_reason
) -> None:
    cfg = make_strategy_config()
    decision = evaluate_state(
        make_state_inputs(
            contract_bnb=contract_bnb,
            consumption_rate=consumption_rate,
            previous_state=previous_state,
        ),
        cfg,
    )
    assert decision.raw_candidate_state == expected_candidate
    assert decision.candidate_reason == expected_candidate_reason


def test_reason_becomes_hysteresis_confirmed_on_second_non_urgent_transition() -> None:
    cfg = make_strategy_config()
    decision = evaluate_state(
        make_state_inputs(
            contract_bnb=D("30"),
            consumption_rate=(D("30") - D("25")) / D("72"),
            previous_state=ReplenishmentState.IDLE,
            previous_candidate_state=ReplenishmentState.WATCH,
            candidate_streak=1,
        ),
        cfg,
    )
    assert decision.confirmed_state == ReplenishmentState.WATCH
    assert decision.reason == TransitionReason.HYSTERESIS_CONFIRMED


def test_reason_becomes_exit_urgent_buffer_when_buffer_blocks_exit() -> None:
    cfg = make_strategy_config()
    decision = evaluate_state(
        make_state_inputs(
            contract_bnb=D("25.8"),
            previous_state=ReplenishmentState.URGENT,
            previous_candidate_state=ReplenishmentState.ACCUMULATE,
            candidate_streak=1,
        ),
        cfg,
    )
    assert decision.raw_candidate_state == ReplenishmentState.ACCUMULATE
    assert decision.candidate_state == ReplenishmentState.URGENT
    assert decision.reason == TransitionReason.EXIT_URGENT_BUFFER


def test_monotonicity_by_depletion_time_does_not_relax_when_time_shortens() -> None:
    cfg = make_strategy_config()
    ordered_states = [
        classify_candidate_state(D("30"), D("100"), cfg.thresholds),
        classify_candidate_state(D("30"), D("72"), cfg.thresholds),
        classify_candidate_state(D("30"), D("24"), cfg.thresholds),
        classify_candidate_state(D("30"), D("6"), cfg.thresholds),
    ]
    urgency_rank = {
        ReplenishmentState.IDLE: 0,
        ReplenishmentState.WATCH: 1,
        ReplenishmentState.ACCUMULATE: 2,
        ReplenishmentState.URGENT: 3,
    }
    numeric = [urgency_rank[state] for state in ordered_states]
    assert numeric == sorted(numeric)
