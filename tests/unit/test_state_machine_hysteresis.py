from __future__ import annotations

import pytest

from core.models import ReplenishmentState, TransitionReason
from core.state_machine import evaluate_state
from tests.helpers import D, make_state_inputs, make_strategy_config


def test_watch_to_accumulate_requires_two_confirmations() -> None:
    cfg = make_strategy_config()
    first = evaluate_state(
        make_state_inputs(
            contract_bnb=D("27.5"),
            previous_state=ReplenishmentState.WATCH,
            previous_candidate_state=ReplenishmentState.IDLE,
            candidate_streak=0,
        ),
        cfg,
    )
    assert first.confirmed_state == ReplenishmentState.WATCH
    assert first.candidate_state == ReplenishmentState.ACCUMULATE
    assert first.candidate_streak == 1
    assert first.transitioned is False

    second = evaluate_state(
        make_state_inputs(
            contract_bnb=D("27.5"),
            previous_state=ReplenishmentState.WATCH,
            previous_candidate_state=first.candidate_state,
            candidate_streak=first.candidate_streak,
        ),
        cfg,
    )
    assert second.confirmed_state == ReplenishmentState.ACCUMULATE
    assert second.candidate_streak == 0
    assert second.transitioned is True
    assert second.reason == TransitionReason.HYSTERESIS_CONFIRMED


def test_candidate_streak_resets_when_candidate_changes() -> None:
    cfg = make_strategy_config()
    decision = evaluate_state(
        make_state_inputs(
            contract_bnb=D("27.5"),
            previous_state=ReplenishmentState.IDLE,
            previous_candidate_state=ReplenishmentState.WATCH,
            candidate_streak=1,
        ),
        cfg,
    )
    assert decision.candidate_state == ReplenishmentState.ACCUMULATE
    assert decision.candidate_streak == 1
    assert decision.confirmed_state == ReplenishmentState.IDLE


def test_candidate_streak_clears_when_candidate_matches_previous_state() -> None:
    cfg = make_strategy_config()
    decision = evaluate_state(
        make_state_inputs(
            contract_bnb=D("30"),
            consumption_rate=(D("30") - D("25")) / D("72"),
            previous_state=ReplenishmentState.WATCH,
            previous_candidate_state=ReplenishmentState.ACCUMULATE,
            candidate_streak=1,
        ),
        cfg,
    )
    assert decision.candidate_state == ReplenishmentState.WATCH
    assert decision.confirmed_state == ReplenishmentState.WATCH
    assert decision.candidate_streak == 0
    assert decision.transitioned is False


def test_urgent_bypasses_confirmation_cycles() -> None:
    cfg = make_strategy_config()
    decision = evaluate_state(
        make_state_inputs(
            contract_bnb=D("25"),
            previous_state=ReplenishmentState.WATCH,
            previous_candidate_state=ReplenishmentState.WATCH,
            candidate_streak=1,
            consumption_rate=D("1"),
        ),
        cfg,
    )
    assert decision.confirmed_state == ReplenishmentState.URGENT
    assert decision.transitioned is True
    assert decision.candidate_streak == 0


@pytest.mark.parametrize(
    ("contract_bnb", "expected_candidate", "expected_confirmed", "expected_streak", "expected_reason"),
    [
        (
            D("25.99"),
            ReplenishmentState.URGENT,
            ReplenishmentState.URGENT,
            0,
            TransitionReason.EXIT_URGENT_BUFFER,
        ),
        (
            D("26.00"),
            ReplenishmentState.ACCUMULATE,
            ReplenishmentState.URGENT,
            1,
            TransitionReason.LOW_BALANCE,
        ),
    ],
)
def test_urgent_exit_buffer_boundaries(
    contract_bnb, expected_candidate, expected_confirmed, expected_streak, expected_reason
) -> None:
    cfg = make_strategy_config()
    decision = evaluate_state(
        make_state_inputs(
            contract_bnb=contract_bnb,
            previous_state=ReplenishmentState.URGENT,
            previous_candidate_state=ReplenishmentState.ACCUMULATE,
            candidate_streak=0,
        ),
        cfg,
    )
    assert decision.candidate_state == expected_candidate
    assert decision.confirmed_state == expected_confirmed
    assert decision.candidate_streak == expected_streak
    assert decision.reason == expected_reason


def test_urgent_exit_after_second_consecutive_above_buffer() -> None:
    cfg = make_strategy_config()
    decision = evaluate_state(
        make_state_inputs(
            contract_bnb=D("26"),
            previous_state=ReplenishmentState.URGENT,
            previous_candidate_state=ReplenishmentState.ACCUMULATE,
            candidate_streak=1,
        ),
        cfg,
    )
    assert decision.candidate_state == ReplenishmentState.ACCUMULATE
    assert decision.confirmed_state == ReplenishmentState.ACCUMULATE
    assert decision.transitioned is True
    assert decision.reason == TransitionReason.HYSTERESIS_CONFIRMED
