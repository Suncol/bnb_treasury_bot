from __future__ import annotations

import pytest

from core.models import ReplenishmentState
from core.state_machine import (
    classify_candidate_state,
    compute_delta_bnb,
    compute_effective_supply,
    compute_t_depletion,
    evaluate_state,
)
from tests.helpers import D, make_state_inputs, make_strategy_config


@pytest.mark.parametrize(
    ("contract_bnb", "rate", "expected"),
    [
        (D("25"), D("1"), D("0")),
        (D("24"), D("1"), D("0")),
        (D("30"), D("0.01"), D("500")),
    ],
)
def test_compute_t_depletion_boundary_cases(contract_bnb, rate, expected) -> None:
    cfg = make_strategy_config()
    assert compute_t_depletion(contract_bnb, cfg.thresholds.b_low, rate) == expected


@pytest.mark.parametrize("rate", [D("0"), D("-0.01")])
def test_compute_t_depletion_returns_none_for_non_positive_rate(rate) -> None:
    cfg = make_strategy_config()
    assert compute_t_depletion(D("30"), cfg.thresholds.b_low, rate) is None


@pytest.mark.parametrize(
    ("contract_bnb", "t_depletion", "expected"),
    [
        (D("40"), D("1"), ReplenishmentState.IDLE),
        (D("39.99"), D("72"), ReplenishmentState.WATCH),
        (D("28"), D("72"), ReplenishmentState.WATCH),
        (D("28"), D("72.0001"), ReplenishmentState.IDLE),
        (D("28"), None, ReplenishmentState.IDLE),
        (D("27.99"), D("100"), ReplenishmentState.ACCUMULATE),
        (D("25.01"), D("100"), ReplenishmentState.ACCUMULATE),
        (D("25.01"), D("6"), ReplenishmentState.URGENT),
    ],
)
def test_classify_candidate_state_threshold_boundaries(
    contract_bnb, t_depletion, expected
) -> None:
    cfg = make_strategy_config()
    assert (
        classify_candidate_state(contract_bnb, t_depletion, cfg.thresholds) == expected
    )


@pytest.mark.parametrize(
    ("contract_bnb", "rate", "expected_state"),
    [
        (D("32.2"), D("0.1"), ReplenishmentState.WATCH),
        (D("29.8"), D("0.2"), ReplenishmentState.ACCUMULATE),
        (D("25.6"), D("0.1"), ReplenishmentState.URGENT),
    ],
)
def test_t_depletion_exact_time_boundaries(contract_bnb, rate, expected_state) -> None:
    cfg = make_strategy_config()
    inputs = make_state_inputs(
        contract_bnb=contract_bnb,
        consumption_rate=rate,
        previous_state=ReplenishmentState.IDLE,
    )
    decision = evaluate_state(inputs, cfg)
    assert decision.raw_candidate_state == expected_state


def test_state_remains_urgent_even_if_effective_supply_exceeds_target() -> None:
    cfg = make_strategy_config()
    inputs = make_state_inputs(
        contract_bnb=D("24.5"),
        spot_bnb=D("15"),
        filled_untransferred_bnb=D("10"),
        open_buy_remaining_qty=D("10"),
        previous_state=ReplenishmentState.IDLE,
    )
    decision = evaluate_state(inputs, cfg)
    assert decision.confirmed_state == ReplenishmentState.URGENT
    assert decision.delta_bnb == D("0")


def test_pending_intent_does_not_invent_inventory_absent_from_balances() -> None:
    inputs = make_state_inputs(
        contract_bnb=D("24"),
        spot_bnb=D("0.7"),
        pending_bnb_to_contract=D("1.5"),
    )
    effective_supply = compute_effective_supply(inputs, D("0.5"))
    assert effective_supply == D("24.2")


@pytest.mark.parametrize("effective_supply", [D("32"), D("40")])
def test_compute_delta_bnb_clamps_to_zero_at_or_above_target(effective_supply) -> None:
    assert compute_delta_bnb(effective_supply, D("32")) == D("0")
