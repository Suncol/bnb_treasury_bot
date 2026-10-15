from __future__ import annotations

from dataclasses import replace

import pytest

from core.state_machine import compute_delta_bnb, compute_effective_supply
from tests.helpers import D, make_state_inputs


@pytest.fixture
def supply_inputs():
    return make_state_inputs(
        contract_bnb=D("24"),
        spot_bnb=D("2.5"),
        pending_bnb_to_contract=D("1"),
        open_buy_remaining_qty=D("3"),
        filled_untransferred_bnb=D("2"),
    )


def test_effective_supply_does_not_double_count_pending_bnb_to_contract(supply_inputs) -> None:
    effective = compute_effective_supply(supply_inputs, D("0.5"))
    assert effective == D("31")


def test_filled_untransferred_bnb_reduces_delta_bnb(supply_inputs) -> None:
    base_inputs = replace(supply_inputs, filled_untransferred_bnb=D("0"))
    richer_inputs = replace(supply_inputs, filled_untransferred_bnb=D("2"))
    base_delta = compute_delta_bnb(compute_effective_supply(base_inputs, D("0.5")), D("32"))
    richer_delta = compute_delta_bnb(compute_effective_supply(richer_inputs, D("0.5")), D("32"))
    assert richer_delta < base_delta


def test_open_buy_remaining_qty_can_only_reduce_delta(supply_inputs) -> None:
    no_orders = replace(supply_inputs, open_buy_remaining_qty=D("0"))
    with_orders = replace(supply_inputs, open_buy_remaining_qty=D("3"))
    no_orders_delta = compute_delta_bnb(compute_effective_supply(no_orders, D("0.5")), D("32"))
    with_orders_delta = compute_delta_bnb(compute_effective_supply(with_orders, D("0.5")), D("32"))
    assert with_orders_delta <= no_orders_delta


def test_spot_transferable_bnb_zero_when_spot_below_reserve(supply_inputs) -> None:
    inputs = replace(supply_inputs, spot_bnb=D("0.4"), pending_bnb_to_contract=D("0"))
    effective = compute_effective_supply(inputs, D("0.5"))
    assert effective == D("29")
