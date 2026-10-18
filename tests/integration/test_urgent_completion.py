"""Round completion must take precedence over the ordinary URGENT exit buffer."""

from dataclasses import replace
from pathlib import Path

import pytest

from core.config import load_config
from core.models import ReplenishmentState, TransitionReason
from core.replenishment_engine import build_cycle_plan
from core.state_machine import evaluate_state
from tests.helpers import D, make_account, make_engine_inputs, make_state_inputs

ROOT = Path(__file__).resolve().parents[2]


def narrow_config(tmp_path, target, high):
    source = (ROOT / 'config/strategy.toml').read_text()
    source = source.replace('b_alert = 28.0', 'b_alert = 25.2')
    source = source.replace('b_target = 32.0', f'b_target = {target}')
    source = source.replace('b_high = 40.0', f'b_high = {high}')
    path = tmp_path / 'strategy.toml'
    path.write_text(source)
    return load_config(path)


@pytest.mark.parametrize('target,high,balance,tolerance', [
    ('25.5', '26', '25.5', '0'),
    ('25.5', '25.8', '25.8', '0'),
    ('26.04', '27', '25.95', '.1'),
    ('26', '27', '26', '0'),
    ('32', '40', '32', '0'),
])
def test_r4_completed_round_exits_urgent(tmp_path, target, high, balance, tolerance):
    cfg = narrow_config(tmp_path, target, high)
    inputs = make_state_inputs(contract_bnb=D(balance), previous_state=ReplenishmentState.URGENT)
    trace = []
    for _ in range(10):
        decision = evaluate_state(inputs, cfg, target_tolerance=D(tolerance))
        trace.append((decision.raw_candidate_state.value, decision.confirmed_state.value, str(decision.delta_bnb)))
        assert decision.confirmed_state == ReplenishmentState.IDLE, trace
        assert decision.reason == TransitionReason.HIGH_BALANCE
        assert decision.candidate_streak == 0
        inputs = replace(inputs, previous_state=decision.confirmed_state,
                         previous_candidate_state=decision.candidate_state,
                         candidate_streak=decision.candidate_streak)
    assert decision.confirmed_state == ReplenishmentState.IDLE, trace


def test_r4_completed_round_sweeps_idle_usd(tmp_path):
    cfg = narrow_config(tmp_path, '25.5', '26')
    account = make_account(contract_bnb=D('25.5'), spot_usd=D('2000'))
    inputs = make_engine_inputs(account=account, previous_state=ReplenishmentState.URGENT)
    completed = build_cycle_plan(inputs, cfg)
    control = build_cycle_plan(replace(inputs, previous_state=ReplenishmentState.IDLE), cfg)
    assert completed.inventory_valid and control.inventory_valid
    assert completed.state_decision.confirmed_state == ReplenishmentState.IDLE
    assert not completed.buy_plan.orders and completed.state_decision.delta_bnb == 0
    assert completed.spot_usd_sweep_plan == control.spot_usd_sweep_plan
    assert completed.spot_usd_sweep_plan.amount == D('1500')


@pytest.mark.parametrize('invalid', ['stale', 'future', 'inconsistent', 'pending_bnb',
                                    'unknown_order', 'unknown_transfer', 'unsettled_fill'])
def test_round_completion_cannot_bypass_invalid_or_unsettled_inventory(tmp_path, invalid):
    from datetime import timedelta

    cfg = narrow_config(tmp_path, '25.5', '26')
    inputs = make_engine_inputs(account=make_account(contract_bnb=D('25.5'), spot_usd=D('2000')),
                               previous_state=ReplenishmentState.URGENT,
                               previous_candidate_state=ReplenishmentState.ACCUMULATE, candidate_streak=1)
    if invalid in {'stale', 'future'}:
        inputs = replace(inputs, account=replace(inputs.account,
            ts=inputs.now + timedelta(seconds=-11 if invalid == 'stale' else 1)))
    elif invalid == 'inconsistent':
        inputs = replace(inputs, snapshot_consistent=False)
    else:
        pending = {
            'pending_bnb': {'pending_bnb_to_contract': D('.5')},
            'unknown_order': {'has_unknown_orders': True},
            'unknown_transfer': {'has_unknown_transfers': True},
            'unsettled_fill': {'has_pending_orders': True},
        }[invalid]
        inputs = replace(inputs, pending=replace(inputs.pending, **pending))
    plan = build_cycle_plan(inputs, cfg)
    assert not plan.inventory_valid
    assert plan.state_decision.confirmed_state == ReplenishmentState.URGENT
    assert plan.state_decision.candidate_streak == 1
    assert not plan.buy_plan.orders and not plan.transfer_decision.allow
    assert plan.spot_usd_sweep_plan is None and plan.bnb_transfer_plan is None


def test_exact_tolerance_boundary_keeps_urgent_exit_buffer(tmp_path):
    cfg = narrow_config(tmp_path, '25.5', '26')
    inputs = make_engine_inputs(account=make_account(contract_bnb=D('25.4')),
                               previous_state=ReplenishmentState.URGENT)
    plan = build_cycle_plan(inputs, cfg)
    assert plan.inventory_valid
    assert plan.state_decision.raw_candidate_state == ReplenishmentState.ACCUMULATE
    assert plan.state_decision.confirmed_state == ReplenishmentState.URGENT
    assert plan.state_decision.reason == TransitionReason.EXIT_URGENT_BUFFER
