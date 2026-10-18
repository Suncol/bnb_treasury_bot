"""Net BNB fees cannot leave a safe round running with no executable successor."""

from dataclasses import replace

import pytest

from core.models import PaperConfig, ReplenishmentState
from core.replenishment_engine import build_cycle_plan
from services.runner import Runner
from tests.helpers import D, make_account, make_engine_inputs, make_strategy_config
from tests.paper_helpers import paper_setup


@pytest.mark.parametrize('minimum', ['0.1', '0.105'])
@pytest.mark.parametrize('urgent', [False, True])
def test_net_fee_tail_finishes_safe_round_or_explains_urgent_block(tmp_path, minimum, urgent):
    cfg = make_strategy_config()
    if urgent:
        cfg = replace(cfg, thresholds=replace(cfg.thresholds, b_alert=D('25.05'), b_target=D('25.10'), b_high=D('26')))
    contract = D('25') if urgent else D('31.9')
    p = paper_setup(tmp_path / 'dust.db', cfg=cfg, config=replace(PaperConfig(),
        initial_contract_bnb=contract, initial_spot_usd=D('100'), bnb_consumption_per_hour=D('0')))
    try:
        p.market.filters = replace(p.market.filters, min_transfer_bnb=D(minimum))
        p.ex.min_transfer_bnb = D(minimum)
        p.repo.save('runtime', replace(p.repo.runtime(), state=ReplenishmentState.URGENT if urgent else ReplenishmentState.ACCUMULATE))
        runner = Runner(p.ex, p.repo, cfg, clock=p.clock.utc)
        runner.run_once(market=p.feed.quote())
        p.clock.elapse(2)
        p.feed.publish(ask='580', qty='100')
        p.ex.advance()
        plan = runner.run_once(market=p.feed.quote(), inventory_cycle=False)
        assert plan.inventory_valid and not p.repo.operations(unresolved_only=True)
        assert not plan.buy_plan.orders and plan.bnb_transfer_plan is None
        if urgent:
            assert plan.state_decision.confirmed_state == ReplenishmentState.URGENT
            assert any(alert.code == 'INVENTORY_UNAVAILABLE' for alert in plan.alerts)
        else:
            assert plan.state_decision.confirmed_state == ReplenishmentState.IDLE
        account = p.ex.fetch_account_snapshot()
        assert account.contract_bnb + max(account.spot_bnb - cfg.thresholds.b_spot_reserve, D('0')) <= cfg.thresholds.b_target
        writes = len(p.repo.operations())
        for _ in range(48):
            p.clock.elapse(1800)
            p.feed.publish(ask='580', qty='100')
            p.ex.advance()
            runner.run_once(market=p.feed.quote())
        assert len(p.repo.operations()) == writes
    finally:
        p.repo.close()


@pytest.mark.parametrize('case', ['no_dust', 'open_order', 'frozen', 'pending', 'stale'])
def test_tail_completion_does_not_use_potential_or_unverified_supply(case):
    inputs = make_engine_inputs(account=make_account(contract_bnb=D('31.9'), spot_bnb=D('.5999')),
                                previous_state=ReplenishmentState.ACCUMULATE)
    if case == 'no_dust':
        inputs = replace(inputs, account=replace(inputs.account, spot_bnb=D('.5')))
    elif case == 'open_order':
        inputs = replace(inputs, pending=replace(inputs.pending, open_buy_remaining_qty=D('.01')))
    elif case == 'frozen':
        inputs = replace(inputs, account=replace(inputs.account, reserved_spot_bnb=D('.0999')))
    elif case == 'pending':
        inputs = replace(inputs, pending=replace(inputs.pending, has_pending_orders=True))
    else:
        inputs = replace(inputs, snapshot_consistent=False)
    assert build_cycle_plan(inputs, make_strategy_config()).state_decision.confirmed_state != ReplenishmentState.IDLE


def test_transfer_minimum_is_rounded_up_before_completion_and_last_batch():
    cfg = make_strategy_config()
    inputs = make_engine_inputs(account=make_account(contract_bnb=D('31.79'), spot_bnb=D('.61')),
                                previous_state=ReplenishmentState.ACCUMULATE)
    inputs = replace(inputs, filters=replace(inputs.filters, min_transfer_bnb=D('.105')))
    plan = build_cycle_plan(inputs, cfg)
    assert plan.bnb_transfer_plan.amount == D('.11')
    completed = build_cycle_plan(replace(inputs, account=replace(inputs.account, contract_bnb=D('31.895'), spot_bnb=D('.5'))), cfg)
    assert completed.state_decision.confirmed_state == ReplenishmentState.IDLE
