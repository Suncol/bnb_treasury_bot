from dataclasses import replace
from datetime import timedelta

import pytest

from core.models import AssetTransferPlan, FuturesIncome, OrderPlan, ReplenishmentState
from services.exchange_adapter import ExchangeError
from services.qualification import reconciliation_status
from tests.execution_helpers import seed_bid
from tests.helpers import D
from tests.integration.test_margin_adjusted_target import margin_setup


@pytest.mark.parametrize('kind', ['loss', 'capital', 'spot'])
def test_idle_inventory_and_first_order_need_no_old_income(tmp_path, kind):
    ex, repo, runner = margin_setup(tmp_path, spot_usd='10000')
    if kind == 'spot':
        ex.account = replace(ex.account, spot_usd=D('9000'))
    else:
        ex.futures_change('USDT', D('-1000') if kind == 'loss' else D('1000'),
                          'REALIZED_PNL' if kind == 'loss' else 'INTERNAL_TRANSFER')
    def forbidden(*args):
        pytest.fail('Idle inventory must not query old futures income')
    ex.fetch_futures_income = forbidden
    plan = runner.run_once()
    assert plan.inventory_valid and repo.runtime().last_inventory_at is not None
    assert runner.cached_inputs.consumption_rate is None
    assert runner.cached_inputs.margin_change_24h is None
    assert len(ex.writes) == 3
    assert all(op.settlement_epoch == 1 for op in ex.writes)
    assert repo.settlement_window_open() and not repo.operations(unresolved_only=True)
    assert repo.load('spot_acceptance')['account'].contract_quote_balance == ex.account.contract_quote_balance
    repo.close()


def test_idle_direct_execution_replaces_legacy_audit_errors_after_complete_observation(tmp_path):
    ex, repo, runner = margin_setup(tmp_path, spot_usd='10000')
    repo.save('futures_settlement_issue', {'margin': {'reason': 'old missing history'},
                                        'quote': {'reason': 'old source audit'}})
    repo.save('spot_settlement_issue', {'reason': 'old unexplained credit'})
    ex.account = replace(ex.account, spot_usd=D('11000'))
    op = runner.executor.execute(OrderPlan('BNBUSDT', 'BUY', 'LIMIT_MAKER', D('1'), D('598'), 'GTC', True),
                                 ex.now, ReplenishmentState.ACCUMULATE)
    assert op.balance_before.spot_usd == D('11000')
    assert not repo.load('futures_settlement_issue')
    assert len(ex.writes) == 1
    repo.close()


def test_same_external_futures_change_inside_open_gtc_still_requires_income(tmp_path):
    ex, repo, runner = margin_setup(tmp_path, spot_usd='5000')
    op = seed_bid(ex, repo)
    runner.reconciler.refresh(ex.now)
    boundary = repo.load('spot_acceptance')
    ex.futures_change('USDT', D('-10'), 'REALIZED_PNL')
    missing = ex.income.pop()
    for _ in range(2):
        assert runner.reconciler.refresh(ex.now)[2].unresolved
        assert repo.load('spot_acceptance') == boundary
        assert reconciliation_status(repo).futures_settlement
    ex.income.append(missing)
    assert not runner.reconciler.refresh(ex.now)[2].unresolved
    assert repo.operation(op.client_id).exchange_id == op.exchange_id
    assert len(ex.writes) == 1
    repo.close()


def test_late_income_cannot_replay_across_accepted_boundary(tmp_path):
    ex, repo, runner = margin_setup(tmp_path, spot_usd='5000')
    # The accepted wallet version is older than the observation; a late receipt
    # in that gap has no causal proof of whether the accepted wallet included it.
    op = seed_bid(ex, repo)
    boundary = repo.load('spot_acceptance')
    ex.income.append(FuturesIncome('COMMISSION', 'late', 'USDT', D('-1'),
                                  boundary['account'].ts - timedelta(milliseconds=500)))
    ex.futures_change('USDT', D('-1'), 'COMMISSION')
    runner.reconciler.refresh(ex.now)
    assert repo.load('futures_settlement_issue')['quote']['ambiguous_receipts']
    assert repo.load('spot_acceptance') == boundary
    assert len(ex.writes) == 1 and repo.operation(op.client_id).balance_pending
    repo.close()


def test_budget_read_failure_cannot_authorize_idle_funding(tmp_path):
    ex, repo, runner = margin_setup(tmp_path)
    def failure(*args):
        raise ExchangeError('budget unavailable')
    ex.fetch_recent_transfers = failure
    with pytest.raises(ExchangeError, match='budget unavailable'):
        runner.executor.execute(AssetTransferPlan('USDT', D('500'), 'USDⓈ-M Futures', 'SPOT', 'fund'),
                                ex.now, ReplenishmentState.ACCUMULATE)
    assert not ex.writes and not repo.operations()
    assert repo.settlement_epoch() == 0
    repo.close()


def test_late_external_trade_after_acceptance_cannot_exhaust_old_funding(tmp_path):
    from core.models import Fill
    from tests.integration.test_replenishment_continuation import funded_setup, external_spend
    ex, repo, runner, funding = funded_setup(tmp_path)
    runner.reconciler.refresh(ex.now)
    assert not repo.settlement_window_open()
    external_spend(ex, funding.payload.amount, receipt=False)
    spent_at = ex.now
    runner.reconciler.refresh(ex.now)
    assert repo.settlement_epoch() > funding.settlement_epoch
    # Import after acceptance, with a timestamp after funding but before the
    # accepted balance. A local insertion cursor cannot prove current coverage.
    repo.save_fills((Fill('BTCUSDT', 'late-import', 'external', spent_at, D('0.1'),
                         quote_qty=funding.payload.amount, base_asset='BTC', quote_asset='USDT'),))
    runner.run_once(inventory_cycle=False)
    assert repo.runtime().resume_replenishment
    assert repo.runtime().funding_exhausted_at is None
    assert len(ex.writes) == 1
    repo.close()


def test_current_external_freeze_remains_verifiable_after_acceptance(tmp_path):
    from tests.integration.test_replenishment_continuation import funded_setup, external_freeze
    ex, repo, runner, funding = funded_setup(tmp_path)
    runner.reconciler.refresh(ex.now)
    external_freeze(ex, funding.payload.amount)
    runner.reconciler.refresh(ex.now)
    assert repo.settlement_epoch() > funding.settlement_epoch
    runner.run_once(inventory_cycle=False)
    assert not repo.runtime().resume_replenishment
    assert repo.runtime().funding_exhausted_at is not None
    assert len(ex.writes) == 1
    repo.close()
