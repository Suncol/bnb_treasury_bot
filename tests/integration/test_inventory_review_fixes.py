from dataclasses import replace
from datetime import timedelta

import pytest

from core.models import Fill, OrderPlan
from services.runner import Runner
from storage.repository import Repository
from tests.execution_helpers import seed_bid
from tests.helpers import D
from tests.integration.test_margin_adjusted_target import margin_setup


@pytest.mark.parametrize('same_order_id', [False, True])
def test_foreign_symbol_late_receipt_cannot_cover_new_debit(tmp_path, same_order_id):
    ex, repo, runner = margin_setup(tmp_path, spot_usd='5000')
    op = seed_bid(ex, repo)
    boundary = repo.load('spot_acceptance')
    cfg = replace(runner.cfg, spot_activity_symbols=('BTCUSDT',))
    runner = Runner(ex, repo, cfg, clock=lambda: ex.now)
    ex.fills.append(Fill('BTCUSDT', 'old-btc-trade',
        op.exchange_id if same_order_id else 'foreign-order',
        boundary['account'].ts - timedelta(milliseconds=500), D('0.001'),
        quote_qty=D('100'), base_asset='BTC', quote_asset='USDT'))
    ex.account = replace(ex.account, spot_usd=D('4900'))
    for restart in (False, True):
        if restart:
            path = repo.path
            repo.close()
            repo = Repository(path)
            runner = Runner(ex, repo, cfg, clock=lambda: ex.now)
        account, _, pending, _ = runner.reconciler.refresh(ex.now)
        assert pending.unresolved
        assert repo.load('spot_settlement_issue')['ambiguous_receipts'] == ('trade:BTCUSDT:old-btc-trade',)
        assert repo.load('spot_acceptance') == boundary
        with pytest.raises(RuntimeError, match='reconciled'):
            runner.executor.execute(OrderPlan('BNBUSDT', 'BUY', 'LIMIT_MAKER', D('1'), D('598'), 'GTC', True),
                                    ex.now, repo.runtime().state, account=account)
        assert len(ex.writes) == 1
    repo.close()
