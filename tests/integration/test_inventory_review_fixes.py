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


@pytest.mark.parametrize('restart', [False, True])
def test_retired_order_recovers_late_terminal_fill_by_original_identity(tmp_path, restart):
    ex, repo, runner = margin_setup(tmp_path, spot_usd='5000')
    # Model only open orders freezing cash; terminal queries remain available.
    ex.fetch_account_snapshot = lambda: replace(ex.account, ts=ex.now,
        contract_total_margin_balance=ex.account.contract_quote_balance, contract_margin_asset='USDT',
        reserved_spot_usd=sum((o.remaining_qty * o.price for o in ex.orders.values() if o.is_open), D('0')))
    op = seed_bid(ex, repo, qty='2')
    ex.fill(op.exchange_id, D('1'))
    ex.orders[op.exchange_id] = replace(ex.orders[op.exchange_id], status='CANCELED')
    executed_at = ex.now
    assert not runner.reconciler.refresh(ex.now)[2].unresolved
    assert not repo.settlement_window_open()
    boundary = repo.load('spot_acceptance')
    # A trade executed at cancellation is published later together with the
    # corrected terminal quantity and both wallet sides, not a new execution.
    ex.now += timedelta(seconds=1)
    ex.orders[op.exchange_id] = replace(ex.orders[op.exchange_id], filled_qty=D('1.5'))
    ex.fills.append(Fill('BNBUSDT', 'late-terminal', op.exchange_id, executed_at,
                        D('.5'), quote_qty=D('299')))
    ex.account = replace(ex.account, spot_bnb=ex.account.spot_bnb + D('.5'),
                         spot_usd=ex.account.spot_usd - D('299'))
    assert runner.reconciler.refresh(ex.now)[2].unresolved
    assert repo.operation(op.client_id).balance_pending
    if restart:
        path = repo.path
        repo.close()
        repo = Repository(path)
        runner = Runner(ex, repo, runner.cfg, clock=lambda: ex.now)
    assert not runner.reconciler.refresh(ex.now)[2].unresolved
    assert repo.tracked_order(op.client_id).filled_qty == D('1.5')
    assert not repo.settlement_window_open()
    assert repo.load('spot_acceptance') == boundary
    assert repo.operation(op.client_id).exchange_id == op.exchange_id
    assert repo.budget_used(ex.now, 'USDT') == 0
    assert len(ex.writes) == 1
    # Repeated receipts and observations do not create another execution.
    assert not runner.reconciler.refresh(ex.now)[2].unresolved
    assert len(repo.fills_since(executed_at)) == 2
    assert len(ex.writes) == 1
    repo.close()


@pytest.mark.parametrize('receipt', [False, True])
def test_settled_funding_shortfall_exits_without_source_proof_and_waits_for_next_cycle(tmp_path, receipt):
    from core.models import OperationKind, SliceState
    from tests.execution_helpers import run
    from tests.integration.test_replenishment_continuation import funded_setup, external_spend
    ex, repo, runner, funding = funded_setup(tmp_path)
    repo.save('runtime', replace(repo.runtime(), slice_state=SliceState(True, ex.now + timedelta(minutes=30))))
    run(runner, ex, inventory=False)
    assert not repo.operation(funding.client_id).unresolved
    assert not repo.settlement_window_open()
    external_spend(ex, funding.payload.amount, receipt=receipt)
    runner.reconciler.refresh(ex.now)
    assert repo.settlement_epoch() > funding.settlement_epoch
    assert repo.runtime().resume_replenishment  # Baseline acceptance alone has no exit side effect.
    ex.now += timedelta(minutes=31)
    run(runner, ex, inventory=False)
    assert not repo.runtime().resume_replenishment
    assert repo.runtime().funding_exhausted_at is not None
    assert len(ex.writes) == 1
    from storage.codec import loads
    exits = [loads(row[0]) for row in repo.db.execute(
        "SELECT data FROM events WHERE kind='FUNDING_INSUFFICIENT_FUNDS'")]
    assert len(exits) == 1
    assert exits[0]['reason'] == 'INSUFFICIENT_AVAILABLE_FUNDS'
    assert exits[0]['available_quote'] == 0
    assert not repo.db.execute("SELECT 1 FROM events WHERE kind='FUNDING_EXTERNAL_USAGE'").fetchone()
    path = repo.path
    repo.close()
    repo = Repository(path)
    runner = Runner(ex, repo, runner.cfg, clock=lambda: ex.now)
    ex.now += timedelta(hours=25)
    for _ in range(2):
        run(runner, ex, inventory=False)
    assert len(ex.writes) == 1  # Restart, elapsed time and budget release cannot refill.
    run(runner, ex)
    assert sum(o.kind == OperationKind.TRANSFER and o.payload.to_account == 'SPOT' for o in ex.writes) == 2
    repo.close()


def test_idle_shortfall_needs_no_external_spend_or_order_history(tmp_path):
    from tests.execution_helpers import run
    from tests.integration.test_replenishment_continuation import funded_setup, external_spend
    ex, repo, runner, funding = funded_setup(tmp_path)
    assert not runner.reconciler.refresh(ex.now)[2].unresolved
    external_spend(ex, funding.payload.amount, receipt=False)
    def forbidden(*args):
        pytest.fail('Idle funding exit must not require external provenance')
    ex.fetch_recent_fills = ex.fetch_futures_income = forbidden
    read_orders = ex.fetch_open_orders
    ex.fetch_open_orders = lambda symbol: read_orders(symbol) if symbol == 'BNBUSDT' else forbidden()
    assert run(runner, ex, inventory=False).inventory_valid
    assert not repo.runtime().resume_replenishment
    assert len(ex.writes) == 1
    repo.close()


@pytest.mark.parametrize('gate', ['cooldown', 'budget', 'granularity', 'ratio', 'absolute'])
def test_next_inventory_after_current_shortfall_retains_funding_gates(tmp_path, gate):
    from tests.execution_helpers import run
    from tests.integration.test_replenishment_continuation import funded_setup, external_spend
    ex, repo, runner, funding = funded_setup(tmp_path)
    assert not runner.reconciler.refresh(ex.now)[2].unresolved
    external_spend(ex, funding.payload.amount, receipt=False)
    run(runner, ex, inventory=False)
    assert not repo.runtime().resume_replenishment
    assert len(ex.writes) == 1
    ex.now += timedelta(minutes=30) if gate == 'budget' else timedelta(hours=25)
    cfg = runner.cfg
    if gate == 'cooldown':
        repo.save('runtime', replace(repo.runtime(), read_retry_at=ex.now + timedelta(minutes=5)))
    elif gate in {'granularity', 'ratio', 'absolute'}:
        ex.account = replace(ex.account, contract_max_withdraw_amount={
            'granularity': D('4000'), 'ratio': D('10000'), 'absolute': D('4500')}[gate])
        if gate == 'absolute':
            cfg = replace(cfg, risk=replace(cfg.risk, alpha_warn=D('1'), alpha_crit=D('1')))
    path = repo.path
    repo.close()
    repo = Repository(path)
    runner = Runner(ex, repo, cfg, clock=lambda: ex.now)
    plan = run(runner, ex)
    if gate == 'cooldown':
        assert plan is None
        assert repo.runtime().funding_exhausted_at is not None
    else:
        assert plan.inventory_valid and not plan.transfer_decision.allow
    assert len(ex.writes) == 1
    assert not repo.operations(unresolved_only=True)
    repo.close()
