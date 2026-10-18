"""Legacy acquisition evidence belongs to its acceptance, independent of history size."""

from dataclasses import replace
from datetime import timedelta

import pytest

import storage.repository as repository_module
from core.models import Fill, Operation, OperationStatus, OrderPlan, ReplenishmentState
from core.validation import valid_account_snapshot
from services.exchange_adapter import RequestNotSent, RequestRejected
from services.runner import Runner
from storage.codec import dumps
from storage.repository import Repository
from tests.execution_helpers import seed_bid, setup
from tests.fake_exchange import FakeExchange
from tests.helpers import D, make_account
from tests.integration.test_binance_sdk import sdk
from tests.integration.test_read_interleavings import account_reader, legacy_observations


@pytest.mark.parametrize('restart', [False, True])
@pytest.mark.parametrize('rejection', ['not_sent', 'rejected'])
def test_matching_legacy_observations_cannot_borrow_a_previous_acceptance(
    sdk, tmp_path, restart, rejection,
):
    ex = FakeExchange(make_account(contract_bnb=D('24'), spot_bnb=D('.5'), spot_usd=D('10000')))
    ex, repo, runner = setup(tmp_path, ex, state=ReplenishmentState.ACCUMULATE)
    start = ex.now
    plan = OrderPlan('BNBUSDT', 'BUY', 'LIMIT_MAKER', D('.1'), D('598'), 'GTC', True)
    submit = ex.submit

    def create():
        with runner.executor.execution_batch():
            account, _, _, consistent = runner.reconciler.refresh(ex.now)
            assert consistent
            return runner.executor.execute(plan, ex.now, ReplenishmentState.ACCUMULATE, account=account)

    def reject(_):
        if rejection == 'not_sent':
            raise RequestNotSent('Final order budget veto', orders_only=True)
        raise RequestRejected('Order rejected')

    ex.submit = reject
    account_reader(sdk, ex, during_balance=lambda: setattr(ex, 'now', start + timedelta(milliseconds=100)),
                   clock_skew_ms=800, time_read_ms=0)
    old = create()
    assert old.status == OperationStatus.FAILED
    assert old.not_sent == (rejection == 'not_sent')
    assert old.created_at == start + timedelta(milliseconds=100)
    assert old.balance_before.spot_event_upper_bound == start + timedelta(milliseconds=900)

    # The host steps back 100ms. Both service clock offsets remain permissible;
    # service time and acquisition completion still advance between acceptances.
    ex.now = start
    ex.submit = submit
    account_reader(sdk, ex, during_balance=lambda: setattr(ex, 'now', start + timedelta(milliseconds=800)),
                   clock_skew_ms=900, time_read_ms=0)
    current = create()
    assert current.status == OperationStatus.CONFIRMED
    assert current.created_at == start + timedelta(milliseconds=800)
    assert current.balance_before.ts == old.balance_before.ts == start
    assert current.balance_before.spot_event_upper_bound == start + timedelta(milliseconds=1700)
    assert current.settlement_epoch > old.settlement_epoch
    legacy_observations(repo)
    assert repo.operation(old.client_id).balance_before == repo.operation(current.client_id).balance_before
    path, cfg = repo.path, runner.cfg
    if restart:
        repo.close()
        repo = Repository(path)
        runner = Runner(ex, repo, cfg, clock=lambda: ex.now)

    frozen = ex.account
    ex.now = start + timedelta(seconds=3)
    ex.fill(current.exchange_id, D('.1'))
    ex.fills[-1] = replace(ex.fills[-1], ts=start + timedelta(milliseconds=3900))
    # The earlier external SELL was absorbed by the second idle observation.
    # Its delayed receipt cannot offset a new BUY missing from the wallet read.
    ex.fills.append(Fill('BNBUSDT', 'old-sell', 'external', start + timedelta(milliseconds=1600),
                         D('.1'), quote_qty=D('59.8'), side='SELL'))
    ex.now = start + timedelta(seconds=4)
    account_reader(sdk, ex, spot_balance=lambda: frozen, clock_skew_ms=900, time_read_ms=0)
    account, _, pending, consistent = runner.reconciler.refresh(ex.now)
    assert consistent and account.spot_bnb == D('.5') and ex.account.spot_bnb == D('.6')
    assert pending.unresolved and repo.operation(current.client_id).balance_pending
    assert repo.load('spot_settlement_issue')['ambiguous_receipts'] == ('trade:BNBUSDT:old-sell',)
    if restart:
        repo.close()
        repo = Repository(path)
        runner = Runner(ex, repo, cfg, clock=lambda: ex.now)
    ex.now += timedelta(milliseconds=100)
    with pytest.raises(RuntimeError, match='New operations require an unpaused, reconciled runtime'):
        create()
    assert len(ex.writes) == 1
    assert len(repo.operations()) == 2
    repo.close()


@pytest.mark.parametrize('history', [0, 6000, 36500])
def test_legacy_reconciliation_work_is_independent_of_cold_history(tmp_path, monkeypatch, history):
    ex, repo, runner = setup(tmp_path, FakeExchange(replace(
        make_account(contract_bnb=D('27'), spot_usd=D('10000')),
        contract_total_margin_balance=D('6000'), contract_margin_asset='USDT')),
        state=ReplenishmentState.ACCUMULATE)
    active = seed_bid(ex, repo)
    legacy_observations(repo)
    before = repo.operation(active.client_id).balance_before
    # Failed submissions within this still-open generation can have a later
    # observation; its reserved USD must include the existing GTC.
    ex.now += timedelta(seconds=1)
    cold_before = replace(ex.fetch_account_snapshot(), wallet_event_cursor=repo.wallet_cursor(),
                          futures_event_upper_bound=None, spot_event_upper_bound=None)
    cold = Operation('cold', active.kind, active.scope, active.payload, cold_before.ts,
                     status=OperationStatus.FAILED, balance_before=cold_before, not_sent=True,
                     settlement_epoch=active.settlement_epoch)
    assert not cold.unresolved and valid_account_snapshot(cold_before, cold.created_at, runner.cfg)
    previous_epoch = replace(cold, balance_before=before, created_at=before.ts,
                             settlement_epoch=active.settlement_epoch - 1)
    with repo.db:
        repo.db.executemany(
            'INSERT INTO operations(client_id,scope,status,created,data,kind,active,unresolved) '
            'VALUES (?,?,?,?,?,?,0,0)',
            ((f'cold-{i}', op.scope, op.status.value, op.created_at.timestamp(),
              dumps(replace(op, client_id=f'cold-{i}')), op.kind.value)
             for i in range(history) for op in (cold if i % 2 else previous_epoch,)),
        )
    path, cfg = repo.path, runner.cfg
    loads = repository_module.loads
    # A deterministic work budget, not a machine-dependent elapsed-time limit.
    # This fails quickly on full-history decoding even with the largest fixture.
    decoded = []

    def bounded_loads(value):
        result = loads(value)
        if isinstance(result, Operation):
            decoded.append(result.client_id)
            assert len(decoded) <= 150, 'Reconciliation decoded cold operation history'
            ex.now += timedelta(milliseconds=1)
        return result

    monkeypatch.setattr(repository_module, 'loads', bounded_loads)
    for restart in (False, True):
        if restart:
            decoded.clear()
            # Opening a pre-index schema-2 journal builds the index without
            # rewriting its records or decoding unrelated operations in Python.
            repo.db.execute('DROP INDEX operations_observation')
            repo.close()
            repo = Repository(path)
            runner = Runner(ex, repo, cfg, clock=lambda: ex.now)
        for _ in range(2):
            decoded.clear()
            queries = []
            repo.db.set_trace_callback(queries.append)
            ex.now += timedelta(seconds=2)
            with runner.executor.execution_batch():
                account, _, pending, consistent = runner.reconciler.refresh(ex.now)
                assert consistent and not pending.unresolved
                assert valid_account_snapshot(account, ex.now, cfg)
                assert runner.executor.execution_qualified(ex.now, account=account)
            assert decoded and not any(key.startswith('cold-') for key in decoded)
            repo.db.set_trace_callback(None)
            lookups = [q for q in queries if q.startswith('SELECT data FROM operations WHERE kind!=')]
            assert lookups
            for query in lookups:
                plan = ' '.join(row[3] for row in repo.db.execute('EXPLAIN QUERY PLAN ' + query))
                assert 'SEARCH operations USING INDEX operations_observation' in plan
                assert 'TEMP B-TREE' not in plan
            assert repo.operations(active_only=True)[0].client_id == active.client_id
    assert len(ex.writes) == 1
    repo.close()
