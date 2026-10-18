"""Restore databases exported by dc8f3f and recover with the current controller."""
import json
import sqlite3
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest

from core.models import OperationKind, OperationStatus, OrderPlan, ReplenishmentState
from services import cli
from services.qualification import reconciliation_status
from services.reconciliation import Reconciler
from services.runner import Runner
from storage.codec import loads
from storage.repository import Repository
from tests.execution_helpers import setup, seed_bid
from tests.fake_exchange import FakeExchange
from tests.helpers import D, make_account, make_strategy_config

FIXTURES = json.loads((Path(__file__).parents[1] / 'fixtures/legacy_inventory.json').read_text())


def restore(tmp_path, case):
    data = FIXTURES['cases'][case]
    path = tmp_path / 'legacy.db'
    db = sqlite3.connect(path)
    db.executescript(data['sql'])
    db.close()
    state = loads(data['exchange'])
    exchange = FakeExchange(state['account'])
    for field, value in state.items():
        setattr(exchange, field, value)
    repo = Repository(path)
    return exchange, repo, Reconciler(exchange, repo, make_strategy_config(), clock=lambda: exchange.now)


def reopen(repo, exchange):
    path = repo.path
    repo.close()
    repo = Repository(path)
    return repo, Reconciler(exchange, repo, make_strategy_config(), clock=lambda: exchange.now)


@pytest.mark.parametrize('case', tuple(FIXTURES['cases']))
def test_real_baseline_databases_preserve_identity_budget_and_runtime_on_open(tmp_path, case):
    exchange, repo, reconciler = restore(tmp_path, case)
    assert FIXTURES['source_commit'] == 'dc8f3f3438d47f0ea75748e799ea8e35ca7faaf6'
    assert 'settlement_epoch' not in FIXTURES['cases'][case]['sql']
    operations, runtime = repo.operations(), repo.runtime()
    budget = repo.budget_used(exchange.now, 'USDT')
    assert all(op.settlement_epoch == 0 for op in operations)
    for _ in range(2):
        repo, reconciler = reopen(repo, exchange)
        assert repo.operations() == operations and repo.runtime() == runtime
        assert repo.budget_used(exchange.now, 'USDT') == budget
        assert repo.settlement_epoch() == 0
    repo.close()


def test_idle_legacy_source_errors_clear_only_after_valid_current_observation(tmp_path):
    ex, repo, reconciler = restore(tmp_path, 'idle')
    before, runtime = repo.load('spot_checkpoint'), repo.runtime()
    read = ex.fetch_account_snapshot
    ex.fetch_account_snapshot = lambda: replace(read(), ts=ex.now-timedelta(seconds=11))
    assert reconciler.refresh(ex.now)[2].unresolved
    assert repo.load('spot_checkpoint') == before and repo.settlement_epoch() == 0
    repo, reconciler = reopen(repo, ex)
    ex.fetch_account_snapshot = read
    assert not reconciler.refresh(ex.now)[2].unresolved
    assert not reconciliation_status(repo).blocked
    assert repo.runtime() == runtime and repo.settlement_epoch() == 1
    for epoch in (2, 3):
        repo, reconciler = reopen(repo, ex)
        ex.now += timedelta(seconds=1)
        reconciler.refresh(ex.now)
        assert repo.settlement_epoch() == epoch
    assert not ex.writes
    repo.close()


@pytest.mark.parametrize('case', ['open', 'partial'])
def test_legacy_open_gtc_preserves_strict_window_and_fill_identity(tmp_path, case):
    ex, repo, reconciler = restore(tmp_path, case)
    op = repo.operations()[0]
    baseline = repo.load('spot_checkpoint')
    original = ex.account
    ex.account = replace(original, spot_usd=original.spot_usd+D('1'))
    for _ in range(2):
        assert reconciler.refresh(ex.now)[2].unresolved
        assert repo.load('spot_checkpoint') == baseline
        assert repo.settlement_epoch() == 0
        repo, reconciler = reopen(repo, ex)
    ex.account = original
    ex.fill(op.exchange_id, ex.orders[op.exchange_id].remaining_qty)
    assert not reconciler.refresh(ex.now)[2].unresolved
    assert not repo.settlement_window_open()
    assert repo.fill_totals('BNBUSDT',(op.exchange_id,))[op.exchange_id] == D('1')
    assert len(ex.writes) == 1
    repo.close()


@pytest.mark.parametrize('case', ['order_unknown', 'cancel_unknown'])
def test_legacy_unknown_order_and_cancel_recover_same_identity_without_resubmit(tmp_path, case):
    ex, repo, reconciler = restore(tmp_path, case)
    identities = {op.client_id for op in repo.operations()}
    writes = len(ex.writes)
    if case == 'order_unknown':
        read_order, read_open = ex.fetch_order, ex.fetch_open_orders
        ex.fetch_order, ex.fetch_open_orders = lambda *a, **k: None, lambda *a: ()
        assert reconciler.refresh(ex.now)[2].unresolved
        repo, reconciler = reopen(repo, ex)
        assert repo.settlement_epoch() == 0
        ex.fetch_order, ex.fetch_open_orders = read_order, read_open
    reconciler.refresh(ex.now)
    assert {op.client_id for op in repo.operations()} == identities
    assert not repo.operations(unresolved_only=True)
    assert len(ex.writes) == writes
    repo.close()


@pytest.mark.parametrize('case', ['bnb', 'funding', 'sweep'])
def test_legacy_three_transfer_directions_wait_for_income_across_restart(tmp_path, case):
    ex, repo, reconciler = restore(tmp_path, case)
    op = repo.operations()[0]
    income, ex.income = ex.income, []
    assert reconciler.refresh(ex.now)[2].unresolved
    assert repo.operation(op.client_id).balance_pending
    assert repo.settlement_epoch() == 0
    repo, reconciler = reopen(repo, ex)
    ex.income = income
    assert not reconciler.refresh(ex.now)[2].unresolved
    assert repo.operation(op.client_id).exchange_id == op.exchange_id
    assert not repo.settlement_window_open() and len(ex.writes) == 1
    assert repo.budget_used(ex.now, 'USDT') == (D('500') if case == 'funding' else 0)
    repo.close()


def test_legacy_missing_transfer_identity_requires_explicit_verified_binding(tmp_path):
    ex, repo, reconciler = restore(tmp_path, 'funding_unknown')
    op = repo.operations()[0]
    for _ in range(2):
        reconciler.refresh(ex.now)
        assert repo.operation(op.client_id).status == OperationStatus.UNKNOWN
        assert 'transfer_identity' in reconciliation_status(repo).responsibilities[0]['missing_recovery_evidence']
        assert repo.settlement_epoch() == 0
        repo, reconciler = reopen(repo, ex)
    reconciler.bind_transfer_id(op.client_id, next(iter(ex.transfers)), ex.now)
    assert not reconciler.refresh(ex.now)[2].unresolved
    assert len(ex.writes) == 1 and repo.budget_used(ex.now,'USDT') == D('500')
    repo.close()


@pytest.mark.parametrize("missing", ["snapshot", "timestamp"])
def test_legacy_missing_transfer_baseline_remains_blocked_and_diagnosed(tmp_path, missing):
    ex, repo, reconciler = restore(tmp_path, 'bnb')
    op = repo.operations()[0]
    repo.update_operation(replace(op, balance_before=None if missing == "snapshot"
                                  else replace(op.balance_before, ts=None)))
    repo, reconciler = reopen(repo, ex)
    assert reconciler.refresh(ex.now)[2].unresolved
    status = reconciliation_status(repo)
    assert 'pre_transfer_baseline' in status.responsibilities[0]['missing_recovery_evidence']
    assert status.futures_settlement[op.client_id]['reason'] == 'Missing pre-transfer futures baseline'
    assert repo.settlement_epoch() == 0 and len(ex.writes) == 1
    repo.close()


def test_legacy_retired_order_late_receipt_reopens_without_duplicate_guard_credit(tmp_path):
    ex, repo, reconciler = restore(tmp_path, 'retired')
    op = repo.operations()[0]
    receipt = replace(ex.fills[0], trade_id='late-contradiction')
    ex.fills.append(receipt)
    ex.account = replace(ex.account, spot_bnb=ex.account.spot_bnb+receipt.qty)
    assert not reconciler.refresh(ex.now)[3]
    total = repo.load('episode:legacy-protection').filled_bnb
    repo, reconciler = reopen(repo, ex)
    assert not reconciler.refresh(ex.now)[3]
    assert repo.operation(op.client_id).balance_pending
    assert repo.load('episode:legacy-protection').filled_bnb == total
    assert repo.settlement_epoch() == 0 and len(ex.writes) == 1
    repo.close()


def test_status_is_readonly_for_real_legacy_database_and_does_not_create_missing_database(tmp_path, monkeypatch, capsys):
    ex, repo, reconciler = restore(tmp_path, 'funding_unknown')
    path = Path(repo.path)
    before = '\n'.join(repo.db.iterdump())
    repo.close()
    monkeypatch.setattr(cli, 'BinanceAdapter', lambda *a, **k: pytest.fail('status must remain offline'))
    monkeypatch.setattr(cli.os.sys, 'argv', ['cli','--db',str(path),'--status'])
    cli.main()
    status = loads(capsys.readouterr().out)['reconciliation']
    assert status['observation_mode'] == 'strict_window'
    assert status['responsibilities'][0]['missing_recovery_evidence'] == ('transfer_identity',)
    db = sqlite3.connect(path)
    assert '\n'.join(db.iterdump()) == before
    db.close()
    missing = tmp_path / 'missing.db'
    monkeypatch.setattr(cli.os.sys, 'argv', ['cli','--db',str(missing),'--status'])
    with pytest.raises(sqlite3.OperationalError): cli.main()
    assert not missing.exists()


@pytest.mark.parametrize('phase', ['acceptance','intent','sent','receipt','settlement','close'])
def test_inventory_boundary_crash_matrix_recovers_without_duplicate_writes(tmp_path, phase):
    ex, repo, runner = setup(tmp_path, FakeExchange(make_account(spot_usd=D('5000'))))
    runner.reconciler.refresh(ex.now)
    initial_epoch = repo.settlement_epoch()
    if phase == 'sent': ex.lose_response = OperationKind.ORDER
    trigger = {
        'acceptance': "BEFORE INSERT ON events WHEN NEW.kind='EXTERNAL_INVENTORY_ACCEPTED'",
        'intent': 'BEFORE INSERT ON operations',
        'receipt': 'BEFORE INSERT ON fills',
        'settlement': "BEFORE UPDATE ON kv WHEN NEW.key='spot_checkpoint'",
        'close': 'BEFORE UPDATE ON operations WHEN NEW.active=0 AND OLD.active=1',
    }
    if phase in {'receipt','settlement','close'}:
        op = seed_bid(ex, repo)
        ex.fill(op.exchange_id,D('1'))
        initial_epoch = op.settlement_epoch
    if phase in trigger:
        repo.db.execute(f"CREATE TRIGGER fail_boundary {trigger[phase]} BEGIN SELECT RAISE(ABORT,'boundary failure'); END")
        repo.db.commit()
        with pytest.raises(sqlite3.IntegrityError,match='boundary failure'):
            if phase == 'intent': seed_bid(ex,repo)
            else:
                if phase == 'acceptance': ex.account=replace(ex.account,spot_usd=D('5100'))
                runner.reconciler.refresh(ex.now)
        if phase in {'receipt','settlement','close'}:
            assert repo.settlement_window_open()
        else:
            assert not repo.operations() and not ex.writes
        if phase == 'intent':
            # Preparation accepts another observation before the intent fails.
            assert repo.settlement_epoch() > initial_epoch
        else:
            assert repo.settlement_epoch() == initial_epoch
    else:
        op=runner.executor.execute(OrderPlan('BNBUSDT','BUY','LIMIT_MAKER',D('1'),D('598'),'GTC',True),ex.now,ReplenishmentState.ACCUMULATE)
        assert op.status == OperationStatus.UNKNOWN and len(ex.writes)==1
    identities={op.client_id for op in repo.operations()}
    repo,reconciler=reopen(repo,ex)
    if phase in trigger:
        repo.db.execute('DROP TRIGGER fail_boundary');repo.db.commit()
    reconciler.refresh(ex.now)
    for _ in range(2):
        repo,reconciler=reopen(repo,ex)
        reconciler.refresh(ex.now)
    assert {op.client_id for op in repo.operations()}==identities
    assert len(ex.writes)==(0 if phase in {'acceptance','intent'} else 1)
    assert not repo.operations(unresolved_only=True)
    assert repo.settlement_window_open()==(phase=='sent')
    repo.close()


def test_legacy_urgent_requires_two_valid_fixed_threshold_cycles_across_restart(tmp_path):
    ex, repo, reconciler = restore(tmp_path,'urgent')
    runner=Runner(ex,repo,make_strategy_config(),clock=lambda:ex.now,execute=False)
    read=ex.fetch_account_snapshot
    ex.fetch_account_snapshot=lambda:replace(read(),ts=ex.now-timedelta(seconds=11))
    runner.run_once()
    assert repo.runtime().candidate_streak==0
    ex.fetch_account_snapshot=read
    runner.run_once()
    assert repo.runtime().state==ReplenishmentState.URGENT
    assert repo.runtime().candidate_streak==1
    repo,_=reopen(repo,ex)
    runner=Runner(ex,repo,make_strategy_config(),clock=lambda:ex.now,execute=False)
    ex.now+=timedelta(minutes=30)
    runner.run_once()
    assert repo.runtime().state==ReplenishmentState.ACCUMULATE
    assert not ex.writes
    repo.close()


def test_legacy_unsent_failure_and_aged_unknown_reservation_survive_observation(tmp_path):
    ex,repo,reconciler=restore(tmp_path,'not_sent')
    before=repo.operations()[0]
    for _ in range(2):
        reconciler.refresh(ex.now)
        assert repo.operation(before.client_id)==before
        assert before.not_sent and before.status==OperationStatus.FAILED
    repo.close()
    other=tmp_path/'unknown';other.mkdir()
    ex,repo,reconciler=restore(other,'funding_unknown')
    ex.now+=timedelta(hours=30)
    reconciler.refresh(ex.now)
    repo,reconciler=reopen(repo,ex)
    assert repo.budget_used(ex.now,'USDT')==D('500')
    assert repo.runtime().resume_replenishment
    assert repo.operations(unresolved_only=True) and len(ex.writes)==1
    repo.close()
