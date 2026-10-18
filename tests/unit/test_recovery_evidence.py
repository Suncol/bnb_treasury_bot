from dataclasses import replace
from datetime import timedelta
import sqlite3

import pytest

from core.models import FuturesIncome, Operation, OperationKind, OperationStatus, OrderPlan
from core.time_utils import utc
from services.executor import Executor
from storage.repository import Repository
from tests.recovery_helpers import funded, initialize, transfer
from tests.helpers import D, make_account, make_strategy_config


@pytest.mark.parametrize('terminal', ['retired', 'failed', 'not_sent'])
def test_original_intent_keeps_legacy_acquisition_bound_after_terminal_result(tmp_path, terminal):
    path = tmp_path / 'legacy-bound.db'
    repo = Repository(path)
    before = replace(make_account(), wallet_event_cursor=0)
    repo.initialize_spot_checkpoint(before, {'reason': 'quiescent fixture'})
    repo.accept_inventory_observation(before)
    op = Operation('original', OperationKind.ORDER, 'order:BNBUSDT',
                   OrderPlan('BNBUSDT', 'BUY', 'LIMIT_MAKER', D('1'), D('598'), 'GTC', True),
                   before.ts + timedelta(seconds=3), balance_before=before,
                   settlement_epoch=repo.settlement_epoch())
    repo.record_intent(op, repo.runtime())
    expected = utc(op.created_at) + timedelta(seconds=1)
    for wallet in ('spot', 'futures'):
        assert repo.account_event_boundary(before, wallet) == expected
    repo.update_operation(replace(op, status=(OperationStatus.CONFIRMED if terminal == 'retired'
                                              else OperationStatus.FAILED),
                                  not_sent=terminal == 'not_sent'))
    if terminal == 'retired':
        repo.retire_order(op.client_id)
    assert not repo.operations(active_only=True)
    repo.close()
    repo = Repository(path)
    assert repo.operation(op.client_id).balance_before == before
    for wallet in ('spot', 'futures'):
        assert repo.account_event_boundary(before, wallet) == expected
    repo.close()


def test_transfer_status_receipt_and_wallet_event_commit_atomically(tmp_path):
    ex = funded()
    repo = Repository(tmp_path / "receipt.db")
    initialize(repo, ex)
    op = transfer(repo, ex, "BNB", "USDⓈ-M Futures", "2")
    result = ex.query_operation(op)
    confirmed = replace(op, status=result.status, checked_at=ex.now)
    repo.db.execute("CREATE TRIGGER reject_result BEFORE UPDATE ON operations BEGIN SELECT RAISE(ABORT,'injected'); END")
    with pytest.raises(sqlite3.IntegrityError, match="injected"):
        Executor(ex, repo, make_strategy_config()).record_result(confirmed, transfer=result.transfer)
    assert repo.operation(op.client_id) == op
    assert repo.transfer_record(op.exchange_id) is None
    assert repo.wallet_cursor() == 0
    repo.db.execute("DROP TRIGGER reject_result")
    Executor(ex, repo, make_strategy_config()).record_result(confirmed, transfer=result.transfer)
    assert repo.transfer_record(op.exchange_id) == result.transfer
    assert repo.wallet_cursor() == 1
    with pytest.raises(ValueError, match="identity"):
        Executor(ex, repo, make_strategy_config()).record_result(confirmed, transfer=replace(result.transfer, amount=D("3")))
    repo.close()


def test_futures_income_identity_is_scoped_by_type_and_conflicts_rollback(tmp_path):
    repo = Repository(tmp_path / "income.db")
    ex = funded()
    first = FuturesIncome("COMMISSION", "1", "BNB", D("-0.01"), ex.now)
    other = replace(first, income_type="REALIZED_PNL", amount=D("1"))
    repo.save_futures_income((first, other, first))
    assert len(repo.futures_income_since(ex.now)) == 2
    with pytest.raises(ValueError, match="Conflicting"):
        repo.save_futures_income((replace(first, transaction_id="2"), replace(first, asset="USDT")))
    assert len(repo.futures_income_since(ex.now)) == 2
    repo.close()


def test_v1_migration_preserves_checkpoint_and_legacy_intent(tmp_path):
    ex, path = funded(), tmp_path / "v1.db"
    repo = Repository(path)
    initialize(repo, ex)
    op = transfer(repo, ex, "BNB", "USDⓈ-M Futures", "2")
    baseline = repo.load("spot_checkpoint")
    repo.db.execute("DROP TABLE futures_income")
    repo.db.execute("PRAGMA user_version=1")
    repo.close()
    repo = Repository(path)
    assert repo.db.execute("PRAGMA user_version").fetchone()[0] == 2
    assert repo.operation(op.client_id) == op
    assert repo.load("spot_checkpoint") == baseline
    assert repo.operations(unresolved_only=True) == (op,)
    repo.close()
