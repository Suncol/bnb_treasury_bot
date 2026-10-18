from dataclasses import replace
from datetime import timedelta
import sqlite3

import pytest

from core.models import OperationKind, ReplenishmentState, Fill, OperationStatus, TransferRecord
from services.qualification import reconciliation_status
from services.runner import Runner
from storage.repository import Repository
from tests.execution_helpers import setup, seed_bid
from tests.fake_exchange import FakeExchange
from tests.helpers import D, make_account, make_strategy_config


@pytest.mark.parametrize("field,value", [("spot_usd", "4500"), ("spot_usd", "6000"), ("spot_bnb", "1.5")])
def test_idle_external_spot_balance_becomes_new_baseline_and_next_order_settles(tmp_path, field, value):
    exchange, repo, runner = setup(tmp_path, FakeExchange(make_account(
        contract_bnb=D("27"), spot_usd=D("5000"))), ReplenishmentState.ACCUMULATE)
    exchange.account = replace(exchange.account, **{field: D(value)})
    account, _, pending, consistent = runner.reconciler.refresh(exchange.now)
    assert consistent and not pending.unresolved
    assert repo.settlement_epoch() == 1
    assert repo.load("spot_checkpoint")["account"] == account
    assert repo.budget_used(exchange.now, "USDT") == 0
    op = seed_bid(exchange, repo)
    assert op.settlement_epoch == repo.settlement_epoch()
    exchange.fill(op.exchange_id, D("1"))
    runner.reconciler.refresh(exchange.now)
    assert not repo.operation(op.client_id).unresolved
    assert not repo.settlement_window_open()
    assert len(exchange.writes) == 1
    repo.close()


def test_open_gtc_rejects_balance_first_and_delayed_order_views_after_restart(tmp_path):
    exchange, repo, runner = setup(tmp_path, FakeExchange(make_account(spot_usd=D("5000"))))
    op = seed_bid(exchange, repo, qty="2")
    runner.reconciler.refresh(exchange.now)
    assert not repo.operations(unresolved_only=True)
    old_order = exchange.orders[op.exchange_id]
    exchange.fill(op.exchange_id, D("1"))
    read = exchange.fetch_open_orders
    exchange.fetch_open_orders = lambda symbol: (old_order,)
    exchange.hide_fills = True
    for restart in (False, True):
        if restart:
            repo.close()
            repo = Repository(tmp_path / "test.sqlite3")
            runner = Runner(exchange, repo, make_strategy_config(), clock=lambda: exchange.now)
        assert runner.reconciler.refresh(exchange.now)[2].unresolved
        assert repo.settlement_epoch() == op.settlement_epoch
        assert reconciliation_status(repo).strict_window
    exchange.fetch_open_orders = read
    exchange.hide_fills = False
    assert not runner.reconciler.refresh(exchange.now)[2].unresolved
    assert repo.settlement_window_open()
    assert len(exchange.writes) == 1
    repo.close()


def test_retired_order_contradictory_fill_cannot_be_external_inventory(tmp_path):
    exchange, repo, runner = setup(tmp_path, FakeExchange(make_account(spot_usd=D("5000"))))
    op = seed_bid(exchange, repo)
    exchange.fill(op.exchange_id, D("1"))
    runner.reconciler.refresh(exchange.now)
    assert not repo.settlement_window_open()
    exchange.fills.append(replace(exchange.fills[0], trade_id="late"))
    exchange.account = replace(exchange.account, spot_bnb=exchange.account.spot_bnb+D("1"))
    assert not runner.reconciler.refresh(exchange.now)[3]
    assert repo.operation(op.client_id).balance_pending
    assert repo.settlement_epoch() == op.settlement_epoch
    repo.close()


def test_external_acceptance_is_atomic_and_does_not_send_on_storage_failure(tmp_path):
    exchange, repo, runner = setup(tmp_path, FakeExchange(make_account(spot_usd=D("5000"))))
    before = repo.load("spot_checkpoint")
    exchange.account = replace(exchange.account, spot_usd=D("5100"))
    repo.db.execute("CREATE TRIGGER reject_acceptance BEFORE INSERT ON events WHEN NEW.kind='EXTERNAL_INVENTORY_ACCEPTED' BEGIN SELECT RAISE(ABORT, 'fault'); END")
    with pytest.raises(sqlite3.IntegrityError):
        runner.reconciler.refresh(exchange.now)
    assert repo.load("spot_checkpoint") == before
    assert repo.settlement_epoch() == 0 and not exchange.writes
    repo.close()


def test_external_pending_transfer_blocks_acceptance(tmp_path):
    exchange, repo, runner = setup(tmp_path)
    exchange.transfers["external"] = TransferRecord("external", "USDT", D("500"),
        "USDⓈ-M Futures", "SPOT", exchange.now, OperationStatus.PENDING)
    exchange.account = replace(exchange.account, spot_usd=D("500"))
    assert runner.reconciler.refresh(exchange.now)[2].unresolved
    assert repo.settlement_epoch() == 0
    repo.close()


def test_new_window_does_not_replay_ambiguous_old_external_fill(tmp_path):
    exchange, repo, runner = setup(tmp_path, FakeExchange(make_account(spot_usd=D("5000"))))
    exchange.account = replace(exchange.account, spot_usd=D("5100"))
    runner.reconciler.refresh(exchange.now)
    op = seed_bid(exchange, repo)
    accepted = repo.load("spot_acceptance")
    exchange.fills.append(Fill("BNBUSDT", "old", "external", exchange.now, D("1"), quote_qty=D("600")))
    assert runner.reconciler.refresh(exchange.now)[2].unresolved
    assert repo.load("spot_settlement_issue")["ambiguous_receipts"]
    assert repo.load("spot_acceptance") == accepted
    assert len(exchange.writes) == 1
    repo.close()


def test_imported_retired_owned_fill_reopens_responsibility_before_polling(tmp_path):
    exchange, repo, runner = setup(tmp_path, FakeExchange(make_account(spot_usd=D("5000"))))
    op = seed_bid(exchange, repo)
    exchange.fill(op.exchange_id, D("1"))
    runner.reconciler.refresh(exchange.now)
    assert not repo.settlement_window_open()
    receipt = runner.reconciler._attribute_fill(replace(exchange.fills[0], trade_id="imported"))
    repo.save_fills((receipt,))
    assert repo.operation(op.client_id).balance_pending
    exchange.account = replace(exchange.account, spot_bnb=exchange.account.spot_bnb+D("1"))
    assert not runner.reconciler.refresh(exchange.now)[3]
    assert repo.settlement_epoch() == op.settlement_epoch
    repo.close()
