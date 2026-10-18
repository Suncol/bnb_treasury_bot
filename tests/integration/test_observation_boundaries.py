"""Accepted inventory and settlement evidence must share one durable boundary."""

from dataclasses import replace
from datetime import timedelta
import sqlite3

import pytest

from core.models import Fill, FuturesIncome, OperationKind
from services.runner import Runner
from storage.repository import Repository
from tests.execution_helpers import run, seed_bid, setup
from tests.fake_exchange import FakeExchange
from tests.helpers import D, make_account


@pytest.mark.parametrize("restart", [False, True])
@pytest.mark.parametrize("version", ["contract_bnb_updated_at", "contract_quote_updated_at"])
def test_old_asset_version_cannot_replace_settled_inventory(tmp_path, restart, version):
    ex, repo, runner = setup(tmp_path, FakeExchange(make_account(
        contract_bnb=D("24"), spot_bnb=D("8.5"), spot_usd=D("20000"))))
    run(runner, ex)
    assert ex.account.contract_bnb == 32
    assert not repo.settlement_window_open()
    checkpoint = repo.load("spot_checkpoint")
    writes = len(ex.writes)
    ex.now += timedelta(seconds=31)
    ex.account = replace(ex.account, contract_bnb=D("24"), **{
        version: getattr(checkpoint["account"], version) - timedelta(seconds=1)})
    if restart:
        path, cfg = repo.path, runner.cfg
        repo.close()
        repo = Repository(path)
        runner = Runner(ex, repo, cfg, clock=lambda: ex.now)
    plan = run(runner, ex)
    assert not plan.inventory_valid
    assert repo.load("spot_checkpoint") == checkpoint
    assert len(ex.writes) == writes
    repo.close()


def test_idle_external_spending_with_forward_or_equal_asset_version_is_accepted(tmp_path):
    ex, repo, runner = setup(tmp_path, FakeExchange(make_account(contract_bnb=D("32"))))
    runner.reconciler.refresh(ex.now)
    for advance in (timedelta(seconds=1), timedelta(0)):
        ex.now += timedelta(seconds=30)
        ex.account = replace(ex.account, contract_bnb=ex.account.contract_bnb - D("1"),
                            contract_bnb_updated_at=ex.account.contract_bnb_updated_at + advance)
        account, _, pending, consistent = runner.reconciler.refresh(ex.now)
        assert consistent and not pending.unresolved
        assert repo.load("spot_checkpoint")["account"] == account
    repo.close()


@pytest.mark.parametrize("restart", [False, True])
@pytest.mark.parametrize("offset_ms", [-500, 0])
def test_unchanged_inventory_cannot_replay_old_sell_to_settle_new_buy(tmp_path, restart, offset_ms):
    ex, repo, runner = setup(tmp_path, FakeExchange(make_account(
        contract_bnb=D("24"), spot_bnb=D("0.5"), spot_usd=D("20000"))))
    runner.reconciler.refresh(ex.now)
    epoch = repo.settlement_epoch()
    ex.now += timedelta(seconds=100)
    runner.reconciler.refresh(ex.now)
    # The external round trip has already restored the accepted balances. Only
    # its final SELL will be visible in the next query's one-second overlap.
    ex.fills.append(Fill("BNBUSDT", "external-sell", "external", ex.now + timedelta(milliseconds=offset_ms),
                         D("8"), quote_qty=D("4813.68"), side="SELL"))
    wallet = ex.account
    read = ex.fetch_account_snapshot
    ex.fetch_account_snapshot = lambda: replace(read(), spot_bnb=wallet.spot_bnb, spot_usd=wallet.spot_usd)
    if restart:
        path, cfg = repo.path, runner.cfg
        repo.close()
        repo = Repository(path)
        runner = Runner(ex, repo, cfg, clock=lambda: ex.now)
    run(runner, ex)
    buys = [op for op in ex.writes if op.kind == OperationKind.ORDER]
    assert len(buys) == 1
    assert repo.operation(buys[0].client_id).balance_pending
    assert repo.settlement_window_open()
    assert repo.load("spot_settlement_issue")["ambiguous_receipts"] == ("trade:BNBUSDT:external-sell",)
    assert repo.settlement_epoch() > epoch
    ex.now += timedelta(seconds=11)
    runner.tick()
    assert len(ex.writes) == 1
    repo.close()


def test_unchanged_observation_moves_income_boundary_without_another_audit_event(tmp_path):
    ex, repo, runner = setup(tmp_path)
    runner.reconciler.refresh(ex.now)
    epoch = repo.settlement_epoch()
    ex.now += timedelta(seconds=100)
    runner.reconciler.refresh(ex.now)
    boundary = repo.load("spot_acceptance")
    assert boundary["account"].ts == ex.now
    assert boundary["epoch"] == epoch + 1
    assert repo.db.execute("SELECT COUNT(*) FROM events WHERE kind='EXTERNAL_INVENTORY_ACCEPTED'").fetchone()[0] == 1
    income = FuturesIncome("COMMISSION", "late", "BNB", D("-.01"), ex.now - timedelta(milliseconds=500))
    repo.save_futures_income((income, income))
    assert repo.ambiguous_income((income,)) == (("COMMISSION", "late"),)
    repo.close()


@pytest.mark.parametrize("elapsed", [0, 30])
def test_old_owned_receipt_cannot_cross_new_acceptance(tmp_path, elapsed):
    ex, repo, runner = setup(tmp_path, FakeExchange(make_account(spot_usd=D("5000"))))
    op = seed_bid(ex, repo)
    ex.fill(op.exchange_id, D("1"))
    runner.reconciler.refresh(ex.now)
    assert not repo.settlement_window_open()
    ex.now += timedelta(seconds=elapsed)
    runner.reconciler.refresh(ex.now)
    boundary = repo.load("spot_acceptance")
    assert boundary["epoch"] > op.settlement_epoch
    receipt = runner.reconciler._attribute_fill(replace(ex.fills[0], trade_id="late-owned"))
    repo.save_fills((receipt, receipt))
    assert repo.ambiguous_wallet_events(boundary["cursor"], repo.wallet_cursor()) == ("trade:BNBUSDT:late-owned",)
    assert repo.operation(op.client_id).balance_pending
    repo.close()


def test_unchanged_acceptance_rolls_back_boundary_and_checkpoint_together(tmp_path):
    ex, repo, runner = setup(tmp_path)
    runner.reconciler.refresh(ex.now)
    boundary, checkpoint = repo.load('spot_acceptance'), repo.load('spot_checkpoint')
    ex.now += timedelta(seconds=30)
    repo.db.execute("CREATE TRIGGER reject_checkpoint BEFORE UPDATE ON kv WHEN NEW.key='spot_checkpoint' "
                    "BEGIN SELECT RAISE(ABORT, 'checkpoint fault'); END")
    with pytest.raises(sqlite3.IntegrityError, match='checkpoint fault'):
        runner.reconciler.refresh(ex.now)
    assert repo.load('spot_acceptance') == boundary
    assert repo.load('spot_checkpoint') == checkpoint
    assert not ex.writes
    repo.close()


@pytest.mark.parametrize('restart', [False, True])
@pytest.mark.parametrize('legacy_checkpoint', [False, True])
def test_clock_rollback_cannot_reopen_previously_accepted_history(tmp_path, restart, legacy_checkpoint):
    ex, repo, runner = setup(tmp_path, FakeExchange(make_account(
        contract_bnb=D('24'), spot_bnb=D('.5'), spot_usd=D('20000'))))
    ex.now += timedelta(seconds=100)
    runner.reconciler.refresh(ex.now)
    boundary, checkpoint = repo.load('spot_acceptance'), repo.load('spot_checkpoint')
    if legacy_checkpoint:
        # The prior implementation could update only the checkpoint's time.
        checkpoint = {**checkpoint, 'account': replace(
            checkpoint['account'], ts=ex.now - timedelta(milliseconds=900))}
        repo.save('spot_checkpoint', checkpoint)
    ex.fills.append(Fill('BNBUSDT', 'external-sell', 'external',
                         ex.now - timedelta(milliseconds=250), D('8'),
                         quote_qty=D('4813.68'), side='SELL'))
    wallet, read = ex.account, ex.fetch_account_snapshot
    ex.fetch_account_snapshot = lambda: replace(read(), spot_bnb=wallet.spot_bnb, spot_usd=wallet.spot_usd)
    ex.now -= timedelta(milliseconds=500)
    if restart:
        path, cfg = repo.path, runner.cfg
        repo.close()
        repo = Repository(path)
        runner = Runner(ex, repo, cfg, clock=lambda: ex.now)
    plan = run(runner, ex)
    assert not plan.inventory_valid
    assert repo.load('spot_acceptance') == boundary
    assert repo.load('spot_checkpoint') == checkpoint
    assert not ex.writes and not repo.operations()
    ex.now += timedelta(seconds=12)
    runner.tick()
    buys = [op for op in ex.writes if op.kind == OperationKind.ORDER]
    assert len(buys) == 1
    assert repo.operation(buys[0].client_id).balance_pending
    ex.now += timedelta(seconds=11)
    runner.tick()
    assert len(ex.writes) == 1
    repo.close()
