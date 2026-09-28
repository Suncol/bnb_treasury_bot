from dataclasses import replace
from datetime import timedelta
import sqlite3

import pytest

from core.models import AssetTransferPlan, OperationKind, OperationStatus, RunMode
from services.runner import Runner
from storage.repository import Repository
from tests.fake_exchange import FakeExchange
from tests.helpers import D, make_account, make_strategy_config
from tests.integration.test_execution import setup
from tests.integration.test_execution_boundaries import seed_bid


@pytest.mark.parametrize("stale_field", ["contract_bnb", "spot_bnb"])
def test_confirmed_bnb_transfer_waits_for_both_balances_across_restart(tmp_path, stale_field):
    exchange, repo, runner = setup(tmp_path, FakeExchange(make_account(
        contract_bnb=D("24"), spot_bnb=D("8.5"), spot_usd=D("5000"),
    )))
    before = exchange.fetch_account_snapshot()
    read = exchange.fetch_account_snapshot
    exchange.fetch_account_snapshot = lambda: replace(
        read(), **{stale_field: getattr(before, stale_field)}
    ) if exchange.transfers else read()
    runner.run_once()
    op = repo.operations()[0]
    assert op.status == OperationStatus.CONFIRMED and op.unresolved
    assert len(exchange.writes) == 1 and op.payload.asset == "BNB"
    repo.close()
    repo = Repository(tmp_path / "test.sqlite3")
    runner = Runner(exchange, repo, make_strategy_config(), clock=lambda: exchange.now)
    for _ in range(3):
        exchange.now += timedelta(seconds=11)
        assert runner.run_once() is not None
        assert len(exchange.writes) == 1
        assert repo.operations(unresolved_only=True)
    exchange.fetch_account_snapshot = read
    runner.run_once()
    assert not repo.operations(unresolved_only=True)
    assert not any(o.kind == OperationKind.ORDER for o in exchange.writes)
    assert exchange.account.contract_bnb == D("32")
    repo.close()


@pytest.mark.parametrize("to_spot", [True, False])
@pytest.mark.parametrize("stale_field", ["spot_usd", "contract_quote_balance"])
def test_usd_settlement_uses_both_wallets_and_blocks_direct_resubmission(tmp_path, to_spot, stale_field):
    exchange, repo, runner = setup(tmp_path, FakeExchange(make_account(spot_usd=D("5000"))))
    before = exchange.fetch_account_snapshot()
    source, destination = ("USDⓈ-M Futures", "SPOT") if to_spot else ("SPOT", "USDⓈ-M Futures")
    transfer = AssetTransferPlan("USDT", D("500"), source, destination, "test")
    runner.executor.execute(transfer, exchange.now, repo.runtime().state, account=before)
    read = exchange.fetch_account_snapshot
    exchange.fetch_account_snapshot = lambda: replace(
        read(), **{stale_field: getattr(before, stale_field)}
    )
    for _ in range(2):
        exchange.now += timedelta(seconds=11)
        assert runner.reconciler.refresh(exchange.now)[2].unresolved
        with pytest.raises(RuntimeError, match="reconciled"):
            runner.executor.execute(transfer, exchange.now, repo.runtime().state)
    exchange.fetch_account_snapshot = read
    assert not runner.reconciler.refresh(exchange.now)[2].unresolved
    assert len(exchange.writes) == 1
    assert repo.budget_used(exchange.now, "USDT") == (D("500") if to_spot else 0)
    repo.close()


def test_later_futures_fee_revision_releases_gate_but_elapsed_time_does_not(tmp_path):
    exchange, repo, runner = setup(tmp_path, FakeExchange(make_account(spot_bnb=D("8.5"))))
    transfer = AssetTransferPlan("BNB", D("8"), "SPOT", "USDⓈ-M Futures", "test")
    runner.executor.execute(transfer, exchange.now, repo.runtime().state)
    exchange.account = replace(
        exchange.account, contract_bnb=exchange.account.contract_bnb - D("0.01"),
        contract_bnb_updated_at=exchange.now,
    )
    assert runner.reconciler.refresh(exchange.now)[2].unresolved
    confirmed_at = repo.operations()[0].checked_at
    exchange.now += timedelta(seconds=11)
    assert runner.reconciler.refresh(exchange.now)[2].unresolved
    assert repo.operations()[0].checked_at == confirmed_at
    exchange.account = replace(exchange.account, contract_bnb_updated_at=exchange.now)
    assert not runner.reconciler.refresh(exchange.now)[2].unresolved
    assert len(exchange.writes) == 1
    repo.close()


@pytest.mark.parametrize("asset,to_spot,commission_asset", [
    ("BNB", False, "BNB"), ("USDT", True, "USDT"), ("USDT", False, "BNB"),
])
def test_fill_and_commission_during_transfer_are_reconciled_once(tmp_path, asset, to_spot, commission_asset):
    exchange, repo, runner = setup(tmp_path, FakeExchange(make_account(
        spot_bnb=D("3.5"), spot_usd=D("5000"),
    )))
    order = seed_bid(exchange, repo, qty="2")
    runner.reconciler.refresh(exchange.now)
    exchange.hold_transfers = True
    source, destination = ("USDⓈ-M Futures", "SPOT") if to_spot else ("SPOT", "USDⓈ-M Futures")
    transfer = AssetTransferPlan(asset, D("1") if asset == "BNB" else D("500"), source, destination, "test")
    op = runner.executor.execute(transfer, exchange.now, repo.runtime().state)
    exchange.now += timedelta(seconds=2)
    exchange.fill(order.exchange_id, D("0.5"))
    commission = D("0.001")
    exchange.fills[-1] = replace(exchange.fills[-1], commission=commission, commission_asset=commission_asset)
    field = "spot_bnb" if commission_asset == "BNB" else "spot_usd"
    exchange.account = replace(exchange.account, **{field: getattr(exchange.account, field) - commission})
    exchange.settle(op.exchange_id)
    for _ in range(2):
        _, _, pending, consistent = runner.reconciler.refresh(exchange.now)
        assert consistent and not pending.unresolved
    assert len(repo.fills_since(order.created_at)) == 1
    assert len(exchange.writes) == 2
    repo.close()


@pytest.mark.parametrize("manual", [False, True])
def test_failed_transfer_result_clears_continuation_and_counts_once(tmp_path, manual):
    exchange, repo, runner = setup(tmp_path)
    exchange.hold_transfers = True
    exchange.lose_response = OperationKind.TRANSFER
    runner.run_once()
    op = repo.operations()[0]
    exchange.transfers["1"] = replace(exchange.transfers["1"], status=OperationStatus.FAILED)
    if manual:
        runner.reconciler.bind_transfer_id(op.client_id, "1", exchange.now)
    else:
        repo.update_operation(replace(op, exchange_id="1"))
        runner.reconciler.refresh(exchange.now)
    assert not repo.runtime().resume_replenishment
    assert repo.runtime().transfer_failures == 1
    assert repo.runtime().run_mode == RunMode.AUTO
    assert not repo.operations(unresolved_only=True)
    repo.close()
    repo = Repository(tmp_path / "test.sqlite3")
    runner = Runner(exchange, repo, make_strategy_config(), clock=lambda: exchange.now)
    exchange.hold_transfers = False
    exchange.now += timedelta(minutes=31)
    runner.run_once()
    assert len(exchange.writes) > 1
    assert any(o.kind == OperationKind.ORDER for o in exchange.writes)
    repo.close()


def test_manual_failed_binding_commits_runtime_and_operation_atomically(tmp_path):
    exchange, repo, runner = setup(tmp_path)
    exchange.hold_transfers = True
    exchange.lose_response = OperationKind.TRANSFER
    runner.run_once()
    op = repo.operations()[0]
    exchange.transfers["1"] = replace(exchange.transfers["1"], status=OperationStatus.FAILED)
    repo.db.execute("""CREATE TRIGGER reject_runtime BEFORE UPDATE ON kv
        WHEN NEW.key='runtime' BEGIN SELECT RAISE(ABORT,'injected failure'); END""")
    with pytest.raises(sqlite3.IntegrityError, match="injected"):
        runner.reconciler.bind_transfer_id(op.client_id, "1", exchange.now)
    assert repo.operations()[0] == op
    assert repo.runtime().resume_replenishment
    repo.db.execute("DROP TRIGGER reject_runtime")
    runner.reconciler.bind_transfer_id(op.client_id, "1", exchange.now)
    assert repo.operations()[0].status == OperationStatus.FAILED
    assert not repo.runtime().resume_replenishment
    repo.close()


def test_failed_pending_funding_can_replan_in_the_same_reconciliation_tick(tmp_path):
    exchange, repo, runner = setup(tmp_path)
    exchange.hold_transfers = True
    runner.run_once()
    assert repo.runtime().resume_replenishment
    exchange.transfers["1"] = replace(exchange.transfers["1"], status=OperationStatus.FAILED)
    exchange.hold_transfers = False
    exchange.now += timedelta(seconds=11)
    runner.run_once(inventory_cycle=False)
    funding = [o for o in exchange.writes if o.kind == OperationKind.TRANSFER and o.payload.to_account == "SPOT"]
    assert len(funding) == 2
    assert any(o.kind == OperationKind.ORDER for o in exchange.writes)
    repo.close()
