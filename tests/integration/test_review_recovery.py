"""Convergence tests derived from the September review's failure inputs."""

from dataclasses import replace
from datetime import timedelta

import pytest

from core.models import AssetTransferPlan, CancelPlan, Fill, OperationKind, OperationStatus, OrderPlan, ReplenishmentState
from services.exchange_adapter import ExchangeError
from services.executor import Executor, MAX_CANCEL_ATTEMPTS
from services.reconciliation import Reconciler
from services.runner import Runner
from storage.repository import Repository
from tests.fake_exchange import FakeExchange
from tests.helpers import D, make_account, make_strategy_config


def funded(cls=FakeExchange):
    return cls(make_account(contract_bnb=D("24"), contract_max_withdraw_amount=D("50000"),
                            spot_bnb=D("10"), spot_usd=D("3000")))


def initialize(repo, ex):
    repo.initialize_spot_checkpoint(ex.fetch_account_snapshot(), {
        "operator": "offline-test", "reason": "quiescent simulation",
    })


def refresh(repo, ex, cfg=None):
    return Reconciler(ex, repo, cfg or make_strategy_config()).refresh(ex.now)


def transfer(repo, ex, asset, to_account, amount):
    source = "SPOT" if to_account != "SPOT" else "USDⓈ-M Futures"
    return Executor(ex, repo, make_strategy_config()).execute(
        AssetTransferPlan(asset, D(amount), source, to_account, "recovery test"),
        ex.now, ReplenishmentState.URGENT, account=ex.fetch_account_snapshot(),
    )


@pytest.mark.parametrize("failure", ["before_send", "after_send", "crash", "429"])
def test_lost_cancel_recovers_the_same_intent_after_restart(tmp_path, failure):
    class LostCancel(FakeExchange):
        attempts = 0

        def submit(self, op):
            if op.kind == OperationKind.CANCEL:
                self.attempts += 1
                if self.attempts == 1:
                    if failure == "crash":
                        raise SystemExit("intent committed before process death")
                    if failure == "after_send":
                        super().submit(op)
                    if failure == "429":
                        raise ExchangeError("rate limited", http_status=429, retry_after=120)
                    raise TimeoutError("lost cancel")
            return super().submit(op)

    ex, path, cfg = funded(LostCancel), tmp_path / "cancel.db", make_strategy_config()
    repo = Repository(path)
    initialize(repo, ex)
    executor = Executor(ex, repo, cfg)
    buy = executor.execute(OrderPlan(cfg.symbol, "BUY", "LIMIT", D("1"), D("590"), "GTC", False),
                           ex.now, ReplenishmentState.WATCH)
    refresh(repo, ex)
    cancel = CancelPlan(cfg.symbol, buy.exchange_id, "protection")
    if failure == "crash":
        with pytest.raises(SystemExit):
            executor.execute(cancel, ex.now, ReplenishmentState.WATCH)
    else:
        executor.execute(cancel, ex.now, ReplenishmentState.WATCH)
    original = repo.cancel_operation(cfg.symbol, buy.exchange_id)
    repo.close()
    repo = Repository(path)
    executor = Executor(ex, repo, cfg)
    if failure == "429":
        ex.now += timedelta(seconds=119)
        executor.execute(cancel, ex.now, ReplenishmentState.WATCH)
        assert ex.attempts == 1
    ex.now += timedelta(seconds=10)
    # Retried cancel can race with a fill. Settlement must still cover both assets.
    if failure != "after_send":
        ex.cancel_fill_qty = D("0.25")
    runner = Runner(ex, repo, cfg, clock=lambda: ex.now)
    runner.run_once(inventory_cycle=False)
    assert not ex.orders[buy.exchange_id].is_open
    assert ex.attempts == (1 if failure == "after_send" else 2)
    recovered = repo.cancel_operation(cfg.symbol, buy.exchange_id)
    assert recovered.client_id == original.client_id
    assert recovered.status == OperationStatus.CONFIRMED
    assert not refresh(repo, ex)[2].unresolved
    assert sum(f.qty for f in repo.fills_since(buy.created_at)) == (0 if failure == "after_send" else D("0.25"))
    assert len([o for o in repo.operations() if o.kind == OperationKind.CANCEL]) == 1
    repo.close()


def test_cancel_attempts_are_bounded_and_audited_without_releasing_exposure(tmp_path):
    class OfflineCancel(FakeExchange):
        attempts = 0

        def submit(self, op):
            if op.kind == OperationKind.CANCEL:
                self.attempts += 1
                raise TimeoutError()
            return super().submit(op)

    ex, cfg = funded(OfflineCancel), make_strategy_config()
    repo = Repository(tmp_path / "bounded.db")
    initialize(repo, ex)
    executor = Executor(ex, repo, cfg)
    buy = executor.execute(OrderPlan(cfg.symbol, "BUY", "LIMIT", D("1"), D("590"), "GTC", False),
                           ex.now, ReplenishmentState.WATCH)
    refresh(repo, ex)
    cancel = CancelPlan(cfg.symbol, buy.exchange_id, "protect")
    for _ in range(10):
        executor.execute(cancel, ex.now, ReplenishmentState.WATCH)
        ex.now += timedelta(seconds=61)
    assert ex.attempts == MAX_CANCEL_ATTEMPTS
    assert ex.orders[buy.exchange_id].is_open
    assert refresh(repo, ex)[2].unresolved
    assert repo.db.execute("SELECT COUNT(*) FROM events WHERE kind='CANCEL_SENT'").fetchone()[0] == MAX_CANCEL_ATTEMPTS
    runner = Runner(ex, repo, cfg, clock=lambda: ex.now)
    runner.run_once(inventory_cycle=False)
    assert any(a.code == "CANCEL_RECOVERY_EXHAUSTED" for _, a in repo.pending_alerts())
    repo.close()


@pytest.mark.parametrize("response", ["missing", "changed_price", "pending_cancel", "query_timeout"])
def test_cancel_retry_requires_fresh_unambiguous_open_order(tmp_path, response):
    ex, cfg = funded(), make_strategy_config()
    repo = Repository(tmp_path / "identity.db")
    initialize(repo, ex)
    executor = Executor(ex, repo, cfg)
    buy = executor.execute(OrderPlan(cfg.symbol, "BUY", "LIMIT", D("1"), D("590"), "GTC", False),
                           ex.now, ReplenishmentState.WATCH)
    refresh(repo, ex)
    submit = ex.submit
    def disconnected(op):
        if op.kind == OperationKind.CANCEL:
            raise TimeoutError()
        return submit(op)
    ex.submit = disconnected
    payload = CancelPlan(cfg.symbol, buy.exchange_id, "protect")
    cancel = executor.execute(payload, ex.now, ReplenishmentState.WATCH)
    ex.submit = submit
    ex.now += timedelta(seconds=10)
    def query(*args, **kwargs):
        order = ex.orders[buy.exchange_id]
        if response == "query_timeout":
            raise TimeoutError()
        if response == "missing":
            return None
        return replace(order, price=D("591")) if response == "changed_price" else replace(order, status="PENDING_CANCEL")
    ex.fetch_order = query
    if response in {"query_timeout", "changed_price"}:
        with pytest.raises((TimeoutError, ExchangeError)):
            executor.execute(payload, ex.now, ReplenishmentState.WATCH)
    else:
        executor.execute(payload, ex.now, ReplenishmentState.WATCH)
    assert len(ex.writes) == 1
    assert repo.operation(cancel.client_id).cancel_attempts == 1
    assert repo.operation(cancel.client_id).unresolved
    repo.close()


@pytest.mark.parametrize("query_fails", [False, True])
def test_slow_cancel_query_cannot_overwrite_newer_protective_result(tmp_path, query_fails):
    ex, cfg = funded(), make_strategy_config()
    repo = Repository(tmp_path / "slow-query.db")
    initialize(repo, ex)
    executor = Executor(ex, repo, cfg)
    buy = executor.execute(OrderPlan(cfg.symbol, "BUY", "LIMIT", D("1"), D("590"), "GTC", False),
                           ex.now, ReplenishmentState.WATCH)
    refresh(repo, ex)
    payload = CancelPlan(cfg.symbol, buy.exchange_id, "protect")
    # Leave the first attempt unknown and unexecuted.
    submit = ex.submit
    ex.submit = lambda op: (_ for _ in ()).throw(TimeoutError())
    cancel = executor.execute(payload, ex.now, ReplenishmentState.WATCH)
    ex.submit = submit
    ex.now += timedelta(seconds=10)
    query = ex.query_operation
    def stale_query(op):
        result = query(op)
        if op.kind == OperationKind.CANCEL:
            executor.execute(payload, ex.now, ReplenishmentState.WATCH)
            if query_fails:
                raise TimeoutError("older query failed after recovery")
        return result
    ex.query_operation = stale_query
    if query_fails:
        with pytest.raises(TimeoutError):
            refresh(repo, ex)
    else:
        refresh(repo, ex)
    recovered = repo.operation(cancel.client_id)
    assert recovered.status == OperationStatus.CONFIRMED
    assert recovered.cancel_attempts == 2
    assert not ex.orders[buy.exchange_id].is_open
    repo.close()


def test_slow_failed_cancel_recovery_query_does_not_stop_controller(tmp_path):
    ex, cfg = funded(), make_strategy_config()
    repo = Repository(tmp_path / "slow-failure.db")
    initialize(repo, ex)
    executor = Executor(ex, repo, cfg, clock=lambda: ex.now)
    buy = executor.execute(OrderPlan(cfg.symbol, "BUY", "LIMIT", D("1"), D("590"), "GTC", False),
                           ex.now, ReplenishmentState.WATCH)
    refresh(repo, ex)
    submit = ex.submit
    ex.submit = lambda op: (_ for _ in ()).throw(TimeoutError())
    cancel = executor.execute(CancelPlan(cfg.symbol, buy.exchange_id, "protect"), ex.now, ReplenishmentState.WATCH)
    ex.submit = submit
    ex.now += timedelta(seconds=10)
    def slow_failure(*args, **kwargs):
        ex.now += timedelta(seconds=6)
        raise ExchangeError("order query unavailable")
    read = ex.fetch_order
    ex.fetch_order = slow_failure
    runner = Runner(ex, repo, cfg, clock=lambda: ex.now)
    assert runner.run_once(inventory_cycle=False) is None
    assert repo.operation(cancel.client_id).unresolved
    assert repo.runtime().read_retry_at > ex.now
    ex.fetch_order = read
    ex.now += timedelta(seconds=120)
    runner.run_once(inventory_cycle=False)
    assert repo.operation(cancel.client_id).status == OperationStatus.CONFIRMED
    repo.close()


@pytest.mark.parametrize("hours", [1, 48, 49.01, 72, 24 * 60])
@pytest.mark.parametrize("asset,to_account,amount", [
    ("USDT", "SPOT", "500"), ("USDT", "USDⓈ-M Futures", "500"), ("BNB", "USDⓈ-M Futures", "2"),
])
def test_old_confirmed_transfer_backfills_full_evidence(tmp_path, hours, asset, to_account, amount):
    ex, path = funded(), tmp_path / "old.db"
    repo = Repository(path)
    initialize(repo, ex)
    op = transfer(repo, ex, asset, to_account, amount)
    # Simulate the old status-only query result being committed before restart.
    repo.update_operation(replace(op, status=OperationStatus.CONFIRMED))
    repo.close()
    ex.now += timedelta(hours=hours)
    repo = Repository(path)
    for _ in range(3):
        assert not refresh(repo, ex)[2].unresolved
        ex.now += timedelta(seconds=10)
    assert repo.transfer_record(op.exchange_id) == ex.transfers[op.exchange_id]
    assert not repo.operation(op.client_id).balance_pending
    assert len(ex.writes) == 1
    assert len(repo.wallet_changes(0)) == 1
    repo.close()


@pytest.mark.parametrize("change", ["-0.01", "0.01"])
@pytest.mark.parametrize("asset,to_account,amount", [
    ("USDT", "SPOT", "500"), ("USDT", "USDⓈ-M Futures", "500"), ("BNB", "USDⓈ-M Futures", "2"),
])
def test_concurrent_futures_income_settles_only_with_receipts(tmp_path, change, asset, to_account, amount):
    ex, path = funded(), tmp_path / "income.db"
    repo = Repository(path)
    initialize(repo, ex)
    op = transfer(repo, ex, asset, to_account, amount)
    ex.futures_change(asset, D(change), "COMMISSION" if asset == "BNB" else "REALIZED_PNL")
    receipt = ex.income.pop()
    ex.now += timedelta(seconds=10)
    assert refresh(repo, ex)[2].unresolved
    assert repo.load("spot_settlement_issue") is None
    assert repo.load("futures_settlement_issue")
    repo.close()
    repo = Repository(path)
    ex.income.append(receipt)
    for _ in range(3):
        assert not refresh(repo, ex)[2].unresolved
    assert not repo.operation(op.client_id).balance_pending
    assert len(repo.futures_income_since(op.created_at)) == 2
    assert len(ex.writes) == 1
    repo.close()


def test_transfer_income_is_required_even_if_an_unrelated_change_mimics_credit(tmp_path):
    ex = funded()
    repo = Repository(tmp_path / "not-credited.db")
    initialize(repo, ex)
    op = transfer(repo, ex, "BNB", "USDⓈ-M Futures", "2")
    ex.income.clear()
    ex.account = replace(ex.account, contract_bnb=op.balance_before.contract_bnb)
    ex.futures_change("BNB", D("2"), "REALIZED_PNL")
    assert refresh(repo, ex)[2].unresolved
    assert repo.operation(op.client_id).balance_pending
    repo.close()


@pytest.mark.parametrize("delay", [30, 120, 3600])
def test_late_declared_trades_rewind_to_frozen_checkpoint(tmp_path, delay):
    cfg = replace(make_strategy_config(), spot_activity_symbols=("ETHUSDT",))
    ex, path = funded(), tmp_path / "late.db"
    repo = Repository(path)
    initialize(repo, ex)
    refresh(repo, ex, cfg)
    ex.now += timedelta(seconds=1)
    ex.fills.append(Fill("ETHUSDT", "late", "external", ex.now, D("1"), base_asset="ETH",
                         quote_asset="USDT", quote_qty=D("100"), commission=D("0.01"), commission_asset="BNB"))
    ex.account = replace(ex.account, spot_usd=ex.account.spot_usd - D("100"), spot_bnb=ex.account.spot_bnb - D("0.01"))
    ex.hide_fills = True
    for _ in range(delay // 30):
        ex.now += timedelta(seconds=30)
        assert refresh(repo, ex, cfg)[2].unresolved
    repo.close()
    repo = Repository(path)
    ex.hide_fills = False
    for _ in range(3):
        assert not refresh(repo, ex, cfg)[2].unresolved
        ex.now += timedelta(seconds=10)
    assert len(repo.wallet_changes(0)) == 1
    assert not ex.writes
    repo.close()


@pytest.mark.parametrize("race", [False, True])
def test_new_baseline_requires_explicit_quiescent_initialization(tmp_path, race):
    cfg = replace(make_strategy_config(), spot_activity_symbols=("ETHUSDT",))
    ex = funded()
    repo = Repository(tmp_path / "bootstrap.db")
    reconciler = Reconciler(ex, repo, cfg, clock=lambda: ex.now)
    assert reconciler.refresh(ex.now)[2].unresolved
    assert repo.load("spot_checkpoint") is None
    with pytest.raises(RuntimeError, match="reconciled"):
        transfer(repo, ex, "BNB", "USDⓈ-M Futures", "2")
    if race:
        read = ex.fetch_account_snapshot
        def concurrent_read():
            ex.now += timedelta(milliseconds=100)
            ex.fills.append(Fill("ETHUSDT", str(len(ex.fills)), "outside", ex.now, D("1"),
                                 base_asset="ETH", quote_asset="USDT", quote_qty=D("100")))
            ex.account = replace(ex.account, spot_usd=ex.account.spot_usd - D("100"))
            return read()
        ex.fetch_account_snapshot = concurrent_read
        with pytest.raises(ValueError, match="activity|executions"):
            reconciler.initialize_spot_baseline(operator="test", reason="attempt quiescence", now=ex.now)
        assert repo.load("spot_checkpoint") is None
        ex.fetch_account_snapshot = read
        ex.now += timedelta(seconds=10)
    reconciler.initialize_spot_baseline(operator="test", reason="external activity stopped", now=ex.now)
    assert not refresh(repo, ex, cfg)[2].unresolved
    with pytest.raises(ValueError, match="no checkpoint"):
        reconciler.initialize_spot_baseline(operator="test", reason="cannot reset", now=ex.now)
    assert not ex.writes
    repo.close()
