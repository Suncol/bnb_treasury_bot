from dataclasses import replace
from datetime import timedelta
import sqlite3

import pytest

from core.models import AssetTransferPlan, CrashGuardState, OperationStatus, PaperConfig, ReplenishmentState, RunMode
from services.executor import Executor
from services.reconciliation import Reconciler
from services.runner import Runner
from storage.paper_ledger import PAPER_TABLES
from storage.repository import Repository
from tests.helpers import D, make_strategy_config
from tests.paper_helpers import order_operation, paper_setup


def reopen(p):
    p.repo.close()
    return paper_setup(p.path, config=p.config, cfg=p.cfg, clock=p.clock, feed=p.feed)


@pytest.mark.parametrize("phase", ["before_submit", "lost_response", "result_commit"])
def test_order_crashes_keep_original_intent_and_never_resend(tmp_path, monkeypatch, phase):
    p = paper_setup(tmp_path / "crash.db")
    original_submit = p.ex.submit
    attempts = []

    def submit(op):
        attempts.append(op.client_id)
        if phase == "before_submit":
            raise SystemExit("crash after intent")
        result = original_submit(op)
        if phase == "lost_response":
            raise TimeoutError("accepted response lost")
        return result

    monkeypatch.setattr(p.ex, "submit", submit)
    if phase == "result_commit":
        monkeypatch.setattr(p.repo, "update_operation", lambda *a, **k: (_ for _ in ()).throw(sqlite3.OperationalError("result commit failed")))
    payload = order_operation(p.clock, price="601", order_type="LIMIT", tif="IOC").payload
    executor = Executor(p.ex, p.repo, p.cfg, clock=p.clock.utc)
    try:
        if phase in {"before_submit", "result_commit"}:
            with pytest.raises((SystemExit, sqlite3.OperationalError)):
                executor.execute(payload, p.clock.utc(), ReplenishmentState.URGENT)
        else:
            executor.execute(payload, p.clock.utc(), ReplenishmentState.URGENT)
        original_id = p.repo.operations()[0].client_id
        assert len(attempts) == 1
        p = reopen(p)
        reconciler = Reconciler(p.ex, p.repo, p.cfg, clock=p.clock.utc)
        for _ in range(3):
            reconciler.refresh(p.clock.utc())
        assert p.repo.operations()[0].client_id == original_id
        assert p.ex.ledger.state()["next_order"] == (0 if phase == "before_submit" else 1)
        if phase == "before_submit":
            assert p.repo.operations(unresolved_only=True)[0].status == OperationStatus.UNKNOWN
            with pytest.raises(RuntimeError, match="reconciled"):
                Executor(p.ex, p.repo, p.cfg).execute(payload, p.clock.utc(), ReplenishmentState.URGENT)
        else:
            assert not p.repo.operations(unresolved_only=True)
            assert p.ex.fetch_account_snapshot().spot_bnb == D("1.499")
            assert p.repo.db.execute("SELECT COUNT(*) FROM fills").fetchone()[0] == 1
        assert len(attempts) == 1
    finally:
        p.repo.close()


def test_unknown_transfer_needs_tran_id_binding_after_restart(tmp_path, monkeypatch):
    p = paper_setup(tmp_path / "unknown.db")
    sends = []
    original_submit = p.ex.submit

    def lost(op):
        sends.append(op.client_id)
        original_submit(op)
        raise TimeoutError("transfer response lost")

    monkeypatch.setattr(p.ex, "submit", lost)
    payload = AssetTransferPlan("USDT", D("500"), "USDⓈ-M Futures", "SPOT", "test")
    executor = Executor(p.ex, p.repo, p.cfg, clock=p.clock.utc)
    try:
        op = executor.execute(payload, p.clock.utc(), ReplenishmentState.URGENT)
        assert op.status == OperationStatus.UNKNOWN and op.exchange_id is None
        p.clock.elapse(2)
        p.ex.advance()
        p = reopen(p)
        reconciler = Reconciler(p.ex, p.repo, p.cfg, clock=p.clock.utc)
        for _ in range(3):
            assert reconciler.refresh(p.clock.utc())[2].unresolved
        assert p.ex.fetch_account_snapshot().spot_usd == 10500
        assert p.repo.operation(op.client_id).exchange_id is None
        with pytest.raises(RuntimeError, match="reconciled"):
            Executor(p.ex, p.repo, p.cfg).execute(payload, p.clock.utc(), ReplenishmentState.URGENT)
        receipt = p.ex.fetch_recent_transfers(op.created_at, p.clock.utc())[0]
        reconciler.bind_transfer_id(op.client_id, receipt.transfer_id, p.clock.utc())
        assert not reconciler.refresh(p.clock.utc())[2].unresolved
        assert p.repo.budget_used(p.clock.utc(), "USDT") == 500
        assert len(sends) == 1 and p.ex.ledger.state()["next_transfer"] == 1
    finally:
        p.repo.close()


@pytest.mark.parametrize("asset,source,field", [
    ("BNB", "SPOT", "spot_bnb"), ("BNB", "SPOT", "contract_bnb"),
    ("USDT", "USDⓈ-M Futures", "spot_usd"), ("USDT", "USDⓈ-M Futures", "contract_quote_balance"),
    ("USDT", "SPOT", "spot_usd"), ("USDT", "SPOT", "contract_quote_balance"),
])
def test_confirmed_transfer_waits_for_both_balances_across_restart(tmp_path, asset, source, field):
    p = paper_setup(tmp_path / "settlement.db", config=replace(PaperConfig(), initial_spot_usd=D("10000"),
        initial_spot_bnb=D("5"), bnb_consumption_per_hour=D("0")))
    payload = AssetTransferPlan(asset, D("2") if asset == "BNB" else D("500"), source,
                                "SPOT" if source != "SPOT" else "USDⓈ-M Futures", "test")
    before = p.ex.fetch_account_snapshot()
    try:
        op = Executor(p.ex, p.repo, p.cfg, clock=p.clock.utc).execute(payload, p.clock.utc(), ReplenishmentState.URGENT)
        p.clock.elapse(2)
        p.ex.advance()
        p = reopen(p)
        read = p.ex.fetch_account_snapshot
        p.ex.fetch_account_snapshot = lambda: replace(read(), **{field: getattr(before, field)})
        reconciler = Reconciler(p.ex, p.repo, p.cfg, clock=p.clock.utc)
        for _ in range(3):
            assert reconciler.refresh(p.clock.utc())[2].unresolved
            assert p.repo.operation(op.client_id).status == OperationStatus.CONFIRMED
            with pytest.raises(RuntimeError, match="reconciled"):
                Executor(p.ex, p.repo, p.cfg).execute(payload, p.clock.utc(), ReplenishmentState.URGENT)
        p.ex.fetch_account_snapshot = read
        assert not reconciler.refresh(p.clock.utc())[2].unresolved
        assert p.ex.ledger.state()["next_transfer"] == 1
    finally:
        p.repo.close()


@pytest.mark.parametrize("missing", ["fill", "income", "conflict"])
def test_missing_or_conflicting_receipts_block_then_recover(tmp_path, missing):
    p = paper_setup(tmp_path / "receipts.db")
    executor = Executor(p.ex, p.repo, p.cfg, clock=p.clock.utc)
    reconciler = Reconciler(p.ex, p.repo, p.cfg, clock=p.clock.utc)
    try:
        if missing == "income":
            payload = AssetTransferPlan("USDT", D("500"), "USDⓈ-M Futures", "SPOT", "test")
            executor.execute(payload, p.clock.utc(), ReplenishmentState.URGENT)
            p.clock.elapse(2)
            p.ex.advance()
            method = "fetch_futures_income"
        else:
            payload = order_operation(p.clock, price="601", order_type="LIMIT", tif="IOC").payload
            executor.execute(payload, p.clock.utc(), ReplenishmentState.URGENT)
            method = "fetch_recent_fills"
        original = getattr(p.ex, method)
        if missing == "conflict":
            reconciler.refresh(p.clock.utc())
            setattr(p.ex, method, lambda *a: tuple(replace(f, commission=f.commission + D(".01")) for f in original(*a)))
            with pytest.raises(ValueError, match="Conflicting"):
                reconciler.refresh(p.clock.utc())
        else:
            setattr(p.ex, method, lambda *a: ())
            assert reconciler.refresh(p.clock.utc())[2].unresolved
            with pytest.raises(RuntimeError, match="reconciled"):
                executor.execute(payload, p.clock.utc(), ReplenishmentState.URGENT)
        setattr(p.ex, method, original)
        assert not reconciler.refresh(p.clock.utc())[2].unresolved
    finally:
        p.repo.close()


def test_bootstrap_rolls_back_identity_balance_and_baseline_together(tmp_path, monkeypatch):
    repo = Repository(tmp_path / "atomic.db")
    initialize = Repository._initialize_spot_checkpoint

    def fail(self, *args):
        initialize(self, *args)
        raise sqlite3.OperationalError("bootstrap commit failed")

    monkeypatch.setattr(Repository, "_initialize_spot_checkpoint", fail)
    from services.paper_exchange import PaperExchange
    try:
        with pytest.raises(sqlite3.OperationalError):
            PaperExchange(repo, make_strategy_config())
        tables = {r[0] for r in repo.db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert not tables & PAPER_TABLES
        assert repo.load("account_identity") is None and repo.load("spot_checkpoint") is None
    finally:
        repo.close()


def test_fill_failure_rolls_back_simulated_finances_and_liquidity(tmp_path, monkeypatch):
    p = paper_setup(tmp_path / "fill-atomic.db")
    before = p.ex.ledger.state()
    monkeypatch.setattr(p.ex.ledger, "save_fill", lambda *a: (_ for _ in ()).throw(sqlite3.OperationalError("fill failed")))
    try:
        with pytest.raises(sqlite3.OperationalError):
            p.ex.submit(order_operation(p.clock, price="601", order_type="LIMIT", tif="IOC"))
        after = p.ex.ledger.state()
        for field in ("spot_bnb", "spot_usd", "quote_remaining", "next_order", "next_trade"):
            assert after[field] == before[field]
        assert not p.ex.fetch_open_orders("BNBUSDT")
    finally:
        p.repo.close()


def test_hard_veto_uses_max_withdraw_even_in_urgent_with_high_available_balance(tmp_path):
    p = paper_setup(tmp_path / "veto.db", config=replace(PaperConfig(), initial_spot_usd=D("0"),
        contract_unavailable_usd=D("96000"), bnb_consumption_per_hour=D("0")))
    read = p.ex.fetch_account_snapshot
    p.ex.fetch_account_snapshot = lambda: replace(read(), contract_available_balance=D("100000"))
    try:
        runner = Runner(p.ex, p.repo, p.cfg, clock=p.clock.utc)
        plan = runner.run_once(market=p.feed.quote())
        assert plan is not None and plan.state_decision.confirmed_state == ReplenishmentState.URGENT
        assert plan.transfer_decision.gate_status.value == "HARD_VETO"
        assert p.ex.ledger.state()["next_order"] == p.ex.ledger.state()["next_transfer"] == 0
    finally:
        p.repo.close()


def test_paused_keeps_existing_exchange_activity_and_blocks_new_actions(tmp_path):
    p = paper_setup(tmp_path / "paused.db", config=replace(PaperConfig(), initial_spot_usd=D("10000")))
    try:
        executor = Executor(p.ex, p.repo, p.cfg, clock=p.clock.utc)
        executor.execute(order_operation(p.clock).payload, p.clock.utc(), ReplenishmentState.WATCH)
        Reconciler(p.ex, p.repo, p.cfg, clock=p.clock.utc).refresh(p.clock.utc())
        executor.execute(AssetTransferPlan("USDT", D("500"), "USDⓈ-M Futures", "SPOT", "test"),
                         p.clock.utc(), ReplenishmentState.WATCH)
        runner = Runner(p.ex, p.repo, p.cfg, clock=p.clock.utc)
        runner.set_run_mode(RunMode.PAUSED)
        p.clock.elapse(60)
        p.feed.publish(ask="580")
        p.ex.advance()
        runner.tick()
        s = p.ex.ledger.state()
        assert s["next_order"] == s["next_transfer"] == 1
        assert s["gross_bought_bnb"] == 1 and s["consumed_bnb"] > 0
        assert p.ex.fetch_account_snapshot().spot_usd == D("9910")
        assert p.repo.runtime().run_mode == RunMode.PAUSED
    finally:
        p.repo.close()


def test_cold_start_keeps_real_warmup_gate(tmp_path):
    p = paper_setup(tmp_path / "warmup.db")
    p.feed.ready = False
    try:
        runner = Runner(p.ex, p.repo, p.cfg, clock=p.clock.utc)
        assert runner.tick() is not None
        assert p.ex.ledger.state()["next_order"] == p.ex.ledger.state()["next_transfer"] == 0
    finally:
        p.repo.close()


def test_guard_gross_fill_allowance_survives_paper_restart(tmp_path):
    p = paper_setup(tmp_path / "guard.db")
    try:
        executor = Executor(p.ex, p.repo, p.cfg, clock=p.clock.utc)
        executor.execute(order_operation(p.clock).payload, p.clock.utc(), ReplenishmentState.WATCH)
        Reconciler(p.ex, p.repo, p.cfg, clock=p.clock.utc).refresh(p.clock.utc())
        p.repo.save_guard(replace(p.repo.runtime(), guard=CrashGuardState(
            active=True, episode_id="paper-episode", started_at=p.clock.utc(),
            last_trigger_at=p.clock.utc(), keep_price_ceiling=D("590"))), p.clock.utc())
        p.clock.elapse(1)
        p.feed.publish(ask="580")
        p.ex.advance()
        Reconciler(p.ex, p.repo, p.cfg, clock=p.clock.utc).refresh(p.clock.utc())
        assert p.repo.runtime().guard.filled_bnb == 1
        p = reopen(p)
        runner = Runner(p.ex, p.repo, p.cfg, clock=p.clock.utc)
        assert p.repo.runtime().guard.filled_bnb == 1
        runner.reconciler.refresh(p.clock.utc())
        assert p.repo.runtime().guard.active and p.repo.runtime().guard.filled_bnb == 1
        assert p.ex.ledger.state()["gross_bought_bnb"] == 1
    finally:
        p.repo.close()


def test_clock_rollback_preserves_committed_paper_state(tmp_path):
    p = paper_setup(tmp_path / "clock.db", config=PaperConfig())
    try:
        p.clock.elapse(60)
        p.ex.advance()
        before = p.ex.ledger.state()
        p.clock.now -= timedelta(seconds=1)
        from services.exchange_adapter import ExchangeError
        with pytest.raises(ExchangeError, match="backwards"):
            p.ex.advance()
        assert p.ex.ledger.state() == before
    finally:
        p.repo.close()
