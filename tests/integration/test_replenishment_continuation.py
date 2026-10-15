from dataclasses import replace
from datetime import timedelta
import json
import sqlite3
from urllib.parse import urlsplit

import pytest
import requests

from core.models import OperationKind, OperationStatus, ReplenishmentState, SliceState
from services.binance_adapter import BinanceAdapter
from services.runner import Runner
from storage.repository import Repository
from tests.fake_exchange import FakeExchange
from tests.helpers import D, make_account, make_strategy_config
from tests.execution_helpers import run, setup, seed_bid


@pytest.mark.parametrize("message", ["Market is closed.", "This symbol is not permitted for this account."])
@pytest.mark.parametrize("continuation", ["funded", "replanning"])
@pytest.mark.parametrize("result_commit_fails", [False, True])
def test_sdk_rejection_preserves_first_group_continuation_and_slice_after_restart(
    tmp_path, monkeypatch, message, continuation, result_commit_fails,
):
    calls = []

    def request(self, **kwargs):
        assert (kwargs["method"], urlsplit(kwargs["url"]).path) == ("POST", "/api/v3/order")
        calls.append(kwargs)
        response = requests.Response()
        response.status_code = 400
        response.headers["Content-Type"] = "application/json"
        response._content = json.dumps({"code": -2010, "msg": message}).encode()
        return response

    monkeypatch.setattr(requests.Session, "request", request)

    class RejectFirstGroup(FakeExchange):
        armed = False

        def submit(self, op):
            if op.kind == OperationKind.ORDER and self.armed:
                self.armed = False
                return adapter.submit(op)
            return super().submit(op)

    ex, repo, runner = setup(
        tmp_path, RejectFirstGroup(make_account(
            contract_bnb=D("27"), spot_usd=D("0") if continuation == "funded" else D("5000"),
            contract_max_withdraw_amount=D("50000"),
        )), ReplenishmentState.ACCUMULATE,
    )
    adapter = BinanceAdapter("fixture-key", "fixture-secret", make_strategy_config(),
                             live=True, clock=lambda: ex.now)
    if continuation == "replanning":
        seed_bid(ex, repo, age_hours=2, qty="3")
    repo.save("runtime", replace(repo.runtime(), slice_state=SliceState(active=True)))
    ex.armed = True
    if result_commit_fails:
        repo.db.executescript("""
            CREATE TRIGGER fail_rejection_result BEFORE UPDATE ON kv
            WHEN NEW.key='runtime' AND EXISTS (
                SELECT 1 FROM operations WHERE status='FAILED'
            ) BEGIN SELECT RAISE(ABORT, 'rejection result failed'); END;
        """)
        with pytest.raises(sqlite3.IntegrityError, match="rejection result failed"):
            run(runner, ex)
    else:
        run(runner, ex)
    assert len(calls) == 1
    rejected = [op for op in repo.operations() if op.kind == OperationKind.ORDER][-1]
    deadline = repo.runtime().slice_state.next_at
    assert deadline == ex.now + timedelta(minutes=30)
    assert rejected.status == (OperationStatus.PENDING if result_commit_fails else OperationStatus.FAILED)
    assert not rejected.not_sent
    flag = "resume_replenishment" if continuation == "funded" else "resume_replanning"
    assert getattr(repo.runtime(), flag) == (not result_commit_fails)
    assert repo.runtime().api_errors == (0 if result_commit_fails else 1)
    prior_writes = len(ex.writes)
    repo.close()
    repo = Repository(tmp_path / "test.sqlite3")
    if result_commit_fails:
        repo.db.execute("DROP TRIGGER fail_rejection_result")
        repo.db.commit()
    runner = Runner(ex, repo, make_strategy_config(), clock=lambda: ex.now)
    ex.now += timedelta(seconds=11)
    run(runner, ex, inventory=False)
    assert len(ex.writes) == prior_writes
    assert repo.runtime().slice_state.next_at == deadline
    ex.now = deadline
    run(runner, ex, inventory=False)
    if result_commit_fails:
        assert len(ex.writes) == prior_writes
        assert repo.operation(rejected.client_id).unresolved
    else:
        assert sum(op.payload.qty for op in ex.writes[prior_writes:]
                   if op.kind == OperationKind.ORDER) == D("3")
        assert not getattr(repo.runtime(), flag)
    assert len(calls) == 1
    assert sum(op.kind == OperationKind.TRANSFER and op.payload.asset == "USDT"
               for op in ex.writes) == (1 if continuation == "funded" else 0)
    repo.close()


@pytest.mark.parametrize("spot_usd", ["0", "3000"])
@pytest.mark.parametrize("restart", [False, True])
@pytest.mark.parametrize("lost_response", [False, True])
@pytest.mark.parametrize("contract_bnb,spot_bnb", [("27", "0.8"), ("28.2", "2.5")])
def test_bnb_return_resumes_unstarted_group_without_sweeping(
    tmp_path, spot_usd, restart, lost_response, contract_bnb, spot_bnb
):
    ex, repo, runner = setup(
        tmp_path,
        FakeExchange(make_account(
            contract_bnb=D(contract_bnb), spot_bnb=D(spot_bnb), spot_usd=D(spot_usd),
            contract_max_withdraw_amount=D("100000"),
        )),
        ReplenishmentState.ACCUMULATE,
    )
    try:
        ex.hold_transfers = True
        ex.lose_response = OperationKind.TRANSFER if lost_response else None
        run(runner, ex)
        assert len(ex.writes) == 1 and ex.writes[0].payload.asset == "BNB"
        for _ in range(2):
            run(runner, ex, inventory=False)
        assert len(ex.writes) == 1
        if restart:
            repo.close()
            repo = Repository(tmp_path / "test.sqlite3")
            runner = Runner(ex, repo, make_strategy_config(), clock=lambda: ex.now)
        ex.settle("1")
        ex.hold_transfers = False
        ex.lose_response = None
        if lost_response:
            run(runner, ex, inventory=False)
            assert len(ex.writes) == 1
            assert repo.operation(ex.writes[0].client_id).status == OperationStatus.UNKNOWN
            runner.reconciler.bind_transfer_id(ex.writes[0].client_id, "1", ex.now)
        run(runner, ex, inventory=False)
        assert any(o.kind == OperationKind.ORDER for o in ex.writes)
        usd = [o.payload for o in ex.writes
               if o.kind == OperationKind.TRANSFER and o.payload.asset == "USDT"]
        assert len(usd) == (1 if spot_usd == "0" else 0)
        assert all(t.to_account == "SPOT" for t in usd)
        assert sum(o.kind == OperationKind.TRANSFER and o.payload.asset == "BNB"
                   for o in ex.writes) == 1
        writes = len(ex.writes)
        for _ in range(3):
            ex.now += timedelta(seconds=10)
            run(runner, ex, inventory=False)
        assert len(ex.writes) == writes
        assert not repo.operations(unresolved_only=True)
    finally:
        repo.close()


@pytest.mark.parametrize("delayed", [False, True])
def test_known_pending_bnb_return_preserves_safe_resting_bid(tmp_path, delayed):
    ex, repo, runner = setup(
        tmp_path,
        FakeExchange(make_account(
            contract_bnb=D("27"), spot_bnb=D("0.8"), spot_usd=D("3000"),
            contract_max_withdraw_amount=D("100000"),
        )),
        ReplenishmentState.ACCUMULATE,
    )
    try:
        bid = seed_bid(ex, repo, qty="1")
        ex.hold_transfers = delayed
        run(runner, ex)
        assert runner.cached_inputs.snapshot_consistent
        assert not any(o.kind == OperationKind.CANCEL for o in ex.writes)
        assert ex.orders[bid.exchange_id].is_open
        if delayed:
            assert len(ex.writes) == 2
            assert repo.operations(unresolved_only=True)
            # Pending transfers still permit actual price protection.
            market = replace(ex.fetch_market_snapshot("BNBUSDT"),
                             best_bid=D("579"), best_ask=D("580"),
                             mid_price=D("579.5"), vwap_5m=D("580"), smooth_price=D("580"))
            runner.run_once(market=market, inventory_cycle=False)
            assert ex.orders[bid.exchange_id].status == "CANCELED"
            assert len(ex.writes) == 3
    finally:
        repo.close()


@pytest.mark.parametrize("previous", [ReplenishmentState.ACCUMULATE, ReplenishmentState.WATCH])
def test_round_survives_recovery_above_alert_and_restart_until_target(tmp_path, previous):
    ex, repo, runner = setup(
        tmp_path,
        FakeExchange(make_account(contract_bnb=D("27.8"), spot_usd=D("5000"))),
        previous,
    )
    try:
        run(runner, ex)
        bid = next(o for o in ex.orders.values() if o.is_open)
        ex.now += timedelta(minutes=30)
        ex.fill(bid.order_id, D("0.5"))
        run(runner, ex)
        assert ex.account.contract_bnb == D("28.3")
        repo.close()
        repo = Repository(tmp_path / "test.sqlite3")
        runner = Runner(ex, repo, make_strategy_config(), clock=lambda: ex.now)
        for _ in range(2):
            ex.now += timedelta(minutes=30)
            run(runner, ex)
        assert repo.runtime().state == ReplenishmentState.ACCUMULATE
        assert any(o.is_open for o in ex.orders.values())
        assert not any(o.kind == OperationKind.TRANSFER and o.payload.asset == "USDT"
                       for o in ex.writes)
        for o in tuple(ex.orders.values()):
            if o.is_open:
                ex.fill(o.order_id, o.remaining_qty)
        for _ in range(3):
            ex.now += timedelta(minutes=30)
            run(runner, ex)
        assert D("31.9") < ex.account.contract_bnb <= D("32")
        assert repo.runtime().state == ReplenishmentState.IDLE
        assert not ex.fetch_open_orders("BNBUSDT")
        ex.futures_change("BNB", D("-4.1"))
        for _ in range(2):
            ex.now += timedelta(minutes=30)
            run(runner, ex)
        assert repo.runtime().state == ReplenishmentState.ACCUMULATE
        assert ex.fetch_open_orders("BNBUSDT")
    finally:
        repo.close()


def funded_setup(tmp_path, *, urgent=False, unknown=False):
    ex, repo, _ = setup(
        tmp_path,
        FakeExchange(make_account(contract_bnb=D("24" if urgent else "27"), spot_usd=D("0"),
                                  contract_max_withdraw_amount=D("100000"))),
        ReplenishmentState.URGENT if urgent else ReplenishmentState.ACCUMULATE,
    )
    cfg = replace(make_strategy_config(), spot_activity_symbols=("BTCUSDT",))
    runner = Runner(ex, repo, cfg, clock=lambda: ex.now)
    ex.hold_transfers = True
    if unknown:
        ex.lose_response = OperationKind.TRANSFER
    run(runner, ex)
    funding = ex.writes[0]
    assert funding.kind == OperationKind.TRANSFER and funding.payload.asset == "USDT"
    assert repo.runtime().resume_replenishment and len(ex.writes) == 1
    ex.settle("1")
    ex.hold_transfers = False
    return ex, repo, runner, funding


def external_spend(ex, amount, *, symbol="BTCUSDT", receipt=True):
    from core.models import Fill
    ex.now += timedelta(seconds=1)
    ex.account = replace(ex.account, spot_usd=ex.account.spot_usd - amount)
    if receipt:
        ex.fills.append(Fill(symbol, "external-1", "external-order", ex.now, D("0.1"),
                             quote_qty=amount, base_asset="BTC", quote_asset="USDT"))


@pytest.mark.parametrize("urgent", [False, True])
def test_confirmed_external_spend_ends_funding_continuation_until_next_inventory(tmp_path, urgent):
    ex, repo, runner, funding = funded_setup(tmp_path, urgent=urgent)
    try:
        external_spend(ex, funding.payload.amount)
        # Budget has reopened before exit; it cannot conceal a bad same-tick refill.
        ex.now += timedelta(hours=25)
        run(runner, ex, inventory=False)
        assert not repo.runtime().resume_replenishment
        assert not repo.operations(unresolved_only=True)
        assert len(ex.writes) == 1
        repo.close()
        repo = Repository(tmp_path / "test.sqlite3")
        runner = Runner(ex, repo, runner.cfg, clock=lambda: ex.now)
        for _ in range(2):
            ex.now += timedelta(seconds=1)
            run(runner, ex, inventory=False)
        assert len(ex.writes) == 1
        ex.now += timedelta(minutes=30)
        run(runner, ex)
        funding_writes = [o for o in ex.writes if o.kind == OperationKind.TRANSFER
                          and o.payload.asset == "USDT" and o.payload.to_account == "SPOT"]
        assert len(funding_writes) == 2
    finally:
        repo.close()


def external_freeze(ex, amount, *, symbol="BTCUSDT", strategy_id=None, created_at=None):
    from core.models import OrderView
    ex.now += timedelta(seconds=1)
    ex.orders["external-order"] = OrderView(
        symbol, "external-order", "bt-external-prefix-is-not-ownership", strategy_id,
        amount, D("1"), D("0"), created_at or ex.now,
    )


@pytest.mark.parametrize("symbol", ["BTCUSDT", "BNBUSDT"])
def test_verified_external_freeze_ends_continuation_without_ignoring_bnb_exposure(tmp_path, symbol):
    ex, repo, runner, funding = funded_setup(tmp_path)
    try:
        external_freeze(ex, funding.payload.amount, symbol=symbol,
                        strategy_id=runner.cfg.strategy_id)
        plan = run(runner, ex, inventory=False)
        assert not repo.runtime().resume_replenishment
        assert plan.state_decision.delta_bnb == (D("4") if symbol == "BNBUSDT" else D("5"))
        assert len(ex.writes) == 1
        assert ex.orders["external-order"].is_open
        ex.now += timedelta(hours=25)
        run(runner, ex, inventory=False)
        assert len(ex.writes) == 1
        ex.now += timedelta(minutes=30)
        run(runner, ex)
        assert sum(o.kind == OperationKind.TRANSFER and o.payload.to_account == "SPOT"
                   for o in ex.writes) == 2
        assert ex.orders["external-order"].is_open
    finally:
        repo.close()


def test_legacy_funded_boolean_uses_its_latest_settled_funding_journal(tmp_path):
    ex, repo, runner, funding = funded_setup(tmp_path)
    try:
        external_spend(ex, funding.payload.amount)
        # Serialize the pre-upgrade shape, not a fabricated new marker.
        import json
        from storage.codec import dumps
        legacy = json.loads(dumps(repo.runtime()))
        legacy["fields"].pop("funding_client_id", None)
        legacy["fields"].pop("funding_exhausted_at", None)
        repo.db.execute("UPDATE kv SET data=? WHERE key='runtime'", (json.dumps(legacy),))
        repo.db.commit()
        repo.close()
        repo = Repository(tmp_path / "test.sqlite3")
        runner = Runner(ex, repo, runner.cfg, clock=lambda: ex.now)
        run(runner, ex, inventory=False)
        assert not repo.runtime().resume_replenishment
        assert len(ex.writes) == 1
    finally:
        repo.close()


def test_replaced_external_spend_cannot_blame_later_price_jump_for_exhaustion(tmp_path):
    from core.models import TransferRecord
    ex, repo, runner, funding = funded_setup(tmp_path)
    try:
        external_spend(ex, funding.payload.amount)
        ex.transfers["replacement"] = TransferRecord(
            "replacement", "USDT", funding.payload.amount, "USDⓈ-M Futures", "SPOT",
            ex.now, OperationStatus.PENDING,
        )
        ex.settle("replacement")
        price = ex.fetch_market_snapshot
        ex.fetch_market_snapshot = lambda symbol: replace(
            price(symbol), best_bid=D("399999"), best_ask=D("400001"),
            mid_price=D("400000"), vwap_5m=D("400000"), smooth_price=D("400000"),
        )
        plan = run(runner, ex, inventory=False)
        assert plan.inventory_valid and not plan.buy_plan.orders
        assert repo.runtime().resume_replenishment
        assert repo.runtime().funding_exhausted_at is None
        assert len(ex.writes) == 1
    finally:
        repo.close()


@pytest.mark.parametrize("legacy", [False, True])
def test_existing_strategy_reservation_does_not_hide_new_external_freeze(tmp_path, legacy):
    ex, repo, _ = setup(
        tmp_path,
        FakeExchange(make_account(contract_bnb=D("27"), spot_usd=D("2870.4"),
                                  contract_max_withdraw_amount=D("100000"))),
        ReplenishmentState.ACCUMULATE,
    )
    cfg = replace(make_strategy_config(), spot_activity_symbols=("BTCUSDT",))
    runner = Runner(ex, repo, cfg, clock=lambda: ex.now)
    try:
        bid = seed_bid(ex, repo, qty="4.8")
        ex.hold_transfers = True
        run(runner, ex)
        funding = ex.writes[-1]
        assert funding.kind == OperationKind.TRANSFER and funding.payload.amount == D("500")
        ex.settle(funding.exchange_id or str(len(ex.writes)))
        ex.hold_transfers = False
        if legacy:
            repo.save("runtime", replace(repo.runtime(), funding_client_id=None))
        external_freeze(ex, funding.payload.amount)
        plan = run(runner, ex, inventory=False)
        assert plan.state_decision.delta_bnb == D("0.2")
        assert not repo.runtime().resume_replenishment
        assert len(ex.writes) == 2
        assert ex.orders[bid.exchange_id].is_open
    finally:
        repo.close()


@pytest.mark.parametrize("blocker", ["chase", "paused", "slice", "market"])
def test_external_spend_does_not_end_continuation_while_another_gate_blocks(tmp_path, blocker):
    from core.models import RunMode, SliceState
    ex, repo, runner, funding = funded_setup(tmp_path)
    try:
        external_spend(ex, funding.payload.amount)
        market = ex.fetch_market_snapshot("BNBUSDT")
        if blocker == "paused":
            runner.set_run_mode(RunMode.PAUSED)
        elif blocker == "slice":
            repo.save("runtime", replace(repo.runtime(), slice_state=SliceState(
                True, ex.now + timedelta(minutes=30))))
        elif blocker == "chase":
            market = replace(market, return_1h=D("0.04"))
        else:
            market = replace(market, drawdown_15m=None)
        runner.run_once(market=market, inventory_cycle=False)
        assert repo.runtime().resume_replenishment
        assert repo.runtime().funding_exhausted_at is None
        assert len(ex.writes) == 1
        repo.close()
        repo = Repository(tmp_path / "test.sqlite3")
        runner = Runner(ex, repo, runner.cfg, clock=lambda: ex.now)
        runner.set_run_mode(RunMode.AUTO)
        ex.now += timedelta(minutes=31)
        run(runner, ex, inventory=False)
        assert not repo.runtime().resume_replenishment
        assert len(ex.writes) == 1
    finally:
        repo.close()


def test_external_spend_with_remaining_cash_places_a_smaller_compliant_group(tmp_path):
    ex, repo, runner, funding = funded_setup(tmp_path)
    try:
        external_spend(ex, funding.payload.amount - D("100"))
        run(runner, ex, inventory=False)
        buys = [o.payload for o in ex.writes if o.kind == OperationKind.ORDER]
        assert buys and sum(o.qty for o in buys) < D("5")
        assert sum(o.price * o.qty for o in buys) * D("1.001") <= D("100")
        assert sum(o.kind == OperationKind.TRANSFER for o in ex.writes) == 1
        assert not repo.runtime().resume_replenishment
        assert repo.runtime().funding_exhausted_at is None
    finally:
        repo.close()


@pytest.mark.parametrize("created_offset", [0, -1])
def test_ambiguous_or_older_external_lock_cannot_prove_this_funding_was_taken(tmp_path, created_offset):
    ex, repo, runner, funding = funded_setup(tmp_path)
    try:
        external_freeze(ex, funding.payload.amount,
                        created_at=funding.created_at + timedelta(seconds=created_offset))
        run(runner, ex, inventory=False)
        assert repo.runtime().resume_replenishment
        assert repo.runtime().funding_exhausted_at is None
        assert len(ex.writes) == 1
    finally:
        repo.close()


def test_unexplained_cash_loss_waits_for_real_trade_receipts(tmp_path):
    ex, repo, runner, funding = funded_setup(tmp_path)
    try:
        external_spend(ex, funding.payload.amount)
        ex.hide_fills = True
        run(runner, ex, inventory=False)
        assert repo.load("spot_settlement_issue")
        assert repo.runtime().resume_replenishment
        assert repo.runtime().funding_exhausted_at is None
        repo.close()
        repo = Repository(tmp_path / "test.sqlite3")
        runner = Runner(ex, repo, runner.cfg, clock=lambda: ex.now)
        ex.now += timedelta(hours=25)
        run(runner, ex)
        assert repo.runtime().resume_replenishment and len(ex.writes) == 1
        ex.hide_fills = False
        ex.now += timedelta(seconds=1)
        run(runner, ex)
        assert not repo.runtime().resume_replenishment
        assert not repo.load("spot_settlement_issue") and len(ex.writes) == 1
    finally:
        repo.close()


@pytest.mark.parametrize("obstacle", ["budget", "granularity"])
def test_next_inventory_after_external_usage_rechecks_transfer_limits(tmp_path, obstacle):
    ex, repo, runner, funding = funded_setup(tmp_path)
    try:
        external_spend(ex, funding.payload.amount)
        run(runner, ex, inventory=False)
        assert not repo.runtime().resume_replenishment
        if obstacle == "granularity":
            ex.now += timedelta(hours=25)
            ex.account = replace(ex.account, contract_max_withdraw_amount=D("4000"))
        ex.now += timedelta(minutes=30)
        plan = run(runner, ex)
        assert plan.inventory_valid and not plan.transfer_decision.allow
        assert len(ex.writes) == 1
    finally:
        repo.close()


def test_external_usage_exit_and_next_cycle_barrier_roll_back_together(tmp_path):
    import sqlite3
    ex, repo, runner, funding = funded_setup(tmp_path, urgent=True)
    try:
        external_spend(ex, funding.payload.amount)
        ex.now += timedelta(hours=25)
        repo.db.executescript("""
            CREATE TRIGGER fail_funding_exit BEFORE INSERT ON events
            WHEN NEW.kind='FUNDING_EXTERNAL_USAGE'
            BEGIN SELECT RAISE(ABORT, 'exit transaction failed'); END;
        """)
        with pytest.raises(sqlite3.IntegrityError, match="exit transaction failed"):
            run(runner, ex, inventory=False)
        assert repo.runtime().resume_replenishment
        assert repo.runtime().funding_client_id == funding.client_id
        assert repo.runtime().funding_exhausted_at is None
        assert len(ex.writes) == 1
        repo.close()
        repo = Repository(tmp_path / "test.sqlite3")
        repo.db.execute("DROP TRIGGER fail_funding_exit")
        repo.db.commit()
        runner = Runner(ex, repo, runner.cfg, clock=lambda: ex.now)
        run(runner, ex, inventory=False)
        assert not repo.runtime().resume_replenishment
        assert repo.runtime().funding_exhausted_at == ex.now
        # A same-time inventory replay and an invalid later inventory neither
        # earn permission; the barrier survives another process restart.
        run(runner, ex)
        assert len(ex.writes) == 1
        ex.now += timedelta(minutes=30)
        account_snapshot = ex.fetch_account_snapshot
        ex.fetch_account_snapshot = lambda: replace(
            account_snapshot(), ts=ex.now - timedelta(seconds=30))
        invalid = run(runner, ex)
        assert not invalid.inventory_valid
        assert repo.runtime().funding_exhausted_at is not None
        ex.fetch_account_snapshot = account_snapshot
        repo.close()
        repo = Repository(tmp_path / "test.sqlite3")
        runner = Runner(ex, repo, runner.cfg, clock=lambda: ex.now)
        run(runner, ex, inventory=False)
        assert len(ex.writes) == 1
        run(runner, ex)
        assert sum(o.kind == OperationKind.TRANSFER and o.payload.to_account == "SPOT"
                   for o in ex.writes) == 2
    finally:
        repo.close()


def test_external_spend_does_not_clear_unknown_funding_identity(tmp_path):
    ex, repo, runner, funding = funded_setup(tmp_path, unknown=True)
    try:
        external_spend(ex, funding.payload.amount)
        ex.now += timedelta(hours=25)
        for inventory in (False, True, True):
            run(runner, ex, inventory=inventory)
            ex.now += timedelta(minutes=30)
        assert repo.operation(funding.client_id).status == OperationStatus.UNKNOWN
        assert repo.runtime().resume_replenishment
        assert repo.runtime().funding_exhausted_at is None
        assert len(ex.writes) == 1
    finally:
        repo.close()


def test_funding_after_the_next_inventory_remains_unique_when_its_response_is_lost(tmp_path):
    ex, repo, runner, funding = funded_setup(tmp_path, urgent=True)
    try:
        external_spend(ex, funding.payload.amount)
        ex.now += timedelta(hours=25)
        run(runner, ex, inventory=False)
        ex.now += timedelta(minutes=30)
        ex.lose_response = OperationKind.TRANSFER
        run(runner, ex)
        assert len(ex.writes) == 2
        replacement = ex.writes[-1]
        assert replacement.kind == OperationKind.TRANSFER
        assert repo.runtime().funding_client_id == replacement.client_id
        assert repo.operation(replacement.client_id).status == OperationStatus.UNKNOWN
        repo.close()
        repo = Repository(tmp_path / "test.sqlite3")
        runner = Runner(ex, repo, runner.cfg, clock=lambda: ex.now)
        for inventory in (False, True, True):
            ex.now += timedelta(minutes=30)
            run(runner, ex, inventory=inventory)
        assert len(ex.writes) == 2
        assert repo.operation(replacement.client_id).unresolved
    finally:
        repo.close()


def test_executor_cannot_bypass_the_persisted_next_inventory_barrier(tmp_path):
    from services.executor import Executor
    ex, repo, runner, funding = funded_setup(tmp_path)
    try:
        external_spend(ex, funding.payload.amount)
        ex.now += timedelta(hours=25)
        run(runner, ex, inventory=False)
        executor = Executor(ex, repo, runner.cfg, clock=lambda: ex.now)
        with pytest.raises(RuntimeError, match="inventory cycle"):
            executor.execute(funding.payload, ex.now, ReplenishmentState.ACCUMULATE)
        assert len(ex.writes) == 1
        assert len(repo.operations()) == 1
    finally:
        repo.close()


@pytest.mark.parametrize("evidence_gap", ["changing_order", "unexplained_reservation", "unknown_order"])
def test_external_freeze_requires_stable_orders_covering_the_account_reservation(tmp_path, evidence_gap):
    from core.models import OrderView
    ex, repo, runner, funding = funded_setup(tmp_path)
    try:
        external_freeze(ex, funding.payload.amount - D("1"))
        if evidence_gap == "unexplained_reservation":
            ex.orders["unconfigured"] = OrderView(
                "ETHUSDT", "unconfigured", "outside", None, D("1"), D("1"), D("0"), ex.now,
            )
        elif evidence_gap == "unknown_order":
            ex.orders["external-order"] = replace(ex.orders["external-order"], status="UNKNOWN")
        else:
            read_orders = ex.fetch_open_orders
            calls = 0

            def changing_orders(symbol):
                nonlocal calls
                rows = read_orders(symbol)
                if symbol == "BTCUSDT":
                    calls += 1
                    if calls % 2 == 0:
                        return tuple(replace(o, qty=D("0.99")) for o in rows)
                return rows

            ex.fetch_open_orders = changing_orders
        plan = run(runner, ex, inventory=False)
        assert plan.inventory_valid == (evidence_gap != "changing_order")
        assert repo.runtime().resume_replenishment
        assert repo.runtime().funding_exhausted_at is None
        assert len(ex.writes) == 1
    finally:
        repo.close()


@pytest.mark.parametrize("legacy", [False, True])
def test_previous_funding_trade_receipts_cannot_end_a_later_funded_continuation(tmp_path, legacy):
    from core.models import TransferRecord
    ex, repo, runner, funding = funded_setup(tmp_path)
    try:
        external_spend(ex, funding.payload.amount)
        ex.now += timedelta(hours=25)
        run(runner, ex, inventory=False)
        ex.now += timedelta(minutes=30)
        ex.hold_transfers = True
        run(runner, ex)
        replacement = ex.writes[-1]
        assert len(ex.writes) == 2 and replacement.kind == OperationKind.TRANSFER
        ex.settle("2")
        ex.transfers["external-out"] = TransferRecord(
            "external-out", "USDT", replacement.payload.amount, "SPOT", "USDⓈ-M Futures",
            ex.now, OperationStatus.PENDING,
        )
        ex.settle("external-out")
        if legacy:
            repo.save("runtime", replace(repo.runtime(), funding_client_id=None))
        run(runner, ex, inventory=False)
        assert not repo.operations(unresolved_only=True)
        assert repo.runtime().resume_replenishment
        assert repo.runtime().funding_exhausted_at is None
        assert len(ex.writes) == 2
    finally:
        repo.close()
