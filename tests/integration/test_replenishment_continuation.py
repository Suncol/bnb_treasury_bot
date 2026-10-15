from dataclasses import replace
from datetime import timedelta

import pytest

from core.models import OperationKind, OperationStatus, ReplenishmentState
from services.runner import Runner
from storage.repository import Repository
from tests.fake_exchange import FakeExchange
from tests.helpers import D, make_account, make_strategy_config
from tests.execution_helpers import run, setup, seed_bid


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
