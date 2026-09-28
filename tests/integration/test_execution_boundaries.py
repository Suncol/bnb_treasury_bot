from dataclasses import replace
from datetime import timedelta
from threading import Event, Thread

import pytest

from core.models import (
    Alert,
    CrashGuardState,
    OperationKind,
    OperationResult,
    OperationStatus,
    OrderPlan,
    ReplenishmentState,
    RunMode,
    SliceState,
)
from services.alert_service import AlertService
from services.executor import Executor
from services.runner import Runner
from storage.repository import Repository
from tests.fake_exchange import FakeExchange
from tests.helpers import D, make_account, make_strategy_config
from tests.integration.test_execution import run, setup


def accumulate_setup(tmp_path):
    return setup(
        tmp_path,
        FakeExchange(make_account(contract_bnb=D("27"), spot_usd=D("5000"))),
        ReplenishmentState.ACCUMULATE,
    )


def seed_bid(exchange, repo, *, age_hours=0, qty="1"):
    cfg = make_strategy_config()
    op = Executor(exchange, repo, cfg).execute(
        OrderPlan(cfg.symbol, "BUY", "LIMIT_MAKER", D(qty), D("598"), "GTC", True),
        exchange.now - timedelta(hours=age_hours),
        ReplenishmentState.ACCUMULATE,
    )
    exchange.orders[op.exchange_id] = replace(
        exchange.orders[op.exchange_id], created_at=op.created_at
    )
    return op


def test_timeout_slice_wait_survives_fast_ticks_and_reconciliation(tmp_path):
    exchange = FakeExchange(make_account(contract_bnb=D("19"), spot_usd=D("30000")))
    exchange, repo, runner = setup(tmp_path, exchange)
    exchange.lose_response = OperationKind.ORDER
    runner.tick()
    deadline = repo.runtime().slice_state.next_at
    assert deadline == exchange.now + timedelta(minutes=30)
    assert repo.operations()[0].status == OperationStatus.UNKNOWN
    for _ in range(11):
        exchange.now += timedelta(seconds=1)
        runner.tick()
        assert repo.runtime().slice_state.next_at == deadline
    assert sum(
        o.payload.qty for o in exchange.writes if o.kind == OperationKind.ORDER
    ) == D("3")
    exchange.now = deadline
    runner.tick()
    assert sum(
        o.payload.qty for o in exchange.writes if o.kind == OperationKind.ORDER
    ) == D("6")


@pytest.mark.parametrize("action", ["buy", "fund", "bnb", "sweep"])
def test_expired_plan_is_discarded_before_any_new_submission(tmp_path, action):
    account = make_account(
        contract_bnb=D("27"),
        spot_usd=D("5000") if action not in {"fund", "bnb"} else D("0"),
        spot_bnb=D("2.5") if action == "bnb" else D("0.5"),
        contract_max_withdraw_amount=D("50000"),
    )
    exchange, repo, runner = setup(
        tmp_path, FakeExchange(account), ReplenishmentState.ACCUMULATE
    )
    publish = runner._publish
    market = exchange.fetch_market_snapshot
    delayed = False

    def delay_after_planning(alerts):
        nonlocal delayed
        publish(alerts)
        if not delayed:
            delayed = True
            exchange.now += timedelta(seconds=11 if action == "bnb" else 6)
            exchange.fetch_market_snapshot = lambda symbol: replace(
                market(symbol), drawdown_1m=D("0.02")
            )
            if action == "bnb":
                exchange.account = replace(exchange.account, spot_bnb=D("0.5"))
            elif action == "sweep":
                exchange.account = replace(exchange.account, spot_usd=D("0"))

    runner._publish = delay_after_planning
    run(runner, exchange, inventory=action != "sweep")
    assert not any(
        o.kind == OperationKind.ORDER
        or (o.kind == OperationKind.TRANSFER and o.payload.to_account == "SPOT")
        for o in exchange.writes
    )
    if action in {"bnb", "sweep"}:
        assert not exchange.writes
    assert repo.runtime().guard.active


def test_blocked_alert_sink_does_not_block_protective_tick(tmp_path):
    exchange, repo, runner = accumulate_setup(tmp_path)
    bid = seed_bid(exchange, repo)
    entered, release, finished = Event(), Event(), Event()

    class SlowSink:
        name = "slow"

        def send(self, alert, key):
            entered.set()
            assert release.wait(5)

    service = AlertService(repo, (SlowSink(),))
    runner.alerts = service
    runner._publish((Alert("WARNING", "TEST", "slow notification"),))
    worker = None
    try:
        assert entered.wait(2)
        exchange.now += timedelta(seconds=6)
        market = replace(
            exchange.fetch_market_snapshot("BNBUSDT"), drawdown_1m=D("0.02")
        )

        def tick():
            try:
                runner.tick(market)
            finally:
                finished.set()

        worker = Thread(target=tick)
        worker.start()
        assert finished.wait(2), "notification delivery held up the trading loop"
        assert exchange.orders[bid.exchange_id].status == "CANCELED"
        assert repo.runtime().guard.active
    finally:
        release.set()
        if worker is not None:
            worker.join(2)
        service.close()


@pytest.mark.parametrize("failing_read", [
    "fetch_account_snapshot", "fetch_recent_transfers", "fetch_symbol_filters",
    "fetch_recent_fills", "fetch_open_orders",
])
@pytest.mark.parametrize("mode", [RunMode.AUTO, RunMode.PAUSED])
def test_partial_read_failure_still_cancels_only_journaled_bids(
    tmp_path, failing_read, mode
):
    exchange, repo, runner = accumulate_setup(tmp_path)
    bids = [seed_bid(exchange, repo), seed_bid(exchange, repo)]
    exchange.orders["manual"] = replace(
        exchange.orders[bids[0].exchange_id], order_id="manual", client_id="unowned"
    )
    runner.set_run_mode(mode)

    def fail(*args):
        raise TimeoutError("partial read failed")

    setattr(exchange, failing_read, fail)
    market = replace(exchange.fetch_market_snapshot("BNBUSDT"), drawdown_1m=D("0.02"))
    runner.run_once(market=market)
    assert repo.runtime().guard.active
    assert any(alert.code == "CRASH_GUARD" for _, alert in repo.pending_alerts())
    assert all(exchange.orders[o.exchange_id].status == "CANCELED" for o in bids)
    assert exchange.orders["manual"].is_open
    assert all(o.kind == OperationKind.CANCEL for o in exchange.writes[2:])
    runner.run_once(market=market)
    assert len(exchange.writes) == 4, "terminal/pending cancels must not be resubmitted"


def test_degraded_cancel_timeout_stays_unknown_without_duplicate(tmp_path):
    exchange, repo, runner = accumulate_setup(tmp_path)
    seed_bid(exchange, repo)
    exchange.read_failure = True
    exchange.lose_response = OperationKind.CANCEL
    runner.tick()
    cancel = next(o for o in repo.operations() if o.kind == OperationKind.CANCEL)
    assert cancel.status == OperationStatus.UNKNOWN
    runner.tick()
    assert len(exchange.writes) == 2


def test_degraded_observe_mode_never_submits_cancels(tmp_path):
    exchange, repo, runner = accumulate_setup(tmp_path)
    seed_bid(exchange, repo)
    runner.execute_enabled = False
    exchange.read_failure = True
    runner.tick()
    assert len(exchange.writes) == 1


@pytest.mark.parametrize("age_hours", [2, 8])
def test_non_inventory_reprice_replaces_net_gap_and_keeps_quote_funds(
    tmp_path, age_hours
):
    exchange, repo, runner = accumulate_setup(tmp_path)
    bid = seed_bid(exchange, repo, age_hours=age_hours, qty="5")
    repo.save("runtime", replace(repo.runtime(), last_inventory_at=exchange.now))
    runner.tick()
    assert exchange.orders[bid.exchange_id].status == "CANCELED"
    replacements = [o for o in exchange.writes[1:] if o.kind == OperationKind.ORDER]
    assert len(replacements) == 3
    assert sum(o.payload.qty for o in replacements) == D("5")
    assert not any(o.kind == OperationKind.TRANSFER for o in exchange.writes)
    assert not repo.runtime().resume_repricing


def test_reprice_unknown_cancel_waits_and_resumes_after_restart(tmp_path):
    exchange, repo, runner = accumulate_setup(tmp_path)
    bid = seed_bid(exchange, repo, age_hours=2, qty="5")
    exchange.lose_response = OperationKind.CANCEL
    exchange.query_operation = lambda op: OperationResult(OperationStatus.UNKNOWN)
    exchange.cancel_fill_qty = D("1")
    run(runner, exchange, inventory=False)
    assert len(exchange.writes) == 2
    assert repo.runtime().resume_repricing
    repo.close()
    repo = Repository(tmp_path / "test.sqlite3")
    runner = Runner(exchange, repo, make_strategy_config(), clock=lambda: exchange.now)
    run(runner, exchange, inventory=False)
    assert len(exchange.writes) == 2
    exchange.query_operation = lambda op: FakeExchange.query_operation(exchange, op)
    run(runner, exchange, inventory=False)
    replacements = [o for o in exchange.writes[2:] if o.kind == OperationKind.ORDER]
    assert sum(o.payload.qty for o in replacements) == D("4")
    assert len([o for o in exchange.writes if o.kind == OperationKind.CANCEL]) == 1
    assert exchange.orders[bid.exchange_id].filled_qty == D("1")
    assert not any(
        o.kind == OperationKind.TRANSFER and o.payload.asset == "USDT"
        for o in exchange.writes
    )


def test_reprice_waiting_for_slice_retains_continuation_and_funds(tmp_path):
    exchange, repo, runner = accumulate_setup(tmp_path)
    seed_bid(exchange, repo, age_hours=2, qty="3")
    deadline = exchange.now + timedelta(minutes=10)
    repo.save(
        "runtime", replace(repo.runtime(), slice_state=SliceState(True, deadline))
    )
    run(runner, exchange, inventory=False)
    assert len(exchange.writes) == 2
    assert repo.runtime().resume_repricing
    exchange.now = deadline
    run(runner, exchange, inventory=False)
    replacements = [o for o in exchange.writes[2:] if o.kind == OperationKind.ORDER]
    assert sum(o.payload.qty for o in replacements) == D("3")
    assert not any(o.kind == OperationKind.TRANSFER for o in exchange.writes)


@pytest.mark.parametrize("restart", [False, True])
def test_rejected_reprice_retains_funds_and_resumes_on_account_tick(tmp_path, restart):
    exchange, repo, runner = accumulate_setup(tmp_path)
    bid = seed_bid(exchange, repo, age_hours=2, qty="5")
    repo.save("runtime", replace(repo.runtime(), last_inventory_at=exchange.now))
    exchange.reject_kind = OperationKind.ORDER
    runner.tick()
    assert exchange.orders[bid.exchange_id].status == "CANCELED"
    assert len(exchange.writes) == 3  # Old bid, cancel, one rejected replacement.
    assert any(o.status == OperationStatus.FAILED for o in repo.operations())
    assert repo.runtime().resume_repricing
    exchange.reject_kind = None
    if restart:
        repo.close()
        repo = Repository(tmp_path / "test.sqlite3")
        runner = Runner(exchange, repo, make_strategy_config(), clock=lambda: exchange.now)
    exchange.now += timedelta(seconds=11)
    runner.tick(replace(exchange.fetch_market_snapshot("BNBUSDT"), return_1h=D("0.10")))
    assert len(exchange.writes) == 3
    assert exchange.account.spot_usd == D("5000")
    assert repo.runtime().resume_repricing
    exchange.now += timedelta(seconds=11)
    runner.tick()
    replacements = exchange.writes[3:]
    assert len(replacements) == 3
    assert all(o.kind == OperationKind.ORDER for o in replacements)
    assert sum(o.payload.qty for o in replacements) == D("5")
    assert not repo.runtime().resume_repricing
    exchange.now += timedelta(seconds=11)
    runner.tick()
    assert len(exchange.writes) == 6


def test_rejected_first_buy_after_funding_resumes_without_another_transfer(tmp_path):
    exchange, repo, runner = setup(
        tmp_path,
        FakeExchange(make_account(contract_bnb=D("27"), contract_max_withdraw_amount=D("50000"))),
        ReplenishmentState.ACCUMULATE,
    )
    exchange.reject_kind = OperationKind.ORDER
    runner.tick()
    assert [o.kind for o in exchange.writes] == [OperationKind.TRANSFER, OperationKind.ORDER]
    assert repo.runtime().resume_replenishment
    exchange.reject_kind = None
    exchange.now += timedelta(seconds=11)
    runner.tick()
    assert len(exchange.writes) == 5
    assert all(o.kind == OperationKind.ORDER for o in exchange.writes[2:])
    assert not repo.runtime().resume_replenishment


def test_rejected_reprice_preserves_slice_deadline_and_funds(tmp_path):
    exchange, repo, runner = accumulate_setup(tmp_path)
    seed_bid(exchange, repo, age_hours=2, qty="3")
    repo.save("runtime", replace(repo.runtime(), slice_state=SliceState(active=True)))
    exchange.reject_kind = OperationKind.ORDER
    run(runner, exchange, inventory=False)
    deadline = repo.runtime().slice_state.next_at
    assert deadline == exchange.now + timedelta(minutes=30)
    assert repo.runtime().resume_repricing
    exchange.reject_kind = None
    exchange.now += timedelta(seconds=11)
    run(runner, exchange, inventory=False)
    assert len(exchange.writes) == 3
    assert repo.runtime().slice_state.next_at == deadline
    assert repo.runtime().resume_repricing
    exchange.now = deadline
    run(runner, exchange, inventory=False)
    assert sum(o.payload.qty for o in exchange.writes[3:]) == D("3")
    assert not any(o.kind == OperationKind.TRANSFER for o in exchange.writes)


def test_repeated_reprice_rejections_still_pause_automatic_execution(tmp_path):
    exchange, repo, runner = accumulate_setup(tmp_path)
    seed_bid(exchange, repo, age_hours=2, qty="5")
    repo.save("runtime", replace(repo.runtime(), last_inventory_at=exchange.now))
    exchange.reject_kind = OperationKind.ORDER
    for _ in range(3):
        runner.tick()
        exchange.now += timedelta(seconds=11)
    assert repo.runtime().run_mode == RunMode.PAUSED
    assert repo.runtime().resume_repricing
    assert len(exchange.writes) == 5  # Old bid, cancel, three distinct rejections.
    exchange.reject_kind = None
    runner.tick()
    assert len(exchange.writes) == 5


def test_unknown_reprice_order_does_not_restore_continuation_after_restart(tmp_path):
    exchange, repo, runner = accumulate_setup(tmp_path)
    seed_bid(exchange, repo, age_hours=2, qty="5")
    repo.save("runtime", replace(repo.runtime(), last_inventory_at=exchange.now))
    exchange.lose_response = OperationKind.ORDER
    exchange.query_operation = lambda op: OperationResult(OperationStatus.UNKNOWN)
    runner.tick()
    assert len(exchange.writes) == 3
    assert not repo.runtime().resume_repricing
    repo.close()
    repo = Repository(tmp_path / "test.sqlite3")
    runner = Runner(exchange, repo, make_strategy_config(), clock=lambda: exchange.now)
    runner.tick()
    assert all(o.kind == OperationKind.CANCEL for o in exchange.writes[3:])
    assert not repo.runtime().resume_repricing
    assert repo.operations(unresolved_only=True)
    exchange.query_operation = lambda op: FakeExchange.query_operation(exchange, op)
    exchange.now += timedelta(seconds=11)
    runner.tick()
    assert not repo.operations(unresolved_only=True)
    assert sum(o.kind == OperationKind.ORDER for o in exchange.writes) == 2


@pytest.mark.parametrize("blocker", [
    "crash", "chase", "paused", "urgent_only", "funding_veto", "missing_window",
])
def test_reprice_continuation_rechecks_all_gates_after_cancel(tmp_path, blocker):
    exchange, repo, runner = accumulate_setup(tmp_path)
    seed_bid(exchange, repo, age_hours=2, qty="5")
    submit, market = exchange.submit, exchange.fetch_market_snapshot
    exchange.feed = object()

    def cancel_and_change_conditions(op):
        result = submit(op)
        if op.kind == OperationKind.CANCEL:
            if blocker in {"crash", "chase", "missing_window"}:
                changes = {
                    "crash": {"drawdown_1m": D("0.02")},
                    "chase": {"return_1h": D("0.10")},
                    "missing_window": {"drawdown_15m": None},
                }[blocker]
                exchange.fetch_market_snapshot = lambda symbol: replace(
                    market(symbol), **changes
                )
            elif blocker == "funding_veto":
                exchange.account = replace(
                    exchange.account,
                    spot_usd=D("0"),
                    contract_max_withdraw_amount=D("4000"),
                )
            else:
                mode = RunMode.PAUSED if blocker == "paused" else RunMode.URGENT_ONLY
                repo.save("runtime", replace(repo.runtime(), run_mode=mode))
        return result

    exchange.submit = cancel_and_change_conditions
    run(runner, exchange, inventory=False)
    assert [o.kind for o in exchange.writes] == [
        OperationKind.ORDER, OperationKind.CANCEL
    ]


def test_failed_order_lookup_still_cancels_journaled_exchange_id(tmp_path):
    exchange, repo, runner = accumulate_setup(tmp_path)
    bid = seed_bid(exchange, repo)

    def fail(*args, **kwargs):
        raise TimeoutError()

    exchange.fetch_account_snapshot = exchange.fetch_order = fail
    runner.tick()
    assert exchange.orders[bid.exchange_id].status == "CANCELED"
    assert len(exchange.writes) == 2


def test_degraded_reconciliation_retains_reported_episode_fills(tmp_path):
    exchange, repo, runner = accumulate_setup(tmp_path)
    bid = seed_bid(exchange, repo)
    guard = CrashGuardState(
        True, "guard-test", exchange.now, exchange.now, keep_price_ceiling=D("590")
    )
    repo.save("runtime", replace(repo.runtime(), guard=guard))
    repo.save("episode:guard-test", guard)
    exchange.fill(bid.exchange_id, D("0.5"))
    exchange.read_failure = True
    runner.tick()
    assert repo.runtime().guard.active
    assert repo.runtime().guard.filled_bnb == D("0.5")
    assert repo.load("episode:guard-test").filled_bnb == D("0.5")
    assert exchange.orders[bid.exchange_id].status == "CANCELED"


def funded_setup(tmp_path):
    exchange, repo, runner = setup(
        tmp_path,
        FakeExchange(make_account(
            contract_bnb=D("27"), contract_max_withdraw_amount=D("50000"),
        )),
        ReplenishmentState.ACCUMULATE,
    )
    exchange.hold_transfers = True
    runner.tick()
    exchange.settle("1")
    exchange.hold_transfers = False
    exchange.now += timedelta(seconds=11)
    return exchange, repo, runner


@pytest.mark.parametrize("blocker", ["chase", "paused", "urgent_only", "slice", "market"])
@pytest.mark.parametrize("restart", [False, True])
def test_funded_continuation_waits_without_sweeping_or_refunding(
    tmp_path, blocker, restart,
):
    exchange, repo, runner = funded_setup(tmp_path)
    market = exchange.fetch_market_snapshot("BNBUSDT")
    if blocker in {"paused", "urgent_only"}:
        runner.set_run_mode(
            RunMode.PAUSED if blocker == "paused" else RunMode.URGENT_ONLY
        )
    elif blocker == "slice":
        repo.save("runtime", replace(
            repo.runtime(),
            slice_state=SliceState(True, exchange.now + timedelta(seconds=20)),
        ))
    elif blocker == "chase":
        market = replace(market, return_1h=D("0.04"))
    else:
        market = replace(market, drawdown_15m=None)
    runner.tick(market)
    assert repo.runtime().resume_replenishment
    assert len(exchange.writes) == 1 and exchange.account.spot_usd == D("3000")
    if restart:
        repo.close()
        repo = Repository(tmp_path / "test.sqlite3")
        runner = Runner(exchange, repo, make_strategy_config(), clock=lambda: exchange.now)
    runner.set_run_mode(RunMode.AUTO)
    exchange.now += timedelta(seconds=21)
    runner.tick()
    assert any(op.kind == OperationKind.ORDER for op in exchange.writes)
    assert sum(op.kind == OperationKind.TRANSFER for op in exchange.writes) == 1
    assert not repo.runtime().resume_replenishment
    repo.close()


def test_funded_continuation_rebudgets_when_prices_rise(tmp_path):
    exchange, repo, runner = funded_setup(tmp_path)
    old_market = exchange.fetch_market_snapshot
    exchange.fetch_market_snapshot = lambda symbol: replace(
        old_market(symbol), best_bid=D("799.5"), best_ask=D("800.5"),
        mid_price=D("800"), vwap_5m=D("800"), smooth_price=D("800"),
    )
    runner.tick()
    buys = [op.payload for op in exchange.writes if op.kind == OperationKind.ORDER]
    assert buys and sum(o.qty for o in buys) < D("5")
    assert sum(o.qty * o.price for o in buys) * D("1.001") <= D("3000")
    assert sum(op.kind == OperationKind.TRANSFER for op in exchange.writes) == 1
    assert not repo.runtime().resume_replenishment
    repo.close()


@pytest.mark.parametrize("reason", ["idle", "covered", "crash"])
def test_ended_funding_continuation_releases_the_sweep_reservation(tmp_path, reason):
    exchange, repo, runner = funded_setup(tmp_path)
    if reason == "idle":
        exchange.account = replace(exchange.account, contract_bnb=D("29"))
        repo.save("runtime", replace(repo.runtime(), state=ReplenishmentState.IDLE))
    elif reason == "covered":
        exchange.account = replace(exchange.account, spot_bnb=D("5.5"))
    else:
        market = exchange.fetch_market_snapshot
        exchange.fetch_market_snapshot = lambda symbol: replace(
            market(symbol), drawdown_1m=D("0.02"),
        )
    runner.tick()
    assert not repo.runtime().resume_replenishment
    assert all(op.kind != OperationKind.ORDER for op in exchange.writes)
    exchange.now += timedelta(seconds=11)
    runner.tick()
    assert any(
        op.kind == OperationKind.TRANSFER
        and op.payload.asset == "USDT"
        and op.payload.to_account == "USDⓈ-M Futures"
        for op in exchange.writes
    )
    repo.close()
