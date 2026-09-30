from dataclasses import replace
from datetime import timedelta
from threading import Event, Thread

import pytest

from core.models import (
    Alert,
    CrashGuardState,
    OperationKind,
    ReplenishmentState,
    RunMode,
)
from services.alert_service import AlertService
from services.exchange_adapter import ExchangeError
from services.market_data import MarketWindows
from services.runner import Runner
from storage.repository import Repository
from tests.fake_exchange import FakeExchange
from tests.helpers import D, make_account, make_market, make_strategy_config
from tests.integration.test_execution import setup
from tests.integration.test_execution_boundaries import seed_bid


def test_known_crash_cancels_precede_slow_history_reads(tmp_path):
    exchange, repo, runner = setup(
        tmp_path,
        FakeExchange(make_account(contract_bnb=D("27"), spot_usd=D("5000"))),
        ReplenishmentState.ACCUMULATE,
    )
    bids = [seed_bid(exchange, repo), seed_bid(exchange, repo)]
    start = exchange.now
    sent = []
    for name in (
        "fetch_recent_fills",
        "fetch_recent_transfers",
        "fetch_account_snapshot",
    ):
        method = getattr(exchange, name)

        def slow(*args, _method=method):
            exchange.now += timedelta(seconds=4)
            return _method(*args)

        setattr(exchange, name, slow)
    submit = exchange.submit

    def capture(op):
        if op.kind == OperationKind.CANCEL:
            sent.append(exchange.now - start)
        return submit(op)

    exchange.submit = capture
    runner.run_once(
        market=replace(exchange.fetch_market_snapshot("BNBUSDT"), drawdown_1m=D(".02")),
        inventory_cycle=False,
    )
    assert sent == [timedelta(0), timedelta(0)]
    assert all(not exchange.orders[op.exchange_id].is_open for op in bids)
    events = [r[0] for r in repo.db.execute("SELECT kind FROM events")]
    assert all(
        kind in events
        for kind in ("CRASH_GUARD", "CANCEL_INTENT", "CANCEL_SENT", "CANCEL_TERMINAL")
    )
    repo.close()


def test_stream_trigger_during_history_read_preempts_next_read(tmp_path):
    exchange, repo, runner = setup(
        tmp_path,
        FakeExchange(make_account(contract_bnb=D("27"), spot_usd=D("5000"))),
        ReplenishmentState.ACCUMULATE,
    )
    seed_bid(exchange, repo)
    start = exchange.now
    dropped = False

    class Feed:
        def quote(self):
            return replace(
                make_market(),
                ts=exchange.now,
                drawdown_1m=D(".02") if dropped else D("0"),
            )

    exchange.feed = Feed()
    read = exchange.fetch_recent_fills

    def slow(*args):
        nonlocal dropped
        exchange.now += timedelta(seconds=4)
        dropped = True
        return read(*args)

    exchange.fetch_recent_fills = slow
    cancellations = []
    submit = exchange.submit

    def capture(op):
        if op.kind == OperationKind.CANCEL:
            cancellations.append(exchange.now - start)
        return submit(op)

    exchange.submit = capture
    runner.run_once(inventory_cycle=False)
    assert cancellations == [timedelta(seconds=4)]
    assert repo.runtime().guard.active
    repo.close()


def test_retry_after_is_persistent_and_pause_does_not_restart_read_storm(
    tmp_path, monkeypatch
):
    monkeypatch.setattr("services.runner.random.uniform", lambda *_: 0)
    exchange, repo, runner = setup(tmp_path)
    calls = []

    def limited():
        calls.append(exchange.now)
        raise ExchangeError(
            "rate limited", http_status=429, retry_after=60, endpoint="account"
        )

    exchange.fetch_account_snapshot = limited
    start = exchange.now
    for second in range(0, 125):
        exchange.now = start + timedelta(seconds=second)
        runner.tick()
        if second == 20:
            repo.close()
            repo = Repository(tmp_path / "test.sqlite3")
            runner = Runner(
                exchange, repo, make_strategy_config(), clock=lambda: exchange.now
            )
    assert calls == [start + timedelta(seconds=n) for n in (0, 60, 120)]
    assert repo.runtime().run_mode == RunMode.PAUSED
    assert not exchange.writes
    assert repo.runtime().read_retry_at == start + timedelta(seconds=180)
    repo.close()


def test_lock_contention_defers_tick_without_api_failure(tmp_path):
    exchange, repo, runner = setup(tmp_path)
    other = Repository(repo.path)
    with other.exclusive():
        assert runner.tick() is None
        assert not exchange.writes
    assert repo.runtime().api_errors == 0
    assert runner.tick() is not None
    assert exchange.writes
    other.close()
    repo.close()


def test_controller_lease_rejects_competing_writer_and_allows_status(tmp_path):
    first = Repository(tmp_path / "lease.db")
    second = Repository(first.path)
    first.acquire_controller()
    assert second.runtime() == first.runtime()
    with pytest.raises(RuntimeError, match="already owns"):
        second.acquire_controller()
    first.close()
    second.acquire_controller()
    second.close()


def test_outbox_can_deliver_during_slow_account_read(tmp_path):
    exchange, repo, runner = setup(tmp_path)
    delivered, reading, release = Event(), Event(), Event()

    class Sink:
        name = "test"

        def send(self, alert, key):
            delivered.set()

    alerts = AlertService(repo, [Sink()])
    repo.publish_alerts((Alert("WARNING", "test", "test"),))
    read = exchange.fetch_recent_fills

    def slow(*args):
        reading.set()
        assert release.wait(3)
        return read(*args)

    exchange.fetch_recent_fills = slow
    thread = Thread(target=runner.tick)
    thread.start()
    try:
        assert reading.wait(2)
        alerts.notify()
        assert delivered.wait(1), "Outbox waited for the account execution lock"
    finally:
        release.set()
        thread.join(3)
        alerts.close()
        repo.close()


def test_slow_controller_recovers_from_sampler_evidence_but_disconnect_resets():
    from core.crash_guard import update_guard_from_market

    cfg = make_strategy_config()
    windows = MarketWindows(cfg.crash_guard)
    start = make_market().ts
    guard = CrashGuardState(
        active=True,
        episode_id="audit",
        started_at=start,
        last_trigger_at=start,
        keep_price_ceiling=D("594"),
    )
    for second in range(1, 1201):
        now = start + timedelta(seconds=second)
        sample = windows.update(replace(make_market(), ts=now), now)
        if second % 4 == 0:
            guard = update_guard_from_market(
                guard, sample, now, cfg, recovery_allowed=True
            )
    assert not guard.active
    old_version = sample.sample_continuity
    now += timedelta(seconds=5)
    broken = windows.update(replace(make_market(), ts=now), now)
    assert broken.sample_continuity != old_version
    assert broken.sample_stable_since is None and not broken.windows_ready
    active = replace(guard, active=True, ended_at=None)
    assert update_guard_from_market(
        active, broken, now, cfg, recovery_allowed=True
    ).active


def test_priority_binding_survives_inflight_query_with_changed_client_id(tmp_path):
    from core.models import OperationResult, OperationStatus, OrderPlan

    exchange, repo, runner = setup(
        tmp_path,
        FakeExchange(make_account(contract_bnb=D("27"), spot_usd=D("5000"))),
        ReplenishmentState.ACCUMULATE,
    )
    exchange.lose_response = OperationKind.ORDER
    op = runner.executor.execute(
        OrderPlan("BNBUSDT", "BUY", "LIMIT", D("1"), D("598"), "GTC", False),
        exchange.now,
        ReplenishmentState.ACCUMULATE,
    )
    assert op.exchange_id is None
    submit = exchange.submit

    def rename_after_cancel(operation):
        result = submit(operation)
        if operation.kind == OperationKind.CANCEL:
            exchange.orders[operation.payload.order_id] = replace(
                exchange.orders[operation.payload.order_id], client_id="cancel-id"
            )
        return result

    exchange.submit = rename_after_cancel
    trigger = False

    class Feed:
        def quote(self):
            return replace(
                make_market(),
                ts=exchange.now,
                drawdown_1m=D(".02") if trigger else D("0"),
            )

    exchange.feed = Feed()

    def query(operation):
        nonlocal trigger
        if operation.kind == OperationKind.ORDER and operation.exchange_id is None:
            trigger = True
            return OperationResult(OperationStatus.UNKNOWN)
        return OperationResult(OperationStatus.CONFIRMED, operation.exchange_id)

    exchange.query_operation = query
    runner.run_once(inventory_cycle=False)
    assert repo.operation(op.client_id).exchange_id == "1"
    runner.run_once(inventory_cycle=False)
    assert repo.operation(op.client_id).status == OperationStatus.CONFIRMED
    assert len([w for w in exchange.writes if w.kind == OperationKind.CANCEL]) == 1
    repo.close()


def test_failed_query_preserves_identity_bound_by_its_priority_checkpoint(tmp_path):
    from core.models import OrderPlan

    exchange, repo, runner = setup(
        tmp_path,
        FakeExchange(make_account(contract_bnb=D("27"), spot_usd=D("5000"))),
        ReplenishmentState.ACCUMULATE,
    )
    exchange.lose_response = OperationKind.ORDER
    op = runner.executor.execute(
        OrderPlan("BNBUSDT", "BUY", "LIMIT", D("1"), D("598"), "GTC", False),
        exchange.now,
        repo.runtime().state,
    )
    original_query = exchange.query_operation

    def failed(operation):
        if operation.kind == OperationKind.ORDER and operation.exchange_id is None:
            runner._priority_market = replace(
                make_market(), ts=exchange.now, drawdown_1m=D(".02")
            )
            runner._priority_checkpoint()
            assert repo.operation(op.client_id).exchange_id == "1"
            raise TimeoutError("query failed after protection bound order identity")
        return original_query(operation)

    exchange.query_operation = failed
    runner.run_once(inventory_cycle=False)
    assert repo.operation(op.client_id).exchange_id == "1"
    assert len([w for w in exchange.writes if w.kind == OperationKind.CANCEL]) == 1
    repo.close()
