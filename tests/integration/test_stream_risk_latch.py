from dataclasses import replace
from datetime import datetime, timedelta
import sqlite3
from types import SimpleNamespace

import pytest

from core.models import OperationKind
from services.market_stream import MarketStream
from services.runner import Runner
from storage.repository import Repository
from tests.helpers import D, make_strategy_config
from tests.integration.test_execution_boundaries import accumulate_setup, seed_bid


@pytest.fixture
def live_quotes(tmp_path, monkeypatch):
    exchange, repo, runner = accumulate_setup(tmp_path)
    stream = MarketStream("BNBUSDT", runner.cfg.crash_guard, client=SimpleNamespace())

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return exchange.now

    monkeypatch.setattr("services.market_stream.datetime", Clock)

    def emit(price, count=1):
        for _ in range(count):
            exchange.now += timedelta(seconds=1)
            price = D(price)
            stream.on_message({
                "s": "BNBUSDT", "e": "24hrTicker", "E": int(exchange.now.timestamp() * 1000),
                "b": str(price - D("0.5")), "a": str(price + D("0.5")), "P": "0",
            })

    emit("600", 901)
    assert stream.quote().windows_ready
    exchange.feed = stream
    exchange.fetch_market_snapshot = lambda symbol: stream.quote()
    yield exchange, repo, runner, stream, emit
    repo.close()


@pytest.mark.parametrize("fill_during_drop", [False, True])
def test_crash_and_rebound_during_rest_keep_onset_and_fill_budget(live_quotes, fill_during_drop):
    exchange, repo, runner, stream, emit = live_quotes
    order = seed_bid(exchange, repo, qty="3") if fill_during_drop else None
    read = exchange.fetch_recent_transfers
    triggered_at = None
    injected = False

    def slow_read(since, until):
        nonlocal triggered_at, injected
        if not injected:
            injected = True
            for _ in range(4):
                emit("570")
                if triggered_at is None and stream.quote().drawdown_1m >= runner.cfg.crash_guard.drawdown_1m_enter:
                    triggered_at = exchange.now
            if order:
                exchange.fill(order.exchange_id, D("1.25"))
            emit("600", 4)
        return read(since, until)

    exchange.fetch_recent_transfers = slow_read
    runner.run_once()
    guard = repo.runtime().guard
    assert stream.quote().drawdown_1m == 0
    assert guard.active and guard.started_at == triggered_at
    assert guard.keep_price_ceiling < D("570")
    assert guard.filled_bnb == (D("1.25") if order else 0)
    assert sum(o.kind == OperationKind.ORDER for o in exchange.writes) == bool(order)
    if order:
        assert not exchange.orders[order.exchange_id].is_open
        assert len(repo.fills_since(guard.started_at)) == 1
    assert stream.pending_risk() is None


def test_acknowledgment_does_not_erase_new_triggers_or_disconnect_risk(live_quotes):
    exchange, repo, runner, stream, emit = live_quotes
    emit("570", 4)
    first = stream.pending_risk()
    emit("540", 4)
    stream._invalidate()
    assert stream.pending_risk() is first
    stream.acknowledge_risk(first)
    second = stream.pending_risk()
    assert second.started_at > first.started_at
    assert second.keep_price_ceiling < first.keep_price_ceiling
    stream.acknowledge_risk(first)
    assert stream.pending_risk() is second
    stream.acknowledge_risk(second)
    assert stream.pending_risk() is None


def test_risk_is_acknowledged_only_after_atomic_commit_and_survives_restart(live_quotes, tmp_path):
    exchange, repo, runner, stream, emit = live_quotes
    emit("570", 4)
    event = stream.pending_risk()
    repo.db.execute("""CREATE TRIGGER reject_guard BEFORE INSERT ON events
        WHEN NEW.kind='CRASH_GUARD' BEGIN SELECT RAISE(ABORT,'injected failure'); END""")
    with pytest.raises(sqlite3.IntegrityError, match="injected"):
        runner._consume_stream_risk()
    assert stream.pending_risk() is event
    assert not repo.runtime().guard.active
    assert repo.load("episode:" + event.episode_id) is None
    repo.db.execute("DROP TRIGGER reject_guard")
    assert runner._consume_stream_risk()
    assert stream.pending_risk() is None
    other = Repository(tmp_path / "test.sqlite3")
    Runner(exchange, other, make_strategy_config(), clock=lambda: exchange.now)
    assert other.runtime().guard.active
    assert other.runtime().guard.episode_id == event.episode_id
    other.close()


def test_crash_during_plan_publication_discards_buy_queue(live_quotes):
    exchange, repo, runner, stream, emit = live_quotes
    publish = runner._publish
    injected = False

    def publish_then_crash(alerts):
        nonlocal injected
        publish(alerts)
        if not injected:
            injected = True
            emit("570", 4)
            emit("600", 4)

    runner._publish = publish_then_crash
    runner.run_once()
    assert repo.runtime().guard.active
    assert not any(
        op.kind == OperationKind.ORDER
        or (op.kind == OperationKind.TRANSFER and op.payload.to_account == "SPOT")
        for op in exchange.writes
    )


def test_new_drop_after_closed_episode_gets_new_identity(live_quotes):
    exchange, repo, runner, stream, emit = live_quotes
    emit("570", 4)
    emit("600", 4)
    runner.run_once()
    previous = repo.runtime().guard
    repo.save_guard(replace(repo.runtime(), guard=replace(
        previous, active=False, ended_at=exchange.now,
    )), exchange.now)
    emit("540", 4)
    runner.run_once()
    current = repo.runtime().guard
    assert current.active and current.episode_id != previous.episode_id
    assert current.started_at > previous.last_trigger_at
    assert current.filled_bnb == 0
