from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from core.crash_guard import market_invalid_reasons
from services.exchange_adapter import ExchangeError
from services.market_stream import MarketStream
from tests.helpers import depth_messages, make_strategy_config


def test_continuous_diff_book_keeps_windows_without_partial_ticker_or_trade(monkeypatch):
    cfg = make_strategy_config()
    stream = MarketStream("BNBUSDT", cfg.crash_guard, client=object())
    now = datetime(2026, 10, 13, tzinfo=timezone.utc)
    start = now
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now
    monkeypatch.setattr("services.market_stream.datetime", Clock)
    version = stream.windows.continuity
    samples = 0
    previous_sample = None
    # 31 minutes of continuous update ranges. No partial stream is subscribed.
    for tenth in range(18601):
        now = start + timedelta(milliseconds=100*tenth)
        snapshot, event = depth_messages(now, 100+tenth*7)
        event["u"] = event["U"] + 6
        # Include non-best changes first, with a known literal best quantity.
        event["a"] = [["602", "19"], ["601", str(2 + tenth % 3)]]
        event["b"] = [["598", "21"], ["599", "4"]]
        stream.on_message(event)
        if tenth == 0:
            snapshot["asks"].append(["605", "1"])
            snapshot["bids"].append(["595", "1"])
            stream.on_snapshot(snapshot)
        q = stream.quote()
        execution = stream.execution_quote()
        assert execution.ts == now
        assert (execution.best_bid, execution.best_ask) == (Decimal(599), Decimal(601))
        assert execution.ask_qty == Decimal(2 + tenth % 3)
        assert stream.windows.continuity == version
        if q.sampled_at != previous_sample:
            samples += 1
            previous_sample = q.sampled_at
        if tenth >= 9000:
            assert q.windows_ready
            assert not market_invalid_reasons(q, now, cfg)
            assert q.drawdown_15m == 0
    assert samples == 1861
    assert len(stream.windows.smoothed) == 901
    assert stream.health()['matched_quotes'] == 18601


@pytest.mark.parametrize("kind", ["gap", "stale", "future", "invalid_quantity"])
def test_depth_failure_closes_quotes_and_rebuild_requires_full_prewarm(monkeypatch, kind):
    stream = MarketStream("BNBUSDT", make_strategy_config().crash_guard, client=object())
    now = datetime(2026, 10, 13, tzinfo=timezone.utc)
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now
    monkeypatch.setattr("services.market_stream.datetime", Clock)
    snapshot, event = depth_messages(now)
    stream.on_message(event)
    stream.on_snapshot(snapshot)
    original = stream.execution_quote()
    now += timedelta(seconds=1)
    snapshot, event = depth_messages(now, 3)
    if kind == "gap":
        event["U"], event["u"] = 5, 8
    elif kind == "invalid_quantity":
        event["a"] = [["601", "NaN"]]
    else:
        ts = now + (timedelta(seconds=1) if kind == "future" else timedelta(seconds=-6))
        event["E"] = int(ts.timestamp() * 1000)
    stream.on_message(event)
    assert original is not None and stream.execution_quote() is None
    with pytest.raises(ExchangeError):
        stream.quote()
    for second in range(901):
        now += timedelta(seconds=1)
        snapshot, event = depth_messages(now, 10+second)
        stream.on_message(event)
        if second == 0:
            stream.on_snapshot(snapshot)
        assert stream.quote().windows_ready == (second == 900)


def test_raw_messages_ping_and_replayed_updates_cannot_renew_quote_or_liquidity(monkeypatch):
    stream = MarketStream("BNBUSDT", make_strategy_config().crash_guard, client=object())
    now = datetime(2026, 10, 13, tzinfo=timezone.utc)
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now
    monkeypatch.setattr("services.market_stream.datetime", Clock)
    snapshot, event = depth_messages(now)
    stream.on_message(event)
    stream.on_snapshot(snapshot)
    original = stream.execution_quote()
    now += timedelta(seconds=6)
    for frame in (snapshot, event, {"result": None, "id": 1}):
        stream.on_message(frame)
    stream._ping()
    assert stream.execution_quote() is original
    assert "MARKET_TIMESTAMP" in market_invalid_reasons(stream.quote(), now, make_strategy_config())
    stream.close()
    stream.on_message(depth_messages(now, 3)[1])
    stream.on_snapshot(depth_messages(now, 3)[0])
    assert stream.execution_quote() is None
