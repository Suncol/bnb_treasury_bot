from datetime import datetime, timedelta, timezone

import pytest

from core.crash_guard import market_invalid_reasons
from services.market_stream import MarketStream
from tests.helpers import depth_messages, make_strategy_config


def test_continuous_depth_observations_keep_windows_and_freshness_without_any_trade_or_ticker(monkeypatch):
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
    # 31 minutes, unchanged best prices and quantity, no ticker or trade event.
    # Frames arrive in either order; snapshots skip IDs as full snapshots may.
    for tenth in range(18601):
        now = start + timedelta(milliseconds=100*tenth)
        frames = depth_messages(now, 100+tenth*7)
        for frame in (frames if tenth % 2 else frames[::-1]):
            stream.on_message(frame)
        q = stream.quote()
        assert stream.execution_quote().ts == now
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


@pytest.mark.parametrize("kind", ["missing_pair", "duplicate", "stale", "future"])
def test_depth_failures_never_refresh_or_reuse_quotes_across_invalidation(monkeypatch, kind):
    cfg = make_strategy_config()
    stream = MarketStream("BNBUSDT", cfg.crash_guard, client=object())
    now = datetime(2026, 10, 13, tzinfo=timezone.utc)
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now
    monkeypatch.setattr("services.market_stream.datetime", Clock)
    frames = depth_messages(now)
    for frame in frames: stream.on_message(frame)
    original = stream.execution_quote()
    now += timedelta(seconds=6)
    if kind == "missing_pair":
        stream.on_message(depth_messages(now, 2)[0])
        assert stream.execution_quote() is original
    elif kind == "duplicate":
        # Old frames cannot be re-stamped with the receipt time.
        for frame in frames: stream.on_message(frame)
        assert stream.execution_quote() is None
    else:
        ts = now + (timedelta(seconds=1) if kind == "future" else timedelta(seconds=-6))
        for frame in depth_messages(ts, 2): stream.on_message(frame)
        assert stream.execution_quote() is None
    stream._invalidate("TEST_DISCONNECT")
    stream.on_message(depth_messages(now, 2)[1])
    assert stream.execution_quote() is None
    stream.on_message(depth_messages(now, 2)[0])
    assert not stream.quote().windows_ready
    assert stream.execution_quote().ts == now
