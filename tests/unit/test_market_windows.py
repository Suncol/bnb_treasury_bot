from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from services.market_data import MarketWindows
from tests.helpers import make_market, make_strategy_config


@pytest.mark.parametrize(
    "source_ms,received_ms,reset",
    [
        (3000, 3001, False),  # Live ticker: 3 s source gap, 1 ms delivery jitter.
        (3000, 3100, False),  # Inclusive jitter boundary, without float rounding.
        (3000, 3101, True),
        (3001, 3001, True),  # Longer source gaps always reset.
        (3001, 2900, True),  # Compressed delivery cannot hide a source outage.
        (1000, 3001, True),  # A stalled receiver cannot use queued source data.
        (2000, 2500, False),
        (3000, 2999, False),
        (4000, 4000, True),
    ],
)
def test_warmed_windows_distinguish_delivery_jitter_from_sample_gaps(source_ms, received_ms, reset):
    windows = MarketWindows(make_strategy_config().crash_guard)
    start = datetime(2026, 10, 12, tzinfo=timezone.utc)
    latency = timedelta(milliseconds=200)
    for seconds in range(901):
        source = start + timedelta(seconds=seconds)
        warmed = windows.update(replace(make_market(), ts=source), source + latency)
    assert warmed.windows_ready
    version = warmed.sample_continuity
    source = start + timedelta(seconds=900, milliseconds=source_ms)
    received = start + timedelta(seconds=900, milliseconds=received_ms) + latency
    sampled = windows.update(replace(make_market(), ts=source), received)
    assert (sampled.sample_continuity != version) == reset
    assert sampled.windows_ready == (not reset)
    if reset:
        assert sampled.sample_stable_since is None
        assert len(windows.smoothed) == 1


@pytest.mark.parametrize("source_ms", [0, -1, 4000])
def test_invalid_or_future_source_time_still_clears_windows(source_ms):
    windows = MarketWindows(make_strategy_config().crash_guard)
    start = datetime(2026, 10, 12, tzinfo=timezone.utc)
    first = windows.update(replace(make_market(), ts=start), start)
    source = start + timedelta(milliseconds=source_ms)
    sampled = windows.update(replace(make_market(), ts=source), start + timedelta(seconds=1))
    assert not sampled.windows_ready
    assert windows.continuity != first.sample_continuity
    assert not windows.smoothed


@pytest.mark.parametrize("gap_seconds", [3, 5])
def test_continuous_ticks_keep_15m_coverage_across_allowed_gap_and_delivery_jitter(gap_seconds):
    cfg = replace(make_strategy_config().crash_guard, max_sample_gap_seconds=gap_seconds)
    windows = MarketWindows(cfg)
    start = datetime(2026, 10, 13, tzinfo=timezone.utc)
    version = windows.continuity
    # Replay the live failure: the old boundary point falls 219 us outside
    # 900 + max_gap retention, while the next point is 260 us inside 15m.
    end = 1801 + gap_seconds
    for seconds in range(end + 61):
        if 901 < seconds < 901 + gap_seconds:
            continue
        micros = {901: 145904, 901 + gap_seconds: 146383, end: 146123}.get(seconds, 146000)
        source = start + timedelta(seconds=seconds)
        sampled = windows.update(replace(make_market(), ts=source), source + timedelta(microseconds=micros))
        assert sampled.sample_continuity == version
        if seconds >= 901:
            assert sampled.windows_ready, seconds
            assert sampled.sample_stable_since is not None
    # Historical coverage must remain bounded after the continuous replay.
    boundary = sampled.sampled_at - timedelta(seconds=900)
    assert windows.smoothed[0][0] <= boundary
    assert windows.smoothed[1][0] > boundary


def test_retained_coverage_predecessor_is_not_a_price_inside_the_window():
    from decimal import Decimal

    cfg = replace(make_strategy_config().crash_guard, smooth_seconds=1)
    windows = MarketWindows(cfg)
    start = datetime(2026, 10, 13, tzinfo=timezone.utc)
    for seconds in range(905):
        if seconds == 2:
            continue
        ts = start + timedelta(seconds=seconds)
        price = Decimal('1000') if seconds == 0 else Decimal('600')
        sampled = windows.update(replace(make_market(), ts=ts, best_bid=price,
                                        best_ask=price, mid_price=price), ts)
        if seconds == 900:
            assert sampled.drawdown_15m == Decimal('0.4')
        if seconds >= 902:
            assert sampled.windows_ready
            assert sampled.drawdown_15m == 0


def test_retention_does_not_complete_warmup_early_or_preserve_a_real_outage():
    windows = MarketWindows(make_strategy_config().crash_guard)
    start = datetime(2026, 10, 13, tzinfo=timezone.utc)
    for seconds in range(900):
        ts = start + timedelta(seconds=seconds)
        sampled = windows.update(replace(make_market(), ts=ts), ts)
        assert not sampled.windows_ready
    ts = start + timedelta(seconds=900, microseconds=-1)
    assert not windows.update(replace(make_market(), ts=ts), ts).windows_ready
    ts = start + timedelta(seconds=900)
    warm = windows.update(replace(make_market(), ts=ts), ts)
    assert warm.windows_ready
    ts += timedelta(seconds=3, microseconds=1)
    sampled = windows.update(replace(make_market(), ts=ts), ts)
    assert sampled.sample_continuity != warm.sample_continuity
    assert not sampled.windows_ready
    assert len(windows.smoothed) == 1
