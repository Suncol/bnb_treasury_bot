from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from services.market_depth import DepthSnapshots
from tests.helpers import depth_messages


NOW = datetime(2026, 10, 13, tzinfo=timezone.utc)


@pytest.mark.parametrize("reverse", [False, True])
def test_only_equal_depth_versions_supply_source_timestamp_and_full_best_prices(reverse):
    matcher = DepthSnapshots("BNBUSDT", 5)
    pair = depth_messages()
    if reverse:
        pair = pair[::-1]
    assert matcher.update(pair[0], NOW) is None
    quote = matcher.update(pair[1], NOW + timedelta(seconds=1))
    assert quote.ts == NOW  # Receipt time must not extend the source lifetime.
    assert (quote.best_bid, quote.best_ask, quote.ask_qty) == (Decimal(599), Decimal(601), Decimal('3.5'))


def test_mismatched_or_missing_frames_cannot_refresh_a_quote_or_build_liquidity():
    matcher = DepthSnapshots("BNBUSDT", 5)
    snapshot, event = depth_messages()
    assert matcher.update(snapshot, NOW) is None
    assert matcher.update({**event, "u": 2}, NOW) is None
    assert matcher.update({**event, "s": "BTCUSDT"}, NOW) is None
    assert matcher.last_source_at is None
    quote = matcher.update(event, NOW)
    for second in range(1, 5):
        for frame in (snapshot, event):
            assert matcher.update(frame, NOW + timedelta(seconds=second)) is None
    assert matcher.last_source_at == quote.ts == NOW


@pytest.mark.parametrize("age", [timedelta(seconds=5), timedelta(seconds=5, microseconds=1), timedelta(milliseconds=-1)])
def test_source_freshness_keeps_exact_five_second_boundary(age):
    matcher = DepthSnapshots("BNBUSDT", 5)
    snapshot, event = depth_messages()
    now = NOW + age
    matcher.update(snapshot, now)
    if age == timedelta(seconds=5):
        assert matcher.update(event, now).ts == NOW
    else:
        with pytest.raises(ValueError, match="Stale or future"):
            matcher.update(event, now)


def test_out_of_order_versions_and_expired_unmatched_frames_never_refresh():
    matcher = DepthSnapshots("BNBUSDT", 5)
    for frame in depth_messages(update_id=10):
        quote = matcher.update(frame, NOW)
    assert quote.ts == NOW
    for frame in depth_messages(NOW + timedelta(seconds=1), update_id=9):
        assert matcher.update(frame, NOW + timedelta(seconds=1)) is None
    snapshot, event = depth_messages(NOW + timedelta(seconds=6), update_id=11)
    matcher.update(snapshot, NOW)
    assert matcher.update(event, NOW + timedelta(seconds=6)) is None
    assert matcher.last_source_at == NOW


def test_unmatched_storage_is_bounded_and_same_version_needs_a_new_exchange_event():
    matcher = DepthSnapshots("BNBUSDT", 5)
    for i in range(1000):
        assert matcher.update(depth_messages(update_id=i)[0], NOW) is None
    assert len(matcher.snapshots) == matcher.MAX_PENDING
    assert matcher.update(depth_messages(update_id=999)[1], NOW).ts == NOW
    next_at = NOW + timedelta(seconds=1)
    snapshot, event = depth_messages(next_at, update_id=999)
    assert matcher.update(event, next_at) is None
    assert matcher.update(snapshot, next_at).ts == next_at


@pytest.mark.parametrize("quantity", ["NaN", "Infinity", "-1", "bad", None])
def test_invalid_quantity_cannot_supply_paper_liquidity_but_keeps_price_evidence(quantity):
    matcher = DepthSnapshots("BNBUSDT", 5)
    for frame in depth_messages(ask_qty=quantity):
        quote = matcher.update(frame, NOW)
    assert quote.ask_qty is None
    assert quote.best_ask == 601


@pytest.mark.parametrize("bid,ask", [("NaN", "601"), ("599", "Infinity"), ("0", "601"), ("602", "601")])
def test_invalid_snapshot_prices_are_rejected(bid, ask):
    matcher = DepthSnapshots("BNBUSDT", 5)
    snapshot, event = depth_messages()
    snapshot["bids"][0][0], snapshot["asks"][0][0] = bid, ask
    matcher.update(snapshot, NOW)
    with pytest.raises(ValueError, match="prices"):
        matcher.update(event, NOW)
