from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from services.market_depth import DepthSnapshots


NOW = datetime(2026, 10, 13, tzinfo=timezone.utc)


def event(first=100, last=103, *, ts=NOW, bids=None, asks=None):
    return {"e": "depthUpdate", "s": "BNBUSDT", "E": int(ts.timestamp() * 1000),
            "U": first, "u": last, "b": bids or [], "a": asks or []}


def snapshot(version=101):
    return {"lastUpdateId": version, "bids": [["599", "2"], ["598", "4"]],
            "asks": [["601", "3.5"], ["602", "7"]]}


def test_buffered_diff_is_applied_to_rest_book_without_matching_partial_version():
    book = DepthSnapshots("BNBUSDT", 5)
    # First changed level is not best. The snapshot lies inside the diff range,
    # so the changes must actually be applied before publishing.
    diff = event(bids=[["598", "9"], ["600", "6"]],
                 asks=[["602", "8"], ["601", "0"]])
    assert book.update(diff, NOW) is None
    quotes = book.initialize(snapshot(), NOW + timedelta(seconds=1))
    quote, received = quotes[0]
    assert received == NOW
    assert quote.ts == NOW
    assert (quote.best_bid, quote.best_ask, quote.ask_qty) == (
        Decimal("600"), Decimal("602"), Decimal("8"))
    next_quote = book.update(event(104, 109, ts=NOW + timedelta(seconds=1),
                                  bids=[["600", "0"]], asks=[["600.5", "1.25"]]),
                             NOW + timedelta(seconds=1))
    assert (next_quote.best_bid, next_quote.best_ask, next_quote.ask_qty) == (
        Decimal("599"), Decimal("600.5"), Decimal("1.25"))


def test_exhausted_known_levels_cannot_promote_unknown_outside_snapshot():
    book = DepthSnapshots("BNBUSDT", 5)
    book.update(event(), NOW)
    book.initialize(snapshot(), NOW)
    # This changed level lies beyond REST's known interval. There might be
    # unchanged hidden levels between 602 and 605, so 605 is not a proved best.
    with pytest.raises(ValueError, match="known depth"):
        book.update(event(104, 105, ts=NOW + timedelta(seconds=1),
                          asks=[["601", "0"], ["602", "0"], ["605", "9"]]),
                    NOW + timedelta(seconds=1))


def test_snapshot_buffer_overflow_requires_a_new_subscription_baseline():
    book = DepthSnapshots("BNBUSDT", 5)
    for number in range(book.MAX_PENDING):
        book.update(event(number, number), NOW)
    with pytest.raises(ValueError, match="buffer"):
        book.update(event(book.MAX_PENDING, book.MAX_PENDING), NOW)
    assert len(book.pending) <= book.MAX_PENDING


def test_old_snapshot_retries_then_bridges_boundary_and_skips_covered_updates():
    book = DepthSnapshots("BNBUSDT", 5)
    book.update(event(100, 101, asks=[["601", "9"]]), NOW)
    book.update(event(102, 103, ts=NOW + timedelta(seconds=1),
                      asks=[["601", "2"]]), NOW + timedelta(seconds=1))
    assert book.initialize(snapshot(99), NOW + timedelta(seconds=1)) == []
    assert book.needs_snapshot
    quotes = book.initialize(snapshot(101), NOW + timedelta(seconds=1))
    assert len(quotes) == 1
    assert quotes[0][0].ask_qty == Decimal(2)
    assert quotes[0][0].ts == NOW + timedelta(seconds=1)
    assert not book.needs_snapshot


def test_rest_snapshot_without_a_new_diff_cannot_publish_source_time():
    book = DepthSnapshots("BNBUSDT", 5)
    book.update(event(100, 101), NOW)
    assert book.initialize(snapshot(105), NOW) == []
    assert book.last_source_at is None
    assert book.update(event(102, 105), NOW) is None
    assert book.update(event(106, 110), NOW).ts == NOW


@pytest.mark.parametrize("age", [timedelta(seconds=5), timedelta(seconds=5, microseconds=1),
                                 timedelta(microseconds=-1)])
def test_source_freshness_uses_exact_five_second_boundary(age):
    book = DepthSnapshots("BNBUSDT", 5)
    if age == timedelta(seconds=5):
        book.update(event(), NOW + age)
        assert book.initialize(snapshot(), NOW + age)[0][0].ts == NOW
    else:
        with pytest.raises(ValueError, match="Stale or future"):
            book.update(event(), NOW + age)


def test_buffered_event_that_expires_during_snapshot_request_cannot_publish():
    book = DepthSnapshots("BNBUSDT", 5)
    book.update(event(), NOW)
    with pytest.raises(ValueError, match="Stale or future"):
        book.initialize(snapshot(), NOW + timedelta(seconds=5, microseconds=1))


def test_duplicates_and_out_of_order_versions_never_renew_quote_time_or_quantity():
    book = DepthSnapshots("BNBUSDT", 5)
    book.update(event(), NOW)
    quote = book.initialize(snapshot(), NOW)[0][0]
    for version in (103, 102, 100):
        assert book.update(event(100, version, ts=NOW + timedelta(seconds=1),
                                 asks=[["601", "100"]]), NOW + timedelta(seconds=1)) is None
    # A real later update keeps the known quantity, not the replayed mutation.
    q = book.update(event(104, 104, ts=NOW + timedelta(seconds=2)), NOW + timedelta(seconds=2))
    assert q.ask_qty == quote.ask_qty == Decimal("3.5")
    # Distinct IDs in the same source millisecond apply but cannot mint liquidity.
    assert book.update(event(105, 106, ts=q.ts, asks=[["601", "7"]]), q.ts) is None
    q = book.update(event(107, 107, ts=NOW + timedelta(seconds=3)), NOW + timedelta(seconds=3))
    assert q.ask_qty == 7


@pytest.mark.parametrize("diff", [event(105, 108), event(104, 106, ts=NOW - timedelta(seconds=1))])
def test_missing_sequence_or_backwards_source_time_require_rebuild(diff):
    book = DepthSnapshots("BNBUSDT", 5)
    book.update(event(), NOW)
    book.initialize(snapshot(), NOW)
    with pytest.raises(ValueError, match="gap|backwards"):
        book.update(diff, NOW)


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-1", "bad", None])
def test_invalid_quantity_invalidates_book_instead_of_assuming_zero(value):
    book = DepthSnapshots("BNBUSDT", 5)
    with pytest.raises((ValueError, TypeError, ArithmeticError)):
        book.update(event(asks=[["601", value]]), NOW)


def test_levels_inside_known_range_are_bounded(monkeypatch):
    book = DepthSnapshots("BNBUSDT", 5)
    book.update(event(), NOW)
    book.initialize(snapshot(), NOW)
    monkeypatch.setattr(book, "MAX_LEVELS", 2)
    with pytest.raises(ValueError, match="known depth"):
        book.update(event(104, 105, bids=[["598.5", "1"]]), NOW)
