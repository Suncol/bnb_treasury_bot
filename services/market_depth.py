"""A bounded known order book bootstrapped from REST and advanced by diff depth."""

from collections import deque
from decimal import Decimal

from core.models import ExecutionQuote
from core.time_utils import fresh, utc
from .sdk_transport import timestamp


class DepthSnapshots:
    SNAPSHOT_LIMIT = 5000
    MAX_PENDING = 64
    MAX_LEVELS = 10000

    def __init__(self, symbol, max_age_seconds):
        self.symbol, self.max_age_seconds = symbol, max_age_seconds
        self.pending = deque()
        self.last_update_id = None
        self.last_source_at = None
        self.bids, self.asks = {}, {}
        self.bid_floor = self.ask_ceiling = None

    @property
    def needs_snapshot(self):
        return self.last_update_id is None

    @staticmethod
    def _version(value):
        if type(value) is not int or value < 0:
            raise ValueError("Invalid depth update ID")
        return value

    @classmethod
    def _levels(cls, rows):
        if not isinstance(rows, list) or len(rows) > cls.MAX_LEVELS:
            raise ValueError("Invalid or oversized depth levels")
        result = {}
        for row in rows:
            price, qty = Decimal(row[0]), Decimal(row[1])
            if not price.is_finite() or price <= 0 or not qty.is_finite() or qty < 0:
                raise ValueError("Invalid depth prices or quantities")
            if price in result:
                raise ValueError("Duplicate depth price level")
            result[price] = qty
        return result

    def update(self, data, now):
        if data.get("e") != "depthUpdate" or data.get("s") != self.symbol:
            return None
        first, last = self._version(data["U"]), self._version(data["u"])
        if first > last or type(data["E"]) is not int:
            raise ValueError("Invalid depth event metadata")
        if self.last_update_id is not None and last <= self.last_update_id:
            return None
        source_at, now = timestamp(data["E"]), utc(now)
        if not fresh(source_at, now, self.max_age_seconds):
            raise ValueError("Stale or future depth event")
        bids, asks = self._levels(data["b"]), self._levels(data["a"])
        if self.needs_snapshot:
            if self.pending and last <= self.pending[-1][1]:
                return None
            if len(self.pending) >= self.MAX_PENDING:
                raise ValueError("Depth initialization buffer exhausted")
            self.pending.append((first, last, source_at, bids, asks, now))
            return None
        return self._apply(first, last, source_at, bids, asks)

    def initialize(self, data, now):
        """Apply every buffered update; REST alone has no quote source time."""
        version = self._version(data["lastUpdateId"])
        if not self.pending or version < self.pending[0][0]:
            return []  # Snapshot predates the subscription; fetch a newer one.
        bids, asks = self._levels(data["bids"]), self._levels(data["asks"])
        if not bids or not asks or any(q <= 0 for q in (*bids.values(), *asks.values())):
            raise ValueError("Incomplete depth snapshot")
        self.bids, self.asks = bids, asks
        self.bid_floor, self.ask_ceiling = min(bids), max(asks)
        self.last_update_id = version
        quotes = []
        while self.pending:
            first, last, source_at, bids, asks, received = self.pending.popleft()
            if last <= version:
                continue
            if not fresh(source_at, now, self.max_age_seconds):
                raise ValueError("Stale or future depth event")
            quote = self._apply(first, last, source_at, bids, asks)
            if quote is not None:
                quotes.append((quote, received))
        return quotes

    def _apply(self, first, last, source_at, bids, asks):
        if last <= self.last_update_id:
            return None
        if first > self.last_update_id + 1:
            raise ValueError("Depth sequence gap")
        if self.last_source_at is not None and source_at < self.last_source_at:
            raise ValueError("Depth source time moved backwards")
        for book, changes, inside in (
            (self.bids, bids, lambda price: price >= self.bid_floor),
            (self.asks, asks, lambda price: price <= self.ask_ceiling),
        ):
            for price, qty in changes.items():
                if not inside(price):
                    continue  # Unchanged levels beyond the snapshot are unknown.
                if qty == 0:
                    book.pop(price, None)
                else:
                    book[price] = qty
            if not book or len(book) > self.MAX_LEVELS:
                raise ValueError("Insufficient or exhausted known depth")
        self.last_update_id = last
        bid, ask = max(self.bids), min(self.asks)
        if bid > ask:
            raise ValueError("Invalid crossed depth prices")
        if self.last_source_at is not None and source_at <= self.last_source_at:
            return None
        self.last_source_at = source_at
        return ExecutionQuote(source_at, self.symbol, bid, ask, self.asks[ask])
