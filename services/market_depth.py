"""Match full top-of-book snapshots to exchange-timestamped depth versions."""

from datetime import timedelta
from decimal import Decimal, InvalidOperation

from core.models import ExecutionQuote
from core.time_utils import fresh, utc
from .sdk_transport import timestamp


class DepthSnapshots:
    # Bound unmatched messages independently of traffic rate and disconnects.
    MAX_PENDING = 32

    def __init__(self, symbol, max_age_seconds):
        self.symbol, self.max_age_seconds = symbol, max_age_seconds
        self.snapshots, self.events = {}, {}
        self.last_update_id = None
        self.last_source_at = None

    def update(self, data, now):
        now = utc(now)
        if data.get("e") == "depthUpdate" and data.get("s") == self.symbol:
            key, target = data["u"], self.events
        elif "lastUpdateId" in data:
            # This callback is bound to the configured symbol's partial stream.
            key, target = data["lastUpdateId"], self.snapshots
        else:
            return None
        if type(key) is not int or key < 0:
            raise ValueError("Invalid depth update ID")
        if self.last_update_id is not None and key < self.last_update_id:
            return None
        target[key] = (data, now)
        cutoff = now - timedelta(seconds=self.max_age_seconds)
        for pending in (self.snapshots, self.events):
            for old in tuple(pending):
                if pending[old][1] < cutoff or (self.last_update_id is not None and old < self.last_update_id):
                    del pending[old]
            while len(pending) > self.MAX_PENDING:
                del pending[next(iter(pending))]
        if key not in self.snapshots or key not in self.events:
            return None
        snapshot, _ = self.snapshots.pop(key)
        event, _ = self.events.pop(key)
        if type(event["E"]) is not int or type(event["U"]) is not int or not 0 <= event["U"] <= key:
            raise ValueError("Invalid depth event metadata")
        source_at = timestamp(event["E"])
        if not fresh(source_at, now, self.max_age_seconds):
            raise ValueError("Stale or future depth event")
        if self.last_source_at is not None and source_at <= self.last_source_at:
            return None  # Replayed frames never renew age or paper liquidity.
        bid, ask = Decimal(snapshot["bids"][0][0]), Decimal(snapshot["asks"][0][0])
        if not bid.is_finite() or not ask.is_finite() or not 0 < bid <= ask:
            raise ValueError("Invalid depth snapshot prices")
        try:
            ask_qty = Decimal(snapshot["asks"][0][1])
            if not ask_qty.is_finite() or ask_qty < 0:
                ask_qty = None
        except (IndexError, InvalidOperation, TypeError, ValueError):
            ask_qty = None  # Price/risk evidence remains usable, fills do not.
        self.last_update_id, self.last_source_at = key, source_at
        return ExecutionQuote(source_at, self.symbol, bid, ask, ask_qty)
