from collections import deque
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from statistics import median
from uuid import uuid4

from core.crash_guard import trigger_reasons
from core.models import CrashGuardConfig, MarketSnapshot
from core.time_utils import fresh, utc


# Event timestamps define source continuity; allow only bounded changes in
# delivery latency at the receipt-time boundary, never longer source gaps.
MAX_DELIVERY_JITTER = timedelta(milliseconds=100)


class MarketWindows:
    """A continuous sequence of sampled, smoothed mid prices. Restart warms up."""

    def __init__(self, cfg: CrashGuardConfig):
        self.cfg = cfg
        self.raw = deque()
        self.smoothed = deque()
        self.continuity = uuid4().hex
        self.stable_since = None
        self.last_source_ts = None

    def reset(self):
        self.raw.clear()
        self.smoothed.clear()
        self.continuity = uuid4().hex
        self.stable_since = None
        self.last_source_ts = None

    def update(self, market: MarketSnapshot, now) -> MarketSnapshot:
        market = replace(
            market,
            smooth_price=None,
            drawdown_1m=None,
            drawdown_5m=None,
            drawdown_15m=None,
            sample_continuity=None,
            sample_stable_since=None,
            sampled_at=None,
        )
        if (
            not fresh(market.ts, now, self.cfg.max_market_age_seconds)
            or not all(
                x.is_finite() and x > 0
                for x in (market.best_bid, market.best_ask, market.mid_price)
            )
            or not market.best_bid <= market.mid_price <= market.best_ask
        ):
            self.reset()
            return market
        ts = utc(now)
        if self.raw and (
            ts <= self.raw[-1][0] or utc(market.ts) <= self.last_source_ts
        ):
            # Replaying one quote must not manufacture a continuous window.
            self.reset()
            return market
        if self.raw:
            limit = timedelta(seconds=self.cfg.max_sample_gap_seconds)
            source_gap = utc(market.ts) - self.last_source_ts
            received_gap = ts - self.raw[-1][0]
            if source_gap > limit or (
                received_gap > limit
                and received_gap > source_gap + MAX_DELIVERY_JITTER
            ):
                self.reset()
        self.last_source_ts = utc(market.ts)
        self.raw.append((ts, market.mid_price))
        while self.raw[0][0] < ts - timedelta(seconds=self.cfg.smooth_seconds):
            self.raw.popleft()
        price = median(p for _, p in self.raw)
        self.smoothed.append((ts, price))
        # Keep the last actual sample at/before the longest window boundary.
        # Fixed 900 + max_gap retention can drop that point by microseconds
        # after allowed delivery jitter, briefly turning a warm window cold.
        cutoff = ts - timedelta(seconds=900)
        while len(self.smoothed) > 1 and self.smoothed[1][0] <= cutoff:
            self.smoothed.popleft()
        drawdowns = []
        for seconds in (60, 300, 900):
            boundary = ts - timedelta(seconds=seconds)
            if self.smoothed[0][0] > boundary:
                drawdowns.append(None)
            else:
                high = max(p for t, p in self.smoothed if t >= boundary)
                drawdowns.append(max(Decimal("0"), 1 - price / high))
        sampled = replace(
            market,
            smooth_price=price,
            drawdown_1m=drawdowns[0],
            drawdown_5m=drawdowns[1],
            drawdown_15m=drawdowns[2],
        )
        if (
            sampled.windows_ready
            and not trigger_reasons(sampled, self.cfg)
            and sampled.drawdown_1m < self.cfg.drawdown_1m_exit
        ):
            self.stable_since = self.stable_since or ts
        else:
            self.stable_since = None
        return replace(
            sampled,
            sample_continuity=self.continuity,
            sample_stable_since=self.stable_since,
            sampled_at=ts,
        )
