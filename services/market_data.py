from collections import deque
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from statistics import median

from core.models import CrashGuardConfig, MarketSnapshot
from core.time_utils import age_seconds, fresh, utc


class MarketWindows:
    """A continuous sequence of sampled, smoothed mid prices. Restart warms up."""

    def __init__(self, cfg: CrashGuardConfig):
        self.cfg = cfg
        self.raw = deque()
        self.smoothed = deque()

    def update(self, market: MarketSnapshot, now) -> MarketSnapshot:
        market = replace(
            market,
            smooth_price=None,
            drawdown_1m=None,
            drawdown_5m=None,
            drawdown_15m=None,
        )
        if (
            not fresh(market.ts, now, self.cfg.max_market_age_seconds)
            or not all(
                x.is_finite() and x > 0
                for x in (market.best_bid, market.best_ask, market.mid_price)
            )
            or not market.best_bid <= market.mid_price <= market.best_ask
        ):
            self.raw.clear()
            self.smoothed.clear()
            return market
        ts = utc(now)
        if self.raw and ts <= self.raw[-1][0]:
            # Replaying one quote must not manufacture a continuous window.
            return market
        if (
            self.raw
            and age_seconds(ts, self.raw[-1][0]) > self.cfg.max_sample_gap_seconds
        ):
            self.raw.clear()
            self.smoothed.clear()
        self.raw.append((ts, market.mid_price))
        while self.raw[0][0] < ts - timedelta(seconds=self.cfg.smooth_seconds):
            self.raw.popleft()
        price = median(p for _, p in self.raw)
        self.smoothed.append((ts, price))
        cutoff = ts - timedelta(seconds=900 + self.cfg.max_sample_gap_seconds)
        while self.smoothed[0][0] < cutoff:
            self.smoothed.popleft()
        drawdowns = []
        for seconds in (60, 300, 900):
            boundary = ts - timedelta(seconds=seconds)
            if self.smoothed[0][0] > boundary:
                drawdowns.append(None)
            else:
                high = max(p for t, p in self.smoothed if t >= boundary)
                drawdowns.append(max(Decimal("0"), 1 - price / high))
        return replace(
            market,
            smooth_price=price,
            drawdown_1m=drawdowns[0],
            drawdown_5m=drawdowns[1],
            drawdown_15m=drawdowns[2],
        )
