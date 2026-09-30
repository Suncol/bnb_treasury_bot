from dataclasses import replace
from datetime import datetime
from decimal import Decimal

from .math_utils import floor_to_multiple
from .models import (
    CrashGuardConfig,
    CrashGuardState,
    EngineInputs,
    MarketSnapshot,
    StrategyConfig,
)
from .order_planner import compute_ref_price
from .time_utils import age_seconds, fresh, utc
from .validation import valid_account_snapshot


def valid_market(inputs: EngineInputs, cfg: StrategyConfig) -> bool:
    return valid_market_snapshot(inputs.market, inputs.now, cfg)


def valid_market_snapshot(
    m: MarketSnapshot, now: datetime, cfg: StrategyConfig
) -> bool:
    values = (m.best_bid, m.best_ask, m.mid_price, m.vwap_5m)
    drawdowns = (m.drawdown_1m, m.drawdown_5m, m.drawdown_15m)
    return (
        m.symbol == cfg.symbol
        and fresh(m.ts, now, cfg.crash_guard.max_market_age_seconds)
        and all(v.is_finite() and v > 0 for v in values)
        and m.best_bid <= m.mid_price <= m.best_ask
        and all(v is None or (v.is_finite() and 0 <= v <= 1) for v in drawdowns)
        and (
            m.smooth_price is None
            or (m.smooth_price.is_finite() and m.smooth_price > 0)
        )
        and m.return_1h.is_finite()
        and m.return_24h.is_finite()
    )


def update_crash_guard(inputs: EngineInputs, cfg: StrategyConfig) -> CrashGuardState:
    return update_guard_from_market(
        inputs.crash_guard,
        inputs.market,
        inputs.now,
        cfg,
        price_tick=inputs.filters.price_tick,
        recovery_allowed=not inputs.pending.unresolved
        and inputs.snapshot_consistent
        and inputs.allow_guard_recovery
        and valid_account_snapshot(inputs.account, inputs.now, cfg),
    )


def trigger_reasons(m: MarketSnapshot, c: CrashGuardConfig) -> tuple[str, ...]:
    return tuple(
        name
        for name, value, threshold in (
            ("1m", m.drawdown_1m, c.drawdown_1m_enter),
            ("5m", m.drawdown_5m, c.drawdown_5m_enter),
            ("15m", m.drawdown_15m, c.drawdown_15m_enter),
        )
        if value is not None and value >= threshold
    )


def latch_guard_trigger(
    guard: CrashGuardState, m: MarketSnapshot, now: datetime, c: CrashGuardConfig
) -> CrashGuardState:
    """Accumulate risk only; stream sampling must never release protection."""
    reasons = trigger_reasons(m, c)
    if reasons and not guard.active:
        guard = CrashGuardState(
            active=True, episode_id=f"guard-{now.isoformat()}", started_at=now
        )
    if not guard.active:
        return guard
    ceiling = guard.keep_price_ceiling
    if m.smooth_price is not None:
        new_ceiling = min(compute_ref_price(m), m.smooth_price) * (
            1 - c.keep_price_discount
        )
        ceiling = new_ceiling if ceiling is None else min(ceiling, new_ceiling)
    return replace(
        guard,
        last_trigger_at=now if reasons else guard.last_trigger_at,
        keep_price_ceiling=ceiling,
        reasons=reasons or guard.reasons,
    )


def update_guard_from_market(
    guard: CrashGuardState,
    m: MarketSnapshot,
    now: datetime,
    cfg: StrategyConfig,
    *,
    price_tick: Decimal | None = None,
    recovery_allowed: bool = False,
) -> CrashGuardState:
    """Tighten risk without account/filter reads; require full inputs to recover."""
    c = cfg.crash_guard
    if not valid_market_snapshot(m, now, cfg):
        return replace(guard, stable_since=None, last_checked_at=now)
    reasons = trigger_reasons(m, c)
    guard = latch_guard_trigger(guard, m, now, c)
    if not guard.active:
        return guard
    ceiling = guard.keep_price_ceiling
    if ceiling is not None and price_tick is not None:
        ceiling = floor_to_multiple(ceiling, price_tick)
    stable = guard.stable_since
    if m.sample_continuity is not None:
        # Continuous sampling is independent of REST/controller latency. A
        # reconnect replaces this proof; a lone precomputed snapshot cannot.
        stable = m.sample_stable_since
        if (
            not fresh(m.sampled_at, now, c.max_market_age_seconds)
            or stable is None
            or utc(stable) > utc(m.sampled_at)
        ):
            stable = None
        elif guard.last_trigger_at is not None:
            stable = max(stable, guard.last_trigger_at, key=utc)
    elif (
        guard.last_checked_at is None
        or age_seconds(now, guard.last_checked_at) > c.max_sample_gap_seconds
    ):
        stable = None
    if reasons or not m.windows_ready or m.drawdown_1m >= c.drawdown_1m_exit:
        stable = None
    elif stable is None and m.sample_continuity is None:
        stable = now
    last_trigger = now if reasons else guard.last_trigger_at
    recovered = (
        not reasons
        and stable is not None
        and last_trigger is not None
        and age_seconds(now, stable) >= c.stable_seconds
        and age_seconds(now, last_trigger) >= c.cooldown_seconds
        and recovery_allowed
    )
    return replace(
        guard,
        active=not recovered,
        last_trigger_at=last_trigger,
        stable_since=stable,
        last_checked_at=now,
        keep_price_ceiling=ceiling,
        reasons=reasons or guard.reasons,
        ended_at=now if recovered else None,
    )
