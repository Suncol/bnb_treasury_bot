from dataclasses import replace
from datetime import timedelta

import pytest

from core.crash_guard import update_crash_guard
from core.models import CrashGuardState
from core.replenishment_engine import build_cycle_plan
from services.market_data import MarketWindows
from tests.helpers import D, make_account, make_engine_inputs, make_strategy_config
from tests.unit.test_lifecycle import inputs_with_orders, order


@pytest.mark.parametrize(
    "window,threshold",
    [("drawdown_1m", "0.008"), ("drawdown_5m", "0.015"), ("drawdown_15m", "0.03")],
)
def test_guard_enters_at_each_exact_threshold(window, threshold):
    inputs = make_engine_inputs()
    inputs = replace(inputs, market=replace(inputs.market, **{window: D(threshold)}))
    assert update_crash_guard(inputs, make_strategy_config()).active
    lower = replace(
        inputs, market=replace(inputs.market, **{window: D(threshold) - D("0.0001")})
    )
    assert not update_crash_guard(lower, make_strategy_config()).active


def test_24h_drop_does_not_trigger_guard():
    inputs = make_engine_inputs()
    assert not update_crash_guard(
        replace(inputs, market=replace(inputs.market, return_24h=D("-0.30"))),
        make_strategy_config(),
    ).active


def test_guard_selects_lowest_bids_and_cancels_whole_oversized_orders():
    inputs = inputs_with_orders(
        order("high", "598"), order("middle", "590", "2"), order("low", "580", "2")
    )
    inputs = replace(inputs, market=replace(inputs.market, drawdown_1m=D("0.008")))
    plan = build_cycle_plan(inputs, make_strategy_config())
    assert {c.order_id for c in plan.cancellations} == {"high", "middle"}
    assert not plan.buy_plan.orders and not plan.transfer_decision.allow


def test_guard_ceiling_never_rises_or_resets_fill_allowance_on_retrigger():
    inputs = make_engine_inputs()
    first = update_crash_guard(
        replace(inputs, market=replace(inputs.market, drawdown_1m=D("0.01"))),
        make_strategy_config(),
    )
    first = replace(first, filled_bnb=D("2"))
    rebound = replace(
        inputs.market,
        best_bid=D("699"),
        mid_price=D("700"),
        best_ask=D("701"),
        vwap_5m=D("700"),
        smooth_price=D("700"),
        drawdown_1m=D("0.01"),
    )
    next_guard = update_crash_guard(
        replace(inputs, market=rebound, crash_guard=first), make_strategy_config()
    )
    assert next_guard.episode_id == first.episode_id
    assert next_guard.keep_price_ceiling == first.keep_price_ceiling
    assert next_guard.filled_bnb == D("2")


def test_urgent_guard_targets_26_and_shares_gross_episode_limit():
    inputs = make_engine_inputs(
        account=make_account(contract_bnb=D("24"), spot_usd=D("10000"))
    )
    guard = CrashGuardState(
        True,
        "episode",
        inputs.now,
        inputs.now,
        filled_bnb=D("2"),
        keep_price_ceiling=D("594"),
    )
    plan = build_cycle_plan(replace(inputs, crash_guard=guard), make_strategy_config())
    assert plan.active_buy_target == D("26")
    assert sum(o.qty for o in plan.buy_plan.orders) == D("1")
    exhausted = build_cycle_plan(
        replace(inputs, crash_guard=replace(guard, filled_bnb=D("3"))),
        make_strategy_config(),
    )
    assert not exhausted.buy_plan.orders


def test_guard_cancel_pending_retains_exposure_and_blocks_ioc():
    old = order("low", qty="2", cancel_pending=True)
    inputs = inputs_with_orders(
        old, account=make_account(contract_bnb=D("24"), spot_usd=D("10000"))
    )
    inputs = replace(
        inputs,
        pending=replace(inputs.pending, has_pending_cancels=True),
        crash_guard=CrashGuardState(
            True, "episode", inputs.now, inputs.now, keep_price_ceiling=D("594")
        ),
    )
    plan = build_cycle_plan(inputs, make_strategy_config())
    assert not plan.buy_plan.orders and not plan.cancellations


def test_guard_recovery_needs_continuous_observation_and_reconciled_operations():
    cfg = make_strategy_config()
    cfg = replace(
        cfg, crash_guard=replace(cfg.crash_guard, cooldown_seconds=3, stable_seconds=2)
    )
    inputs = make_engine_inputs()
    guard = update_crash_guard(
        replace(inputs, market=replace(inputs.market, drawdown_1m=D("0.01"))), cfg
    )
    for seconds in (1, 2, 3):
        now = inputs.now + timedelta(seconds=seconds)
        sample = replace(
            inputs, now=now, market=replace(inputs.market, ts=now), crash_guard=guard
        )
        guard = update_crash_guard(sample, cfg)
    assert not guard.active
    # A time jump, even with a fresh quote, cannot count as observed stability.
    restarted = replace(
        guard, active=True, stable_since=inputs.now, last_checked_at=inputs.now
    )
    now = inputs.now + timedelta(hours=1)
    result = update_crash_guard(
        replace(
            inputs,
            now=now,
            market=replace(inputs.market, ts=now),
            crash_guard=restarted,
        ),
        cfg,
    )
    assert result.active and result.stable_since == now


def test_market_windows_require_continuity_and_restart_warmup():
    cfg, inputs = make_strategy_config(), make_engine_inputs()
    windows = MarketWindows(cfg.crash_guard)
    for second in range(901):
        now = inputs.now + timedelta(seconds=second)
        sample = windows.update(replace(inputs.market, ts=now), now)
        if second == 60:
            assert sample.drawdown_1m == 0 and sample.drawdown_5m is None
    assert sample.windows_ready
    now += timedelta(seconds=4)
    assert not windows.update(replace(inputs.market, ts=now), now).windows_ready
    assert (
        not MarketWindows(cfg.crash_guard)
        .update(replace(inputs.market, ts=now), now)
        .windows_ready
    )
