from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal

from core.config import validate_config
from core.crash_guard import update_guard_from_market
from core.models import (
    Alert,
    AssetTransferPlan,
    EngineInputs,
    GateStatus,
    MarketSnapshot,
    OperationKind,
    OperationStatus,
    ReplenishmentState,
    RunMode,
)
from core.replenishment_engine import build_cycle_plan
from core.time_utils import age_seconds, fresh, utc
from .executor import Executor
from .history import historical_metrics
from .market_data import MarketWindows
from .reconciliation import Reconciler


class Runner:
    """Serial orchestration. After each exchange mutation, discard the snapshot.

    Inventory ticks advance hysteresis. Market ticks only update risk, manage
    orders and allow urgent replenishment. One cycle has at most one funding
    transfer, one BNB return and one group of at most three buys.
    """

    def __init__(
        self, exchange, repository, cfg, *, clock=None, alerts=None, execute=True
    ):
        validate_config(cfg)
        self.exchange, self.repository, self.cfg = exchange, repository, cfg
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.alerts = alerts
        self.execute_enabled = execute
        self.executor = Executor(exchange, repository, cfg, clock=self.clock)
        self.reconciler = Reconciler(exchange, repository, cfg, clock=self.clock)
        self.windows = MarketWindows(cfg.crash_guard)
        self.cached_inputs = None
        self.last_reconciled_at = None
        with repository.exclusive():
            runtime = repository.runtime()
            repository.save(
                "runtime",
                replace(
                    runtime,
                    guard=replace(
                        runtime.guard, stable_since=None, last_checked_at=None
                    ),
                ),
            )

    def set_run_mode(self, mode: RunMode):
        with self.repository.exclusive():
            runtime = self.repository.runtime()
            self.repository.save(
                "runtime",
                replace(
                    runtime,
                    run_mode=mode,
                    api_errors=0,
                    transfer_failures=0,
                    pause_reason="",
                ),
            )

    def tick(self, raw_market=None):
        """Call each check_seconds; HTTP account reconciliation has a slower cadence."""
        with self.repository.exclusive():
            market = None
            try:
                now = self.clock()
                raw = raw_market or self._read_market()
                market = (
                    raw
                    if raw.smooth_price is not None
                    else self.windows.update(raw, now)
                )
                latched = self._consume_stream_risk()
                runtime = self.repository.runtime()
                inventory_due = (
                    runtime.last_inventory_at is None
                    or age_seconds(now, runtime.last_inventory_at)
                    >= self.cfg.scheduler.t_check_minutes * 60
                )
                reconcile_due = self.last_reconciled_at is None or age_seconds(
                    now, self.last_reconciled_at
                ) >= min(
                    self.cfg.scheduler.reconciliation_seconds,
                    self.cfg.risk.max_account_age_seconds,
                )
                if self.cached_inputs is not None:
                    inputs = replace(
                        self.cached_inputs,
                        market=market,
                        now=now,
                        crash_guard=runtime.guard,
                        run_mode=runtime.run_mode,
                        previous_state=runtime.state,
                        previous_candidate_state=runtime.candidate,
                        candidate_streak=runtime.candidate_streak,
                        slice_state=runtime.slice_state,
                        reserve_replenishment_funds=runtime.resume_repricing
                        or runtime.resume_replenishment,
                        advance_state=False,
                        allow_new_cycle=False,
                        allow_guard_recovery=False,
                    )
                    plan = build_cycle_plan(inputs, self.cfg)
                    risk_changed = latched or (
                        plan.crash_guard.active != runtime.guard.active
                        or plan.crash_guard.reasons != runtime.guard.reasons
                    )
                    # A cached account cannot retire or overwrite execution state.
                    self._persist_plan(
                        plan, inventory=False, now=now, update_slice=False
                    )
                    if (
                        not inventory_due
                        and not reconcile_due
                        and not risk_changed
                        and not plan.cancellations
                    ):
                        self._publish(plan.alerts)
                        return plan
                return self._cycle(market, inventory_due)
            except Exception as exc:
                return self._failed_read(exc, market)

    def run_once(self, *, market=None, inventory_cycle=True):
        """Explicit cycle entry, also useful for deterministic replay and tests."""
        with self.repository.exclusive():
            try:
                if market is None:
                    raw = self._read_market()
                    market = (
                        raw
                        if raw.smooth_price is not None
                        else self.windows.update(raw, self.clock())
                    )
                return self._cycle(market, inventory_cycle)
            except Exception as exc:
                return self._failed_read(exc, market)

    def _failed_read(self, exc, market=None):
        self.executor.note_failure(reason=type(exc).__name__)
        self.repository.event(self.clock(), "API_ERROR", type(exc).__name__)
        # Never reuse a partially reconciled view for trading or guard recovery.
        self.cached_inputs = None
        self.last_reconciled_at = None
        if market is None or not fresh(
            market.ts, self.clock(), self.cfg.crash_guard.max_market_age_seconds
        ):
            raw = self._read_market()
            market = (
                raw
                if raw.smooth_price is not None
                else self.windows.update(raw, self.clock())
            )
        self._consume_stream_risk()
        runtime = self.repository.runtime()
        guard = update_guard_from_market(runtime.guard, market, self.clock(), self.cfg)
        if guard.episode_id:
            episode = self.repository.load("episode:" + guard.episode_id, guard)
            guard = replace(guard, filled_bnb=max(guard.filled_bnb, episode.filled_bnb))
        guard = replace(guard, stable_since=None)
        self.repository.save_guard(replace(runtime, guard=guard), self.clock())
        if self.execute_enabled:
            # Exposure is unverified: cancel only journaled ordinary bids. No
            # filters, balances or transfer history are needed to address them.
            for cancel in self.reconciler.protective_cancellations():
                self.executor.execute(cancel, self.clock(), runtime.state)
        alerts = (
            Alert(
                "WARNING",
                "API_ERROR",
                "Exchange reconciliation or data validation failed",
            ),
            Alert(
                "WARNING",
                "PROTECTIVE_CANCEL_ONLY",
                "Incomplete data: new actions blocked; protecting known strategy bids",
            ),
        )
        if guard.active:
            alerts += (
                Alert(
                    "WARNING", "CRASH_GUARD",
                    "Crash protection active: " + ",".join(guard.reasons),
                ),
            )
        self._publish(alerts)
        return None

    def _read_market(self):
        try:
            return self.exchange.fetch_market_snapshot(self.cfg.symbol)
        except Exception:
            self.executor.note_failure(reason="Market data unavailable")
            # Missing prices must not prevent reconciliation and known-order cancels.
            zero = Decimal("0")
            return MarketSnapshot(
                None, self.cfg.symbol, zero, zero, zero, zero, zero, zero
            )

    def _publish(self, alerts):
        runtime = self.repository.runtime()
        if runtime.pause_reason:
            alerts = tuple(alerts) + (
                Alert("CRITICAL", "AUTO_PAUSED", runtime.pause_reason),
            )
        self.repository.publish_alerts(alerts)
        if self.alerts is not None:
            self.alerts.notify()

    def _persist_plan(self, plan, *, inventory, now, update_slice=True):
        runtime = self.repository.runtime()
        self.repository.save_guard(
            replace(
                runtime,
                state=plan.state_decision.confirmed_state,
                candidate=plan.state_decision.candidate_state
                if inventory
                else runtime.candidate,
                candidate_streak=plan.state_decision.candidate_streak
                if inventory
                else runtime.candidate_streak,
                last_inventory_at=now if inventory else runtime.last_inventory_at,
                guard=plan.crash_guard,
                slice_state=plan.slice_state if update_slice else runtime.slice_state,
            ),
            now,
        )

    def _consume_stream_risk(self):
        feed = getattr(self.exchange, "feed", None)
        if feed is None or not hasattr(feed, "pending_risk"):
            return False
        consumed = False
        for _ in range(2):
            event = feed.pending_risk()
            if event is None:
                break
            runtime = self.repository.runtime()
            guard = runtime.guard
            if guard.active:
                ceilings = [
                    v for v in (guard.keep_price_ceiling, event.keep_price_ceiling)
                    if v is not None
                ]
                guard = replace(
                    guard,
                    started_at=min(guard.started_at, event.started_at, key=utc),
                    last_trigger_at=max(
                        guard.last_trigger_at or event.last_trigger_at,
                        event.last_trigger_at, key=utc,
                    ),
                    keep_price_ceiling=min(ceilings) if ceilings else None,
                    reasons=tuple(dict.fromkeys(guard.reasons + event.reasons)),
                    stable_since=None,
                    last_checked_at=None,
                )
            elif (
                guard.ended_at is None
                or utc(event.last_trigger_at) > utc(guard.ended_at)
            ):
                guard = replace(
                    event,
                    started_at=max(event.started_at, guard.ended_at, key=utc)
                    if guard.ended_at else event.started_at,
                )
            else:
                feed.acknowledge_risk(event)
                continue
            now = self.clock()
            guard = self.reconciler.guard_with_fills(guard, now)
            self.repository.save_guard(replace(runtime, guard=guard), now)
            # A failed commit leaves the leased event available for retry; new
            # samples have their own slot and cannot be erased by this ack.
            feed.acknowledge_risk(event)
            consumed = True
        return consumed

    def _inputs(self, market, inventory, *, continuing=False):
        self._consume_stream_risk()
        now = self.clock()
        account, orders, pending, consistent = self.reconciler.refresh(now)
        filters = self.exchange.fetch_symbol_filters(self.cfg.symbol)
        # Reconciliation may be slow. Never timestamp an old quote as a new one.
        if getattr(self.exchange, "feed", None) is not None or not fresh(
            market.ts, self.clock(), self.cfg.crash_guard.max_market_age_seconds
        ):
            raw = self._read_market()
            market = (
                raw
                if raw.smooth_price is not None
                else self.windows.update(raw, self.clock())
            )
        self._consume_stream_risk()
        now = self.clock()
        runtime = self.repository.runtime()
        rate, margin = historical_metrics(self.repository, account, now)
        if (
            inventory and consistent and not pending.unresolved
            and fresh(account.ts, now, self.cfg.risk.max_account_age_seconds)
        ):
            self.repository.save_snapshot(account)
        inputs = EngineInputs(
            account,
            pending,
            market,
            filters,
            runtime.run_mode,
            rate,
            runtime.state,
            runtime.candidate,
            runtime.candidate_streak,
            self.repository.budget_used(now, self.cfg.quote_asset),
            now=now,
            orders=orders,
            crash_guard=runtime.guard,
            margin_change_24h=margin,
            slice_state=runtime.slice_state,
            advance_state=inventory,
            allow_new_cycle=inventory or continuing,
            snapshot_consistent=consistent,
            reserve_replenishment_funds=runtime.resume_repricing
            or runtime.resume_replenishment,
        )
        self.cached_inputs = inputs
        self.last_reconciled_at = now
        return inputs

    def _cycle(self, market, inventory):
        used = set()
        queue = None
        submitted_qty = Decimal("0")
        group_qty = Decimal("0")
        # A bounded cycle can manage many old orders but never resubmit an intent.
        for index in range(64):
            runtime = self.repository.runtime()
            inputs = self._inputs(
                market,
                inventory and index == 0,
                continuing=inventory
                or runtime.resume_replenishment
                or runtime.resume_repricing
                or bool(queue),
            )
            saved_slice = inputs.slice_state
            # A funded continuation spends settled balances, and a queued group
            # may finish its slice without funding or opening another group.
            inputs = replace(
                inputs,
                allow_usd_funding=(
                    "fund" not in used and queue is None
                    and not self.repository.runtime().resume_replenishment
                ),
                slice_state=replace(saved_slice, next_at=None) if queue else saved_slice,
            )
            plan = build_cycle_plan(inputs, self.cfg)
            if queue:
                plan = replace(
                    plan,
                    slice_state=replace(plan.slice_state, next_at=saved_slice.next_at),
                )
            self._persist_plan(plan, inventory=inventory and index == 0, now=inputs.now)
            if index == 0:
                self.repository.event(inputs.now, "CYCLE", plan)
            self._publish(plan.alerts)
            if not self.execute_enabled:
                return plan
            if self._consume_stream_risk():
                if submitted_qty:
                    used.add("buys")
                queue = None
                market = inputs.market
                continue
            if not plan.cancellations and (
                plan.buy_plan.orders
                or plan.transfer_decision.allow
                or plan.bnb_transfer_plan
                or plan.spot_usd_sweep_plan
            ):
                # Local persistence/queueing can also take time. Check the actual
                # submission time, and discard stale candidates before journaling.
                now = self.clock()
                needs_market = bool(
                    plan.buy_plan.orders
                    or plan.transfer_decision.allow
                    or plan.spot_usd_sweep_plan
                )
                if not fresh(
                    inputs.account.ts, now, self.cfg.risk.max_account_age_seconds
                ) or (
                    needs_market
                    and not fresh(
                        inputs.market.ts,
                        now,
                        self.cfg.crash_guard.max_market_age_seconds,
                    )
                ):
                    if submitted_qty:
                        used.add("buys")
                    queue = None
                    market = inputs.market
                    continue
            action = None
            tag = None
            if plan.cancellations:
                candidates = [c for c in plan.cancellations if c.order_id not in used]
                if candidates:
                    action, tag = candidates[0], candidates[0].order_id
            elif inputs.pending.unresolved:
                return plan
            elif plan.bnb_transfer_plan and "bnb" not in used:
                action, tag = plan.bnb_transfer_plan, "bnb"
            elif plan.transfer_decision.allow:
                action = AssetTransferPlan(
                    self.cfg.quote_asset,
                    plan.transfer_decision.adjusted_amount,
                    "USDⓈ-M Futures",
                    "SPOT",
                    "Fund next replenishment after confirmation",
                )
                tag = "fund"
            elif plan.buy_plan.orders and "buys" not in used:
                if queue is None:
                    queue = list(plan.buy_plan.orders)
                    group_qty = sum((o.qty for o in queue), Decimal("0"))
                if queue:
                    candidate = queue[0]
                    allowed = sum((o.qty for o in plan.buy_plan.orders), Decimal("0"))
                    max_price = max(o.price for o in plan.buy_plan.orders)
                    free = inputs.account.spot_usd - inputs.account.reserved_spot_usd
                    if (
                        candidate.qty <= min(allowed, group_qty - submitted_qty)
                        and candidate.price <= max_price
                        and candidate.qty % inputs.filters.qty_step == 0
                        and candidate.price % inputs.filters.price_tick == 0
                        and candidate.qty >= inputs.filters.min_qty
                        and candidate.qty * candidate.price
                        >= inputs.filters.min_notional
                        and candidate.qty
                        * candidate.price
                        * (1 + self.cfg.risk.fee_reserve_rate)
                        <= free
                        and candidate.time_in_force
                        == plan.buy_plan.orders[0].time_in_force
                    ):
                        action = queue.pop(0)
                        submitted_qty += action.qty
                    else:
                        used.add("buys")
                if not queue:
                    used.add("buys")
            elif (
                plan.spot_usd_sweep_plan
                and "sweep" not in used
                and "fund" not in used
                and queue is None
            ):
                action, tag = plan.spot_usd_sweep_plan, "sweep"
            if action is None:
                if (
                    not self.repository.operations(unresolved_only=True)
                    and plan.gates.data_gate == GateStatus.OPEN
                ):
                    runtime = self.repository.runtime()
                    state = plan.state_decision.confirmed_state
                    continuing = plan.state_decision.delta_bnb > 0 and (
                        state == ReplenishmentState.URGENT
                        or (
                            state in {
                                ReplenishmentState.WATCH,
                                ReplenishmentState.ACCUMULATE,
                            }
                            and not plan.crash_guard.active
                        )
                    )
                    self.repository.save(
                        "runtime",
                        replace(
                            runtime,
                            api_errors=0,
                            resume_replenishment=runtime.resume_replenishment
                            and continuing,
                            resume_repricing=runtime.resume_repricing
                            and continuing
                            and state != ReplenishmentState.URGENT,
                        ),
                    )
                return plan
            if tag is not None:
                used.add(tag)
            op = self.executor.execute(
                action,
                self.clock(),
                plan.state_decision.confirmed_state,
                account=inputs.account,
                starts_slice=hasattr(action, "qty")
                and plan.slice_state.active
                and submitted_qty == action.qty,
            )
            if op.status in {OperationStatus.UNKNOWN, OperationStatus.FAILED}:
                self._publish(
                    plan.alerts
                    + (
                        Alert(
                            "WARNING",
                            "EXECUTION_UNRESOLVED",
                            f"{op.kind.value}: {op.status.value}",
                        ),
                    )
                )
                # Continue only protective cancels, never replacement orders.
                if op.kind != OperationKind.CANCEL:
                    return plan
            market = inputs.market
        self.repository.event(
            self.clock(),
            "CYCLE_LIMIT",
            "Remaining work deferred to next reconciliation",
        )
        return plan
