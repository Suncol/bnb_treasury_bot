from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from core.config import validate_config
from core.crash_guard import market_invalid_reasons, update_guard_from_market
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
from core.validation import valid_account_snapshot

from .exchange_adapter import AccountObservationInconsistent, ExchangeError, RequestDeferred
from .executor import ExecutionBlocked, MAX_CANCEL_ATTEMPTS
from .market_data import MarketWindows
from .reconciliation import Reconciler
from .read_backoff import clear_read_backoff, record_read_failure, retry_deadline
from .price_refresh import PriceRefreshPending


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
        self.reconciler = Reconciler(exchange, repository, cfg, clock=self.clock)
        self.executor = self.reconciler.executor
        self.windows = MarketWindows(cfg.crash_guard)
        self.cached_inputs = None
        self.last_reconciled_at = None
        self._last_refresh_duration = timedelta(0)
        self._data_gate_reasons = None
        self._data_gate_logged_at = None
        self._health_alerts = ()
        self._health_checked_at = None
        self._priority_busy = False
        self._priority_revision = 0
        self._priority_market = None
        self.reconciler.read_checkpoint = self._priority_checkpoint
        if hasattr(exchange, "read_checkpoint"):
            exchange.read_checkpoint = self._priority_checkpoint
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
        try:
            return self._tick(raw_market)
        except BlockingIOError:
            logging.getLogger("bnb_treasury").warning(
                "Account lock busy; deferring tick"
            )
            return None

    def _tick(self, raw_market=None):
        """Call each check_seconds; HTTP account reconciliation has a slower cadence."""
        with self.repository.exclusive():
            market = None
            try:
                if self._waiting_for_reads(raw_market):
                    return None
                raw = raw_market or self._read_market()
                now = self.clock()
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
                if inventory_due and runtime.last_inventory_attempt_at is not None:
                    inventory_due = age_seconds(
                        now, runtime.last_inventory_attempt_at
                    ) >= min(
                        self.cfg.scheduler.reconciliation_seconds,
                        self.cfg.risk.max_account_age_seconds,
                    )
                reconcile_due = self._reconciliation_due(now)
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
                        reserve_replenishment_funds=runtime.resume_replanning
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
                        and not self._inventory_change(plan)
                    ):
                        self._log_data_gate(inputs, plan)
                        self._publish(plan.alerts)
                        return plan
                return self._cycle(market, inventory_due)
            except Exception as exc:
                return self._failed_read(exc, market)

    def _reconciliation_due(self, now):
        if self.cached_inputs is None or self.last_reconciled_at is None:
            return True
        if age_seconds(now, self.last_reconciled_at) >= self.cfg.scheduler.reconciliation_seconds:
            return True
        # Account and dynamic filter ages start at acquisition, not at the end
        # of reconciliation. Leave time for the next tick and a normal refresh;
        # a slower refresh must still pass the unchanged execution freshness gates.
        headroom = (
            timedelta(seconds=self.cfg.crash_guard.check_seconds)
            + self._last_refresh_duration
        )
        observations = [
            (self.cached_inputs.account.ts, self.cfg.risk.max_account_age_seconds),
        ]
        if self.cached_inputs.filters.observed_at is not None:
            observations.append(
                (self.cached_inputs.filters.observed_at,
                 self.cfg.crash_guard.max_market_age_seconds)
            )
        return any(
            not fresh(ts, now, lifetime)
            or utc(now) + headroom >= utc(ts) + timedelta(seconds=lifetime)
            for ts, lifetime in observations
        )

    def run_once(self, *, market=None, inventory_cycle=True):
        try:
            return self._run_once(market=market, inventory_cycle=inventory_cycle)
        except BlockingIOError:
            logging.getLogger("bnb_treasury").warning(
                "Account lock busy; deferring cycle"
            )
            return None

    def _run_once(self, *, market=None, inventory_cycle=True):
        """Explicit cycle entry, also useful for deterministic replay and tests."""
        with self.repository.exclusive():
            try:
                if self._waiting_for_reads(market):
                    return None
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
        if isinstance(exc, PriceRefreshPending):
            # Expected startup/rule-change wait: retain durable intents and run
            # local protection, but never plan from partially refreshed inputs.
            self.cached_inputs = None
            self.last_reconciled_at = None
            self._priority_market = self._local_market(market)
            self._priority_checkpoint()
            return None
        if isinstance(exc, sqlite3.Error) or (
            isinstance(exc, OSError) and exc.errno in {5, 28, 30}
        ):
            logging.getLogger("bnb_treasury").exception(
                "Storage failed; controller stopped without further writes"
            )
            raise exc
        record_read_failure(self.repository, exc, self.clock(), exchange=self.exchange)
        if not isinstance(exc, (RequestDeferred, AccountObservationInconsistent)):
            self.executor.note_failure(reason=type(exc).__name__)
        # Never reuse a partially reconciled view for trading or guard recovery.
        self.cached_inputs = None
        self.last_reconciled_at = None
        market = self._local_market(market)
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
            try:
                for cancel in self.reconciler.protective_cancellations(now=self.clock()):
                    self.executor.execute(cancel, self.clock(), runtime.state)
            except RequestDeferred:
                pass  # Exchange cooldown covers protective requests too.
            except (ExchangeError, TimeoutError, ConnectionError) as protection_error:
                # A retry first queries its bound order. A failed protection
                # query must not escape this failure boundary and stop the loop.
                record_read_failure(
                    self.repository, protection_error, self.clock(),
                    exchange=self.exchange,
                )
                self.executor.note_failure(reason=type(protection_error).__name__)
        alerts = (
            Alert(
                "WARNING",
                "ACCOUNT_INCONSISTENT" if isinstance(exc, AccountObservationInconsistent) else "API_ERROR",
                "Account reads disagree; waiting for a consistent observation"
                if isinstance(exc, AccountObservationInconsistent)
                else "Exchange reconciliation or data validation failed",
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
                    "WARNING",
                    "CRASH_GUARD",
                    "Crash protection active: " + ",".join(guard.reasons),
                ),
            )
        self._publish(alerts)
        return None

    def _read_market(self):
        return self.exchange.fetch_market_snapshot(self.cfg.symbol)

    def _log_data_gate(self, inputs, plan):
        reasons = list(market_invalid_reasons(inputs.market, inputs.now, self.cfg))
        if not valid_account_snapshot(inputs.account, inputs.now, self.cfg):
            reasons.append("ACCOUNT_SNAPSHOT")
        if not inputs.snapshot_consistent:
            reasons.append("ACCOUNT_INCONSISTENT")
        if not inputs.market.windows_ready:
            reasons.append("WINDOWS_WARMING")
        if inputs.filters.observed_at is not None and not fresh(
            inputs.filters.observed_at, inputs.now, self.cfg.crash_guard.max_market_age_seconds
        ):
            reasons.append("FILTER_TIMESTAMP")
        reasons = tuple(reasons)
        # State changes carry exact planner inputs; unchanged state gets a
        # bounded heartbeat. No extra API reads or later feed.quote() here.
        if (reasons == self._data_gate_reasons and self._data_gate_logged_at is not None
                and 0 <= age_seconds(inputs.now, self._data_gate_logged_at) < 60):
            return
        self._data_gate_reasons, self._data_gate_logged_at = reasons, inputs.now

        def age(ts):
            return age_seconds(inputs.now, ts) if ts is not None else None

        record = {
            "evaluated_at": inputs.now.isoformat(),
            "data_gate": plan.gates.data_gate.value,
            "reasons": reasons,
            "market": asdict(inputs.market),
            "market_age_seconds": age(inputs.market.ts),
            "quote_age_seconds": age(inputs.market.quote_ts),
            "vwap_observation_age_seconds": age(inputs.market.vwap_requested_at),
            "vwap_last_trade_age_seconds": age(inputs.market.vwap_close_time),
            "account_age_seconds": age(inputs.account.ts),
            "filters_age_seconds": age(inputs.filters.observed_at),
            "snapshot_consistent": inputs.snapshot_consistent,
            "pending_unresolved": inputs.pending.unresolved,
        }
        logging.getLogger("bnb_treasury").log(
            logging.WARNING if reasons and reasons != ("WINDOWS_WARMING",) else logging.INFO,
            "DATA_GATE %s", json.dumps(record, default=str, ensure_ascii=False),
        )

    def _local_market(self, fallback=None):
        feed = getattr(self.exchange, "feed", None)
        if feed is not None:
            try:
                return feed.quote()
            except Exception:
                pass
        if fallback is not None:
            return fallback
        zero = Decimal("0")
        return MarketSnapshot(None, self.cfg.symbol, zero, zero, zero, zero, zero, zero)

    def _waiting_for_reads(self, market):
        retry_at = retry_deadline(self.repository)
        if retry_at is None or utc(self.clock()) >= utc(retry_at):
            return False
        self._priority_market = self._local_market(market)
        self._priority_checkpoint()
        self._publish(
            (
                Alert(
                    "WARNING",
                    "READ_BACKOFF",
                    f"REST reads deferred until {retry_at.isoformat()}",
                ),
            )
        )
        return True

    def _priority_checkpoint(self):
        if self._priority_busy:
            return
        self._priority_busy = True
        try:
            self._consume_stream_risk()
            runtime = self.repository.runtime()
            now = self.clock()
            market = self._local_market(self._priority_market)
            guard = update_guard_from_market(runtime.guard, market, now, self.cfg)
            if guard != runtime.guard:
                self.repository.save_guard(replace(runtime, guard=guard), now)
            changed = (guard.active, guard.reasons) != (
                runtime.guard.active,
                runtime.guard.reasons,
            )
            if self.execute_enabled:
                for cancel in self.reconciler.protective_cancellations(
                    guard=guard if guard.active else None, now=now,
                    retries_only=not guard.active,
                ):
                    previous = self.repository.cancel_operation(cancel.symbol, cancel.order_id)
                    result = self.executor.execute(cancel, self.clock(), runtime.state)
                    if (previous is None or previous.cancel_attempts != result.cancel_attempts
                            or previous.status != result.status):
                        self._priority_revision += 1
                if changed:
                    self._publish(
                        (
                            Alert(
                                "WARNING",
                                "CRASH_GUARD",
                                "Crash protection active: " + ",".join(guard.reasons),
                            ),
                        )
                    )
        except RequestDeferred:
            pass
        finally:
            self._priority_busy = False

    def _publish(self, alerts):
        now = self.clock()
        if (
            self._health_checked_at is None
            or age_seconds(now, self._health_checked_at) >= 60
        ):
            health = self.repository.health()
            warnings = []
            if (
                health["disk_free_bytes"] is not None
                and health["disk_free_bytes"] < 256 * 1024 * 1024
            ):
                warnings.append(
                    Alert(
                        "CRITICAL",
                        "STORAGE_SPACE",
                        "Database disk has less than 256 MiB free",
                    )
                )
            if health["wal_bytes"] > 64 * 1024 * 1024:
                warnings.append(
                    Alert(
                        "WARNING",
                        "WAL_SIZE",
                        "WAL exceeds 64 MiB; inspect readers and schedule checkpoint maintenance",
                    )
                )
            if health["pending_alerts"] >= 1000:
                warnings.append(
                    Alert(
                        "WARNING",
                        "OUTBOX_BACKLOG",
                        "At least 1000 notifications remain undelivered",
                    )
                )
            if (
                health["oldest_unresolved_at"] is not None
                and now.timestamp() - health["oldest_unresolved_at"] > 3600
            ):
                warnings.append(
                    Alert(
                        "WARNING",
                        "UNRESOLVED_AGE",
                        "An operation remains unresolved for over one hour; inspect status and exchange evidence",
                    )
                )
            self._health_alerts, self._health_checked_at = tuple(warnings), now
        alerts = tuple(alerts) + self._health_alerts
        issue = self.repository.load("spot_settlement_issue")
        if issue:
            alerts = tuple(alerts) + (
                Alert(
                    "WARNING",
                    "SPOT_SETTLEMENT",
                    f"Uncovered wallet changes; verify activity receipts: {issue}",
                ),
            )
        runtime = self.repository.runtime()
        issue = self.repository.load("futures_settlement_issue")
        if issue:
            alerts += (Alert("WARNING", "FUTURES_SETTLEMENT", f"Uncovered futures flow: {issue}"),)
        if any(op.kind == OperationKind.CANCEL and op.cancel_attempts >= MAX_CANCEL_ATTEMPTS
               for op in self.repository.operations(unresolved_only=True)):
            alerts += (Alert("CRITICAL", "CANCEL_RECOVERY_EXHAUSTED",
                             "Cancel recovery exhausted; verify and cancel the bound order at the exchange"),)
        if runtime.pause_reason:
            alerts = tuple(alerts) + (
                Alert("CRITICAL", "AUTO_PAUSED", runtime.pause_reason),
            )
        self.repository.publish_alerts(alerts)
        if self.alerts is not None:
            self.alerts.notify()

    def _inventory_change(self, plan):
        runtime = self.repository.runtime()
        return plan.inventory_valid and (
            plan.state_decision.confirmed_state != runtime.state
            or plan.slice_state != runtime.slice_state
            or plan.state_decision.transitioned and (
                plan.state_decision.candidate_state != runtime.candidate
                or plan.state_decision.candidate_streak != runtime.candidate_streak
            )
        )

    def _persist_plan(self, plan, *, inventory, now, update_slice=True):
        runtime = self.repository.runtime()
        verified = plan.inventory_valid and self.executor.income_audited(self.clock())
        advance_candidate = verified and (
            inventory or plan.state_decision.transitioned
        )
        self.repository.save_guard(
            replace(
                runtime,
                state=plan.state_decision.confirmed_state
                if verified
                else runtime.state,
                candidate=plan.state_decision.candidate_state
                if advance_candidate
                else runtime.candidate,
                candidate_streak=plan.state_decision.candidate_streak
                if advance_candidate
                else runtime.candidate_streak,
                last_inventory_at=now
                if inventory and verified
                else runtime.last_inventory_at,
                last_inventory_attempt_at=now
                if inventory
                else runtime.last_inventory_attempt_at,
                funding_exhausted_at=None
                if (inventory and verified and runtime.funding_exhausted_at is not None
                    and utc(now) > utc(runtime.funding_exhausted_at))
                else runtime.funding_exhausted_at,
                guard=plan.crash_guard,
                slice_state=plan.slice_state
                if update_slice and verified
                else runtime.slice_state,
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
                    v
                    for v in (guard.keep_price_ceiling, event.keep_price_ceiling)
                    if v is not None
                ]
                guard = replace(
                    guard,
                    started_at=min(guard.started_at, event.started_at, key=utc),
                    last_trigger_at=max(
                        guard.last_trigger_at or event.last_trigger_at,
                        event.last_trigger_at,
                        key=utc,
                    ),
                    keep_price_ceiling=min(ceilings) if ceilings else None,
                    reasons=tuple(dict.fromkeys(guard.reasons + event.reasons)),
                    stable_since=None,
                    last_checked_at=None,
                )
            elif guard.ended_at is None or utc(event.last_trigger_at) > utc(
                guard.ended_at
            ):
                guard = replace(
                    event,
                    started_at=max(event.started_at, guard.ended_at, key=utc)
                    if guard.ended_at
                    else event.started_at,
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

    def _continuation_audit_due(self, market, continuing):
        if not self.execute_enabled or not continuing or self.cached_inputs is None:
            return False
        runtime = self.repository.runtime()
        preview = build_cycle_plan(replace(
            self.cached_inputs, market=market, now=self.clock(),
            run_mode=runtime.run_mode, crash_guard=runtime.guard,
            previous_state=runtime.state, previous_candidate_state=runtime.candidate,
            candidate_streak=runtime.candidate_streak, slice_state=runtime.slice_state,
            advance_state=False, allow_new_cycle=True,
            allow_usd_funding=not runtime.resume_replenishment,
            reserve_replenishment_funds=runtime.resume_replanning or runtime.resume_replenishment,
        ), self.cfg)
        return not preview.cancellations and bool(
            preview.buy_plan.orders or preview.transfer_decision.allow
            or preview.bnb_transfer_plan or preview.spot_usd_sweep_plan
        )

    def _inputs(self, market, inventory, *, continuing=False, force_income=False):
        started = self.clock()
        self._consume_stream_risk()
        now = self.clock()
        if inventory:
            self.repository.save(
                "runtime",
                replace(self.repository.runtime(), last_inventory_attempt_at=now),
            )
        self._priority_market = market
        self._priority_checkpoint()
        account, orders, pending, consistent = self.reconciler.refresh(
            now, force_income=force_income or inventory or self._continuation_audit_due(market, continuing),
        )
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
        clear_read_backoff(self.repository)
        runtime = self.repository.runtime()
        trusted = (
            consistent
            and not pending.unresolved
            and valid_account_snapshot(account, now, self.cfg)
        )
        rate, margin = (
            self.reconciler.history
            if trusted
            else (Decimal("0"), None)
        )
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
            reserve_replenishment_funds=runtime.resume_replanning
            or runtime.resume_replenishment,
        )
        self.cached_inputs = inputs
        self.last_reconciled_at = now
        self._last_refresh_duration = max(
            utc(self.clock()) - utc(started), timedelta(0)
        )
        return inputs

    def _cycle(self, market, inventory):
        with self.executor.income_batch():
            return self._run_cycle(market, inventory)

    def _run_cycle(self, market, inventory):
        used = set()
        queue = None
        submitted_qty = Decimal("0")
        group_qty = Decimal("0")
        advance_inventory = inventory
        # A bounded cycle can manage many old orders but never resubmit an intent.
        for index in range(64):
            runtime = self.repository.runtime()
            priority_revision = self._priority_revision
            inputs = self._inputs(
                market,
                advance_inventory,
                continuing=inventory
                or runtime.resume_replenishment
                or runtime.resume_replanning
                or bool(queue),
            )
            saved_slice = inputs.slice_state
            # A funded continuation spends settled balances, and a queued group
            # may finish its slice without funding or opening another group.
            inputs = replace(
                inputs,
                allow_usd_funding=(
                    "fund" not in used
                    and queue is None
                    and not self.repository.runtime().resume_replenishment
                    and (runtime.funding_exhausted_at is None
                         or advance_inventory and utc(inputs.now) > utc(runtime.funding_exhausted_at))
                ),
                slice_state=replace(saved_slice, next_at=None)
                if queue
                else saved_slice,
            )
            candidate = queue[0] if queue else None
            plan = build_cycle_plan(inputs, self.cfg, queued_order=candidate)
            if not self.executor.income_audited(self.clock()) and (
                advance_inventory or self._inventory_change(plan) or self.execute_enabled and (
                    plan.buy_plan.orders or plan.transfer_decision.allow
                    or plan.bnb_transfer_plan or plan.spot_usd_sweep_plan
                )
            ) and plan.inventory_valid and not inputs.pending.unresolved and not plan.cancellations:
                # A maintenance observation can reveal a new decision. Promote
                # it to a full audit and rebuild from its final fresh account.
                audited = self._inputs(inputs.market, advance_inventory,
                                       continuing=inputs.allow_new_cycle, force_income=True)
                inputs = replace(audited, allow_usd_funding=inputs.allow_usd_funding,
                                 slice_state=replace(audited.slice_state, next_at=None)
                                 if queue else audited.slice_state)
                self.cached_inputs = inputs
                plan = build_cycle_plan(inputs, self.cfg, queued_order=candidate)
            if self._priority_revision != priority_revision:
                # All reads, including an income audit, can observe a priority cancel.
                if submitted_qty:
                    used.add("buys")
                queue = None
                self.executor.invalidate_income_audit()
                market = inputs.market
                continue
            if queue:
                plan = replace(
                    plan,
                    slice_state=replace(plan.slice_state, next_at=saved_slice.next_at),
                )
            if advance_inventory and plan.inventory_valid and self.executor.income_audited(self.clock()):
                self.repository.save_snapshot(inputs.account)
            self._persist_plan(plan, inventory=advance_inventory, now=inputs.now)
            if plan.inventory_valid:
                advance_inventory = False
            if index == 0:
                self.repository.event(inputs.now, "CYCLE", plan)
            self._log_data_gate(inputs, plan)
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
                if (
                    not fresh(
                        inputs.account.ts, now, self.cfg.risk.max_account_age_seconds
                    )
                    or (
                        inputs.filters.observed_at is not None
                        and not fresh(
                            inputs.filters.observed_at,
                            now,
                            self.cfg.crash_guard.max_market_age_seconds,
                        )
                    )
                    or (
                        needs_market
                        and not fresh(
                            inputs.market.ts,
                            now,
                            self.cfg.crash_guard.max_market_age_seconds,
                        )
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
                candidates = [
                    c
                    for c in plan.cancellations
                    if c.order_id not in used
                    and self.executor.cancel_due(c, self.clock())
                ]
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
                    if (
                        candidate in plan.buy_plan.orders
                        and candidate.qty <= group_qty - submitted_qty
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
                    and plan.inventory_valid
                ):
                    runtime = self.repository.runtime()
                    state = plan.state_decision.confirmed_state
                    continuing = plan.state_decision.delta_bnb > 0 and (
                        state == ReplenishmentState.URGENT
                        or (
                            state
                            in {
                                ReplenishmentState.WATCH,
                                ReplenishmentState.ACCUMULATE,
                            }
                            and not plan.crash_guard.active
                        )
                    )
                    if (runtime.resume_replenishment and continuing
                            and plan.gates.run_mode_gate == GateStatus.OPEN
                            and not plan.buy_plan.orders and not plan.cancellations):
                        evidence = self.reconciler.funding_occupation(inputs.account)
                        if evidence is not None:
                            restored = build_cycle_plan(replace(
                                inputs, account=replace(inputs.account,
                                    spot_usd=inputs.account.spot_usd + evidence["amount"]),
                                allow_usd_funding=False,
                            ), self.cfg)
                            if restored.buy_plan.orders and self.repository.end_funded_continuation(
                                evidence["funding_client_id"], inputs.now, evidence,
                            ):
                                return plan
                    self.repository.save(
                        "runtime",
                        replace(
                            runtime,
                            api_errors=0,
                            resume_replenishment=runtime.resume_replenishment
                            and continuing,
                            resume_replanning=runtime.resume_replanning
                            and continuing
                            and state != ReplenishmentState.URGENT,
                            funding_client_id=runtime.funding_client_id if continuing else None,
                        ),
                    )
                return plan
            if tag is not None:
                used.add(tag)
            try:
                op = self.executor.execute(
                    action,
                    self.clock(),
                    plan.state_decision.confirmed_state,
                    account=inputs.account,
                    inputs=inputs,
                    starts_slice=hasattr(action, "qty")
                    and plan.slice_state.active
                    and submitted_qty == action.qty,
                    resume_replanning=tag == "bnb"
                    and inputs.allow_new_cycle
                    and submitted_qty == 0
                    and plan.state_decision.confirmed_state != ReplenishmentState.IDLE
                    and plan.state_decision.delta_bnb > 0,
                )
            except ExecutionBlocked as exc:
                # A final verified view invalidated this unsent candidate. End
                # the batch; neither an API failure nor a smaller-amount retry.
                self.cached_inputs = self.last_reconciled_at = None
                self.repository.event(self.clock(), "EXECUTION_BLOCKED", str(exc))
                alert = Alert("WARNING", "EXECUTION_BLOCKED", str(exc))
                self._publish((alert,))
                decision = exc.decision or plan.transfer_decision
                status = decision.gate_status if not decision.allow else GateStatus.BLOCKED
                return replace(
                    plan,
                    transfer_decision=replace(
                        decision, allow=False, message=str(exc),
                        gate_status=status,
                    ),
                    gates=replace(plan.gates, transfer_gate=status),
                    alerts=plan.alerts + (alert,),
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
