from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

from .crash_guard import update_crash_guard, valid_market
from .math_utils import floor_to_multiple, non_negative
from .models import (
    Alert,
    AssetTransferPlan,
    BuyPlan,
    CyclePlan,
    EngineInputs,
    ExecutionGates,
    GateStatus,
    ReplenishmentState,
    RunMode,
    SliceState,
    StateInputs,
    StrategyConfig,
    TransferDecision,
)
from .order_manager import plan_cancellations
from .order_planner import compute_ref_price, plan_buy_orders
from .risk_checks import compute_usd_transfer_needed, evaluate_transfer_gate
from .state_machine import evaluate_state
from .time_utils import fresh, utc

ZERO = Decimal("0")


def _to_state_inputs(inputs: EngineInputs) -> StateInputs:
    a, p = inputs.account, inputs.pending
    return StateInputs(
        contract_bnb=a.contract_bnb,
        contract_max_withdraw_amount=a.contract_max_withdraw_amount,
        contract_available_balance=a.contract_available_balance,
        spot_bnb=a.spot_bnb,
        spot_usd=a.spot_usd,
        reserved_spot_usd=a.reserved_spot_usd,
        pending_bnb_to_contract=p.pending_bnb_to_contract,
        pending_usd_to_spot=p.pending_usd_to_spot,
        open_buy_remaining_qty=p.open_buy_remaining_qty,
        filled_untransferred_bnb=p.filled_untransferred_bnb,
        consumption_rate=inputs.consumption_rate,
        previous_state=inputs.previous_state,
        previous_candidate_state=inputs.previous_candidate_state,
        candidate_streak=inputs.candidate_streak,
        run_mode=inputs.run_mode,
        has_unknown_orders=p.has_unknown_orders,
        has_unknown_transfers=p.has_unknown_transfers,
        bnb_in_transit=p.bnb_in_transit,
    )


def _valid_account(inputs: EngineInputs, cfg: StrategyConfig) -> bool:
    a = inputs.account
    amounts = (
        a.contract_bnb,
        a.contract_max_withdraw_amount,
        a.spot_bnb,
        a.spot_usd,
        a.reserved_spot_usd,
        a.reserved_spot_bnb,
    )
    return (
        inputs.snapshot_consistent
        and fresh(a.ts, inputs.now, cfg.risk.max_account_age_seconds)
        and all(x.is_finite() and x >= 0 for x in amounts)
        and a.reserved_spot_usd <= a.spot_usd
        and a.reserved_spot_bnb <= a.spot_bnb
    )


def _plan_bnb_transfer(
    inputs: EngineInputs,
    cfg: StrategyConfig,
    state: ReplenishmentState,
    target: Decimal,
) -> AssetTransferPlan | None:
    a = inputs.account
    available = non_negative(
        a.spot_bnb - a.reserved_spot_bnb - cfg.thresholds.b_spot_reserve
    )
    amount = floor_to_multiple(
        min(available, non_negative(target - a.contract_bnb)), inputs.filters.qty_step
    )
    minimum = inputs.filters.min_transfer_bnb
    if state == ReplenishmentState.WATCH:
        minimum = max(minimum, cfg.execution.watch_batch_transfer_bnb)
    if amount > 0 and amount >= minimum and a.contract_bnb < cfg.thresholds.b_high:
        return AssetTransferPlan(
            "BNB", amount, "SPOT", "USDⓈ-M Futures", "Replenish futures inventory"
        )
    return None


def _plan_sweep(
    inputs: EngineInputs,
    cfg: StrategyConfig,
    state: ReplenishmentState,
    gross_need: Decimal,
) -> AssetTransferPlan | None:
    a, p = inputs.account, inputs.pending
    free = non_negative(a.spot_usd - a.reserved_spot_usd)
    if (
        not cfg.risk.sweep_enabled
        or inputs.reserve_replenishment_funds
        or state == ReplenishmentState.URGENT
        or p.open_buy_remaining_qty > 0
        or a.reserved_spot_usd > 0
        or free <= cfg.risk.u_spot_idle_max
    ):
        return None
    buffer = max(cfg.risk.u_min, gross_need * Decimal("0.5"))
    amount = floor_to_multiple(non_negative(free - buffer), cfg.risk.u_min)
    if amount < max(cfg.risk.u_min, inputs.filters.min_transfer_usd):
        return None
    return AssetTransferPlan(
        cfg.quote_asset, amount, "SPOT", "USDⓈ-M Futures", "Sweep idle quote balance"
    )


def _alerts(inputs, cfg, decision, transfer, gates, guard) -> tuple[Alert, ...]:
    alerts = []

    def add(level, code, message):
        alerts.append(Alert(level, code, message))

    if decision.transitioned:
        add(
            "INFO",
            "STATE_TRANSITION",
            f"{decision.current_state.value} -> {decision.confirmed_state.value}",
        )
    if gates.reconciliation_gate != GateStatus.OPEN:
        add(
            "WARNING",
            "WAIT_RECONCILE",
            "Pending or unknown operations require reconciliation",
        )
    if gates.run_mode_gate != GateStatus.OPEN:
        add(
            "INFO",
            "RUN_MODE_BLOCK",
            f"Run mode {inputs.run_mode.value} blocks new actions",
        )
    if gates.data_gate != GateStatus.OPEN:
        add(
            "WARNING",
            "DATA_INVALID",
            "Fresh consistent balances and continuous market windows are required",
        )
    if transfer.level not in {"OK", "NO_ACTION"}:
        add(transfer.level, "TRANSFER_GATE", transfer.message)
    if inputs.account.contract_bnb <= cfg.thresholds.b_low:
        add("DANGER", "BNB_URGENT", "Futures BNB is at or below the inventory floor")
    if inputs.account.contract_bnb <= cfg.thresholds.b_low / 2:
        add(
            "CRITICAL",
            "BNB_NEAR_ZERO",
            "Futures BNB is below half of the inventory floor",
        )
    withdraw = inputs.account.contract_max_withdraw_amount
    if withdraw < cfg.risk.m_abs_min:
        add("CRITICAL", "WITHDRAW_LOW", "maxWithdrawAmount is below the absolute floor")
    if withdraw <= 0 or cfg.risk.u_min / withdraw > cfg.risk.alpha_warn:
        add(
            "CRITICAL"
            if withdraw <= 0 or cfg.risk.u_min / withdraw > cfg.risk.alpha_crit
            else "WARNING",
            "GRANULARITY_VETO",
            "Minimum transfer unit is unsafe relative to maxWithdrawAmount",
        )
    if inputs.budget_used_24h >= cfg.risk.u_budget_24h:
        add("WARNING", "BUDGET_EXHAUSTED", "Rolling 24h transfer budget is exhausted")
    elif inputs.budget_used_24h >= cfg.risk.u_budget_24h * Decimal("0.8"):
        add(
            "WARNING",
            "BUDGET_80PCT",
            "Rolling 24h transfer budget has reached 80 percent",
        )
    if guard.active:
        add(
            "WARNING",
            "CRASH_GUARD",
            "Crash protection active: " + ",".join(guard.reasons),
        )
        exposure = sum(
            (
                o.remaining_qty
                for o in inputs.orders
                if o.strategy_id == cfg.strategy_id
                and o.symbol == cfg.symbol
                and o.side == "BUY"
            ),
            ZERO,
        )
        if guard.filled_bnb + exposure > cfg.crash_guard.max_acquisition_bnb:
            add(
                "CRITICAL",
                "GUARD_LIMIT",
                "Executed and still executable buys exceed episode allowance",
            )
        if (
            decision.confirmed_state == ReplenishmentState.URGENT
            and decision.delta_bnb > 0
        ):
            add(
                "WARNING",
                "GUARDED_URGENT",
                "Emergency replenishment remains subject to episode and funding limits",
            )
    elif inputs.crash_guard.active:
        add(
            "INFO",
            "CRASH_RECOVERED",
            "Crash protection recovered after continuous observation and reconciliation",
        )
    if inputs.market.return_24h < Decimal("-0.08"):
        add(
            "WARNING", "DROP_24H", "24h price drop exceeds 8 percent (observation only)"
        )
    return tuple(alerts)


def build_cycle_plan(inputs: EngineInputs, cfg: StrategyConfig) -> CyclePlan:
    """Pure decision entry. Every action is based exclusively on settled funds.

    Cancels precede new actions. A funding decision contains no dependent orders;
    its confirmed completion requires a new snapshot and a new plan.
    """
    decision = evaluate_state(_to_state_inputs(inputs), cfg)
    if (
        not inputs.advance_state
        and decision.raw_candidate_state != ReplenishmentState.URGENT
    ):
        decision = replace(
            decision,
            confirmed_state=inputs.previous_state,
            transitioned=False,
            candidate_streak=inputs.candidate_streak,
        )
    state = decision.confirmed_state
    guard = update_crash_guard(inputs, cfg)
    target = cfg.thresholds.b_target
    if (
        inputs.margin_change_24h is not None
        and inputs.margin_change_24h < -cfg.risk.margin_drop_limit
    ):
        target = min(
            target,
            max(cfg.thresholds.b_low + 2, target - cfg.risk.margin_target_reduction),
        )
    if guard.active and state == ReplenishmentState.URGENT:
        target = min(target, cfg.thresholds.b_low + 1)
    decision = replace(
        decision, delta_bnb=non_negative(target - decision.effective_supply)
    )
    cancels = plan_cancellations(inputs, decision, target, guard, cfg)
    run_gate = (
        GateStatus.BLOCKED
        if (
            inputs.run_mode == RunMode.PAUSED
            or (
                inputs.run_mode == RunMode.URGENT_ONLY
                and state != ReplenishmentState.URGENT
            )
        )
        else GateStatus.OPEN
    )
    reconcile_gate = (
        GateStatus.WAIT_RECONCILE if inputs.pending.unresolved else GateStatus.OPEN
    )
    account_ok = _valid_account(inputs, cfg)
    market_ok = valid_market(inputs, cfg)
    data_gate = (
        GateStatus.OPEN
        if account_ok and market_ok and inputs.market.windows_ready
        else GateStatus.BLOCKED
    )
    transfer = TransferDecision(
        False, GateStatus.OPEN, "NO_ACTION", ZERO, "No USD transfer planned"
    )
    buy = BuyPlan(state, reason="No funded incremental demand")
    bnb_transfer = sweep = None
    slice_state = inputs.slice_state
    if state == ReplenishmentState.IDLE or decision.delta_bnb == 0:
        slice_state = SliceState()
    elif decision.delta_bnb > cfg.execution.slice_threshold_bnb:
        slice_state = replace(slice_state, active=True)
    ready = run_gate == reconcile_gate == GateStatus.OPEN and account_ok and not cancels
    if ready:
        bnb_transfer = _plan_bnb_transfer(inputs, cfg, state, target)
    can_buy = (
        ready
        and data_gate == GateStatus.OPEN
        and state != ReplenishmentState.IDLE
        and decision.delta_bnb > 0
        and (inputs.allow_new_cycle or state == ReplenishmentState.URGENT)
        and (not guard.active or state == ReplenishmentState.URGENT)
        and (
            state == ReplenishmentState.URGENT
            or inputs.market.return_1h <= cfg.execution.chase_return_limit
        )
    )
    qty = decision.delta_bnb
    if slice_state.active:
        qty = min(qty, cfg.execution.slice_qty_bnb)
        if slice_state.next_at is not None and utc(inputs.now) < utc(
            slice_state.next_at
        ):
            can_buy = False
    if guard.active:
        strategy_remaining = sum(
            (
                o.remaining_qty
                for o in inputs.orders
                if o.symbol == cfg.symbol
                and o.side == "BUY"
                and o.strategy_id == cfg.strategy_id
            ),
            ZERO,
        )
        qty = min(
            qty,
            non_negative(
                cfg.crash_guard.max_acquisition_bnb
                - guard.filled_bnb
                - strategy_remaining
            ),
        )
    if can_buy and qty > 0:
        free = non_negative(inputs.account.spot_usd - inputs.account.reserved_spot_usd)
        requested = replace(decision, delta_bnb=qty)
        # Use the same price, lot and notional rules as the eventual orders.
        demand = plan_buy_orders(requested, inputs.market, inputs.filters, cfg, None)
        ref = compute_ref_price(inputs.market)
        if demand.orders and state == ReplenishmentState.URGENT:
            ref = demand.orders[0].price
        raw = (
            compute_usd_transfer_needed(
                sum((o.qty for o in demand.orders), ZERO), ref, free, cfg.risk
            )
            if inputs.allow_usd_funding
            else ZERO
        )
        transfer = evaluate_transfer_gate(
            raw,
            inputs.account.contract_max_withdraw_amount,
            inputs.budget_used_24h,
            cfg.risk,
            inputs.filters.min_transfer_usd,
        )
        if transfer.allow:
            buy = BuyPlan(
                state,
                reason="Await confirmed USD transfer, refreshed balances and replanning",
            )
        else:
            buy = plan_buy_orders(
                requested,
                inputs.market,
                inputs.filters,
                cfg,
                free / (1 + cfg.risk.fee_reserve_rate),
            )
    if ready and data_gate == GateStatus.OPEN and not transfer.allow and not buy.orders:
        gross = (
            decision.delta_bnb
            * compute_ref_price(inputs.market)
            * (1 + cfg.risk.slippage)
            * (1 + cfg.risk.fee_reserve_rate)
        )
        sweep = _plan_sweep(inputs, cfg, state, gross)
    gates = ExecutionGates(run_gate, transfer.gate_status, reconcile_gate, data_gate)
    return CyclePlan(
        decision,
        transfer,
        buy,
        bnb_transfer,
        _alerts(inputs, cfg, decision, transfer, gates, guard),
        gates,
        sweep,
        cancels,
        guard,
        slice_state,
        target,
    )
