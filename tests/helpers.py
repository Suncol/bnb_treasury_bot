from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal

from core.models import (
    AccountSnapshot,
    EngineInputs,
    ExecutionConfig,
    MarketSnapshot,
    OrderView,
    PendingState,
    ReplenishmentState,
    RiskConfig,
    RunMode,
    SchedulerConfig,
    StateDecision,
    StateInputs,
    StrategyConfig,
    SymbolFilters,
    ThresholdConfig,
    TransitionReason,
)


def D(value: str | int | float) -> Decimal:
    return Decimal(str(value))


def depth_messages(now=None, update_id=1, ask_qty="3.5"):
    now = now or datetime(2026, 10, 13, tzinfo=timezone.utc)
    return (
        {"lastUpdateId": update_id, "bids": [["599", "1"]], "asks": [["601", ask_qty]]},
        {"e": "depthUpdate", "s": "BNBUSDT", "E": int(now.timestamp() * 1000),
         "U": update_id, "u": update_id, "b": [], "a": []},
    )


def make_strategy_config() -> StrategyConfig:
    return StrategyConfig(
        thresholds=ThresholdConfig(
            b_low=D("25"),
            b_alert=D("28"),
            b_target=D("32"),
            b_high=D("40"),
            b_spot_reserve=D("0.5"),
        ),
        risk=RiskConfig(
            u_min=D("500"),
            alpha_warn=D("0.10"),
            alpha_crit=D("0.25"),
            m_abs_min=D("2000"),
            u_budget_24h=D("3000"),
            slippage=D("0.015"),
            u_spot_idle_max=D("1000"),
        ),
        execution=ExecutionConfig(
            watch_discounts=(D("0.005"), D("0.012"), D("0.025")),
            accumulate_discounts=(D("0.003"), D("0.007"), D("0.015")),
            layer_weights=(D("0.30"), D("0.40"), D("0.30")),
            watch_reprice_hours=6,
            accumulate_reprice_hours=2,
            urgent_ioc_buffer=D("0.002"),
        ),
        scheduler=SchedulerConfig(
            t_check_minutes=30,
            state_confirm_cycles=2,
        ),
    )


def make_filters() -> SymbolFilters:
    return SymbolFilters(
        qty_step=D("0.01"),
        price_tick=D("0.01"),
        min_qty=D("0.01"),
        min_notional=D("5"),
        min_transfer_bnb=D("0.1"),
    )


def make_market() -> MarketSnapshot:
    return MarketSnapshot(
        ts=datetime(2026, 3, 31),
        symbol="BNBUSDT",
        best_bid=D("599.50"),
        best_ask=D("600.50"),
        mid_price=D("600"),
        vwap_5m=D("601"),
        return_1h=D("0"),
        return_24h=D("0"),
        smooth_price=D("600"),
        drawdown_1m=D("0"),
        drawdown_5m=D("0"),
        drawdown_15m=D("0"),
    )


def make_account(
    *,
    contract_bnb: Decimal = D("30"),
    contract_max_withdraw_amount: Decimal = D("6000"),
    contract_available_balance: Decimal = D("6200"),
    spot_bnb: Decimal = D("0.5"),
    spot_usd: Decimal = D("0"),
    reserved_spot_usd: Decimal = D("0"),
) -> AccountSnapshot:
    return AccountSnapshot(
        ts=datetime(2026, 3, 31),
        contract_bnb=contract_bnb,
        contract_max_withdraw_amount=contract_max_withdraw_amount,
        contract_available_balance=contract_available_balance,
        spot_bnb=spot_bnb,
        spot_usd=spot_usd,
        reserved_spot_usd=reserved_spot_usd,
    )


def make_pending(
    *,
    pending_bnb_to_contract: Decimal = D("0"),
    pending_usd_to_spot: Decimal = D("0"),
    open_buy_remaining_qty: Decimal = D("0"),
    filled_untransferred_bnb: Decimal = D("0"),
    has_unknown_orders: bool = False,
    has_unknown_transfers: bool = False,
) -> PendingState:
    return PendingState(
        pending_bnb_to_contract=pending_bnb_to_contract,
        pending_usd_to_spot=pending_usd_to_spot,
        open_buy_remaining_qty=open_buy_remaining_qty,
        filled_untransferred_bnb=filled_untransferred_bnb,
        has_unknown_orders=has_unknown_orders,
        has_unknown_transfers=has_unknown_transfers,
    )


def make_state_decision(state: ReplenishmentState, delta_bnb: str = "7") -> StateDecision:
    return StateDecision(
        current_state=state,
        raw_candidate_state=state,
        candidate_state=state,
        confirmed_state=state,
        transitioned=False,
        reason=TransitionReason.HIGH_BALANCE,
        candidate_reason=TransitionReason.HIGH_BALANCE,
        t_depletion=None,
        effective_supply=D("0"),
        delta_bnb=D(delta_bnb),
        candidate_streak=0,
    )


def inputs_with_orders(*orders, **changes):
    inputs = make_engine_inputs(
        account=make_account(contract_bnb=D("27"), spot_usd=D("10000")),
        previous_state=ReplenishmentState.ACCUMULATE,
    )
    return replace(
        inputs,
        orders=orders,
        pending=replace(
            inputs.pending,
            open_buy_remaining_qty=sum((o.remaining_qty for o in orders), D("0")),
        ),
        **changes,
    )


def order(key, price="580", qty="1", **changes):
    now = make_engine_inputs().now
    return replace(
        OrderView(
            "BNBUSDT",
            key,
            "client-" + key,
            "bnb-treasury",
            D(price),
            D(qty),
            D("0"),
            now,
        ),
        **changes,
    )


def make_engine_inputs(
    *,
    account: AccountSnapshot | None = None,
    pending: PendingState | None = None,
    consumption_rate: Decimal = D("0"),
    previous_state: ReplenishmentState = ReplenishmentState.IDLE,
    previous_candidate_state: ReplenishmentState | None = None,
    candidate_streak: int = 0,
    run_mode: RunMode = RunMode.AUTO,
    budget_used_24h: Decimal = D("0"),
) -> EngineInputs:
    return EngineInputs(
        account=account or make_account(),
        pending=pending or make_pending(),
        market=make_market(),
        filters=make_filters(),
        run_mode=run_mode,
        consumption_rate=consumption_rate,
        previous_state=previous_state,
        previous_candidate_state=previous_candidate_state,
        candidate_streak=candidate_streak,
        budget_used_24h=budget_used_24h,
        now=datetime(2026, 3, 31),
    )


def make_state_inputs(
    *,
    contract_bnb: Decimal = D("30"),
    contract_max_withdraw_amount: Decimal = D("6000"),
    contract_available_balance: Decimal = D("6200"),
    spot_bnb: Decimal = D("0.5"),
    spot_usd: Decimal = D("0"),
    reserved_spot_usd: Decimal = D("0"),
    pending_bnb_to_contract: Decimal = D("0"),
    pending_usd_to_spot: Decimal = D("0"),
    open_buy_remaining_qty: Decimal = D("0"),
    filled_untransferred_bnb: Decimal = D("0"),
    consumption_rate: Decimal = D("0"),
    previous_state: ReplenishmentState = ReplenishmentState.IDLE,
    previous_candidate_state: ReplenishmentState | None = None,
    candidate_streak: int = 0,
    run_mode: RunMode = RunMode.AUTO,
    has_unknown_orders: bool = False,
    has_unknown_transfers: bool = False,
) -> StateInputs:
    return StateInputs(
        contract_bnb=contract_bnb,
        contract_max_withdraw_amount=contract_max_withdraw_amount,
        contract_available_balance=contract_available_balance,
        spot_bnb=spot_bnb,
        spot_usd=spot_usd,
        reserved_spot_usd=reserved_spot_usd,
        pending_bnb_to_contract=pending_bnb_to_contract,
        pending_usd_to_spot=pending_usd_to_spot,
        open_buy_remaining_qty=open_buy_remaining_qty,
        filled_untransferred_bnb=filled_untransferred_bnb,
        consumption_rate=consumption_rate,
        previous_state=previous_state,
        previous_candidate_state=previous_candidate_state,
        candidate_streak=candidate_streak,
        run_mode=run_mode,
        has_unknown_orders=has_unknown_orders,
        has_unknown_transfers=has_unknown_transfers,
    )
