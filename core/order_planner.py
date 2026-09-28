from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

from .math_utils import ceil_to_multiple, floor_to_multiple
from .models import (
    BuyPlan,
    ExecutionConfig,
    MarketSnapshot,
    OrderPlan,
    ReplenishmentState,
    StateDecision,
    StrategyConfig,
    SymbolFilters,
)


def compute_ref_price(market: MarketSnapshot) -> Decimal:
    anchor_ask = market.best_ask * Decimal("0.999")
    return min(market.mid_price, market.vwap_5m, anchor_ask)


def _max_affordable_qty(
    delta_bnb: Decimal,
    ref_price: Decimal,
    quote_budget: Decimal | None,
    filters: SymbolFilters,
) -> Decimal:
    if (
        delta_bnb <= Decimal("0")
        or ref_price <= Decimal("0")
        or (quote_budget is not None and quote_budget <= Decimal("0"))
    ):
        return Decimal("0")

    affordable = (
        delta_bnb if quote_budget is None else min(delta_bnb, quote_budget / ref_price)
    )
    capped = floor_to_multiple(affordable, filters.qty_step)
    if filters.max_qty is not None:
        capped = min(capped, floor_to_multiple(filters.max_qty, filters.qty_step))
    if filters.max_notional is not None:
        capped = min(
            capped,
            floor_to_multiple(filters.max_notional / ref_price, filters.qty_step),
        )
    if capped < filters.min_qty or capped * ref_price < filters.min_notional:
        return Decimal("0")
    return capped


def _layered_orders(
    total_qty: Decimal,
    ref_price: Decimal,
    symbol: str,
    filters: SymbolFilters,
    weights: tuple[Decimal, Decimal, Decimal],
    discounts: tuple[Decimal, Decimal, Decimal],
    order_type: str,
    time_in_force: str | None,
    post_only: bool,
) -> tuple[OrderPlan, ...]:
    plans: list[OrderPlan] = []
    carry_qty = Decimal("0")

    for index, (weight, discount) in enumerate(zip(weights, discounts)):
        raw_qty = (total_qty * weight) + carry_qty
        price = floor_to_multiple(
            ref_price * (Decimal("1") - discount), filters.price_tick
        )
        qty = floor_to_multiple(raw_qty, filters.qty_step)

        valid = (
            qty >= filters.min_qty
            and price > Decimal("0")
            and qty * price >= filters.min_notional
        )

        if valid:
            plans.append(
                OrderPlan(
                    symbol=symbol,
                    side="BUY",
                    order_type=order_type,
                    qty=qty,
                    price=price,
                    time_in_force=time_in_force,
                    post_only=post_only,
                )
            )
            carry_qty = Decimal("0")
        else:
            carry_qty = raw_qty

        if index == len(weights) - 1 and carry_qty > Decimal("0") and plans:
            qty = floor_to_multiple(carry_qty, filters.qty_step)
            plans[-1] = replace(plans[-1], qty=plans[-1].qty + qty)

    return tuple(plans)


def plan_watch_orders(
    delta_bnb: Decimal,
    ref_price: Decimal,
    filters: SymbolFilters,
    cfg: ExecutionConfig,
    symbol: str,
    quote_budget: Decimal | None,
) -> BuyPlan:
    total_qty = _max_affordable_qty(delta_bnb, ref_price, quote_budget, filters)
    if total_qty == Decimal("0"):
        return BuyPlan(
            state=ReplenishmentState.WATCH, reason="Insufficient quote budget for WATCH"
        )
    return BuyPlan(
        state=ReplenishmentState.WATCH,
        orders=_layered_orders(
            total_qty=total_qty,
            ref_price=ref_price,
            symbol=symbol,
            filters=filters,
            weights=cfg.layer_weights,
            discounts=cfg.watch_discounts,
            order_type="LIMIT_MAKER",
            time_in_force="GTC",
            post_only=True,
        ),
        reason="Layered WATCH accumulation",
    )


def plan_accumulate_orders(
    delta_bnb: Decimal,
    ref_price: Decimal,
    filters: SymbolFilters,
    cfg: ExecutionConfig,
    symbol: str,
    quote_budget: Decimal | None,
) -> BuyPlan:
    total_qty = _max_affordable_qty(delta_bnb, ref_price, quote_budget, filters)
    if total_qty == Decimal("0"):
        return BuyPlan(
            state=ReplenishmentState.ACCUMULATE,
            reason="Insufficient quote budget for ACCUMULATE",
        )
    return BuyPlan(
        state=ReplenishmentState.ACCUMULATE,
        orders=_layered_orders(
            total_qty=total_qty,
            ref_price=ref_price,
            symbol=symbol,
            filters=filters,
            weights=cfg.layer_weights,
            discounts=cfg.accumulate_discounts,
            order_type="LIMIT",
            time_in_force="GTC",
            post_only=False,
        ),
        reason="Layered ACCUMULATE execution",
    )


def plan_urgent_orders(
    delta_bnb: Decimal,
    market: MarketSnapshot,
    filters: SymbolFilters,
    cfg: ExecutionConfig,
    symbol: str,
    quote_budget: Decimal | None,
) -> BuyPlan:
    ref_price = compute_ref_price(market)
    aggressive_price = ceil_to_multiple(
        max(market.best_ask, ref_price) * (Decimal("1") + cfg.urgent_ioc_buffer),
        filters.price_tick,
    )
    total_qty = _max_affordable_qty(
        delta_bnb,
        aggressive_price,
        quote_budget,
        filters,
    )
    if total_qty == Decimal("0"):
        return BuyPlan(
            state=ReplenishmentState.URGENT,
            reason="Insufficient quote budget for URGENT",
        )

    order = OrderPlan(
        symbol=symbol,
        side="BUY",
        order_type="LIMIT",
        qty=total_qty,
        price=aggressive_price,
        time_in_force="IOC",
        post_only=False,
    )
    return BuyPlan(
        state=ReplenishmentState.URGENT,
        orders=(order,),
        reason="Urgent IOC replenishment",
    )


def plan_buy_orders(
    state_decision: StateDecision,
    market: MarketSnapshot,
    filters: SymbolFilters,
    cfg: StrategyConfig,
    quote_budget: Decimal | None,
) -> BuyPlan:
    """Normalize demand; None is for funding estimates, never executable orders."""
    ref_price = compute_ref_price(market)

    if state_decision.confirmed_state == ReplenishmentState.IDLE:
        return BuyPlan(
            state=ReplenishmentState.IDLE, reason="IDLE does not place buy orders"
        )
    if state_decision.delta_bnb <= Decimal("0"):
        return BuyPlan(
            state=state_decision.confirmed_state,
            reason="No deficit remains after effective supply accounting",
        )

    if (
        state_decision.confirmed_state != ReplenishmentState.URGENT
        and market.return_1h > cfg.execution.chase_return_limit
    ):
        return BuyPlan(
            state=state_decision.confirmed_state,
            reason="Chase filter: keep lower existing bids only",
        )

    if state_decision.confirmed_state == ReplenishmentState.WATCH:
        return plan_watch_orders(
            state_decision.delta_bnb,
            ref_price,
            filters,
            cfg.execution,
            cfg.symbol,
            quote_budget,
        )
    if state_decision.confirmed_state == ReplenishmentState.ACCUMULATE:
        return plan_accumulate_orders(
            state_decision.delta_bnb,
            ref_price,
            filters,
            cfg.execution,
            cfg.symbol,
            quote_budget,
        )
    return plan_urgent_orders(
        state_decision.delta_bnb,
        market,
        filters,
        cfg.execution,
        cfg.symbol,
        quote_budget,
    )
