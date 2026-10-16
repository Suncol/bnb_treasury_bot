from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

from .math_utils import ceil_to_multiple, floor_to_multiple
from .models import (
    BuyPlan,
    ExecutionConfig,
    MarketSnapshot,
    OrderPlan,
    OrderView,
    ReplenishmentState,
    StateDecision,
    StrategyConfig,
    SymbolFilters,
)


def compute_ref_price(market: MarketSnapshot) -> Decimal:
    anchor_ask = market.best_ask * Decimal("0.999")
    return min(market.mid_price, market.vwap_5m, anchor_ask)


def ordinary_layers(
    state: ReplenishmentState, ref_price: Decimal,
    filters: SymbolFilters, cfg: ExecutionConfig,
) -> tuple[tuple[Decimal, Decimal], ...]:
    """One price grid serves new orders and periodic reassessment."""
    discounts = cfg.watch_discounts if state == ReplenishmentState.WATCH else cfg.accumulate_discounts
    layers: dict[Decimal, Decimal] = {}
    for weight, discount in zip(cfg.layer_weights, discounts):
        price = floor_to_multiple(ref_price * (1 - discount), filters.price_tick)
        layers[price] = layers.get(price, Decimal("0")) + weight
    return tuple((weight, price) for price, weight in layers.items())


def order_passes_filters(order: OrderPlan, filters: SymbolFilters) -> bool:
    """The same final-order validation is used for planning and queued execution."""
    price, qty = order.price, order.qty
    return (
        price is not None
        and price.is_finite()
        and qty.is_finite()
        and price > 0
        and qty > 0
        and price >= filters.min_price
        and (filters.max_price is None or price <= filters.max_price)
        and qty % filters.qty_step == 0
        and price % filters.price_tick == 0
        and qty >= filters.min_qty
        and qty * price >= filters.min_notional
        and (filters.max_qty is None or qty <= filters.max_qty)
        and (filters.max_notional is None or qty * price <= filters.max_notional)
    )


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
    delta_bnb: Decimal,
    ref_price: Decimal,
    quote_budget: Decimal | None,
    symbol: str,
    filters: SymbolFilters,
    layers: tuple[tuple[Decimal, Decimal], ...],
    order_type: str,
    time_in_force: str | None,
    post_only: bool,
    existing_orders: tuple[OrderView, ...] = (),
) -> tuple[OrderPlan, ...]:
    occupied = {o.price for o in existing_orders}
    slots = max(0, len(layers) - len(existing_orders))
    unoccupied = tuple((weight, price) for weight, price in layers if price not in occupied)
    available = unoccupied[:slots]
    weight_sum = sum((weight for weight, _ in available), Decimal("0"))
    if not available or weight_sum <= 0:
        return ()
    total_qty = _max_affordable_qty(delta_bnb, ref_price, quote_budget, filters)
    plans: list[OrderPlan] = []
    carry_qty = Decimal("0")

    for index, (weight, price) in enumerate(available):
        raw_qty = (total_qty * weight / weight_sum) + carry_qty
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

        if index == len(available) - 1 and carry_qty > Decimal("0") and plans:
            qty = floor_to_multiple(carry_qty, filters.qty_step)
            plans[-1] = replace(plans[-1], qty=plans[-1].qty + qty)

    orders = tuple(order for order in plans if order_passes_filters(order, filters))
    if orders:
        return orders

    # Reference-price sizing can round below one lot even when a discounted
    # layer is affordable. One fallback needs only one remaining slot, so
    # inspect every unoccupied price, including those outside normal allocation.
    # quote_budget already excludes the engine's fee reserve.
    for _, price in unoccupied:
        if price <= 0:
            continue
        qty = ceil_to_multiple(
            max(filters.min_qty, filters.qty_step, filters.min_notional / price),
            filters.qty_step,
        )
        order = OrderPlan(symbol, "BUY", order_type, qty, price, time_in_force, post_only)
        if (qty <= delta_bnb and order_passes_filters(order, filters)
                and (quote_budget is None or qty * price <= quote_budget)):
            return (order,)
    return ()


def plan_watch_orders(
    delta_bnb: Decimal,
    ref_price: Decimal,
    filters: SymbolFilters,
    cfg: ExecutionConfig,
    symbol: str,
    quote_budget: Decimal | None,
    existing_orders: tuple[OrderView, ...] = (),
) -> BuyPlan:
    return BuyPlan(
        state=ReplenishmentState.WATCH,
        orders=_layered_orders(
            delta_bnb=delta_bnb,
            ref_price=ref_price,
            quote_budget=quote_budget,
            symbol=symbol,
            filters=filters,
            layers=ordinary_layers(ReplenishmentState.WATCH, ref_price, filters, cfg),
            order_type="LIMIT_MAKER",
            time_in_force="GTC",
            post_only=True,
            existing_orders=existing_orders,
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
    existing_orders: tuple[OrderView, ...] = (),
) -> BuyPlan:
    return BuyPlan(
        state=ReplenishmentState.ACCUMULATE,
        orders=_layered_orders(
            delta_bnb=delta_bnb,
            ref_price=ref_price,
            quote_budget=quote_budget,
            symbol=symbol,
            filters=filters,
            layers=ordinary_layers(ReplenishmentState.ACCUMULATE, ref_price, filters, cfg),
            order_type="LIMIT",
            time_in_force="GTC",
            post_only=False,
            existing_orders=existing_orders,
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
        orders=(order,) if order_passes_filters(order, filters) else (),
        reason="Urgent IOC replenishment",
    )


def plan_buy_orders(
    state_decision: StateDecision,
    market: MarketSnapshot,
    filters: SymbolFilters,
    cfg: StrategyConfig,
    quote_budget: Decimal | None,
    *, existing_orders: tuple[OrderView, ...] = (), queued_order: OrderPlan | None = None,
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

    owned = tuple(o for o in existing_orders if o.symbol == cfg.symbol and o.side == "BUY"
                  and o.strategy_id == cfg.strategy_id and o.is_open and o.time_in_force != "IOC")
    if queued_order is not None:
        urgent = state_decision.confirmed_state == ReplenishmentState.URGENT
        layers = () if urgent else ordinary_layers(
            state_decision.confirmed_state, ref_price, filters, cfg.execution)
        ceiling = (
            ceil_to_multiple(max(market.best_ask, ref_price) * (1 + cfg.execution.urgent_ioc_buffer),
                             filters.price_tick)
            if urgent else max(price for _, price in layers)
        )
        available = urgent or (
            len(owned) < len(layers)
            and all(o.price != queued_order.price for o in owned)
        )
        valid = (
            available and queued_order.symbol == cfg.symbol and queued_order.side == "BUY"
            and queued_order.time_in_force == ("IOC" if urgent else "GTC")
            and order_passes_filters(queued_order, filters)
            and queued_order.price <= ceiling
            and queued_order.qty <= _max_affordable_qty(
                state_decision.delta_bnb, queued_order.price, quote_budget, filters)
        )
        return BuyPlan(state_decision.confirmed_state, (queued_order,) if valid else (),
                       "Validate remaining group against current execution limits")

    if state_decision.confirmed_state == ReplenishmentState.WATCH:
        return plan_watch_orders(
            state_decision.delta_bnb,
            ref_price,
            filters,
            cfg.execution,
            cfg.symbol,
            quote_budget,
            owned,
        )
    if state_decision.confirmed_state == ReplenishmentState.ACCUMULATE:
        return plan_accumulate_orders(
            state_decision.delta_bnb,
            ref_price,
            filters,
            cfg.execution,
            cfg.symbol,
            quote_budget,
            owned,
        )
    return plan_urgent_orders(
        state_decision.delta_bnb,
        market,
        filters,
        cfg.execution,
        cfg.symbol,
        quote_budget,
    )
