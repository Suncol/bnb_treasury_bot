from decimal import Decimal

from .crash_guard import valid_market
from .math_utils import non_negative
from .models import (
    CancelPlan,
    CrashGuardState,
    EngineInputs,
    ReplenishmentState,
    StateDecision,
    StrategyConfig,
)
from .order_planner import compute_ref_price
from .time_utils import age_seconds, utc


def plan_cancellations(
    inputs: EngineInputs,
    decision: StateDecision,
    target: Decimal,
    guard: CrashGuardState,
    cfg: StrategyConfig,
) -> tuple[CancelPlan, ...]:
    """Selection is deterministic; requested cancels never release exposure here."""
    state, c = decision.confirmed_state, cfg.execution
    owned = decision.effective_supply - inputs.pending.open_buy_remaining_qty
    external = sum(
        (
            o.remaining_qty
            for o in inputs.orders
            if o.symbol == cfg.symbol
            and o.side == "BUY"
            and o.strategy_id != cfg.strategy_id
        ),
        Decimal("0"),
    )
    cap = non_negative(target - owned - external)
    if guard.active:
        cap = min(
            cap, non_negative(cfg.crash_guard.max_acquisition_bnb - guard.filled_bnb)
        )
        reserved = sum(
            (
                o.remaining_qty
                for o in inputs.orders
                if o.cancel_pending
                and o.strategy_id == cfg.strategy_id
                and o.side == "BUY"
            ),
            Decimal("0"),
        )
        cap = non_negative(cap - reserved)
    market_ok = valid_market(inputs, cfg)
    ref = compute_ref_price(inputs.market) if market_ok else None
    chasing = market_ok and inputs.market.return_1h > c.chase_return_limit
    orders = sorted(
        (
            o
            for o in inputs.orders
            if o.symbol == cfg.symbol
            and o.side == "BUY"
            and o.strategy_id == cfg.strategy_id
            and o.is_open
        ),
        key=lambda o: (o.price, utc(o.created_at).timestamp(), o.order_id),
    )
    cancellations = []
    kept = 0
    for order in orders:
        if order.cancel_pending:
            continue
        ordinary = order.time_in_force != "IOC"
        ttl = (
            c.watch_ttl_hours
            if order.state == ReplenishmentState.WATCH
            else c.accumulate_ttl_hours
        )
        reprice = (
            c.watch_reprice_hours
            if state == ReplenishmentState.WATCH
            else c.accumulate_reprice_hours
        )
        age = age_seconds(inputs.now, order.created_at)
        reason = None
        reprice_needed = False
        if state == ReplenishmentState.IDLE:
            reason = "IDLE inventory does not require outstanding bids"
        elif ordinary and age >= ttl * 3600:
            reason = "Order TTL expired"
            reprice_needed = True
        elif ordinary and (
            not market_ok
            or not inputs.snapshot_consistent
            or inputs.pending.has_unknown_orders
        ):
            if (
                not guard.active
                or guard.keep_price_ceiling is None
                or not inputs.snapshot_consistent
                or inputs.pending.has_unknown_orders
            ):
                reason = "Cannot establish safe price or exposure"
            elif order.price > guard.keep_price_ceiling:
                reason = "Above last valid crash guard ceiling"
        if reason is None and ordinary:
            if state == ReplenishmentState.URGENT and owned < target:
                reason = "Reconcile ordinary bid before urgent IOC replacement"
            elif guard.active:
                if (
                    guard.keep_price_ceiling is None
                    or order.price > guard.keep_price_ceiling
                ):
                    reason = "Above crash guard price ceiling"
                elif kept >= cfg.crash_guard.max_keep_orders:
                    reason = "Crash guard order count limit"
            elif ref is not None:
                if order.price > ref:
                    reason = "Bid above current reference price"
                elif not chasing and (
                    age >= reprice * 3600
                    or (
                        state == ReplenishmentState.ACCUMULATE
                        and order.price > 0
                        and (ref - order.price) / order.price > c.reprice_deviation
                    )
                ):
                    reason = "Periodic or deviation reprice; refresh after cancel"
                    reprice_needed = True
        if reason is None and order.remaining_qty > cap:
            reason = "Remaining quantity exceeds inventory or episode allowance"
        if reason is not None:
            if not order.cancel_pending:
                cancellations.append(
                    CancelPlan(
                        order.symbol,
                        order.order_id,
                        reason,
                        replenish_after_cancel=reprice_needed
                        and not guard.active
                        and not chasing
                        and state
                        in {ReplenishmentState.WATCH, ReplenishmentState.ACCUMULATE},
                    )
                )
        else:
            cap -= order.remaining_qty
            kept += 1
    return tuple(cancellations)
