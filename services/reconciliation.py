from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

from core.models import CancelPlan, OperationKind, OperationStatus, PendingState
from core.time_utils import fresh, utc
from .exchange_adapter import ExchangeError, order_matches_operation
from .executor import Executor

ZERO = Decimal("0")


class Reconciler:
    def __init__(self, exchange, repository, cfg, *, clock=None):
        self.exchange, self.repository, self.cfg = exchange, repository, cfg
        self.clock = clock or getattr(exchange, "clock", None)
        self.executor = Executor(exchange, repository, cfg, clock=self.clock)

    def protective_cancellations(self):
        """Best-effort known-order management when a complete view is unavailable.

        Absence/query failure never releases exposure. A journaled exchange ID
        remains safe to cancel, but unidentified submissions and external orders
        are never guessed. Pending and terminal cancels are not submitted again.
        """
        repo, cfg = self.repository, self.cfg
        operations = repo.operations()
        blocked = {
            (op.payload.symbol, op.payload.order_id)
            for op in operations
            if op.kind == OperationKind.CANCEL
            and op.status != OperationStatus.FAILED
        }
        tracked = repo.load("tracked_orders", {})
        count = 0
        for op in operations:
            if op.kind != OperationKind.ORDER or op.status == OperationStatus.FAILED:
                continue
            p = op.payload
            if p.symbol != cfg.symbol or p.side != "BUY" or p.time_in_force == "IOC":
                continue
            previous = tracked.get(op.client_id)
            if previous is not None and not previous.is_open:
                continue
            order_id = op.exchange_id or (previous.order_id if previous else None)
            if (p.symbol, order_id) in blocked:
                continue
            try:
                identity = (
                    {"order_id": order_id}
                    if order_id
                    else {"client_id": op.client_id}
                )
                order = self.exchange.fetch_order(p.symbol, **identity)
            except Exception:
                order = None
            if order is not None:
                if not order_matches_operation(order, replace(op, exchange_id=order_id)):
                    # Do not infer identity from a contradictory query response.
                    continue
                order_id = order.order_id
                if op.exchange_id is None:
                    repo.update_operation(replace(op, exchange_id=order_id))
                tracked[op.client_id] = replace(
                    order, strategy_id=cfg.strategy_id, state=op.state
                )
                repo.save("tracked_orders", tracked)
                if not order.is_open:
                    continue
            if order_id is None or (p.symbol, order_id) in blocked:
                continue
            blocked.add((p.symbol, order_id))
            yield CancelPlan(
                p.symbol,
                order_id,
                "Incomplete reconciliation; cannot establish safe exposure",
            )
            count += 1
            if count >= 64:
                return

    def refresh(self, now):
        repo, exchange, cfg = self.repository, self.exchange, self.cfg
        tracked = repo.load("tracked_orders", {})
        for op in repo.operations(unresolved_only=True):
            if op.status == OperationStatus.CONFIRMED:
                continue  # Terminal transfer: only its balance settlement remains.
            if op.kind == OperationKind.TRANSFER and not op.balance_pending:
                op = replace(op, balance_pending=True)
            # Older journals may have learned the ID only in tracked_orders.
            previous = tracked.get(op.client_id)
            if op.kind == OperationKind.ORDER and op.exchange_id is None and previous:
                bound = replace(op, exchange_id=previous.order_id)
                if order_matches_operation(previous, bound):
                    op = bound
                    repo.update_operation(op)
            try:
                result = exchange.query_operation(op)
            except Exception as exc:
                repo.update_operation(
                    replace(
                        op,
                        status=OperationStatus.UNKNOWN,
                        checked_at=now,
                        error=type(exc).__name__,
                    )
                )
                self.executor.note_failure(reason="Operation query failed")
                # A failed lookup cannot release the intent, but must not prevent
                # protective cancellation of other orders whose ownership is known.
                continue
            self.executor.record_result(
                replace(
                    op,
                    status=result.status,
                    exchange_id=result.exchange_id or op.exchange_id,
                    checked_at=self.clock() if self.clock else now,
                )
            )
        operations = repo.operations()
        buys = {op.client_id: op for op in operations if op.kind == OperationKind.ORDER}
        buys_by_id = {
            op.exchange_id: op for op in buys.values() if op.exchange_id is not None
        }
        pending_cancels = {
            op.payload.order_id
            for op in operations
            if op.kind == OperationKind.CANCEL and op.unresolved
        }
        # Missing from openOrders does not mean canceled. Query journaled orders.
        orders = {o.order_id: o for o in exchange.fetch_open_orders(cfg.symbol)}
        for client_id, op in buys.items():
            previous = tracked.get(client_id)
            if op.status == OperationStatus.FAILED or (
                previous is not None and not previous.is_open
            ):
                continue
            present = (
                op.exchange_id in orders
                if op.exchange_id is not None
                else any(o.client_id == client_id for o in orders.values())
            )
            if not present:
                order = (
                    exchange.fetch_order(cfg.symbol, order_id=op.exchange_id)
                    if op.exchange_id
                    else exchange.fetch_order(cfg.symbol, client_id=client_id)
                )
                if order is not None:
                    orders[order.order_id] = order
                elif op.status == OperationStatus.CONFIRMED:
                    # A formerly acknowledged order disappearing is unresolved.
                    repo.update_operation(
                        replace(op, status=OperationStatus.UNKNOWN, checked_at=now)
                    )
        for order_id, order in tuple(orders.items()):
            op = buys_by_id.get(order.order_id)
            if op is None:
                candidate = buys.get(order.client_id)
                if candidate is not None and candidate.exchange_id is None:
                    op = candidate
            if op is not None and op.status == OperationStatus.FAILED:
                op = None
            if op is not None and not order_matches_operation(order, op):
                raise ExchangeError("Order identity conflicts with the operation journal")
            order = replace(
                order,
                strategy_id=cfg.strategy_id if op else None,
                state=op.state if op else order.state,
                cancel_pending=order_id in pending_cancels,
            )
            orders[order_id] = order
            if op:
                if op.exchange_id is None:
                    repo.update_operation(replace(op, exchange_id=order.order_id))
                tracked[op.client_id] = order
        repo.save("tracked_orders", tracked)
        runtime = repo.runtime()
        since = repo.load("fills_since", utc(now) - timedelta(hours=24))
        if runtime.guard.active:
            since = min(utc(since), utc(runtime.guard.started_at))
        if tracked:
            previous_fills = repo.fills_since(
                min(utc(o.created_at) for o in tracked.values())
            )
            totals = _fill_totals(previous_fills)
            missing = [
                o
                for o in tracked.values()
                if totals.get(o.order_id, ZERO) < o.filled_qty
            ]
            if missing:
                since = min(utc(since), min(utc(o.created_at) for o in missing))
        # Recover all fills after an outage; active episodes are replayed in full.
        known_orders = {o.order_id: o for o in tracked.values()}
        fills = tuple(
            replace(
                f, strategy_id=cfg.strategy_id if f.order_id in known_orders else None
            )
            for f in exchange.fetch_recent_fills(cfg.symbol, since, now)
        )
        repo.save_fills(fills)
        if runtime.guard.episode_id and not runtime.guard.active:
            episode = repo.load("episode:" + runtime.guard.episode_id, runtime.guard)
            runtime = replace(
                runtime, guard=replace(runtime.guard, filled_bnb=episode.filled_bnb)
            )
            repo.save("runtime", runtime)
        repo.save("fills_since", utc(now) - timedelta(seconds=60))
        transfers = exchange.fetch_recent_transfers(utc(now) - timedelta(hours=49), now)
        repo.save_transfers(transfers)
        # Balance is the source of truth for net inventory; fills are for audit
        # and gross guard allowance only, never added on top of this balance.
        account = exchange.fetch_account_snapshot()
        after = exchange.fetch_open_orders(cfg.symbol)
        before_open = {
            k: (o.qty, o.filled_qty, o.status) for k, o in orders.items() if o.is_open
        }
        after_open = {o.order_id: (o.qty, o.filled_qty, o.status) for o in after}
        consistent = before_open == after_open
        # Cumulative order fills must be covered by the deduplicated trade ledger.
        if tracked:
            totals = _fill_totals(
                repo.fills_since(min(utc(o.created_at) for o in tracked.values()))
            )
            if any(
                totals.get(o.order_id, ZERO) < o.filled_qty for o in tracked.values()
            ):
                consistent = False
        if consistent and fresh(
            account.ts, self.clock() if self.clock else now, cfg.risk.max_account_age_seconds
        ):
            for op in repo.operations(unresolved_only=True):
                if op.status == OperationStatus.CONFIRMED and _transfer_balances_settled(
                    op, account, repo.fills_after(op.balance_fill_cursor or 0), cfg.symbol
                ):
                    repo.update_operation(replace(op, balance_pending=False))
        active = tuple(o for o in orders.values() if o.is_open)
        unresolved = repo.operations(unresolved_only=True)
        pending = PendingState(
            pending_bnb_to_contract=sum(
                (
                    op.payload.amount
                    for op in unresolved
                    if op.kind == OperationKind.TRANSFER
                    and op.payload.asset == "BNB"
                    and op.payload.to_account == "USDⓈ-M Futures"
                ),
                ZERO,
            ),
            pending_usd_to_spot=sum(
                (
                    op.payload.amount
                    for op in unresolved
                    if op.kind == OperationKind.TRANSFER
                    and op.payload.asset == cfg.quote_asset
                    and op.payload.to_account == "SPOT"
                ),
                ZERO,
            ),
            pending_usd_to_contract=sum(
                (
                    op.payload.amount
                    for op in unresolved
                    if op.kind == OperationKind.TRANSFER
                    and op.payload.asset == cfg.quote_asset
                    and op.payload.to_account == "USDⓈ-M Futures"
                ),
                ZERO,
            ),
            open_buy_remaining_qty=sum(
                (o.remaining_qty for o in active if o.side == "BUY"), ZERO
            ),
            filled_untransferred_bnb=ZERO,
            has_unknown_orders=any(
                op.kind != OperationKind.TRANSFER
                and op.status == OperationStatus.UNKNOWN
                for op in unresolved
            ),
            has_unknown_transfers=any(
                op.kind == OperationKind.TRANSFER
                and op.status == OperationStatus.UNKNOWN
                for op in unresolved
            ),
            has_pending_orders=any(
                op.kind == OperationKind.ORDER and op.unresolved for op in unresolved
            )
            or any(
                o.status in {"PENDING_NEW", "PENDING_CANCEL"}
                or o.time_in_force == "IOC"
                for o in active
            ),
            has_pending_cancels=bool(pending_cancels),
        )
        # Account-wide pending transfers also block the strategy, including manual ones.
        if any(
            t.status in {OperationStatus.PENDING, OperationStatus.UNKNOWN}
            and t.asset in {"BNB", cfg.quote_asset}
            for t in transfers
        ):
            pending = replace(pending, has_unknown_transfers=True)
        if runtime.guard.active:
            repo.save(
                "runtime", replace(runtime, guard=self.guard_with_fills(runtime.guard, now))
            )
        return account, active, pending, consistent

    def guard_with_fills(self, guard, now):
        total = sum(
            (
                f.qty for f in self.repository.fills_since(guard.started_at)
                if f.strategy_id == self.cfg.strategy_id
                and f.symbol == self.cfg.symbol and utc(f.ts) <= utc(now)
            ),
            ZERO,
        )
        return replace(guard, filled_bnb=max(total, guard.filled_bnb))

    def bind_transfer_id(self, client_id, transfer_id, now):
        """Operator-supplied identity; verify exchange evidence before unblocking."""
        with self.repository.exclusive():
            operations = self.repository.operations()
            operation = next(op for op in operations if op.client_id == client_id)
            if operation.kind != OperationKind.TRANSFER or operation.status not in {
                OperationStatus.PENDING, OperationStatus.UNKNOWN
            }:
                raise ValueError("Only unresolved transfers can be bound")
            if operation.exchange_id not in {None, transfer_id}:
                raise ValueError("Operation already has a different exchange identity")
            if any(
                op.kind == OperationKind.TRANSFER
                and op.client_id != client_id
                and op.exchange_id == transfer_id
                for op in operations
            ):
                raise ValueError("Transfer identity is already assigned")
            bound = replace(operation, exchange_id=transfer_id)
            result = self.exchange.query_operation(bound)
            if result.status not in {OperationStatus.CONFIRMED, OperationStatus.FAILED}:
                raise ValueError("No matching terminal exchange record")
            self.executor.record_result(
                replace(
                    bound, status=result.status, balance_pending=True,
                    checked_at=self.clock() if self.clock else now,
                )
            )
            self.repository.event(
                now,
                "MANUAL_TRANSFER_BINDING",
                {"client_id": client_id, "transfer_id": transfer_id},
            )


def _transfer_balances_settled(op, account, fills, symbol):
    """Prove the spot delta and futures settlement before releasing the gate."""
    before, p = op.balance_before, op.payload
    if before is None or op.balance_fill_cursor is None:
        return False  # A legacy intent without a baseline cannot prove settlement.
    bnb = p.asset == "BNB"
    spot_field = "spot_bnb" if bnb else "spot_usd"
    futures_field = "contract_bnb" if bnb else "contract_quote_balance"
    spot_delta = p.amount if p.to_account == "SPOT" else -p.amount
    expected_spot = getattr(before, spot_field) + spot_delta
    for fill in fills:
        if fill.symbol != symbol:
            continue
        if not bnb and fill.quote_qty is None:
            return False
        expected_spot += fill.qty if bnb else -fill.quote_qty
        if fill.commission_asset == p.asset:
            expected_spot -= fill.commission
    if getattr(account, spot_field) != expected_spot:
        return False
    value, previous = getattr(account, futures_field), getattr(before, futures_field)
    if value is None or not value.is_finite():
        return False
    updated_at = (
        account.contract_bnb_updated_at if bnb else account.contract_quote_updated_at
    )
    # Per-asset futures updateTime can prove a later revision containing fees/PnL.
    # The spot account's metadata updateTime is not an asset balance watermark.
    # Include the adapter's permitted 1s clock skew; waiting alone proves nothing.
    if (
        updated_at is not None and op.checked_at is not None
        and utc(op.checked_at) + timedelta(seconds=1) < utc(updated_at)
        <= utc(account.ts) + timedelta(seconds=1)
    ):
        return True
    if previous is None or not previous.is_finite():
        return False
    expected_futures = previous - spot_delta
    return value >= expected_futures if spot_delta < 0 else value <= expected_futures


def _fill_totals(fills):
    totals = {}
    for fill in fills:
        totals[fill.order_id] = totals.get(fill.order_id, ZERO) + fill.qty
    return totals
