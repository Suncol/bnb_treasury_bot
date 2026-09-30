from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

from core.models import CancelPlan, OperationKind, OperationStatus, PendingState
from core.time_utils import utc
from core.validation import valid_account_snapshot

from .exchange_adapter import ExchangeError, RequestDeferred, order_matches_operation
from .executor import Executor

ZERO = Decimal("0")


class Reconciler:
    def __init__(self, exchange, repository, cfg, *, clock=None):
        self.exchange, self.repository, self.cfg = exchange, repository, cfg
        self.clock = clock or getattr(exchange, "clock", None)
        self.read_checkpoint = None
        self.executor = Executor(exchange, repository, cfg, clock=self.clock)

    def protective_cancellations(self, *, guard=None, now=None):
        """Best-effort known-order management when a complete view is unavailable.

        Absence/query failure never releases exposure. A journaled exchange ID
        remains safe to cancel, but unidentified submissions and external orders
        are never guessed. Pending and terminal cancels are not submitted again.
        """
        repo, cfg = self.repository, self.cfg
        operations = repo.operations(active_only=True)
        blocked = {
            (op.payload.symbol, op.payload.order_id)
            for op in operations
            if op.kind == OperationKind.CANCEL and op.status != OperationStatus.FAILED
        }
        tracked = repo.tracked_orders(active_only=True)
        count = kept = 0
        allowance = (
            max(cfg.crash_guard.max_acquisition_bnb - guard.filled_bnb, ZERO)
            if guard
            else ZERO
        )
        operations = sorted(
            operations,
            key=lambda op: (
                getattr(op.payload, "price", None) or ZERO,
                utc(op.created_at),
                op.client_id,
            ),
        )
        if guard is not None:
            for op in operations:
                if (
                    op.kind != OperationKind.ORDER
                    or op.status == OperationStatus.FAILED
                ):
                    continue
                previous = tracked.get(op.client_id) or repo.tracked_order(op.client_id)
                if previous is not None and not previous.is_open:
                    continue
                order_id = op.exchange_id or (previous.order_id if previous else None)
                if op.payload.time_in_force == "IOC" or repo.cancel_recorded(
                    op.payload.symbol, order_id
                ):
                    allowance -= previous.remaining_qty if previous else op.payload.qty
            allowance = max(allowance, ZERO)
        for op in operations:
            if op.kind != OperationKind.ORDER or op.status == OperationStatus.FAILED:
                continue
            p = op.payload
            if p.symbol != cfg.symbol or p.side != "BUY" or p.time_in_force == "IOC":
                continue
            previous = tracked.get(op.client_id) or repo.tracked_order(op.client_id)
            if previous is not None and not previous.is_open:
                continue
            order_id = op.exchange_id or (previous.order_id if previous else None)
            if (p.symbol, order_id) in blocked or repo.cancel_recorded(
                p.symbol, order_id
            ):
                continue
            if guard is not None and order_id is not None:
                # Identity is already bound. Risk-only cancellation needs no
                # history or wallet read; stale remaining sizes overestimate risk.
                remaining = previous.remaining_qty if previous else p.qty
                ttl = (
                    cfg.execution.watch_ttl_hours
                    if op.state.value == "WATCH"
                    else cfg.execution.accumulate_ttl_hours
                )
                if (
                    guard.keep_price_ceiling is not None
                    and p.price <= guard.keep_price_ceiling
                    and kept < cfg.crash_guard.max_keep_orders
                    and remaining <= allowance
                    and utc(now) - utc(op.created_at) < timedelta(hours=ttl)
                ):
                    kept += 1
                    allowance -= remaining
                    continue
                blocked.add((p.symbol, order_id))
                yield CancelPlan(p.symbol, order_id, "Priority crash protection")
                count += 1
                if count >= 64:
                    return
                continue
            try:
                identity = (
                    {"order_id": order_id} if order_id else {"client_id": op.client_id}
                )
                order = self.exchange.fetch_order(p.symbol, **identity)
            except RequestDeferred:
                raise
            except Exception:
                order = None
            if order is not None:
                if not order_matches_operation(
                    order, replace(op, exchange_id=order_id)
                ):
                    # Do not infer identity from a contradictory query response.
                    continue
                order_id = order.order_id
                if op.exchange_id is None:
                    repo.update_operation(replace(op, exchange_id=order_id))
                tracked[op.client_id] = replace(
                    order, strategy_id=cfg.strategy_id, state=op.state
                )
                repo.save_tracked_orders(tracked)
                if not order.is_open:
                    continue
            if (
                order_id is None
                or (p.symbol, order_id) in blocked
                or repo.cancel_recorded(p.symbol, order_id)
            ):
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

    def _read(self, method, *args, **kwargs):
        if self.read_checkpoint is not None:
            self.read_checkpoint()
        result = method(*args, **kwargs)
        if self.read_checkpoint is not None:
            self.read_checkpoint()
        return result

    def refresh(self, now):
        repo, exchange, cfg = self.repository, self.exchange, self.cfg
        tracked = repo.tracked_orders(active_only=True)
        for op in repo.operations(unresolved_only=True):
            if op.status == OperationStatus.CONFIRMED:
                continue  # Submission/transfer accepted; balance coverage remains.
            if op.kind != OperationKind.CANCEL and not op.balance_pending:
                op = replace(op, balance_pending=True)
            # Older journals may have learned the ID only in tracked_orders.
            previous = tracked.get(op.client_id) or repo.tracked_order(op.client_id)
            if op.kind == OperationKind.ORDER and op.exchange_id is None and previous:
                bound = replace(op, exchange_id=previous.order_id)
                if order_matches_operation(previous, bound):
                    op = bound
                    repo.update_operation(op)
            try:
                result = self._read(exchange.query_operation, op)
            except RequestDeferred:
                raise
            except Exception as exc:
                repo.update_operation(
                    replace(
                        repo.operation(op.client_id),
                        status=OperationStatus.UNKNOWN,
                        balance_pending=op.balance_pending,
                        checked_at=self.clock() if self.clock else now,
                        error=type(exc).__name__,
                    )
                )
                # The read circuit breaker handles this once; its protective
                # path can still cancel other journaled orders without guessing.
                raise
            # A priority checkpoint may bind identity while this read is in
            # progress. Never overwrite that newer binding with an old query.
            current = repo.operation(op.client_id)
            if current.exchange_id is not None:
                if (
                    result.exchange_id is not None
                    and result.exchange_id != current.exchange_id
                ):
                    raise ExchangeError(
                        "Operation query contradicts a bound exchange identity"
                    )
                op = replace(op, exchange_id=current.exchange_id)
            self.executor.record_result(
                replace(
                    op,
                    status=result.status,
                    exchange_id=result.exchange_id or op.exchange_id,
                    checked_at=self.clock() if self.clock else now,
                )
            )
        operations = repo.operations(active_only=True)
        for op in operations:
            if op.kind == OperationKind.ORDER:
                previous = repo.tracked_order(op.client_id)
                if previous is not None:
                    tracked[op.client_id] = previous
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
        orders = {
            o.order_id: o for o in self._read(exchange.fetch_open_orders, cfg.symbol)
        }
        for client_id, op in buys.items():
            previous = tracked.get(client_id) or repo.tracked_order(client_id)
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
                    self._read(
                        exchange.fetch_order, cfg.symbol, order_id=op.exchange_id
                    )
                    if op.exchange_id
                    else self._read(
                        exchange.fetch_order, cfg.symbol, client_id=client_id
                    )
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
                raise ExchangeError(
                    "Order identity conflicts with the operation journal"
                )
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
        repo.save_tracked_orders(tracked)
        runtime = repo.runtime()
        since = repo.load("fills_since", utc(now) - timedelta(hours=24))
        if tracked:
            totals = repo.fill_totals(
                cfg.symbol, (o.order_id for o in tracked.values())
            )
            missing = [
                o
                for o in tracked.values()
                if totals.get(o.order_id, ZERO) < o.filled_qty
            ]
            if missing:
                since = min(utc(since), min(utc(o.created_at) for o in missing))
        # Indexed journal lookup preserves ownership for late fills of retired
        # orders without loading every historical operation into the hot path.
        fills = []
        for symbol in dict.fromkeys((cfg.symbol, *cfg.spot_activity_symbols)):
            for fill in self._read(exchange.fetch_recent_fills, symbol, since, now):
                fills.append(self._attribute_fill(fill))
        repo.save_fills(fills)
        runtime = repo.runtime()  # Priority reads may have opened a new episode.
        if runtime.guard.episode_id and not runtime.guard.active:
            episode = repo.load("episode:" + runtime.guard.episode_id, runtime.guard)
            runtime = replace(
                runtime, guard=replace(runtime.guard, filled_bnb=episode.filled_bnb)
            )
            repo.save("runtime", runtime)
        repo.save("fills_since", utc(now) - timedelta(seconds=60))
        transfers = self._read(
            exchange.fetch_recent_transfers, utc(now) - timedelta(hours=49), now
        )
        repo.save_transfers(transfers)
        # Balance is the source of truth for net inventory; fills are for audit
        # and gross guard allowance only, never added on top of this balance.
        account = self._read(exchange.fetch_account_snapshot)
        after = self._read(exchange.fetch_open_orders, cfg.symbol)
        before_open = {
            k: (o.qty, o.filled_qty, o.status) for k, o in orders.items() if o.is_open
        }
        after_open = {o.order_id: (o.qty, o.filled_qty, o.status) for o in after}
        consistent = before_open == after_open
        # Cumulative order fills must be covered by the deduplicated trade ledger.
        if tracked:
            totals = repo.fill_totals(
                cfg.symbol, (o.order_id for o in tracked.values())
            )
            if any(
                totals.get(o.order_id, ZERO) < o.filled_qty for o in tracked.values()
            ):
                consistent = False
        external_pending = any(
            t.status in {OperationStatus.PENDING, OperationStatus.UNKNOWN}
            and t.asset in {"BNB", cfg.quote_asset}
            for t in transfers
        )
        spot_settled = False
        if consistent and valid_account_snapshot(
            account, self.clock() if self.clock else now, cfg
        ):
            if repo.load("spot_checkpoint") is not None or not external_pending:
                spot_settled = self._settle_spot(account)
            for op in repo.operations(active_only=True):
                if (
                    op.status != OperationStatus.CONFIRMED
                    or op.kind == OperationKind.CANCEL
                ):
                    continue
                settled = spot_settled and (
                    op.kind == OperationKind.ORDER
                    or _futures_transfer_settled(op, account)
                )
                if op.balance_pending == settled:
                    op = replace(op, balance_pending=not settled)
                    repo.update_operation(op)
                previous = tracked.get(op.client_id)
                if settled and previous is not None and not previous.is_open:
                    repo.retire_order(op.client_id)
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
            has_pending_orders=not spot_settled
            or any(
                op.kind == OperationKind.ORDER and op.unresolved for op in unresolved
            )
            or any(
                o.status in {"PENDING_NEW", "PENDING_CANCEL"}
                or o.time_in_force == "IOC"
                for o in active
            ),
            has_pending_cancels=bool(pending_cancels),
        )
        # Account-wide pending transfers also block bootstrap and strategy writes.
        if external_pending:
            pending = replace(pending, has_unknown_transfers=True)
        runtime = repo.runtime()
        if runtime.guard.active:
            repo.save(
                "runtime",
                replace(runtime, guard=self.guard_with_fills(runtime.guard, now)),
            )
        return account, active, pending, consistent

    def _settle_spot(self, account):
        """Advance one durable balance/ledger checkpoint only on exact coverage.

        Both spot assets must include all known executions. Request timestamps,
        repeated reads and elapsed time cannot prove wallet settlement.
        """
        repo, cfg = self.repository, self.cfg
        checkpoint = repo.load("spot_checkpoint")
        if checkpoint is None:
            if repo.operations(active_only=True):
                repo.save(
                    "spot_settlement_issue",
                    {"reason": "Legacy active intent has no trusted spot baseline"},
                )
                return False
            repo.save(
                "spot_checkpoint", {"account": account, "cursor": repo.wallet_cursor()}
            )
            repo.save("spot_settlement_issue", None)
            return True
        before = checkpoint["account"]
        expected = {"BNB": before.spot_bnb, cfg.quote_asset: before.spot_usd}
        missing = []
        for _, reference, deltas in repo.wallet_changes(checkpoint["cursor"]):
            for asset in expected:
                delta = deltas.get(asset, ZERO)
                if delta is None:
                    missing.append(reference + ":" + asset)
                else:
                    expected[asset] += delta
        actual = {"BNB": account.spot_bnb, cfg.quote_asset: account.spot_usd}
        differences = {
            asset: actual[asset] - expected[asset]
            for asset in expected
            if actual[asset] != expected[asset]
        }
        if differences or missing:
            repo.save(
                "spot_settlement_issue",
                {
                    "expected": expected,
                    "actual": actual,
                    "differences": differences,
                    "missing_evidence": tuple(missing),
                    "checkpoint_at": before.ts,
                    "operations": tuple(
                        op.client_id for op in repo.operations(active_only=True)
                    ),
                },
            )
            return False
        repo.save(
            "spot_checkpoint", {"account": account, "cursor": repo.wallet_cursor()}
        )
        repo.save("spot_settlement_issue", None)
        return True

    def guard_with_fills(self, guard, now):
        stored = (
            self.repository.load("episode:" + guard.episode_id)
            if guard.episode_id
            else None
        )
        if stored is not None and stored.started_at == guard.started_at:
            total = stored.filled_bnb
        else:
            total = sum(
                (
                    f.qty
                    for f in self.repository.fills_since(
                        guard.started_at, until=guard.ended_at
                    )
                    if f.strategy_id == self.cfg.strategy_id
                    and f.symbol == self.cfg.symbol
                    and f.side == "BUY"
                    and utc(f.ts) <= utc(now)
                ),
                ZERO,
            )
        return replace(guard, filled_bnb=max(total, guard.filled_bnb))

    def _attribute_fill(self, fill):
        owner = (
            self.repository.order_operation(fill.order_id)
            if fill.symbol == self.cfg.symbol
            else None
        )
        owned = (
            owner is not None
            and owner.payload.symbol == fill.symbol
            and owner.payload.side == fill.side
        )
        return replace(fill, strategy_id=self.cfg.strategy_id if owned else None)

    def import_spot_trades(self, symbols, *, operator, reason, now):
        """Maintenance-only evidence repair; no arbitrary balance overrides.

        Query authenticated exchange receipts since the frozen checkpoint. The
        normal balance gate still decides settlement, including futures credit.
        """
        if not operator.strip() or not reason.strip() or not symbols:
            raise ValueError("Operator, reason and activity symbols are required")
        with self.repository.exclusive():
            before = self.repository.load("spot_checkpoint")
            if before is None:
                raise ValueError("A trusted checkpoint is required")
            receipts = []
            for symbol in dict.fromkeys(symbols):
                receipts.extend(
                    self.exchange.fetch_recent_fills(symbol, before["account"].ts, now)
                )
            # Keep strategy ownership when reimporting previously journaled buys.
            receipts = [self._attribute_fill(fill) for fill in receipts]
            self.repository.save_fills(
                receipts,
                evidence={
                    "at": now,
                    "operator": operator,
                    "reason": reason,
                    "symbols": tuple(symbols),
                    "before": before,
                    "trades": tuple((f.symbol, f.trade_id) for f in receipts),
                },
            )
            result = self.refresh(now)
            self.repository.event(
                now,
                "SPOT_EVIDENCE_IMPORT",
                {
                    "operator": operator,
                    "reason": reason,
                    "symbols": tuple(symbols),
                    "trades": tuple((f.symbol, f.trade_id) for f in receipts),
                    "before": before,
                    "after": self.repository.load("spot_checkpoint"),
                    "remaining_issue": self.repository.load("spot_settlement_issue"),
                },
            )
            return result

    def bind_transfer_id(self, client_id, transfer_id, now):
        """Operator-supplied identity; verify exchange evidence before unblocking."""
        with self.repository.exclusive():
            operations = self.repository.operations()
            operation = next(op for op in operations if op.client_id == client_id)
            if operation.kind != OperationKind.TRANSFER or operation.status not in {
                OperationStatus.PENDING,
                OperationStatus.UNKNOWN,
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
                    bound,
                    status=result.status,
                    balance_pending=True,
                    checked_at=self.clock() if self.clock else now,
                )
            )
            self.repository.event(
                now,
                "MANUAL_TRANSFER_BINDING",
                {"client_id": client_id, "transfer_id": transfer_id},
            )


def _futures_transfer_settled(op, account):
    """Spot coverage is checked globally; a transfer also needs futures evidence."""
    before, p = op.balance_before, op.payload
    if before is None or op.balance_fill_cursor is None:
        return False
    bnb = p.asset == "BNB"
    futures_field = "contract_bnb" if bnb else "contract_quote_balance"
    spot_delta = p.amount if p.to_account == "SPOT" else -p.amount
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
        updated_at is not None
        and op.checked_at is not None
        and utc(op.checked_at) + timedelta(seconds=1)
        < utc(updated_at)
        <= utc(account.ts) + timedelta(seconds=1)
    ):
        return True
    if previous is None or not previous.is_finite():
        return False
    expected_futures = previous - spot_delta
    return value >= expected_futures if spot_delta < 0 else value <= expected_futures
