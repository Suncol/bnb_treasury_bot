from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

from core.models import CancelPlan, OperationKind, OperationStatus, PendingState
from core.time_utils import fresh, utc
from core.validation import valid_account_snapshot

from .exchange_adapter import ExchangeError, RequestDeferred, order_matches_operation
from .executor import ExecutionBlocked, Executor
from .history import (
    futures_settlement_evidence,
    historical_metrics,
    INCOME_RECHECK_INTERVAL,
    margin_baseline,
    settlement_policy_issue,
)

ZERO = Decimal("0")


class Reconciler:
    def __init__(self, exchange, repository, cfg, *, clock=None, executor=None):
        self.exchange, self.repository, self.cfg = exchange, repository, cfg
        self.clock = clock or getattr(exchange, "clock", None)
        self.read_checkpoint = None
        self.executor = executor or Executor(exchange, repository, cfg, clock=self.clock)
        self.executor.reconciler = self
        self.history = (ZERO, None)
        self._funding_orders = None

    def prepare_execution(self, now, *, account=None):
        """Return a newly verified account and orders for the current write batch.

        Keep the shared executor's audit and priority read checkpoints. Callers
        must still check their candidate against the returned financial evidence.
        """
        if account is not None:
            consistent = self.audit_income(account, now)
            checked = self.clock() if self.clock else now
            if (not consistent and self.executor.income_scan(checked) is not None
                    and not fresh(account.ts, checked, self.cfg.risk.max_account_age_seconds)
                    and utc(account.ts) <= utc(checked)):
                account, orders, pending, consistent = self.refresh(
                    checked, force_income=True, retry_slow=False,
                )
            else:
                orders, pending = (), PendingState(ZERO, ZERO, ZERO, ZERO)
        else:
            account, orders, pending, consistent = self.refresh(now, force_income=True)
        checked_at = self.clock() if self.clock else now
        if not (
            consistent and not pending.unresolved
            and valid_account_snapshot(account, checked_at, self.cfg)
            and (account.contract_total_margin_balance is None
                 or account.contract_margin_asset == self.cfg.quote_asset)
            and not self.repository.operations(unresolved_only=True)
            and not self.repository.load("spot_settlement_issue")
            and not self.repository.load("futures_settlement_issue")
            and self.repository.load("spot_checkpoint") is not None
            and self.executor.income_audited(checked_at, account=account)
        ):
            self.executor.invalidate_income_audit()
            raise ExecutionBlocked("New operations require reconciled futures income and a valid account")
        return account, orders

    def protective_cancellations(self, *, guard=None, now=None, retries_only=False):
        """Best-effort known-order management when a complete view is unavailable.

        Absence/query failure never releases exposure. A journaled exchange ID
        remains safe to cancel, but unidentified submissions and external orders
        are never guessed. Only verified open orders can retry a pending cancel.
        """
        repo, cfg = self.repository, self.cfg
        operations = repo.operations(active_only=True)
        now = now or (self.clock() if self.clock else None)
        count = 0
        if now is not None:
            for op in operations:
                if op.kind == OperationKind.CANCEL and self.executor.cancel_due(
                    op.payload, now
                ):
                    yield op.payload
                    count += 1
                    if count >= 64:
                        return
        if retries_only:
            return
        blocked = {
            (op.payload.symbol, op.payload.order_id)
            for op in operations
            if op.kind == OperationKind.CANCEL and op.status != OperationStatus.FAILED
        }
        tracked = repo.tracked_orders(active_only=True)
        kept = 0
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

    def refresh(self, now, *, force_income=False, retry_slow=True):
        """At most one fresh observation after a completed slow batch audit."""
        try:
            for attempt in range(2):
                result = self._refresh(now, force_income=force_income)
                account, _, _, consistent = result
                checked = self.clock() if self.clock else now
                scan = self.executor.income_scan(checked)
                if (
                    attempt == 0 and retry_slow and scan is not None and consistent
                    and account.ts is not None and utc(account.ts) <= utc(checked)
                    and not fresh(account.ts, checked, self.cfg.risk.max_account_age_seconds)
                ):
                    now = checked
                    continue
                if (not valid_account_snapshot(account, checked, self.cfg)
                        or not consistent or result[2].unresolved
                        or self.repository.load("futures_settlement_issue")
                        or self.repository.load("spot_settlement_issue")):
                    self.executor.invalidate_income_audit()
                return result
        except BaseException:
            self.executor.invalidate_income_audit()
            raise

    def _refresh(self, now, *, force_income=False):
        self._funding_orders = None
        repo, exchange, cfg = self.repository, self.exchange, self.cfg
        tracked = repo.tracked_orders(active_only=True)
        for op in repo.operations(unresolved_only=True):
            if op.status == OperationStatus.CONFIRMED and not (
                op.kind == OperationKind.TRANSFER
                and repo.transfer_record(op.exchange_id) is None
            ):
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
                current = repo.operation(op.client_id)
                if not (
                    op.kind == OperationKind.CANCEL
                    and current.status == OperationStatus.CONFIRMED
                ):
                    repo.update_operation(
                        replace(
                            current,
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
            if op.kind == OperationKind.CANCEL:
                if current.status == OperationStatus.CONFIRMED:
                    continue  # A protective read boundary already closed it.
                op = replace(
                    op, cancel_attempts=current.cancel_attempts,
                    cancel_retry_at=current.cancel_retry_at,
                )
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
                ),
                transfer=result.transfer,
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
        # Only a live funded continuation needs other symbols' reservation
        # proof. Keep these orders outside BNB exposure and cancellation policy.
        extra_symbols = tuple(dict.fromkeys(
            symbol for symbol in cfg.spot_activity_symbols
            if symbol != cfg.symbol and symbol.endswith(cfg.quote_asset)
        )) if repo.runtime().resume_replenishment else ()
        extra_before = tuple(
            replace(order, strategy_id=None)
            for symbol in extra_symbols
            for order in self._read(exchange.fetch_open_orders, symbol)
        )
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
        checkpoint = repo.load("spot_checkpoint")
        if checkpoint is not None:
            since = min(
                utc(since), utc(checkpoint["account"].ts) - timedelta(seconds=1)
            )
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
        recovery_since = utc(now) - timedelta(hours=49)
        if checkpoint is not None:
            recovery_since = min(
                recovery_since, utc(checkpoint["account"].ts) - timedelta(seconds=1)
            )
        pending_transfers = tuple(
            op for op in repo.operations(unresolved_only=True)
            if op.kind == OperationKind.TRANSFER
        )
        if pending_transfers:
            recovery_since = min(
                recovery_since,
                min(utc(op.created_at) for op in pending_transfers) - timedelta(minutes=1),
            )
        transfers = self._read(
            exchange.fetch_recent_transfers, recovery_since, now
        )
        repo.save_transfers(transfers)
        # Balance is the source of truth for net inventory; fills are for audit
        # and gross guard allowance only, never added on top of this balance.
        account = replace(
            self._read(exchange.fetch_account_snapshot),
            wallet_event_cursor=repo.wallet_cursor(),
        )
        checked_at = self.clock() if self.clock else now
        samples = repo.snapshots_since(utc(checked_at) - timedelta(hours=49))
        baseline = margin_baseline(samples, checked_at, warmup=True)
        quote_before = (
            replace(checkpoint["account"], wallet_event_cursor=checkpoint["cursor"])
            if checkpoint is not None else None
        )
        observations = [
            (op.balance_before, op.payload.asset) for op in pending_transfers
            if op.status == OperationStatus.CONFIRMED and op.balance_before is not None
        ]
        if account.contract_total_margin_balance is not None:
            observations.extend(
                (before, cfg.quote_asset) for before in (baseline, quote_before)
                if before is not None and account.contract_margin_asset == cfg.quote_asset
            )
        coverage = (
            self._refresh_income(
                observations, account,
                force=force_income and not self.executor.income_audited(checked_at),
            )
            if valid_account_snapshot(account, checked_at, cfg) else None
        )
        after = self._read(exchange.fetch_open_orders, cfg.symbol)
        extra_after = tuple(
            replace(order, strategy_id=None)
            for symbol in extra_symbols
            for order in self._read(exchange.fetch_open_orders, symbol)
        )
        before_open = {
            k: (o.qty, o.filled_qty, o.status) for k, o in orders.items() if o.is_open
        }
        after_open = {o.order_id: (o.qty, o.filled_qty, o.status) for o in after}
        # Price, side and identity must also be stable when assigning quote locks.
        def reservation_view(rows):
            return {(o.symbol, o.order_id): (o.client_id, o.side, o.price, o.qty,
                                            o.filled_qty, o.status, o.created_at)
                    for o in rows if o.is_open}
        consistent = before_open == after_open and reservation_view(extra_before) == reservation_view(extra_after)
        reservation_consistent = reservation_view(orders.values()) == reservation_view(after)
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
        observation_valid = consistent and valid_account_snapshot(
            account, self.clock() if self.clock else now, cfg
        )
        futures_issues = {} if observation_valid else dict(
            repo.load("futures_settlement_issue") or {}
        )
        if observation_valid:
            quote_settled = True
            spot_before = checkpoint
            policy = None
            if quote_before is not None and account.contract_total_margin_balance is not None:
                quote_settled, anchor, evidence = futures_settlement_evidence(
                    repo, quote_before, account, cfg.quote_asset, samples=samples
                )
                if not quote_settled:
                    futures_issues["quote"] = evidence
                else:
                    policy = settlement_policy_issue(evidence)
                    if policy is not None:
                        futures_issues["quote"] = policy
                    spot_before = {"account": anchor, "cursor": anchor.wallet_event_cursor}
            if quote_settled and policy is None and (checkpoint is not None or not external_pending):
                spot_settled = self._settle_spot(account, checkpoint=spot_before)
            if spot_settled:
                repo.save("fills_since", utc(now) - timedelta(seconds=60))
            for op in repo.operations(active_only=True):
                if (
                    op.status != OperationStatus.CONFIRMED
                    or op.kind == OperationKind.CANCEL
                ):
                    continue
                futures_settled = (
                    op.kind == OperationKind.ORDER
                    or self._futures_transfer_settled(op, account, futures_issues, samples=samples)
                )
                settled = spot_settled and futures_settled
                if op.balance_pending == settled:
                    op = replace(op, balance_pending=not settled)
                    repo.update_operation(op)
                previous = tracked.get(op.client_id)
                if settled and previous is not None and not previous.is_open:
                    repo.retire_order(op.client_id)
        else:
            futures_issues["observation"] = {"reason": "Account or order observation is invalid"}
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
        self.history = (ZERO, None)
        if observation_valid and not any("since" in i for i in futures_issues.values()):
            rate, margin, issue = historical_metrics(
                repo, account, checked_at, cfg.quote_asset, samples=samples
            )
            if issue is not None:
                futures_issues["margin"] = issue
                consistent = False
            elif not pending.unresolved:
                self.history = rate, margin
        repo.save("futures_settlement_issue", futures_issues or None)
        self._save_income_coverage(
            coverage, futures_issues, observation_valid
            and valid_account_snapshot(account, self.clock() if self.clock else now, cfg)
        )
        if (observation_valid and not futures_issues and not pending.unresolved
                and self.executor.income_scan(self.clock() if self.clock else now) is not None):
            self.executor.qualify_income(account, self.clock() if self.clock else now)
        runtime = repo.runtime()
        if runtime.guard.active:
            repo.save(
                "runtime",
                replace(runtime, guard=self.guard_with_fills(runtime.guard, now)),
            )
        if consistent and observation_valid and reservation_consistent and not pending.unresolved:
            self._funding_orders = active + extra_after
        return account, active, pending, consistent

    def _save_income_coverage(self, coverage, futures_issues, observation_valid):
        repo = self.repository
        incomplete = [i["since"] for i in futures_issues.values() if "since" in i]
        if incomplete and observation_valid:
            # Retain the verified prefix; repair only the span whose proof failed.
            stored = verified = repo.load("futures_income_coverage")
            if verified is not None:
                until = min(
                    verified["until"],
                    *incomplete,
                )
                verified = (
                    {**verified, "until": until}
                    if until >= verified["since"] else None
                )
            if verified != stored:
                repo.save("futures_income_coverage", verified)
        elif (
            coverage is not None and observation_valid and not incomplete
        ):
            repo.save("futures_income_coverage", coverage)

    def _refresh_income(self, observations, account, *, force=False):
        """Reuse a finite cache; recheck old intervals for late publication."""
        repo = self.repository
        now = utc(self.clock() if self.clock else account.ts)
        windows = []
        for before, asset in observations:
            version = "contract_bnb_updated_at" if asset == "BNB" else "contract_quote_updated_at"
            lower = utc(getattr(before, version) or before.ts)
            upper = utc(getattr(account, version) or account.ts)
            if (
                lower <= utc(before.ts) + timedelta(seconds=1)
                and lower < upper <= utc(account.ts) + timedelta(seconds=1)
            ):
                windows.append((utc(before.ts) - timedelta(seconds=1), upper))
        for issue in (repo.load("futures_settlement_issue") or {}).values():
            if "since" in issue:
                version = "contract_bnb_updated_at" if issue.get("asset") == "BNB" else "contract_quote_updated_at"
                upper = utc(getattr(account, version) or account.ts)
                if utc(issue["since"]) <= upper <= utc(account.ts) + timedelta(seconds=1):
                    windows.append((utc(issue["since"]), upper))
        if not windows:
            token = self.executor.begin_income_scan(now)
            self.executor.complete_income_scan(
                token, {"since": None, "until": None, "checked_at": now}, account, now,
            )
            return None
        since, until = min(w[0] for w in windows), max(w[1] for w in windows)
        scan = self.executor.income_scan(now)
        same_scan = scan is not None and scan["since"] is not None and scan["since"] <= since
        coverage = scan if same_scan else repo.load("futures_income_coverage")
        checked = coverage.get("checked_at") if coverage is not None else None
        recheck = not same_scan and (
            force or checked is None or not timedelta(0) <= now - utc(checked) < INCOME_RECHECK_INTERVAL
        )
        if not recheck and coverage["since"] <= since <= until <= coverage["until"]:
            return ({key: coverage[key] for key in ("since", "until", "checked_at")}
                    if same_scan else None)
        start = since
        if not recheck and coverage["since"] <= since <= coverage["until"] <= until:
            start = coverage["until"]  # Inclusive overlap preserves timestamp ties.
        coverage = {"since": since, "until": until,
                    "checked_at": now if start == since else checked}
        if start <= until:
            token = self.executor.begin_income_scan(now)
            repo.save_futures_income(
                self._read(self.exchange.fetch_futures_income, start, until)
            )
            self.executor.complete_income_scan(
                token, coverage if start == since or same_scan else None, account,
                self.clock() if self.clock else account.ts,
            )
        return coverage

    def audit_income(self, account, now):
        """Refresh only income before a new batch; reuse the observed account."""
        repo, cfg = self.repository, self.cfg
        self.executor.invalidate_income_qualification()
        if account.contract_total_margin_balance is None:
            self._refresh_income([], account, force=True)
            self.executor.qualify_income(account, self.clock() if self.clock else now)
            return True
        samples = repo.snapshots_since(utc(now) - timedelta(hours=49))
        checkpoint = repo.load("spot_checkpoint")
        before = (replace(checkpoint["account"], wallet_event_cursor=checkpoint["cursor"])
                  if checkpoint is not None else None)
        baseline = margin_baseline(samples, now, warmup=True)
        observations = [(s, cfg.quote_asset) for s in (baseline, before) if s is not None]
        for snapshot, asset in tuple(observations):
            _, anchor, _ = futures_settlement_evidence(repo, snapshot, account, asset, samples=samples)
            if anchor is not snapshot:
                observations.append((anchor, asset))
        coverage = self._refresh_income(observations, account, force=True)
        issues = {}
        if before is not None:
            valid, _, evidence = futures_settlement_evidence(
                repo, before, account, cfg.quote_asset, samples=samples
            )
            policy = settlement_policy_issue(evidence)
            if not valid or policy is not None:
                issues["quote"] = evidence if not valid else policy
        rate, margin, issue = historical_metrics(repo, account, now, cfg.quote_asset, samples=samples)
        if issue is not None:
            issues["margin"] = issue
        valid = (
            valid_account_snapshot(account, self.clock() if self.clock else now, cfg)
            and not repo.operations(unresolved_only=True)
        )
        if not valid:
            issues["observation"] = {"reason": "Account or runtime changed during income audit"}
        repo.save("futures_settlement_issue", issues or None)
        self._save_income_coverage(coverage, issues, valid)
        self.history = (ZERO, None) if issues else (rate, margin)
        if not issues:
            self.executor.qualify_income(account, self.clock() if self.clock else now)
        return not issues

    def _settle_spot(self, account, *, checkpoint=None):
        """Advance one durable balance/ledger checkpoint only on exact coverage.

        Both spot assets must include all known executions. Request timestamps,
        repeated reads and elapsed time cannot prove wallet settlement.
        """
        repo, cfg = self.repository, self.cfg
        checkpoint = checkpoint or repo.load("spot_checkpoint")
        if checkpoint is None:
            repo.save(
                "spot_settlement_issue",
                {"reason": "No trusted spot baseline; stop external activity and run explicit baseline initialization"},
            )
            return False
        before = checkpoint["account"]
        through = account.wallet_event_cursor
        if through is None:
            through = repo.wallet_cursor()
        expected = {"BNB": before.spot_bnb, cfg.quote_asset: before.spot_usd}
        missing = []
        for _, reference, deltas in repo.wallet_changes(checkpoint["cursor"], through=through):
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
        previous = repo.load("spot_checkpoint")
        if previous is not None and any(
            getattr(previous["account"], field) != getattr(account, field)
            for field in ("contract_quote_updated_at", "contract_bnb_updated_at")
        ):
            # Retain the existing checkpoint for replay if its receipts arrive late.
            repo.save_snapshot(replace(
                previous["account"], wallet_event_cursor=previous["cursor"]
            ))
        repo.save(
            "spot_checkpoint", {"account": account, "cursor": through}
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

    def funding_occupation(self, account):
        """Positive, balance-covered external quote usage since this funding intent."""
        repo, cfg = self.repository, self.cfg
        funding = repo.current_funding(cfg.quote_asset, cfg.symbol)
        if funding is None:
            return None
        if (funding.kind != OperationKind.TRANSFER or funding.status != OperationStatus.CONFIRMED
                or funding.unresolved or funding.payload.asset != cfg.quote_asset
                or funding.payload.from_account != "USDⓈ-M Futures"
                or funding.payload.to_account != "SPOT"
                or funding.balance_before is None or funding.balance_fill_cursor is None
                or funding.balance_before.wallet_event_cursor is None
                or account.wallet_event_cursor is None):
            return None
        external = {
            f"trade:{f.symbol}:{f.trade_id}": f for f in repo.fills_after(funding.balance_fill_cursor)
            if f.strategy_id != cfg.strategy_id and utc(f.ts) >= utc(funding.created_at)
        }
        changes = [
            (reference, deltas[cfg.quote_asset])
            for _, reference, deltas in repo.wallet_changes(
                funding.balance_before.wallet_event_cursor, through=account.wallet_event_cursor)
            if reference in external and deltas.get(cfg.quote_asset) is not None
        ]
        spent = max(-sum((amount for _, amount in changes), ZERO), ZERO)
        frozen = ZERO
        frozen_orders = ()
        if self._funding_orders is not None:
            bids = tuple(o for o in self._funding_orders if o.side == "BUY")
            valid = all(o.price.is_finite() and o.price > 0 and o.qty.is_finite()
                        and o.filled_qty.is_finite() and ZERO <= o.filled_qty <= o.qty
                        and o.status in {"NEW", "PARTIALLY_FILLED"} and not o.cancel_pending
                        and utc(o.created_at) <= utc(account.ts) for o in bids)
            if valid and sum((o.remaining_qty * o.price for o in bids), ZERO) == account.reserved_spot_usd:
                frozen_orders = tuple(o for o in bids if o.strategy_id != cfg.strategy_id
                                      and utc(o.created_at) > utc(funding.created_at))
                frozen = sum((o.remaining_qty * o.price for o in frozen_orders), ZERO)
        before_free = max(funding.balance_before.spot_usd
                          - funding.balance_before.reserved_spot_usd, ZERO)
        current_free = max(account.spot_usd - account.reserved_spot_usd, ZERO)
        # Later credits can replace external spending. Historical receipts alone
        # cannot turn a subsequent price/lot-size obstacle into funding loss.
        missing = max(before_free + funding.payload.amount - current_free, ZERO)
        amount = min(funding.payload.amount, spent + frozen, missing)
        if amount <= 0:
            return None
        return {"funding_client_id": funding.client_id, "amount": amount,
                "quote_asset": cfg.quote_asset, "symbol": cfg.symbol,
                "spent": spent, "frozen": frozen, "trade_references": tuple(r for r, _ in changes),
                "orders": tuple((o.symbol, o.order_id) for o in frozen_orders),
                "wallet_cursor": account.wallet_event_cursor, "account_at": account.ts}

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
                ),
                transfer=result.transfer,
            )
            self.repository.event(
                now,
                "MANUAL_TRANSFER_BINDING",
                {"client_id": client_id, "transfer_id": transfer_id},
            )

    def _futures_transfer_settled(self, op, account, issues, *, samples=()):
        before, p, repo = op.balance_before, op.payload, self.repository
        if before is None or before.ts is None:
            issues[op.client_id] = {"reason": "Missing pre-transfer futures baseline"}
            return False
        valid, _, evidence = futures_settlement_evidence(
            repo, before, account, p.asset, samples=samples
        )
        receipt = repo.transfer_record(op.exchange_id)
        if (
            valid and evidence["from"] < evidence["through"]
            and receipt is not None and receipt.status == OperationStatus.CONFIRMED
            and (receipt.asset, receipt.amount, receipt.from_account, receipt.to_account)
            == (p.asset, p.amount, p.from_account, p.to_account)
            and receipt.transfer_id in evidence["transfer_ids"]
        ):
            return True
        issues[op.client_id] = evidence
        return False

    def initialize_spot_baseline(self, *, operator, reason, now):
        """Operator attests quiescence; bracket reads reject observable races.

        Equal balances alone are not proof of quiescence. This command only
        creates a first checkpoint, never repairs or overwrites an existing one.
        """
        if not operator.strip() or not reason.strip():
            raise ValueError("Baseline initialization requires operator and quiescence reason")
        repo, exchange, cfg = self.repository, self.exchange, self.cfg
        with repo.exclusive():
            if repo.load("spot_checkpoint") is not None or repo.operations(
                active_only=True
            ):
                raise ValueError("Initialization requires no checkpoint or active intents")
            before = self._read(exchange.fetch_account_snapshot)
            before_checked_at = self.clock() if self.clock else now
            self.refresh(self.clock() if self.clock else now)
            after = self._read(exchange.fetch_account_snapshot)
            end = self.clock() if self.clock else now
            if not (
                valid_account_snapshot(before, before_checked_at, cfg)
                and valid_account_snapshot(after, end, cfg)
            ):
                raise ValueError("Initialization requires fresh valid account snapshots")
            fields = ("spot_bnb", "spot_usd", "reserved_spot_bnb", "reserved_spot_usd")
            if (
                any(getattr(before, f) != getattr(after, f) for f in fields)
                or after.reserved_spot_bnb or after.reserved_spot_usd
            ):
                raise ValueError("Spot activity during initialization; restore quiescence first")
            start = utc(before.ts) - timedelta(seconds=1)
            # Catch receipts that appeared between the history cutoff and the
            # second balance read, including offsetting trades with zero net delta.
            for symbol in dict.fromkeys((cfg.symbol, *cfg.spot_activity_symbols)):
                if self._read(exchange.fetch_open_orders, symbol):
                    raise ValueError("Open orders prevent quiescent initialization")
                if self._read(exchange.fetch_recent_fills, symbol, start, end):
                    raise ValueError("Recent spot executions prevent quiescent initialization")
            transfers = self._read(exchange.fetch_recent_transfers, start, end)
            if any(t.asset in {"BNB", cfg.quote_asset} for t in transfers):
                raise ValueError("Recent transfers prevent quiescent initialization")
            if any(
                t.status in {OperationStatus.PENDING, OperationStatus.UNKNOWN}
                for t in repo.transfers_since(utc(now) - timedelta(hours=49))
            ):
                raise ValueError("Pending transfers prevent quiescent initialization")
            repo.initialize_spot_checkpoint(
                after, {
                    "operator": operator, "reason": reason,
                    "before": before, "after": after,
                    "symbols": (cfg.symbol, *cfg.spot_activity_symbols),
                },
            )
            return after
