from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

from core.models import CancelPlan, OperationKind, OperationStatus, PendingState
from core.time_utils import fresh, utc
from core.validation import valid_account_snapshot

from .exchange_adapter import ExchangeError, RequestDeferred, order_matches_operation
from .executor import ExecutionBlocked, Executor
from .evidence_queries import evidence_queries
from .qualification import reconciliation_status
from .history import (
    futures_settlement_evidence,
    INCOME_RECHECK_INTERVAL,
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
        self.history = (None, None)

    def prepare_execution(self, now, *, account=None):
        """Return a newly verified account and orders for the current write batch.

        Keep the shared executor's audit and priority read checkpoints. Callers
        must still check their candidate against the returned financial evidence.
        """
        # Always verify the account together with orders and the budget. A
        # caller-supplied balance is not a substitute for this observation.
        account, orders, pending, consistent = self.refresh(now, force_income=True)
        checked_at = self.clock() if self.clock else now
        if not (
            consistent and not pending.unresolved
            and valid_account_snapshot(account, checked_at, self.cfg)
            and (account.contract_total_margin_balance is None
                 or account.contract_margin_asset == self.cfg.quote_asset)
            and not reconciliation_status(self.repository).blocked
            and self.executor.execution_qualified(checked_at, account=account)
        ):
            self.executor.invalidate_income_audit()
            raise ExecutionBlocked("New operations require reconciled orders, settlement and a valid account")
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
                checker = getattr(self.exchange, "ensure_request_allowed", None)
                if order_id is not None and checker is not None:
                    # A first, already-bound cancellation can skip its optional
                    # query when GET + DELETE would consume the last capacity.
                    checker(protective=True, scope="spot", request_weight=5)
                identity = (
                    {"order_id": order_id} if order_id else {"client_id": op.client_id}
                )
                order = self.exchange.fetch_order(p.symbol, **identity, protective=True)
            except RequestDeferred:
                if (order_id is None or checker is None
                        or repo.cancel_operation(p.symbol, order_id) is not None):
                    raise
                # Recheck for the cheaper DELETE; real bans/full budgets still
                # fail here. UNKNOWN retries above always require a verified GET.
                checker(protective=True, scope="spot", request_weight=1)
                order = None
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
            with self.repository.exclusive():
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
        repo, exchange, cfg = self.repository, self.exchange, self.cfg
        preparation = self.executor.begin_preparation(now)
        strict_at_start = repo.settlement_window_open()
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
        for client_id, op in buys.items():
            previous = tracked.get(client_id) or repo.tracked_order(client_id)
            if op.status == OperationStatus.FAILED or (
                previous is not None and not previous.is_open and not op.balance_pending
            ):
                continue
            # Reopened terminal orders need their final cumulative quantity
            # verified by the same exchange identity before settling late fills.
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
        queries = evidence_queries(repo, cfg, now, strict_at_start=strict_at_start)
        since = queries.fills_since
        checkpoint = repo.load("spot_checkpoint")
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
        for symbol in queries.fill_symbols:
            for fill in self._read(exchange.fetch_recent_fills, symbol, since, now):
                fills.append(self._attribute_fill(fill))
        repo.save_fills(fills)
        late_owned = self._restore_late_owned_fills(fills)
        runtime = repo.runtime()  # Priority reads may have opened a new episode.
        if runtime.guard.episode_id and not runtime.guard.active:
            episode = repo.load("episode:" + runtime.guard.episode_id, runtime.guard)
            runtime = replace(
                runtime, guard=replace(runtime.guard, filled_bnb=episode.filled_bnb)
            )
            repo.save("runtime", runtime)
        recovery_since = queries.transfers_since
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
        samples = repo.settlement_snapshots() if queries.strict else ()
        quote_before = (
            replace(checkpoint["account"], wallet_event_cursor=checkpoint["cursor"])
            if checkpoint is not None else None
        )
        boundary = repo.load("spot_acceptance")
        account_valid = valid_account_snapshot(account, checked_at, cfg, previous=quote_before) and (
            boundary is None or utc(account.ts) >= utc(boundary["account"].ts)
        )  # Legacy checkpoints may already precede the accepted replay boundary.
        strict = strict_at_start or repo.settlement_window_open()
        window_before = quote_before
        if strict:
            anchors = [op.balance_before for op in repo.operations(active_only=True)
                       if op.balance_before is not None and op.balance_before.ts is not None]
            if quote_before is not None:
                anchors.append(quote_before)
            if anchors:
                window_before = min(anchors, key=lambda a: utc(a.ts))
        observations = [
            (op.balance_before, op.payload.asset) for op in pending_transfers
            if op.status == OperationStatus.CONFIRMED and op.balance_before is not None
            and op.balance_before.ts is not None
        ]
        if strict and account.contract_total_margin_balance is not None:
            observations.extend(
                (before, cfg.quote_asset) for before in (quote_before, window_before)
                if before is not None and account.contract_margin_asset == cfg.quote_asset
            )
        coverage = (
            self._refresh_income(
                observations, account,
                force=force_income and not self.executor.income_audited(checked_at),
            )
            if strict and account_valid else None
        )
        after = self._read(exchange.fetch_open_orders, cfg.symbol)
        def order_view(rows):
            return {(o.symbol, o.order_id): (o.client_id, o.side, o.price, o.qty,
                                            o.filled_qty, o.status, o.created_at)
                    for o in rows if o.is_open}
        consistent = not late_owned and order_view(orders.values()) == order_view(after)
        # Cumulative order fills must be covered by the deduplicated trade ledger.
        if tracked:
            totals = repo.fill_totals(
                cfg.symbol, (o.order_id for o in tracked.values())
            )
            if any(
                totals.get(o.order_id, ZERO) != o.filled_qty for o in tracked.values()
            ):
                consistent = False
        external_pending = any(
            t.status in {OperationStatus.PENDING, OperationStatus.UNKNOWN}
            and t.asset in {"BNB", cfg.quote_asset}
            for t in repo.pending_transfer_records()
        )
        spot_settled = False
        observation_valid = consistent and account_valid and valid_account_snapshot(
            account, self.clock() if self.clock else now, cfg
        ) and (account.contract_total_margin_balance is None
               or account.contract_margin_asset == cfg.quote_asset)
        futures_issues = {} if observation_valid else dict(
            repo.load("futures_settlement_issue") or {}
        )
        if observation_valid:
            quote_settled = True
            spot_before = checkpoint
            policy = None
            if strict and quote_before is not None and account.contract_total_margin_balance is not None:
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
                if quote_settled and policy is None and window_before is not quote_before:
                    window_valid, _, window_evidence = futures_settlement_evidence(
                        repo, window_before, account, cfg.quote_asset, samples=samples)
                    window_policy = settlement_policy_issue(window_evidence)
                    if not window_valid or window_policy is not None:
                        futures_issues["window"] = window_evidence if not window_valid else window_policy
                        quote_settled = False
            if quote_settled and policy is None and (checkpoint is not None or not external_pending):
                allow_external = not strict and not external_pending
                if allow_external and checkpoint is not None:
                    repo.accept_inventory_observation(account)
                    spot_settled = True
                else:
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
        # No historical prediction/diagnostic is required by fixed thresholds.
        self.history = (None, None)
        repo.save("futures_settlement_issue", futures_issues or None)
        self._save_income_coverage(
            coverage, futures_issues, observation_valid
            and valid_account_snapshot(account, self.clock() if self.clock else now, cfg)
        )
        if (observation_valid and not futures_issues and not pending.unresolved
                and (not strict or self.executor.income_scan(self.clock() if self.clock else now) is not None)):
            self.executor.qualify_execution(preparation, account, self.clock() if self.clock else now)
        runtime = repo.runtime()
        if runtime.guard.active:
            repo.save(
                "runtime",
                replace(runtime, guard=self.guard_with_fills(runtime.guard, now)),
            )
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
        for key, issue in (repo.load("futures_settlement_issue") or {}).items():
            if key != "margin" and "since" in issue:
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
        boundary = repo.load("spot_acceptance")
        if boundary is not None and (
            checkpoint["cursor"] < boundary["cursor"]
            or utc(checkpoint["account"].ts) < utc(boundary["account"].ts)
        ):
            # A futures replay anchor cannot rewind accepted spot inventory.
            checkpoint = repo.load("spot_checkpoint")
        before = checkpoint["account"]
        through = account.wallet_event_cursor
        if through is None:
            through = repo.wallet_cursor()
        ambiguous = repo.ambiguous_wallet_events(checkpoint["cursor"], through)
        if ambiguous:
            repo.save("spot_settlement_issue", {
                "reason": "Receipt may already be included in accepted inventory",
                "ambiguous_receipts": ambiguous,
            })
            return False
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

    def _restore_late_owned_fills(self, fills):
        """A retired order's contradictory receipt stays our responsibility."""
        repo = self.repository
        changed = False
        for order_id in {f.order_id for f in fills if f.strategy_id == self.cfg.strategy_id}:
            op = repo.order_operation(order_id)
            previous = repo.tracked_order(op.client_id) if op else None
            if op is None or previous is None or previous.is_open:
                continue
            total = repo.fill_totals(self.cfg.symbol, (order_id,)).get(order_id, ZERO)
            if total != previous.filled_qty:
                repo.update_operation(replace(op, balance_pending=True))
                changed = True
        return changed

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
        if op.settlement_epoch != repo.settlement_epoch():
            issues[op.client_id] = {"reason": "Operation precedes accepted inventory boundary"}
            return False
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
