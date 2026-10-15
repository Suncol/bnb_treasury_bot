from dataclasses import replace
from contextlib import contextmanager
from datetime import timedelta
from uuid import uuid4

from core.models import (
    AssetTransferPlan,
    CancelPlan,
    Operation,
    OperationKind,
    OperationStatus,
    OrderPlan,
    ReplenishmentState,
    RunMode,
)
from core.risk_checks import evaluate_transfer_gate
from core.time_utils import fresh, utc
from core.validation import valid_account_snapshot

from .exchange_adapter import (
    ExchangeError, RequestNotSent, RequestRejected, order_matches_operation,
)

MAX_CANCEL_ATTEMPTS = 5


class ExecutionBlocked(RuntimeError):
    """An unsent candidate needs new planning, not an API failure retry."""

    def __init__(self, reason, *, decision=None):
        super().__init__(reason)
        self.decision = decision


def operation_scope(payload):
    if isinstance(payload, OrderPlan):
        return OperationKind.ORDER, f"order:{payload.symbol}"
    if isinstance(payload, CancelPlan):
        return OperationKind.CANCEL, f"cancel:{payload.symbol}:{payload.order_id}"
    if isinstance(payload, AssetTransferPlan):
        return (
            OperationKind.TRANSFER,
            f"transfer:{payload.asset}:{payload.from_account}:{payload.to_account}",
        )
    raise TypeError("Unsupported execution action")


class Executor:
    def __init__(self, exchange, repository, cfg, *, clock=None):
        self.exchange, self.repository, self.cfg = exchange, repository, cfg
        self.clock = clock or getattr(exchange, "clock", None)
        self._income_batch = False
        self._income_audited_at = None
        self.reconciler = None

    def _reconciliation(self):
        if self.reconciler is None:
            from .reconciliation import Reconciler

            Reconciler(self.exchange, self.repository, self.cfg, clock=self.clock,
                       executor=self)
        return self.reconciler

    @contextmanager
    def income_batch(self):
        self._income_batch, self._income_audited_at = True, None
        try:
            yield
        finally:
            self._income_batch, self._income_audited_at = False, None

    def income_audited(self, now):
        return self._income_batch and self._income_audited_at is not None and fresh(
            self._income_audited_at, now, self.cfg.risk.max_account_age_seconds
        )

    def cancel_due(self, payload, now):
        op = self.repository.cancel_operation(payload.symbol, payload.order_id)
        return op is None or (
            op.unresolved
            and op.cancel_attempts < MAX_CANCEL_ATTEMPTS
            and utc(now) >= utc(op.cancel_retry_at or op.created_at + timedelta(seconds=2))
        )

    def _require_reconciled(self):
        runtime = self.repository.runtime()
        if (
            runtime.run_mode == RunMode.PAUSED
            or self.repository.operations(unresolved_only=True)
            or self.repository.load("spot_settlement_issue")
            or self.repository.load("futures_settlement_issue")
            or self.repository.load("spot_checkpoint") is None
        ):
            raise RuntimeError("New operations require an unpaused, reconciled runtime")
        return runtime

    def execute(self, payload, now, state, *, starts_slice=False, account=None,
                resume_replanning=False, inputs=None):
        kind, scope = operation_scope(payload)
        if kind == OperationKind.CANCEL:
            existing = self.repository.cancel_operation(
                payload.symbol, payload.order_id
            )
            if existing is not None:
                return self._recover_cancel(existing, now)
        checker = getattr(self.exchange, "ensure_request_allowed", None)
        if checker is not None:
            checker(
                protective=kind == OperationKind.CANCEL
            )  # No unsent UNKNOWN intent.
        previous_runtime = (
            self.repository.runtime() if kind == OperationKind.CANCEL else self._require_reconciled()
        )
        if (kind == OperationKind.TRANSFER and payload.to_account == "SPOT"
                and previous_runtime.funding_exhausted_at is not None):
            raise RuntimeError("Funding must wait for a later valid inventory cycle")
        runtime = previous_runtime
        if kind != OperationKind.CANCEL and account is None:
            account = self.exchange.fetch_account_snapshot()
        if kind != OperationKind.CANCEL and (
            not valid_account_snapshot(account, self.clock() if self.clock else now, self.cfg)
            or (
                account.contract_total_margin_balance is not None
                and account.contract_margin_asset != self.cfg.quote_asset
            )
        ):
            raise RuntimeError("New operations require a valid, supported account observation")
        if account is not None and account.wallet_event_cursor is None:
            account = replace(account, wallet_event_cursor=self.repository.wallet_cursor())
        funding = kind == OperationKind.TRANSFER and (
            payload.asset, payload.from_account, payload.to_account
        ) == (self.cfg.quote_asset, "USDⓈ-M Futures", "SPOT")
        if funding:
            account, now = self._prepare_funding(payload, account, runtime, now, inputs)
            previous_runtime = runtime = self.repository.runtime()
        elif kind != OperationKind.CANCEL and not self.income_audited(self.clock() if self.clock else now):
            # Direct callers have no batch audit; reuse the supplied balance and
            # the same reconciliation/storage code without another account read.
            reconciler = self._reconciliation()
            if not reconciler.audit_income(account, self.clock() if self.clock else account.ts):
                raise RuntimeError("New operations require reconciled futures income")
            self._income_audited_at = account.ts
            previous_runtime = runtime = self._require_reconciled()
        op = Operation(
            "bt-" + uuid4().hex,
            kind,
            scope,
            payload,
            now,
            state=state,
            balance_before=account if kind != OperationKind.CANCEL else None,
            balance_fill_cursor=self.repository.fill_cursor()
            if kind != OperationKind.CANCEL
            else None,
            balance_pending=kind != OperationKind.CANCEL,
        )
        if starts_slice:
            runtime = replace(
                runtime,
                slice_state=replace(
                    runtime.slice_state,
                    active=True,
                    next_at=now
                    + timedelta(seconds=self.cfg.execution.slice_interval_seconds),
                ),
            )
        if kind == OperationKind.TRANSFER and payload.to_account == "SPOT":
            runtime = replace(runtime, resume_replenishment=True,
                              funding_client_id=op.client_id)
        elif (
            kind == OperationKind.CANCEL and payload.replenish_after_cancel
            or kind == OperationKind.TRANSFER and payload.asset == "BNB" and resume_replanning
        ):
            runtime = replace(runtime, resume_replanning=True)
        elif kind == OperationKind.ORDER:
            runtime = replace(
                runtime, resume_replenishment=False, resume_replanning=False,
                funding_client_id=None,
            )
        self.repository.record_intent(op, runtime)
        if kind == OperationKind.CANCEL:
            self.repository.event(
                now,
                "CANCEL_INTENT",
                {"client_id": op.client_id, "order_id": payload.order_id},
            )
        return self._submit(op, now, previous_runtime, new_intent=True)

    def _prepare_funding(self, payload, original, runtime, now, inputs):
        # Read configuration before final account/history preparation. No slow
        # exchange work may be inserted between the final gate and the intent.
        minimum = getattr(self.exchange, "min_transfer_usd", None)
        if minimum is None:
            minimum = self.exchange.fetch_symbol_filters(self.cfg.symbol).min_transfer_usd
        account, orders = self._reconciliation().prepare_execution(
            self.clock() if self.clock else now
        )
        now = self.clock() if self.clock else now
        if not valid_account_snapshot(account, now, self.cfg):
            raise ExecutionBlocked("Final account observation expired before funding submission")
        current = self.repository.runtime()
        if current.run_mode == RunMode.PAUSED or (
            current.run_mode == RunMode.URGENT_ONLY
            and current.state != ReplenishmentState.URGENT
        ):
            raise ExecutionBlocked("USD funding is blocked by the current run mode")
        guard_fields = ("active", "episode_id", "keep_price_ceiling", "filled_bnb")
        if any(getattr(runtime.guard, field) != getattr(current.guard, field)
               for field in guard_fields):
            raise ExecutionBlocked("Risk protection changed during funding preparation")
        if inputs is not None and (
            not fresh(inputs.market.ts, now, self.cfg.crash_guard.max_market_age_seconds)
            or inputs.filters.observed_at is not None and not fresh(
                inputs.filters.observed_at, now, self.cfg.crash_guard.max_market_age_seconds
            )
        ):
            raise ExecutionBlocked("Market or filter observation expired during funding preparation")
        decision = evaluate_transfer_gate(
            payload.amount, account.contract_max_withdraw_amount,
            self.repository.budget_used(now, self.cfg.quote_asset), self.cfg.risk, minimum,
        )
        if not decision.allow:
            raise ExecutionBlocked(decision.message, decision=decision)
        if decision.adjusted_amount != payload.amount:
            raise ExecutionBlocked("Transfer amount requires replanning: " + decision.message,
                                   decision=decision)
        # Fresh balances/orders can make the old funding demand obsolete even
        # when the monetary limits still allow it. Let normal planning decide.
        fields = ("contract_bnb", "spot_bnb", "spot_usd", "reserved_spot_bnb", "reserved_spot_usd")
        if any(getattr(original, field) != getattr(account, field) for field in fields) or (
            inputs is not None and {o.order_id: o for o in inputs.orders}
            != {o.order_id: o for o in orders}
        ):
            raise ExecutionBlocked("Funding balances or orders changed; replan the candidate")
        return account, now

    def _recover_cancel(self, op, now):
        if not self.cancel_due(op.payload, now):
            return op
        checker = getattr(self.exchange, "ensure_request_allowed", None)
        if checker is not None:
            checker(protective=True)
        # Throttle queries too, including missing/stale terminal evidence. Never
        # turn a missing order or an unverified owner into a new cancel attempt.
        op = replace(op, cancel_retry_at=now + timedelta(seconds=5))
        self.repository.update_operation(op)
        owner = self.repository.order_operation(op.payload.order_id)
        if owner is None or owner.status == OperationStatus.FAILED:
            return op
        try:
            order = self.exchange.fetch_order(
                op.payload.symbol, order_id=op.payload.order_id
            )
        except Exception as exc:
            checked_at = self.clock() if self.clock else now
            delay = max(5, float(getattr(exc, "retry_after", None) or 0))
            self.repository.update_operation(
                replace(
                    op, cancel_retry_at=checked_at + timedelta(seconds=delay),
                    checked_at=checked_at, error=type(exc).__name__,
                )
            )
            raise
        if order is None:
            return op
        if not order_matches_operation(order, owner):
            raise ExchangeError("Cancel recovery found a conflicting order identity")
        if not order.is_open:
            return self.record_result(
                replace(
                    op, status=OperationStatus.CONFIRMED,
                    exchange_id=order.order_id,
                    checked_at=self.clock() if self.clock else now,
                )
            )
        if order.status not in {"NEW", "PARTIALLY_FILLED"}:
            return op
        return self._submit(
            op, self.clock() if self.clock else now, self.repository.runtime()
        )

    def _submit(self, op, now, previous_runtime, *, new_intent=False):
        kind = op.kind
        previous_op = op
        if kind == OperationKind.CANCEL:
            attempts = op.cancel_attempts + 1
            op = replace(
                op, cancel_attempts=attempts,
                cancel_retry_at=now + timedelta(seconds=min(60, 2 ** attempts)),
            )
            # Intent, deadline and attempt number commit before the network.
            self.repository.update_operation(
                op, event=(now, "CANCEL_ATTEMPT_RESERVED", {
                    "client_id": op.client_id, "order_id": op.payload.order_id,
                    "attempt": attempts,
                }),
            )
        cooldown = None
        transfer = None
        not_sent = False
        try:
            result = self.exchange.submit(op)
            transfer = result.transfer
            op = replace(
                op, status=result.status, exchange_id=result.exchange_id, checked_at=now
            )
        except RequestNotSent as exc:
            not_sent = True
            cooldown = (self.clock() if self.clock else now) + timedelta(
                seconds=max(1, float(exc.retry_after or 1))
            )
            # Never infer that a durable PENDING/UNKNOWN from an earlier
            # attempt was unsent. Only this call's newly created intent can
            # become terminal; recovered cancels keep their original evidence.
            op = replace(
                op,
                status=OperationStatus.FAILED if new_intent else previous_op.status,
                not_sent=True if new_intent else previous_op.not_sent,
                balance_pending=False if new_intent else previous_op.balance_pending,
                cancel_attempts=previous_op.cancel_attempts,
                error=f"Not sent: {exc}",
            )
        except RequestRejected as exc:
            op = replace(
                op, status=OperationStatus.FAILED, error=str(exc), checked_at=now
            )
        except Exception as exc:
            retry_after = getattr(exc, "retry_after", None)
            if retry_after is not None:
                cooldown = (self.clock() if self.clock else now) + timedelta(
                    seconds=max(1, float(retry_after))
                )
            # This includes response decoding failures after a successful write.
            op = replace(
                op,
                status=OperationStatus.UNKNOWN,
                error=type(exc).__name__,
                checked_at=now,
            )
        runtime = None
        if (not_sent and new_intent) or (
            op.status == OperationStatus.FAILED and kind == OperationKind.ORDER
        ):
            # A definite rejection or a proven unsent new intent restores the
            # unstarted continuation together with its terminal result.
            runtime = replace(
                self.repository.runtime(),
                resume_replenishment=previous_runtime.resume_replenishment,
                resume_replanning=previous_runtime.resume_replanning,
                funding_client_id=previous_runtime.funding_client_id,
                funding_exhausted_at=previous_runtime.funding_exhausted_at,
            )
        if cooldown is not None:
            runtime = runtime or self.repository.runtime()
            runtime = replace(
                runtime, read_retry_at=max(runtime.read_retry_at or cooldown, cooldown)
            )
            if kind == OperationKind.CANCEL:
                op = replace(op, cancel_retry_at=max(op.cancel_retry_at, cooldown))
        op = replace(op, checked_at=self.clock() if self.clock else now)
        return self.record_result(
            op, runtime=runtime, submission=not not_sent, transfer=transfer,
            not_sent=not_sent,
        )

    def record_result(self, op, *, runtime=None, submission=False, transfer=None,
                      not_sent=False):
        """Commit a result and its continuation/circuit-breaker effects together."""
        runtime = runtime or self.repository.runtime()
        if (op.status == OperationStatus.FAILED and not op.not_sent
                and op.kind == OperationKind.TRANSFER):
            if op.payload.to_account == "SPOT":
                runtime = replace(runtime, resume_replenishment=False, funding_client_id=None)
        if (
            op.status == OperationStatus.FAILED
            or (submission and op.status == OperationStatus.UNKNOWN)
        ) and not op.failure_recorded and not op.not_sent:
            runtime = self._failure_runtime(
                runtime,
                transfer=op.kind == OperationKind.TRANSFER,
                reason=f"{op.kind.value}: {op.status.value}",
            )
            op = replace(op, failure_recorded=True)
        elif op.status == OperationStatus.CONFIRMED:
            runtime = replace(
                runtime,
                # Protection can succeed while the account observation is still
                # invalid. Only normal reconciliation or a new financial write
                # demonstrates recovery; cancelling risk must not hide failures.
                api_errors=0
                if submission and op.kind != OperationKind.CANCEL
                else runtime.api_errors,
                transfer_failures=0
                if op.kind == OperationKind.TRANSFER
                else runtime.transfer_failures,
            )
        event = None
        events = ()
        if op.kind == OperationKind.CANCEL:
            event = (
                op.checked_at,
                "CANCEL_NOT_SENT" if not_sent else
                "CANCEL_TERMINAL" if op.status == OperationStatus.CONFIRMED else "CANCEL_RESULT",
                {
                    "client_id": op.client_id, "order_id": op.payload.order_id,
                    "attempt": op.cancel_attempts, "status": op.status.value,
                    "error": op.error,
                },
            )
            if submission:
                events = ((op.checked_at, "CANCEL_SENT", {
                    "client_id": op.client_id, "order_id": op.payload.order_id,
                    "attempt": op.cancel_attempts,
                }),)
        self.repository.update_operation(
            op, runtime, transfer=transfer, event=event, events=events,
        )
        return op

    @staticmethod
    def _failure_runtime(runtime, *, transfer=False, reason="API failure"):
        errors = runtime.api_errors + 1
        failures = runtime.transfer_failures + int(transfer)
        pause = errors >= 3 or failures >= 2
        return replace(
            runtime,
            api_errors=errors,
            transfer_failures=failures,
            run_mode=RunMode.PAUSED if pause else runtime.run_mode,
            pause_reason=reason if pause else runtime.pause_reason,
        )

    def note_failure(self, *, transfer=False, reason="API failure"):
        self.repository.save(
            "runtime",
            self._failure_runtime(
                self.repository.runtime(), transfer=transfer, reason=reason
            ),
        )
