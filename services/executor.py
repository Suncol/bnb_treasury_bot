from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

from core.models import (
    AssetTransferPlan,
    CancelPlan,
    Operation,
    OperationKind,
    OperationStatus,
    OrderPlan,
    RunMode,
)
from .exchange_adapter import RequestRejected


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

    def execute(self, payload, now, state, *, starts_slice=False, account=None):
        kind, scope = operation_scope(payload)
        previous_runtime = self.repository.runtime()
        runtime = previous_runtime
        if kind != OperationKind.CANCEL:
            if runtime.run_mode == RunMode.PAUSED or self.repository.operations(
                unresolved_only=True
            ):
                raise RuntimeError(
                    "New operations require an unpaused, reconciled runtime"
                )
        if kind == OperationKind.TRANSFER and account is None:
            account = self.exchange.fetch_account_snapshot()
        op = Operation(
            "bt-" + uuid4().hex, kind, scope, payload, now, state=state,
            balance_before=account if kind == OperationKind.TRANSFER else None,
            balance_fill_cursor=self.repository.fill_cursor()
            if kind == OperationKind.TRANSFER else None,
            balance_pending=kind == OperationKind.TRANSFER,
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
            runtime = replace(runtime, resume_replenishment=True)
        elif kind == OperationKind.CANCEL and payload.replenish_after_cancel:
            runtime = replace(runtime, resume_repricing=True)
        elif kind == OperationKind.ORDER:
            runtime = replace(
                runtime, resume_replenishment=False, resume_repricing=False
            )
        self.repository.record_intent(op, runtime)
        try:
            result = self.exchange.submit(op)
            op = replace(
                op, status=result.status, exchange_id=result.exchange_id, checked_at=now
            )
        except RequestRejected as exc:
            op = replace(
                op, status=OperationStatus.FAILED, error=str(exc), checked_at=now
            )
        except Exception as exc:
            # This includes response decoding failures after a successful write.
            op = replace(
                op,
                status=OperationStatus.UNKNOWN,
                error=type(exc).__name__,
                checked_at=now,
            )
        runtime = None
        if op.status == OperationStatus.FAILED and kind == OperationKind.ORDER:
            # Only a definite rejection restores the unstarted continuation.
            # Commit it with FAILED so a restart cannot lose the retry/fund reserve.
            runtime = replace(
                self.repository.runtime(),
                resume_replenishment=previous_runtime.resume_replenishment,
                resume_repricing=previous_runtime.resume_repricing,
            )
        op = replace(op, checked_at=self.clock() if self.clock else now)
        return self.record_result(op, runtime=runtime, submission=True)

    def record_result(self, op, *, runtime=None, submission=False):
        """Commit a result and its continuation/circuit-breaker effects together."""
        runtime = runtime or self.repository.runtime()
        if op.status == OperationStatus.FAILED and op.kind == OperationKind.TRANSFER:
            if op.payload.to_account == "SPOT":
                runtime = replace(runtime, resume_replenishment=False)
        if (
            op.status == OperationStatus.FAILED
            or (submission and op.status == OperationStatus.UNKNOWN)
        ) and not op.failure_recorded:
            runtime = self._failure_runtime(
                runtime, transfer=op.kind == OperationKind.TRANSFER,
                reason=f"{op.kind.value}: {op.status.value}",
            )
            op = replace(op, failure_recorded=True)
        elif op.status == OperationStatus.CONFIRMED:
            runtime = replace(
                runtime,
                api_errors=0 if submission else runtime.api_errors,
                transfer_failures=0
                if op.kind == OperationKind.TRANSFER else runtime.transfer_failures,
            )
        self.repository.update_operation(op, runtime)
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
