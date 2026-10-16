"""One interpretation of durable reconciliation blockers for all write paths.

The result describes evidence, not an execution token. Freshness, mode and batch
invalidation remain the executor's responsibility.
"""

from dataclasses import dataclass, asdict


@dataclass(frozen=True)
class ReconciliationStatus:
    initialized: bool
    own_operations: tuple
    strict_window: bool
    responsibilities: tuple
    accepted_inventory: dict | None
    observation: dict | None
    spot_settlement: dict | None
    futures_settlement: dict
    historical_diagnostics: dict | None

    @property
    def blocked(self):
        return bool(not self.initialized or self.own_operations or self.observation
                    or self.spot_settlement or self.futures_settlement)

    def describe(self):
        return {**asdict(self), "observation_mode": (
            "uninitialized" if not self.initialized else
            "strict_window" if self.strict_window else "idle_observation"
        )}



def reconciliation_status(repository):
    issues = dict(repository.load("futures_settlement_issue") or {})
    return ReconciliationStatus(
        initialized=repository.load("spot_checkpoint") is not None,
        strict_window=repository.settlement_window_open(),
        responsibilities=tuple({
            "client_id": op.client_id, "kind": op.kind.value, "status": op.status.value,
            "exchange_id": op.exchange_id, "balance_pending": op.balance_pending,
            "settlement_epoch": op.settlement_epoch,
            "baseline_at": op.balance_before.ts if op.balance_before else None,
            "missing_recovery_evidence": tuple(reason for condition, reason in (
                (op.kind.value == "TRANSFER" and op.exchange_id is None,
                 "transfer_identity"),
                (op.kind.value == "TRANSFER" and (op.balance_before is None or op.balance_before.ts is None),
                 "pre_transfer_baseline"),
            ) if condition),
        } for op in repository.operations(active_only=True)),
        accepted_inventory=repository.load("spot_acceptance"),
        own_operations=tuple({"client_id": op.client_id, "kind": op.kind.value,
                              "status": op.status.value, "balance_pending": op.balance_pending}
                             for op in repository.operations(unresolved_only=True)),
        observation=issues.pop("observation", None),
        spot_settlement=repository.load("spot_settlement_issue"),
        historical_diagnostics=issues.pop("margin", None),
        futures_settlement=issues,
    )
