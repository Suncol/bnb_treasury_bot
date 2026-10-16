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
    accepted_inventory: dict | None
    observation: dict | None
    spot_settlement: dict | None
    futures_settlement: dict
    historical_diagnostics: dict | None

    @property
    def blocked(self):
        # Compatibility policy: all former blockers still apply in phase 01.
        return bool(not self.initialized or self.own_operations or self.observation
                    or self.spot_settlement or self.futures_settlement
                    or self.historical_diagnostics)

    def describe(self):
        return asdict(self)


def reconciliation_status(repository):
    issues = dict(repository.load("futures_settlement_issue") or {})
    return ReconciliationStatus(
        initialized=repository.load("spot_checkpoint") is not None,
        strict_window=repository.settlement_window_open(),
        accepted_inventory=repository.load("spot_acceptance"),
        own_operations=tuple({"client_id": op.client_id, "kind": op.kind.value,
                              "status": op.status.value, "balance_pending": op.balance_pending}
                             for op in repository.operations(unresolved_only=True)),
        observation=issues.pop("observation", None),
        spot_settlement=repository.load("spot_settlement_issue"),
        historical_diagnostics=issues.pop("margin", None),
        futures_settlement=issues,
    )
