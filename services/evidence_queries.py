"""Historical reads follow current responsibility and the account budget."""
from dataclasses import dataclass
from datetime import datetime, timedelta

from core.time_utils import utc


@dataclass(frozen=True)
class EvidenceQueries:
    fill_symbols: tuple
    fills_since: datetime | None
    transfers_since: datetime
    strict: bool


def evidence_queries(repository, cfg, now, *, strict_at_start=False):
    operations = repository.operations(active_only=True)
    strict = strict_at_start or bool(operations)
    checkpoint = repository.load('spot_checkpoint')
    boundary = repository.load('spot_acceptance')
    lower = utc(boundary['account'].ts) if boundary else None
    anchors = [utc(op.balance_before.ts) if op.balance_before is not None and op.balance_before.ts is not None
               else utc(op.created_at) for op in operations]
    if strict and checkpoint is not None:
        anchors.append(utc(checkpoint['account'].ts))
    if lower is not None:
        anchors = [max(at, lower) for at in anchors]
    since = min(anchors) - timedelta(seconds=1) if anchors else None
    # A persisted unrelated 24h metric never widens these requirements.
    symbols = tuple(dict.fromkeys((cfg.symbol, *cfg.spot_activity_symbols))) if strict else ()
    if not strict and repository.has_order_journal():
        # Watch recent receipts for retired owned-order contradictions. This
        # bounded overlap is not an audit of external account provenance.
        since = utc(now) - timedelta(seconds=60)
        symbols = (cfg.symbol,)
    transfer_since = utc(now) - timedelta(hours=24)
    if strict and since is not None:
        transfer_since = min(transfer_since, since)
    outstanding = repository.pending_transfer_records()
    if outstanding:
        transfer_since = min(transfer_since, min(utc(t.ts) for t in outstanding)-timedelta(seconds=1))
    return EvidenceQueries(symbols, since, transfer_since, strict)
