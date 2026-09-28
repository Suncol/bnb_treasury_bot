from __future__ import annotations

from datetime import datetime
from typing import Protocol

from core.models import (
    AccountSnapshot,
    Fill,
    MarketSnapshot,
    Operation,
    OperationResult,
    OrderView,
    SymbolFilters,
    TransferRecord,
)


class ExchangeError(Exception):
    """Transport, protocol or execution uncertainty; never safe to resubmit."""


class RequestRejected(ExchangeError):
    """A documented, definitive rejection before execution."""


def order_matches_operation(order: OrderView, operation: Operation) -> bool:
    """A bound ID survives client-ID changes and keep-priority quantity reductions."""
    p = operation.payload
    if not order.qty.is_finite() or not order.filled_qty.is_finite():
        return False
    identity_matches = (
        order.order_id == operation.exchange_id
        if operation.exchange_id is not None
        else order.client_id == operation.client_id
    )
    quantity_matches = (
        0 < order.qty <= p.qty
        if operation.exchange_id is not None
        else order.qty == p.qty
    )
    return (
        identity_matches
        and quantity_matches
        and 0 <= order.filled_qty <= order.qty
        and (order.symbol, order.side, order.price) == (p.symbol, p.side, p.price)
    )


class ExchangeAdapter(Protocol):
    """Reads must be complete (paginate or raise), and mutations never auto-retry."""

    def fetch_account_snapshot(self) -> AccountSnapshot: ...
    def fetch_market_snapshot(self, symbol: str) -> MarketSnapshot: ...
    def fetch_symbol_filters(self, symbol: str) -> SymbolFilters: ...
    def fetch_open_orders(self, symbol: str) -> tuple[OrderView, ...]: ...
    def fetch_order(
        self, symbol: str, *, client_id: str | None = None, order_id: str | None = None
    ) -> OrderView | None: ...
    def fetch_recent_fills(
        self, symbol: str, since: datetime, until: datetime
    ) -> tuple[Fill, ...]: ...
    def fetch_recent_transfers(
        self, since: datetime, until: datetime
    ) -> tuple[TransferRecord, ...]: ...
    def submit(self, operation: Operation) -> OperationResult: ...
    def query_operation(self, operation: Operation) -> OperationResult: ...
