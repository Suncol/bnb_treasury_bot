from dataclasses import replace
from datetime import datetime, timezone

from core.models import (
    Fill,
    OperationKind,
    OperationResult,
    OperationStatus,
    OrderView,
    TransferRecord,
)
from services.exchange_adapter import RequestRejected
from tests.helpers import D, make_account, make_filters, make_market


class FakeExchange:
    """Exchange state survives Runner/Repository restarts in recovery tests."""

    def __init__(self, account=None):
        self.now = datetime(2026, 3, 31, tzinfo=timezone.utc)
        self.account = account or make_account(
            contract_bnb=D("24"), contract_max_withdraw_amount=D("50000")
        )
        if self.account.contract_quote_balance is None:
            self.account = replace(
                self.account, contract_quote_balance=self.account.contract_max_withdraw_amount
            )
        self.orders, self.transfers, self.fills, self.writes = {}, {}, [], []
        self.hold_transfers = False
        self.lose_response = None
        self.reject_kind = None
        self.cancel_fill_qty = D("0")
        self.read_failure = False
        self.hide_fills = False

    def fetch_account_snapshot(self):
        if self.read_failure:
            raise TimeoutError()
        frozen = sum(
            (
                o.remaining_qty * o.price
                for o in self.orders.values()
                if o.side == "BUY"
            ),
            D("0"),
        )
        return replace(self.account, ts=self.now, reserved_spot_usd=frozen)

    def fetch_market_snapshot(self, symbol):
        return replace(make_market(), ts=self.now)

    def fetch_symbol_filters(self, symbol):
        return make_filters()

    def fetch_open_orders(self, symbol):
        return tuple(
            o for o in self.orders.values() if o.symbol == symbol and o.is_open
        )

    def fetch_order(self, symbol, *, client_id=None, order_id=None):
        return next(
            (
                o
                for o in self.orders.values()
                if o.symbol == symbol
                and (o.client_id == client_id or o.order_id == order_id)
            ),
            None,
        )

    def fetch_recent_fills(self, symbol, since, until):
        from core.time_utils import utc

        return (
            ()
            if self.hide_fills
            else tuple(f for f in self.fills if utc(since) <= utc(f.ts) <= utc(until))
        )

    def fetch_recent_transfers(self, since, until):
        return tuple(self.transfers.values())

    def fill(self, order_id, qty):
        o = self.orders[order_id]
        qty = min(qty, o.remaining_qty)
        filled = o.filled_qty + qty
        self.orders[order_id] = replace(
            o,
            filled_qty=filled,
            status="FILLED" if filled == o.qty else "PARTIALLY_FILLED",
        )
        self.account = replace(
            self.account,
            spot_bnb=self.account.spot_bnb + qty,
            spot_usd=self.account.spot_usd - qty * o.price,
        )
        self.fills.append(
            Fill(o.symbol, str(len(self.fills) + 1), order_id, self.now, qty, quote_qty=qty * o.price)
        )

    def settle(self, transfer_id):
        t = self.transfers[transfer_id]
        if t.status == OperationStatus.CONFIRMED:
            return
        if t.asset == "BNB":
            self.account = replace(
                self.account,
                spot_bnb=self.account.spot_bnb - t.amount,
                contract_bnb=self.account.contract_bnb + t.amount,
            )
        else:
            amount = t.amount if t.to_account == "SPOT" else -t.amount
            self.account = replace(
                self.account,
                spot_usd=self.account.spot_usd + amount,
                contract_max_withdraw_amount=self.account.contract_max_withdraw_amount
                - amount,
                contract_quote_balance=self.account.contract_quote_balance - amount,
            )
        self.transfers[transfer_id] = replace(t, status=OperationStatus.CONFIRMED)

    def submit(self, op):
        self.writes.append(op)
        if op.kind == self.reject_kind:
            raise RequestRejected("simulated definitive rejection")
        p = op.payload
        exchange_id = str(len(self.writes))
        if op.kind == OperationKind.ORDER:
            self.orders[exchange_id] = OrderView(
                p.symbol,
                exchange_id,
                op.client_id,
                None,
                p.price,
                p.qty,
                D("0"),
                self.now,
                state=op.state,
                time_in_force=p.time_in_force or "GTC",
            )
            if p.time_in_force == "IOC":
                self.fill(exchange_id, p.qty)
            result = OperationResult(OperationStatus.CONFIRMED, exchange_id)
        elif op.kind == OperationKind.CANCEL:
            order = self.orders[p.order_id]
            if self.cancel_fill_qty:
                self.fill(p.order_id, self.cancel_fill_qty)
                order = self.orders[p.order_id]
            self.orders[p.order_id] = replace(order, status="CANCELED")
            result = OperationResult(OperationStatus.CONFIRMED, p.order_id)
        else:
            self.transfers[exchange_id] = TransferRecord(
                exchange_id,
                p.asset,
                p.amount,
                p.from_account,
                p.to_account,
                self.now,
                OperationStatus.PENDING,
            )
            if not self.hold_transfers:
                self.settle(exchange_id)
            result = OperationResult(OperationStatus.PENDING, exchange_id)
        if self.lose_response == op.kind:
            self.lose_response = None
            raise TimeoutError("response lost after exchange accepted request")
        return result

    def query_operation(self, op):
        if op.kind == OperationKind.TRANSFER:
            if op.exchange_id is None or op.exchange_id not in self.transfers:
                return OperationResult(OperationStatus.UNKNOWN)
            return OperationResult(
                self.transfers[op.exchange_id].status, op.exchange_id
            )
        order = self.fetch_order(
            op.payload.symbol,
            client_id=op.client_id,
            order_id=getattr(op.payload, "order_id", None),
        )
        if order is not None and (op.kind == OperationKind.ORDER or not order.is_open):
            return OperationResult(OperationStatus.CONFIRMED, order.order_id)
        return OperationResult(OperationStatus.UNKNOWN)
