"""Quote-driven virtual exchange. Strategy, execution and reconciliation stay shared."""

import logging
import time
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN

from core.config import validate_paper_config
from core.math_utils import floor_to_multiple
from core.models import (ExecutionQuote, Fill, FuturesIncome, OperationKind, OperationResult,
                         OperationStatus, OrderView, PaperConfig, TransferRecord)
from core.order_planner import order_passes_filters
from core.time_utils import fresh, utc
from storage.paper_ledger import PaperLedger
from .exchange_adapter import ExchangeError, RequestDeferred, RequestRejected, order_matches_operation
from .spot_market import SpotMarketData

D = Decimal
ZERO = D("0")
BNB_UNIT = D("0.00000001")


class PaperExchange:
    def __init__(self, repository, cfg, paper_config=None, *, market_data=None, feed=None,
                 clock=None, monotonic=None, min_transfer_bnb=D("0.1"), min_transfer_usd=ZERO):
        self.repository, self.cfg = repository, cfg
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.monotonic = monotonic or time.monotonic
        self.paper_config = paper_config or PaperConfig()
        validate_paper_config(self.paper_config)
        if cfg.spot_activity_symbols:
            raise ValueError("Paper v1 supports one strategy symbol; spot_activity_symbols must be empty")
        if self.paper_config.spot_fee_rate > cfg.risk.fee_reserve_rate:
            raise ValueError("Configured fee reserve is below paper BUY commission")
        if any(not v.is_finite() or v < 0 for v in (min_transfer_bnb, min_transfer_usd)):
            raise ValueError("Transfer minima must be finite and nonnegative")
        self.min_transfer_bnb, self.min_transfer_usd = min_transfer_bnb, min_transfer_usd
        parameters = {"config": self.paper_config, "min_transfer_bnb": min_transfer_bnb,
                      "min_transfer_usd": min_transfer_usd}
        self.ledger = PaperLedger(repository, cfg, parameters, self.clock())
        self.market = market_data if market_data is not None else SpotMarketData(
            cfg, clock=self.clock, min_transfer_bnb=min_transfer_bnb, min_transfer_usd=min_transfer_usd,
            order_count=lambda: len(self.fetch_open_orders(cfg.symbol)),
        )
        if isinstance(self.market, SpotMarketData):
            self.market._order_count = lambda: len(self.fetch_open_orders(cfg.symbol))
        self.feed = feed
        self._filters = None
        self._last_monotonic = D(str(self.monotonic()))

    @property
    def feed(self):
        return self.market.feed

    @feed.setter
    def feed(self, value):
        self.market.feed = value

    @property
    def request_not_before(self):
        return getattr(self.market, "request_not_before", None)

    @request_not_before.setter
    def request_not_before(self, value):
        self.market.request_not_before = value

    @property
    def read_checkpoint(self):
        return getattr(self.market, "read_checkpoint", None)

    @read_checkpoint.setter
    def read_checkpoint(self, value):
        self.market.read_checkpoint = value

    def ensure_request_allowed(self, *, protective=False, scope=None, new_order=False,
                               request_weight=0):
        self.market.ensure_request_allowed(protective=protective, scope=scope,
                                           new_order=new_order, request_weight=request_weight)
        # Never invent future receipts to disambiguate an unadvanced test/system clock.
        if not protective:
            state = self.ledger.state()
            if utc(self.clock()) <= max(state["contract_bnb_updated_at"], state["contract_quote_updated_at"]):
                raise RequestDeferred("Paper clock must advance past committed asset evidence", retry_after=0.001)

    def account_identity(self):
        return self.ledger.identity()

    def start_price_refresh(self):
        if hasattr(self.market, "start_price_refresh"):
            self.market.start_price_refresh()

    def close_price_refresh(self):
        if hasattr(self.market, "close_price_refresh"):
            self.market.close_price_refresh()

    def fetch_market_snapshot(self, symbol):
        if symbol != self.cfg.symbol:
            raise ExchangeError("Unsupported paper symbol")
        if not self._valid_quote(self._current_quote(), utc(self.clock())):
            raise ExchangeError("No fresh valid execution quote for paper market data")
        self.market.check_clock()
        return self.market.fetch_market_snapshot(symbol)

    def fetch_symbol_filters(self, symbol):
        if symbol != self.cfg.symbol:
            raise ExchangeError("Unsupported paper symbol")
        self._filters = self.market.fetch_symbol_filters(symbol)
        return self._filters

    def fetch_account_snapshot(self):
        with self.repository.local_access():
            state = self.ledger.state()
            now = utc(self.clock())
            if now < state["advanced_at"]:
                raise ExchangeError("Paper UTC clock moved backwards")
            return self.ledger.snapshot(state, now)

    def fetch_open_orders(self, symbol):
        with self.repository.local_access():
            return tuple(r["order"] for r in self.ledger.orders(active=True) if r["order"].symbol == symbol)

    def fetch_order(self, symbol, *, client_id=None, order_id=None, protective=False):
        with self.repository.local_access():
            record = self.ledger.order(order_id=order_id, client_id=client_id)
            return record["order"] if record and record["order"].symbol == symbol else None

    def fetch_recent_fills(self, symbol, since, until):
        with self.repository.local_access():
            return self.ledger.fills(symbol, since, until)

    def fetch_recent_transfers(self, since, until):
        with self.repository.local_access():
            return self.ledger.transfers(since, until)

    def fetch_futures_income(self, since, until):
        with self.repository.local_access():
            return self.ledger.income(since, until)

    def _current_quote(self):
        if self.feed is None:
            return None
        try:
            return self.feed.execution_quote()
        except ExchangeError:
            return None  # Timers and accepted transfers still advance during a feed outage.

    def _valid_quote(self, quote, now):
        return (isinstance(quote, ExecutionQuote) and quote.symbol == self.cfg.symbol
                and fresh(quote.ts, now, self.cfg.crash_guard.max_market_age_seconds)
                and all(isinstance(v, D) and v.is_finite() and v > 0 for v in (quote.best_bid, quote.best_ask))
                and quote.best_bid <= quote.best_ask and isinstance(quote.ask_qty, D)
                and quote.ask_qty.is_finite() and quote.ask_qty >= 0)

    def advance(self):
        """Advance only observed quotes and running time. Local reads never call this."""
        with self.repository.exclusive():
            state = self.ledger.state()
            now, tick = utc(self.clock()), D(str(self.monotonic()))
            if now < state["advanced_at"] or tick < self._last_monotonic:
                raise ExchangeError("Paper clock moved backwards")
            quote = self._current_quote()
            elapsed = tick - self._last_monotonic
            state["active_seconds"] += elapsed
            state["advanced_at"] = now
            if self._filters is not None:
                state["qty_step"] = self._filters.qty_step
            valid = self._valid_quote(quote, now)
            newer = valid and (state["last_quote"] is None or utc(quote.ts) > utc(state["last_quote"].ts))
            previous_unmet = state["unmet_consumption_bnb"]
            if self.ledger.db.in_transaction:
                raise RuntimeError("Paper advancement requires a committed business intent")
            with self.ledger.db:
                self.ledger.db.execute("BEGIN IMMEDIATE")
                self._settle_transfers(state, now)
                self._consume(state, now)
                if newer and state["qty_step"] is not None:
                    state["last_quote"] = quote
                    state["quote_remaining"] = floor_to_multiple(
                        quote.ask_qty * self.paper_config.liquidity_participation, state["qty_step"])
                    orders = sorted(self.ledger.orders(active=True),
                                    key=lambda r: (-r["order"].price, r["order"].created_at, r["order"].order_id))
                    for record in orders:
                        if state["quote_remaining"] <= 0:
                            break
                        if utc(quote.ts) > utc(record["quote_at_acceptance"]) and quote.best_ask <= record["order"].price:
                            self._fill(state, record, record["order"].price, now)
                self.ledger.save_state(state)
            self._last_monotonic = tick
            if state["unmet_consumption_bnb"] > previous_unmet:
                logging.getLogger("bnb_treasury").warning(
                    "PAPER_BNB_DEPLETED: virtual BNB consumption unmet=%s", state["unmet_consumption_bnb"])

    def _fill(self, state, record, price, now):
        order = record["order"]
        qty = floor_to_multiple(min(order.remaining_qty, state["quote_remaining"]), state["qty_step"])
        if qty <= 0:
            return
        cost, fee = qty * price, qty * self.paper_config.spot_fee_rate
        state["spot_usd"] -= cost
        state["spot_bnb"] += qty - fee
        state["quote_remaining"] -= qty
        state["gross_bought_bnb"] += qty
        state["quote_spent"] += cost
        state["spot_fee_bnb"] += fee
        filled = order.filled_qty + qty
        record["order"] = replace(order, filled_qty=filled,
                                  status="FILLED" if filled == order.qty else "PARTIALLY_FILLED")
        self.ledger.save_order(record)
        self.ledger.save_fill(Fill(
            order.symbol, self.ledger.next_id(state, "trade"), order.order_id, now, qty,
            commission=fee, commission_asset="BNB", quote_qty=cost, quote_asset=self.cfg.quote_asset,
        ))

    def _settle_transfers(self, state, now):
        for record in self.ledger.pending_transfers():
            if state["active_seconds"] < record["due_seconds"]:
                continue
            t = record["receipt"]
            if t.asset == "BNB":
                state["spot_bnb"] -= t.amount
                state["contract_bnb"] += t.amount
                state["contract_bnb_updated_at"] = now
            else:
                amount = t.amount if t.to_account == "SPOT" else -t.amount
                state["spot_usd"] += amount
                state["contract_usd"] -= amount
                state["contract_quote_updated_at"] = now
            self.ledger.save_income(FuturesIncome(
                "TRANSFER", self.ledger.next_id(state, "income"), t.asset,
                t.amount if t.to_account == "USDⓈ-M Futures" else -t.amount, now,
            ))
            record["receipt"] = replace(t, status=OperationStatus.CONFIRMED)
            self.ledger.save_transfer(record)

    def _consume(self, state, now):
        through = (state["active_seconds"] / 60).to_integral_value(rounding=ROUND_DOWN) * 60
        seconds = through - state["consumed_seconds"]
        if seconds <= 0:
            return
        # Quantize cumulative accrual so recurring decimal division cannot lose a unit
        # at exact hour boundaries. Already-unmet consumption is never charged later.
        rate = self.paper_config.bnb_consumption_per_hour
        accrued = through * rate / 3600
        total_due = floor_to_multiple(accrued, BNB_UNIT)
        due = total_due - floor_to_multiple(state["consumed_seconds"] * rate / 3600, BNB_UNIT)
        actual = min(due, state["contract_bnb"])
        state["consumption_remainder"] = accrued - total_due
        state["consumed_seconds"] = through
        state["consumed_bnb"] += actual
        state["unmet_consumption_bnb"] += due - actual
        if actual > 0:
            state["contract_bnb"] -= actual
            state["contract_bnb_updated_at"] = now
            self.ledger.save_income(FuturesIncome(
                "COMMISSION", self.ledger.next_id(state, "income"), "BNB", -actual, now))

    def submit(self, operation):
        self.ensure_request_allowed(protective=operation.kind == OperationKind.CANCEL)
        # Public rules are fetched outside the financial transaction.
        filters = self.fetch_symbol_filters(operation.payload.symbol) if operation.kind == OperationKind.ORDER else None
        self.advance()
        with self.repository.exclusive():
            state = self.ledger.state()
            now = utc(self.clock())
            if self.ledger.db.in_transaction:
                raise RuntimeError("Paper submission requires a committed business intent")
            with self.ledger.db:
                self.ledger.db.execute("BEGIN IMMEDIATE")
                if operation.kind == OperationKind.ORDER:
                    result = self._submit_order(operation, state, now, filters)
                elif operation.kind == OperationKind.CANCEL:
                    result = self._cancel(operation)
                elif operation.kind == OperationKind.TRANSFER:
                    result = self._submit_transfer(operation, state, now)
                else:
                    raise RequestRejected("Unsupported paper operation")
                state["advanced_at"] = now
                self.ledger.save_state(state)
            return result

    def _submit_order(self, op, state, now, filters):
        p = op.payload
        if (p.symbol != self.cfg.symbol or p.side != "BUY"
                or not ((p.order_type == "LIMIT_MAKER" and p.time_in_force in {None, "GTC"})
                        or (p.order_type == "LIMIT" and p.time_in_force in {"GTC", "IOC"}))):
            raise RequestRejected("Paper supports BUY LIMIT_MAKER, LIMIT GTC and LIMIT IOC only")
        if not order_passes_filters(p, filters):
            raise RequestRejected("Paper order violates symbol filters")
        if filters.observed_at is not None and not fresh(filters.observed_at, now, self.cfg.crash_guard.max_market_age_seconds):
            raise ExchangeError("Paper order filters expired before acceptance")
        quote = self._current_quote()
        if not self._valid_quote(quote, now) or quote != state["last_quote"]:
            raise ExchangeError("No current consistent execution quote for paper order")
        if p.order_type == "LIMIT_MAKER" and p.price >= quote.best_ask:
            raise RequestRejected("Order would immediately match and take.")
        active = self.fetch_open_orders(p.symbol)
        if any(o.client_id == op.client_id for o in active):
            raise ExchangeError("Duplicate active client order identity")
        if ((filters.max_open_orders is not None and len(active) >= filters.max_open_orders)
                or (filters.exchange_order_slots is not None and filters.exchange_order_slots <= 0)):
            raise RequestRejected("Paper order capacity exceeded")
        account = self.ledger.snapshot(state, now)
        if p.qty * p.price > account.spot_usd - account.reserved_spot_usd:
            raise RequestRejected("Account has insufficient balance for requested action.")
        if filters.max_position is not None and account.spot_bnb + sum((o.remaining_qty for o in active), ZERO) + p.qty > filters.max_position:
            raise RequestRejected("Paper MAX_POSITION exceeded")
        order_id = self.ledger.next_id(state, "order")
        record = {"order": OrderView(p.symbol, order_id, op.client_id, None, p.price, p.qty, ZERO, now,
                                     time_in_force=p.time_in_force or "GTC"),
                  "order_type": p.order_type, "quote_at_acceptance": quote.ts}
        self.ledger.save_order(record)
        if p.order_type == "LIMIT" and p.price >= quote.best_ask:
            self._fill(state, record, quote.best_ask, now)
        if p.time_in_force == "IOC" and record["order"].status != "FILLED":
            record["order"] = replace(record["order"], status="EXPIRED")
            self.ledger.save_order(record)
        return OperationResult(OperationStatus.CONFIRMED, order_id)

    def _cancel(self, op):
        p = op.payload
        record = self.ledger.order(order_id=p.order_id)
        if record is None or record["order"].symbol != p.symbol:
            raise ExchangeError("Paper cancel order identity is unknown")
        if record["order"].is_open:
            record["order"] = replace(record["order"], status="CANCELED")
            self.ledger.save_order(record)
        return OperationResult(OperationStatus.CONFIRMED, p.order_id)

    def _submit_transfer(self, op, state, now):
        p = op.payload
        allowed = {(self.cfg.quote_asset, "USDⓈ-M Futures", "SPOT"),
                   (self.cfg.quote_asset, "SPOT", "USDⓈ-M Futures"), ("BNB", "SPOT", "USDⓈ-M Futures")}
        minimum = self.min_transfer_bnb if p.asset == "BNB" else self.min_transfer_usd
        if (p.asset, p.from_account, p.to_account) not in allowed or not p.amount.is_finite() or p.amount <= 0 or p.amount < minimum:
            raise RequestRejected("Unsupported or invalid paper transfer")
        account = self.ledger.snapshot(state, now)
        if p.from_account == "USDⓈ-M Futures":
            free = account.contract_max_withdraw_amount
        elif p.asset == "BNB":
            free = account.spot_bnb - account.reserved_spot_bnb
        else:
            free = account.spot_usd - account.reserved_spot_usd
        if p.amount > free:
            raise RequestRejected("Paper transfer exceeds source available balance")
        version = state["contract_bnb_updated_at"] if p.asset == "BNB" else state["contract_quote_updated_at"]
        if now <= version:
            raise RequestDeferred("Paper transfer needs a later UTC acceptance time", retry_after=0.001)
        transfer_id = self.ledger.next_id(state, "transfer")
        receipt = TransferRecord(transfer_id, p.asset, p.amount, p.from_account, p.to_account, now, OperationStatus.PENDING)
        self.ledger.save_transfer({"receipt": receipt, "due_seconds": state["active_seconds"] + self.paper_config.transfer_delay_seconds})
        # Each accepted call allocates a new tranId: there is no client-ID deduplication.
        return OperationResult(OperationStatus.PENDING, transfer_id, receipt)

    def query_operation(self, operation):
        if operation.kind in {OperationKind.ORDER, OperationKind.CANCEL}:
            if operation.kind == OperationKind.ORDER:
                order = self.fetch_order(operation.payload.symbol, order_id=operation.exchange_id,
                                         client_id=operation.client_id if operation.exchange_id is None else None)
            else:
                order = self.fetch_order(operation.payload.symbol, order_id=operation.payload.order_id)
            if order is None:
                return OperationResult(OperationStatus.UNKNOWN, operation.exchange_id)
            if operation.kind == OperationKind.ORDER and not order_matches_operation(order, operation):
                return OperationResult(OperationStatus.UNKNOWN, operation.exchange_id)
            if operation.kind == OperationKind.ORDER or not order.is_open:
                return OperationResult(OperationStatus.CONFIRMED, order.order_id)
            return OperationResult(OperationStatus.UNKNOWN, order.order_id)
        if operation.exchange_id is None:
            return OperationResult(OperationStatus.UNKNOWN)
        with self.repository.local_access():
            record = self.ledger.transfer(operation.exchange_id)
            if record:
                t, p = record["receipt"], operation.payload
                if (t.asset, t.amount, t.from_account, t.to_account) == (p.asset, p.amount, p.from_account, p.to_account):
                    return OperationResult(t.status, t.transfer_id, t)
        return OperationResult(OperationStatus.UNKNOWN, operation.exchange_id)
