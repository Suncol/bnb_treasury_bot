"""Durable simulated-exchange state, separate from the controller's evidence."""

from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

from core.models import AccountSnapshot
from core.time_utils import utc
from .codec import dumps, loads

PAPER_ENVIRONMENT = "binance-paper-usds-m-v1"
PAPER_TABLES = {"paper_state", "paper_orders", "paper_fills", "paper_transfers", "paper_income"}
ZERO = Decimal("0")


def check_environment(repository, *, paper):
    identity = repository.load("account_identity")
    environment = identity.get("environment") if identity else None
    tables = {row[0] for row in repository.db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if paper:
        if identity is not None and environment != PAPER_ENVIRONMENT:
            raise ValueError("Paper cannot reuse a real or unidentified account database")
    elif (environment is not None and environment != "binance-production-usds-m") or tables & PAPER_TABLES:
        raise ValueError("Real-account mode cannot reuse a paper or incompatible environment database")


class PaperLedger:
    def __init__(self, repository, cfg, parameters, now=None):
        self.repository, self.db = repository, repository.db
        self.cfg, self.parameters = cfg, parameters
        if now is None:
            return  # A status view reads an already-validated committed account.
        repository.acquire_controller()
        with repository.exclusive():
            check_environment(repository, paper=True)
            tables = {r[0] for r in self.db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if tables & PAPER_TABLES:
                if not PAPER_TABLES <= tables:
                    raise ValueError("Incomplete paper ledger; preserve the database for inspection")
                state = self.state()
                if state["model_version"] != 1 or state["parameters"] != parameters:
                    raise ValueError("Paper model/parameters changed; use a new paper database")
                if repository.load("account_identity") != self.identity(state) or repository.load("spot_checkpoint") is None:
                    raise ValueError("Paper identity or trusted baseline is missing or incompatible")
            else:
                business_tables = ("kv", "operations", "fills", "transfers", "snapshots", "events",
                                   "outbox", "orders", "episodes", "wallet_events", "futures_income")
                if any(self.db.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone() for table in business_tables):
                    raise ValueError("Paper bootstrap requires a new empty journal")
                self._bootstrap(utc(now))

    def _bootstrap(self, now):
        p = self.parameters["config"]
        state = {
            "model_version": 1, "account_id": "paper-" + uuid4().hex,
            "parameters": self.parameters, "advanced_at": now,
            "contract_usd": p.initial_contract_usd, "contract_bnb": p.initial_contract_bnb,
            "spot_usd": p.initial_spot_usd, "spot_bnb": p.initial_spot_bnb,
            "contract_quote_updated_at": now - timedelta(microseconds=1),
            "contract_bnb_updated_at": now - timedelta(microseconds=1),
            "active_seconds": ZERO, "consumed_seconds": ZERO, "consumption_remainder": ZERO,
            "last_quote": None, "quote_remaining": ZERO, "qty_step": None,
            "gross_bought_bnb": ZERO, "quote_spent": ZERO, "spot_fee_bnb": ZERO,
            "consumed_bnb": ZERO, "unmet_consumption_bnb": ZERO,
            "next_order": 0, "next_trade": 0, "next_transfer": 0, "next_income": 0,
        }
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            for statement in (
                "CREATE TABLE paper_state (id INTEGER PRIMARY KEY CHECK(id=1), data TEXT NOT NULL)",
                "CREATE TABLE paper_orders (order_id TEXT PRIMARY KEY, client_id TEXT NOT NULL, status TEXT NOT NULL, created REAL NOT NULL, data TEXT NOT NULL)",
                "CREATE INDEX paper_orders_active ON paper_orders(status,created) WHERE status IN ('NEW','PARTIALLY_FILLED')",
                "CREATE INDEX paper_orders_client ON paper_orders(client_id,created)",
                "CREATE UNIQUE INDEX paper_orders_client_open ON paper_orders(client_id) WHERE status IN ('NEW','PARTIALLY_FILLED')",
                "CREATE TABLE paper_fills (symbol TEXT NOT NULL, trade_id TEXT NOT NULL, order_id TEXT NOT NULL, ts REAL NOT NULL, data TEXT NOT NULL, PRIMARY KEY(symbol,trade_id))",
                "CREATE INDEX paper_fills_time ON paper_fills(ts)",
                "CREATE TABLE paper_transfers (transfer_id TEXT PRIMARY KEY, status TEXT NOT NULL, ts REAL NOT NULL, data TEXT NOT NULL)",
                "CREATE INDEX paper_transfers_time ON paper_transfers(ts)",
                "CREATE INDEX paper_transfers_pending ON paper_transfers(status) WHERE status='PENDING'",
                "CREATE TABLE paper_income (income_type TEXT NOT NULL, transaction_id TEXT NOT NULL, ts REAL NOT NULL, data TEXT NOT NULL, PRIMARY KEY(income_type,transaction_id))",
                "CREATE INDEX paper_income_time ON paper_income(ts)",
            ):
                self.db.execute(statement)
            self.save_state(state)
            self.db.execute("INSERT INTO kv VALUES ('account_identity',?)", (dumps(self.identity(state)),))
            # Bootstrap is the only simulator write to the business ledger.
            self.repository._initialize_spot_checkpoint(
                self.snapshot(state, now),
                {"operator": "paper", "reason": "New simulator-owned account, no external activity",
                 "model_version": 1, "parameters": self.parameters},
            )

    def identity(self, state=None):
        state = state or self.state()
        return {"account": state["account_id"], "environment": PAPER_ENVIRONMENT,
                "symbol": self.cfg.symbol, "quote_asset": self.cfg.quote_asset,
                "strategy_id": self.cfg.strategy_id}

    def state(self):
        row = self.db.execute("SELECT data FROM paper_state WHERE id=1").fetchone()
        if row is None:
            raise ValueError("Paper account state is missing; cannot reinitialize")
        return loads(row[0])

    def save_state(self, state):
        self.db.execute("INSERT INTO paper_state VALUES (1,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data",
                        (dumps(state),))

    @staticmethod
    def next_id(state, kind):
        key = "next_" + kind
        state[key] += 1
        return f"paper-{kind}-{state[key]:020d}"

    def orders(self, *, active=False):
        suffix = " WHERE status IN ('NEW','PARTIALLY_FILLED')" if active else ""
        return tuple(loads(r[0]) for r in self.db.execute("SELECT data FROM paper_orders" + suffix + " ORDER BY created,order_id"))

    def order(self, *, order_id=None, client_id=None):
        if order_id is not None:
            row = self.db.execute("SELECT data FROM paper_orders WHERE order_id=?", (order_id,)).fetchone()
        else:
            row = self.db.execute("SELECT data FROM paper_orders WHERE client_id=? ORDER BY created DESC,order_id DESC LIMIT 1", (client_id,)).fetchone()
        return loads(row[0]) if row else None

    def save_order(self, record):
        o = record["order"]
        self.db.execute(
            "INSERT INTO paper_orders VALUES (?,?,?,?,?) ON CONFLICT(order_id) DO UPDATE SET status=excluded.status,data=excluded.data",
            (o.order_id, o.client_id, o.status, utc(o.created_at).timestamp(), dumps(record)),
        )

    def pending_transfers(self):
        return tuple(loads(r[0]) for r in self.db.execute("SELECT data FROM paper_transfers WHERE status='PENDING' ORDER BY ts,transfer_id"))

    def transfer(self, transfer_id):
        row = self.db.execute("SELECT data FROM paper_transfers WHERE transfer_id=?", (transfer_id,)).fetchone()
        return loads(row[0]) if row else None

    def save_transfer(self, record):
        t = record["receipt"]
        self.db.execute(
            "INSERT INTO paper_transfers VALUES (?,?,?,?) ON CONFLICT(transfer_id) DO UPDATE SET status=excluded.status,data=excluded.data",
            (t.transfer_id, t.status.value, utc(t.ts).timestamp(), dumps(record)),
        )

    def save_fill(self, fill):
        self.db.execute("INSERT INTO paper_fills VALUES (?,?,?,?,?)",
                        (fill.symbol, fill.trade_id, fill.order_id, utc(fill.ts).timestamp(), dumps(fill)))

    def save_income(self, income):
        self.db.execute("INSERT INTO paper_income VALUES (?,?,?,?)",
                        (income.income_type, income.transaction_id, utc(income.ts).timestamp(), dumps(income)))

    def fills(self, symbol, since, until):
        return tuple(loads(r[0]) for r in self.db.execute(
            "SELECT data FROM paper_fills WHERE symbol=? AND ts>=? AND ts<=? ORDER BY ts,trade_id",
            (symbol, utc(since).timestamp(), utc(until).timestamp())))

    def transfers(self, since, until):
        return tuple(loads(r[0])["receipt"] for r in self.db.execute(
            "SELECT data FROM paper_transfers WHERE ts>=? AND ts<=? ORDER BY ts,transfer_id",
            (utc(since).timestamp(), utc(until).timestamp())))

    def income(self, since, until):
        return tuple(loads(r[0]) for r in self.db.execute(
            "SELECT data FROM paper_income WHERE ts>=? AND ts<=? ORDER BY ts,transaction_id",
            (utc(since).timestamp(), utc(until).timestamp())))

    def snapshot(self, state, now):
        pending = tuple(r["receipt"] for r in self.pending_transfers())
        usd_locked = sum((r["order"].remaining_qty * r["order"].price for r in self.orders(active=True)), ZERO)
        usd_locked += sum((t.amount for t in pending if t.from_account == "SPOT" and t.asset == self.cfg.quote_asset), ZERO)
        bnb_locked = sum((t.amount for t in pending if t.from_account == "SPOT" and t.asset == "BNB"), ZERO)
        contract_locked = sum((t.amount for t in pending if t.from_account == "USDⓈ-M Futures"), ZERO)
        available = max(state["contract_usd"] - self.parameters["config"].contract_unavailable_usd, ZERO)
        return AccountSnapshot(
            ts=utc(now), contract_bnb=state["contract_bnb"],
            contract_max_withdraw_amount=max(available - contract_locked, ZERO),
            contract_available_balance=available, spot_bnb=state["spot_bnb"], spot_usd=state["spot_usd"],
            reserved_spot_usd=usd_locked, reserved_spot_bnb=bnb_locked,
            contract_quote_balance=state["contract_usd"], contract_total_margin_balance=state["contract_usd"],
            contract_bnb_updated_at=state["contract_bnb_updated_at"],
            contract_quote_updated_at=state["contract_quote_updated_at"],
        )

    @staticmethod
    def status(repository):
        """Read committed state only; no clocks, credentials or market connection."""
        row = repository.db.execute("SELECT data FROM paper_state WHERE id=1").fetchone()
        if row is None:
            raise ValueError("Paper state is missing")
        state = loads(row[0])
        identity = repository.load("account_identity")
        if state["model_version"] != 1 or not identity or identity.get("account") != state["account_id"]:
            raise ValueError("Invalid paper identity or model")
        ledger = PaperLedger(repository, SimpleNamespace(**identity), state["parameters"])
        return {"model_version": 1, "parameters": state["parameters"],
                "account": ledger.snapshot(state, state["advanced_at"]),
                "active_seconds": state["active_seconds"],
                "orders": tuple(r["order"] for r in ledger.orders(active=True)),
                "pending_transfers": tuple(r["receipt"] for r in ledger.pending_transfers()),
                "recent_transfers": tuple(loads(r[0])["receipt"] for r in ledger.db.execute(
                    "SELECT data FROM paper_transfers ORDER BY ts DESC,transfer_id DESC LIMIT 20")),
                "gross_bought_bnb": state["gross_bought_bnb"], "quote_spent": state["quote_spent"],
                "spot_fee_bnb": state["spot_fee_bnb"], "consumed_bnb": state["consumed_bnb"],
                "unmet_consumption_bnb": state["unmet_consumption_bnb"]}
