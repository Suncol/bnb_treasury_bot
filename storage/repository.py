from __future__ import annotations

import fcntl
import shutil
import sqlite3
from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from threading import RLock

from core.models import Operation, OperationKind, OperationStatus, RuntimeState
from core.time_utils import utc

from .codec import dumps, loads


class Repository:
    """One durable SQLite journal and one writer lock per exchange account.

    Intent commits finish before any network mutation. The process lock spans the
    reconcile/plan/execute sequence, without keeping a SQL transaction open.
    """

    def __init__(self, path: str | Path):
        self.path = str(path)
        self._mutex = RLock()
        self._controller_lock = None
        self._lock_depth = 0
        if self.path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path, timeout=5, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS operations (
                client_id TEXT PRIMARY KEY, scope TEXT NOT NULL, status TEXT NOT NULL,
                created REAL NOT NULL, data TEXT NOT NULL);
            CREATE UNIQUE INDEX IF NOT EXISTS unresolved_scope ON operations(scope)
                WHERE status IN ('PENDING','UNKNOWN');
            CREATE TABLE IF NOT EXISTS fills (
                symbol TEXT NOT NULL, trade_id TEXT NOT NULL, ts REAL NOT NULL, data TEXT NOT NULL,
                PRIMARY KEY(symbol, trade_id));
            CREATE TABLE IF NOT EXISTS transfers (
                transfer_id TEXT PRIMARY KEY, ts REAL NOT NULL, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS snapshots (ts REAL PRIMARY KEY, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY, ts REAL NOT NULL, kind TEXT NOT NULL, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS outbox (
                id INTEGER PRIMARY KEY, data TEXT NOT NULL, delivered INTEGER NOT NULL DEFAULT 0);
        """)

        self._migrate()

    def _migrate(self):
        # Preserve rowids: outstanding legacy transfer cursors refer to fills.
        version = self.db.execute("PRAGMA user_version").fetchone()[0]
        if version > 1:
            raise ValueError("Database schema is newer than this controller")
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            version = self.db.execute("PRAGMA user_version").fetchone()[0]
            if version == 0:
                self.db.execute("ALTER TABLE operations ADD COLUMN kind TEXT")
                self.db.execute("ALTER TABLE operations ADD COLUMN exchange_id TEXT")
                self.db.execute(
                    "ALTER TABLE operations ADD COLUMN unresolved INTEGER NOT NULL DEFAULT 0"
                )
                self.db.execute(
                    "ALTER TABLE operations ADD COLUMN active INTEGER NOT NULL DEFAULT 0"
                )
                self.db.execute("ALTER TABLE fills ADD COLUMN order_id TEXT")
                for client_id, data in self.db.execute(
                    "SELECT client_id,data FROM operations"
                ).fetchall():
                    op = loads(data)
                    self.db.execute(
                        "UPDATE operations SET kind=?,exchange_id=?,unresolved=?,active=? WHERE client_id=?",
                        (
                            op.kind.value,
                            op.exchange_id,
                            int(op.unresolved),
                            int(
                                op.kind == OperationKind.ORDER
                                and op.status != OperationStatus.FAILED
                            ),
                            client_id,
                        ),
                    )
                for rowid, data in self.db.execute(
                    "SELECT rowid,data FROM fills"
                ).fetchall():
                    self.db.execute(
                        "UPDATE fills SET order_id=? WHERE rowid=?",
                        (loads(data).order_id, rowid),
                    )
            for statement in (
                "CREATE INDEX IF NOT EXISTS operations_unresolved ON operations(created,client_id) WHERE unresolved=1",
                "CREATE INDEX IF NOT EXISTS operations_active ON operations(created,client_id) WHERE active=1",
                "CREATE INDEX IF NOT EXISTS operations_exchange ON operations(kind,exchange_id)",
                "CREATE INDEX IF NOT EXISTS operations_scope ON operations(scope,status)",
                "CREATE INDEX IF NOT EXISTS fills_time ON fills(ts,trade_id)",
                "CREATE INDEX IF NOT EXISTS fills_order ON fills(symbol,order_id)",
                "CREATE INDEX IF NOT EXISTS transfers_time ON transfers(ts,transfer_id)",
                "CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(id) WHERE delivered=0",
                "CREATE TABLE IF NOT EXISTS orders (client_id TEXT PRIMARY KEY, active INTEGER NOT NULL, data TEXT NOT NULL)",
                "CREATE INDEX IF NOT EXISTS orders_active ON orders(client_id) WHERE active=1",
                "CREATE TABLE IF NOT EXISTS episodes (id TEXT PRIMARY KEY, started REAL NOT NULL, ended REAL, data TEXT NOT NULL)",
                "CREATE INDEX IF NOT EXISTS episodes_started ON episodes(started DESC)",
                "CREATE TABLE IF NOT EXISTS wallet_events (id INTEGER PRIMARY KEY AUTOINCREMENT, reference TEXT UNIQUE NOT NULL, ts REAL NOT NULL, data TEXT NOT NULL)",
            ):
                self.db.execute(statement)
            if version == 0:
                legacy = self.db.execute(
                    "SELECT key,data FROM kv WHERE key='tracked_orders' OR key LIKE 'episode:%'"
                ).fetchall()
                for key, data in legacy:
                    value = loads(data)
                    if key == "tracked_orders":
                        self._save_tracked_orders(value)
                    else:
                        self._save_episode(value)
                    self.db.execute("DELETE FROM kv WHERE key=?", (key,))
                # A proven terminal, settled legacy order belongs to the archive.
                # Keep missing views and any unsettled intent in the hot set.
                self.db.execute(
                    "UPDATE operations SET active=0 WHERE kind='ORDER' AND unresolved=0 "
                    "AND client_id IN (SELECT client_id FROM orders WHERE active=0)"
                )
                # Historic evidence becomes the initial ledger prefix. No cursor
                # is rewritten and no unresolved intent is silently released.
                for (data,) in self.db.execute(
                    "SELECT data FROM fills ORDER BY rowid"
                ).fetchall():
                    fill = loads(data)
                    self._fill_event(fill)
                for (data,) in self.db.execute(
                    "SELECT data FROM transfers ORDER BY rowid"
                ).fetchall():
                    self._transfer_event(loads(data))
                self.db.execute("PRAGMA user_version=1")

    def acquire_controller(self):
        """One process owns execution/maintenance for its entire lifetime."""
        if self._controller_lock is not None or self.path == ":memory:":
            return
        handle = open(self.path + ".controller.lock", "a")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            handle.close()
            raise RuntimeError(
                "A controller or maintenance command already owns this database; stop it before maintenance"
            ) from None
        self._controller_lock = handle

    @contextmanager
    def local_access(self):
        # Outbox uses a separate SQLite connection and short transactions; it
        # does not need the account execution lease or its network-duration lock.
        with self._mutex:
            yield

    @contextmanager
    def exclusive(self):
        with self._mutex:
            lock = None
            if self._lock_depth:
                yield
                return
            try:
                if self.path != ":memory:":
                    lock = open(self.path + ".lock", "a")
                    fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                self._lock_depth += 1
                try:
                    yield
                finally:
                    self._lock_depth -= 1
            finally:
                if lock is not None:
                    lock.close()

    def close(self):
        self.db.close()
        if self._controller_lock is not None:
            self._controller_lock.close()
            self._controller_lock = None

    def load(self, key, default=None):
        if key == "tracked_orders":
            return self.tracked_orders()
        if key.startswith("episode:"):
            row = self.db.execute(
                "SELECT data FROM episodes WHERE id=?", (key[8:],)
            ).fetchone()
            return loads(row[0]) if row else default
        row = self.db.execute("SELECT data FROM kv WHERE key=?", (key,)).fetchone()
        return loads(row[0]) if row else default

    def save(self, key, value):
        with self.db:
            if key == "tracked_orders":
                self.db.execute("DELETE FROM orders")
                self._save_tracked_orders(value)
                return
            if key.startswith("episode:"):
                self._save_episode(value)
                return
            self.db.execute(
                "INSERT INTO kv VALUES (?,?) ON CONFLICT(key) DO UPDATE SET data=excluded.data",
                (key, dumps(value)),
            )

    def bind_identity(self, identity, config_digest, now):
        with self.db:
            previous = self.load("account_identity")
            if previous is not None and previous != identity:
                raise ValueError(
                    "Database account/environment/symbol/strategy identity mismatch"
                )
            self.db.execute(
                "INSERT INTO kv VALUES ('account_identity',?) ON CONFLICT(key) DO NOTHING",
                (dumps(identity),),
            )
            old_digest = self.load("config_digest")
            if old_digest != config_digest:
                self.db.execute(
                    "INSERT INTO events(ts,kind,data) VALUES (?,?,?)",
                    (
                        utc(now).timestamp(),
                        "CONFIGURATION",
                        dumps({"previous": old_digest, "current": config_digest}),
                    ),
                )
                self.db.execute(
                    "INSERT INTO kv VALUES ('config_digest',?) ON CONFLICT(key) DO UPDATE SET data=excluded.data",
                    (dumps(config_digest),),
                )

    def runtime(self) -> RuntimeState:
        return self.load("runtime", RuntimeState())

    def _save_episode(self, guard):
        self.db.execute(
            "INSERT INTO episodes VALUES (?,?,?,?) ON CONFLICT(id) DO UPDATE SET started=excluded.started,ended=excluded.ended,data=excluded.data",
            (
                guard.episode_id,
                utc(guard.started_at).timestamp(),
                utc(guard.ended_at).timestamp() if guard.ended_at else None,
                dumps(guard),
            ),
        )

    def save_guard(self, runtime: RuntimeState, now):
        previous, guard = self.runtime().guard, runtime.guard
        with self.db:
            if guard.episode_id:
                stored = self.load("episode:" + guard.episode_id)
                if (
                    stored is None
                    or stored.started_at != guard.started_at
                    or stored.ended_at != guard.ended_at
                ):
                    total = sum(
                        (
                            f.qty
                            for f in self.fills_since(
                                guard.started_at, until=guard.ended_at
                            )
                            if f.strategy_id is not None and f.side == "BUY"
                        ),
                        Decimal("0"),
                    )
                    guard = replace(guard, filled_bnb=max(guard.filled_bnb, total))
                elif stored.filled_bnb > guard.filled_bnb:
                    guard = replace(guard, filled_bnb=stored.filled_bnb)
                self._save_episode(guard)
            runtime = replace(runtime, guard=guard)
            self.db.execute(
                "INSERT INTO kv VALUES ('runtime',?) ON CONFLICT(key) DO UPDATE SET data=excluded.data",
                (dumps(runtime),),
            )
            if (previous.active, previous.episode_id, previous.reasons) != (
                guard.active,
                guard.episode_id,
                guard.reasons,
            ):
                self.db.execute(
                    "INSERT INTO events(ts,kind,data) VALUES (?,?,?)",
                    (utc(now).timestamp(), "CRASH_GUARD", dumps(guard)),
                )

    def _save_tracked_orders(self, orders):
        self.db.executemany(
            "INSERT INTO orders VALUES (?,?,?) ON CONFLICT(client_id) DO UPDATE SET active=excluded.active,data=excluded.data",
            (
                (client_id, int(order.is_open), dumps(order))
                for client_id, order in orders.items()
            ),
        )

    def save_tracked_orders(self, orders):
        with self.db:
            self._save_tracked_orders(orders)

    def tracked_orders(self, active_only=False):
        query = "SELECT client_id,data FROM orders"
        if active_only:
            query += " WHERE active=1"
        return {row[0]: loads(row[1]) for row in self.db.execute(query)}

    def tracked_order(self, client_id):
        row = self.db.execute(
            "SELECT data FROM orders WHERE client_id=?", (client_id,)
        ).fetchone()
        return loads(row[0]) if row else None

    def retire_order(self, client_id):
        with self.db:
            self.db.execute(
                "UPDATE operations SET active=0 WHERE client_id=? AND unresolved=0",
                (client_id,),
            )

    def operation(self, client_id):
        row = self.db.execute(
            "SELECT data FROM operations WHERE client_id=?", (client_id,)
        ).fetchone()
        if row is None:
            raise KeyError(client_id)
        return loads(row[0])

    def order_operation(self, exchange_id):
        row = self.db.execute(
            "SELECT data FROM operations WHERE kind='ORDER' AND exchange_id=?",
            (exchange_id,),
        ).fetchone()
        return loads(row[0]) if row else None

    def cancel_operation(self, symbol, order_id):
        row = self.db.execute(
            "SELECT data FROM operations WHERE scope=? AND status!='FAILED' LIMIT 1",
            (f"cancel:{symbol}:{order_id}",),
        ).fetchone()
        return loads(row[0]) if row else None

    def cancel_recorded(self, symbol, order_id):
        return self.cancel_operation(symbol, order_id) is not None

    def record_intent(self, operation: Operation, runtime: RuntimeState | None = None):
        if operation.status != OperationStatus.PENDING:
            raise ValueError("New operation must be PENDING")
        with self.db:
            if (
                operation.balance_before is not None
                and self.load("spot_checkpoint") is None
            ):
                self.db.execute(
                    "INSERT INTO kv VALUES ('spot_checkpoint',?)",
                    (
                        dumps(
                            {
                                "account": operation.balance_before,
                                "cursor": self.wallet_cursor(),
                            }
                        ),
                    ),
                )
            self.db.execute(
                "INSERT INTO operations(client_id,scope,status,created,data,kind,exchange_id,unresolved,active) VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    operation.client_id,
                    operation.scope,
                    operation.status.value,
                    utc(operation.created_at).timestamp(),
                    dumps(operation),
                    operation.kind.value,
                    operation.exchange_id,
                    int(operation.unresolved),
                    int(operation.kind == OperationKind.ORDER),
                ),
            )
            if runtime is not None:
                self.db.execute(
                    "INSERT INTO kv VALUES ('runtime',?) ON CONFLICT(key) DO UPDATE SET data=excluded.data",
                    (dumps(runtime),),
                )

    def update_operation(
        self, operation: Operation, runtime: RuntimeState | None = None
    ):
        with self.db:
            result = self.db.execute(
                "UPDATE operations SET status=?,data=?,exchange_id=?,unresolved=?,active=CASE WHEN ?='FAILED' THEN 0 ELSE active END WHERE client_id=?",
                (
                    operation.status.value,
                    dumps(operation),
                    operation.exchange_id,
                    int(operation.unresolved),
                    operation.status.value,
                    operation.client_id,
                ),
            )
            if result.rowcount != 1:
                raise KeyError(operation.client_id)
            if runtime is not None:
                self.db.execute(
                    "INSERT INTO kv VALUES ('runtime',?) ON CONFLICT(key) DO UPDATE SET data=excluded.data",
                    (dumps(runtime),),
                )

    def operations(
        self, unresolved_only=False, *, active_only=False
    ) -> tuple[Operation, ...]:
        query = "SELECT data FROM operations"
        if unresolved_only:
            query += " WHERE unresolved=1"
        elif active_only:
            query = (
                "SELECT data,created,client_id FROM operations WHERE active=1 UNION "
                "SELECT data,created,client_id FROM operations WHERE unresolved=1"
            )
        return tuple(
            loads(row[0])
            for row in self.db.execute(query + " ORDER BY created,client_id")
        )

    def save_fills(self, fills, *, evidence=None):
        with self.db:
            if evidence is not None:
                self.db.execute(
                    "INSERT INTO events(ts,kind,data) VALUES (?,?,?)",
                    (
                        utc(evidence["at"]).timestamp(),
                        "SPOT_EVIDENCE_RECEIPTS",
                        dumps(evidence),
                    ),
                )
            for fill in fills:
                previous = self.db.execute(
                    "SELECT data FROM fills WHERE symbol=? AND trade_id=?",
                    (fill.symbol, fill.trade_id),
                ).fetchone()
                previous = loads(previous[0]) if previous else None
                if (
                    previous is not None
                    and replace(
                        previous,
                        strategy_id=fill.strategy_id,
                        quote_asset=fill.quote_asset
                        if not previous.quote_asset
                        else previous.quote_asset,
                        quote_qty=fill.quote_qty
                        if previous.quote_qty is None
                        else previous.quote_qty,
                    )
                    != fill
                ):
                    raise ValueError(
                        "Conflicting trade data for the same unique fill ID"
                    )
                if previous == fill:
                    continue
                self.db.execute(
                    "INSERT INTO fills(symbol,trade_id,ts,data,order_id) VALUES (?,?,?,?,?) ON CONFLICT(symbol,trade_id) DO UPDATE SET data=excluded.data",
                    (
                        fill.symbol,
                        fill.trade_id,
                        utc(fill.ts).timestamp(),
                        dumps(fill),
                        fill.order_id,
                    ),
                )
                self._fill_event(fill)
                # Guard episodes do not overlap. Only the interval containing
                # this execution can change, including a late ownership binding.
                difference = (
                    fill.qty
                    if fill.strategy_id and fill.side == "BUY"
                    else Decimal("0")
                ) - (
                    previous.qty
                    if previous and previous.strategy_id and previous.side == "BUY"
                    else Decimal("0")
                )
                if not difference:
                    continue
                row = self.db.execute(
                    "SELECT data FROM episodes WHERE started<=? ORDER BY started DESC LIMIT 1",
                    (utc(fill.ts).timestamp(),),
                ).fetchone()
                if row:
                    episode = loads(row[0])
                    if episode.ended_at is None or utc(fill.ts) < utc(episode.ended_at):
                        self._save_episode(
                            replace(episode, filled_bnb=episode.filled_bnb + difference)
                        )

    def _wallet_event(self, reference, ts, deltas):
        self.db.execute(
            "INSERT INTO wallet_events(reference,ts,data) VALUES (?,?,?) ON CONFLICT(reference) DO UPDATE SET data=excluded.data",
            (reference, utc(ts).timestamp(), dumps(deltas)),
        )

    def _fill_event(self, fill):
        if fill.side not in {"BUY", "SELL"} or not fill.base_asset:
            raise ValueError("Trade side and assets are required")
        quote = fill.quote_asset or fill.symbol[len(fill.base_asset) :]
        if fill.symbol != fill.base_asset + quote or not quote:
            raise ValueError("Trade symbol does not match assets")
        if (
            not fill.qty.is_finite()
            or fill.qty <= 0
            or not fill.commission.is_finite()
            or fill.commission < 0
        ):
            raise ValueError("Invalid trade quantity or commission")
        if fill.quote_qty is not None and (
            not fill.quote_qty.is_finite() or fill.quote_qty <= 0
        ):
            raise ValueError("Invalid trade quote quantity")
        sign = Decimal("1") if fill.side == "BUY" else Decimal("-1")
        deltas = {
            fill.base_asset: sign * fill.qty,
            quote: None if fill.quote_qty is None else -sign * fill.quote_qty,
        }
        if (
            fill.commission_asset
            and deltas.get(fill.commission_asset, Decimal("0")) is not None
        ):
            deltas[fill.commission_asset] = (
                deltas.get(fill.commission_asset, Decimal("0")) - fill.commission
            )
        self._wallet_event(f"trade:{fill.symbol}:{fill.trade_id}", fill.ts, deltas)

    def _transfer_event(self, transfer):
        if transfer.status == OperationStatus.CONFIRMED and "SPOT" in {
            transfer.from_account,
            transfer.to_account,
        }:
            sign = 1 if transfer.to_account == "SPOT" else -1
            self._wallet_event(
                "transfer:" + transfer.transfer_id,
                transfer.ts,
                {transfer.asset: sign * transfer.amount},
            )

    def wallet_cursor(self):
        return self.db.execute(
            "SELECT COALESCE(MAX(id),0) FROM wallet_events"
        ).fetchone()[0]

    def wallet_changes(self, cursor):
        return tuple(
            (row[0], row[1], loads(row[2]))
            for row in self.db.execute(
                "SELECT id,reference,data FROM wallet_events WHERE id>? ORDER BY id",
                (cursor,),
            )
        )

    def fill_totals(self, symbol, order_ids):
        totals = {}
        for order_id in order_ids:
            totals[order_id] = sum(
                (
                    loads(row[0]).qty
                    for row in self.db.execute(
                        "SELECT data FROM fills WHERE symbol=? AND order_id=?",
                        (symbol, order_id),
                    )
                ),
                Decimal("0"),
            )
        return totals

    def fill_cursor(self):
        return self.db.execute("SELECT COALESCE(MAX(rowid),0) FROM fills").fetchone()[0]

    def fills_after(self, cursor):
        return tuple(
            loads(row[0])
            for row in self.db.execute(
                "SELECT data FROM fills WHERE rowid>? ORDER BY rowid", (cursor,)
            )
        )

    def fills_since(self, since, *, until=None):
        return tuple(
            loads(row[0])
            for row in self.db.execute(
                "SELECT data FROM fills WHERE ts>=?"
                + (" AND ts<?" if until is not None else "")
                + " ORDER BY ts,trade_id",
                (utc(since).timestamp(),)
                + ((utc(until).timestamp(),) if until is not None else ()),
            )
        )

    def save_transfers(self, transfers):
        with self.db:
            for transfer in transfers:
                if not transfer.amount.is_finite() or transfer.amount <= 0:
                    raise ValueError("Invalid transfer amount")
                previous = self.db.execute(
                    "SELECT data FROM transfers WHERE transfer_id=?",
                    (transfer.transfer_id,),
                ).fetchone()
                if previous:
                    old = loads(previous[0])
                    if old == transfer:
                        continue
                    if replace(old, status=transfer.status) != transfer or (
                        old.status
                        in {OperationStatus.CONFIRMED, OperationStatus.FAILED}
                        and old.status != transfer.status
                    ):
                        raise ValueError("Conflicting transfer evidence")
                self._transfer_event(transfer)
                self.db.execute(
                    "INSERT INTO transfers VALUES (?,?,?) ON CONFLICT(transfer_id) DO UPDATE SET data=excluded.data",
                    (
                        transfer.transfer_id,
                        utc(transfer.ts).timestamp(),
                        dumps(transfer),
                    ),
                )

    def transfers_since(self, since):
        return tuple(
            loads(row[0])
            for row in self.db.execute(
                "SELECT data FROM transfers WHERE ts>=? ORDER BY ts,transfer_id",
                (utc(since).timestamp(),),
            )
        )

    def budget_used(self, now, quote_asset):
        since = utc(now) - timedelta(hours=24)
        transfers = self.transfers_since(since)
        used = sum(
            (
                t.amount
                for t in transfers
                if t.asset == quote_asset
                and t.from_account == "USDⓈ-M Futures"
                and utc(t.ts) <= utc(now)
                and t.status in {OperationStatus.CONFIRMED, OperationStatus.PENDING}
            ),
            Decimal("0"),
        )
        known = {t.transfer_id for t in transfers}
        # Unknown requests reserve budget even when no exchange history is visible.
        for op in self.operations(unresolved_only=True):
            p = op.payload
            if (
                getattr(p, "asset", None) == quote_asset
                and p.from_account == "USDⓈ-M Futures"
                and op.exchange_id not in known
            ):
                used += p.amount
        return used

    def save_snapshot(self, account):
        if account.ts is None:
            raise ValueError("Snapshot timestamp is required")
        with self.db:
            self.db.execute(
                "INSERT OR REPLACE INTO snapshots VALUES (?,?)",
                (utc(account.ts).timestamp(), dumps(account)),
            )
            self.db.execute(
                "DELETE FROM snapshots WHERE ts < ?",
                ((utc(account.ts) - timedelta(hours=49)).timestamp(),),
            )

    def snapshots_since(self, since):
        return tuple(
            loads(row[0])
            for row in self.db.execute(
                "SELECT data FROM snapshots WHERE ts>=? ORDER BY ts",
                (utc(since).timestamp(),),
            )
        )

    def event(self, now, kind, value):
        with self.db:
            self.db.execute(
                "INSERT INTO events(ts,kind,data) VALUES (?,?,?)",
                (utc(now).timestamp(), kind, dumps(value)),
            )

    def health(self):
        page_size = self.db.execute("PRAGMA page_size").fetchone()[0]
        pages = self.db.execute("PRAGMA page_count").fetchone()[0]
        wal = Path(self.path + "-wal")
        oldest = self.db.execute(
            "SELECT MIN(created) FROM operations WHERE unresolved=1"
        ).fetchone()[0]
        return {
            "database_bytes": page_size * pages,
            "wal_bytes": wal.stat().st_size
            if self.path != ":memory:" and wal.exists()
            else 0,
            "disk_free_bytes": shutil.disk_usage(Path(self.path).parent).free
            if self.path != ":memory:"
            else None,
            "pending_alerts": self.db.execute(
                "SELECT COUNT(*) FROM outbox WHERE delivered=0"
            ).fetchone()[0],
            "oldest_unresolved_at": oldest,
        }

    def publish_alerts(self, alerts):
        """Edge triggered outbox; failures remain queued, recovery resets dedupe."""
        current = {a.code: (a.level, a.message) for a in alerts}
        previous = self.load("active_alerts", {})
        with self.db:
            for alert in alerts:
                if previous.get(alert.code) != current[alert.code]:
                    self.db.execute(
                        "INSERT INTO outbox(data) VALUES (?)", (dumps(alert),)
                    )
            self.db.execute(
                "INSERT INTO kv VALUES ('active_alerts',?) ON CONFLICT(key) DO UPDATE SET data=excluded.data",
                (dumps(current),),
            )

    def pending_alerts(self, limit=100):
        return tuple(
            (row[0], loads(row[1]))
            for row in self.db.execute(
                "SELECT id,data FROM outbox WHERE delivered=0 ORDER BY id LIMIT ?",
                (limit,),
            )
        )

    def mark_delivered(self, alert_id):
        with self.db:
            self.db.execute("UPDATE outbox SET delivered=1 WHERE id=?", (alert_id,))
