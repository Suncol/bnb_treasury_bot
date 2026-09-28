from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
import fcntl
from pathlib import Path
import sqlite3
from threading import RLock

from core.models import Operation, OperationStatus, RuntimeState
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

    @contextmanager
    def exclusive(self):
        with self._mutex:
            lock = None
            try:
                if self.path != ":memory:":
                    lock = open(self.path + ".lock", "a")
                    fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                yield
            finally:
                if lock is not None:
                    lock.close()

    def close(self):
        self.db.close()

    def load(self, key, default=None):
        row = self.db.execute("SELECT data FROM kv WHERE key=?", (key,)).fetchone()
        return loads(row[0]) if row else default

    def save(self, key, value):
        with self.db:
            self.db.execute(
                "INSERT INTO kv VALUES (?,?) ON CONFLICT(key) DO UPDATE SET data=excluded.data",
                (key, dumps(value)),
            )

    def runtime(self) -> RuntimeState:
        return self.load("runtime", RuntimeState())

    def save_guard(self, runtime: RuntimeState, now):
        previous, guard = self.runtime().guard, runtime.guard
        rows = [("runtime", dumps(runtime))]
        if guard.episode_id:
            rows.append(("episode:" + guard.episode_id, dumps(guard)))
        with self.db:
            self.db.executemany(
                "INSERT INTO kv VALUES (?,?) ON CONFLICT(key) DO UPDATE SET data=excluded.data",
                rows,
            )
            if (previous.active, previous.episode_id, previous.reasons) != (
                guard.active, guard.episode_id, guard.reasons
            ):
                self.db.execute(
                    "INSERT INTO events(ts,kind,data) VALUES (?,?,?)",
                    (utc(now).timestamp(), "CRASH_GUARD", dumps(guard)),
                )

    def record_intent(self, operation: Operation, runtime: RuntimeState | None = None):
        if operation.status != OperationStatus.PENDING:
            raise ValueError("New operation must be PENDING")
        with self.db:
            self.db.execute(
                "INSERT INTO operations VALUES (?,?,?,?,?)",
                (
                    operation.client_id,
                    operation.scope,
                    operation.status.value,
                    utc(operation.created_at).timestamp(),
                    dumps(operation),
                ),
            )
            if runtime is not None:
                self.db.execute(
                    "INSERT INTO kv VALUES ('runtime',?) ON CONFLICT(key) DO UPDATE SET data=excluded.data",
                    (dumps(runtime),),
                )

    def update_operation(self, operation: Operation, runtime: RuntimeState | None = None):
        with self.db:
            result = self.db.execute(
                "UPDATE operations SET status=?,data=? WHERE client_id=?",
                (operation.status.value, dumps(operation), operation.client_id),
            )
            if result.rowcount != 1:
                raise KeyError(operation.client_id)
            if runtime is not None:
                self.db.execute(
                    "INSERT INTO kv VALUES ('runtime',?) ON CONFLICT(key) DO UPDATE SET data=excluded.data",
                    (dumps(runtime),),
                )

    def operations(self, unresolved_only=False) -> tuple[Operation, ...]:
        query = "SELECT data FROM operations"
        if unresolved_only:
            query += " WHERE status IN ('PENDING','UNKNOWN','CONFIRMED')"
        operations = (
            loads(row[0])
            for row in self.db.execute(query + " ORDER BY created,client_id")
        )
        return tuple(op for op in operations if not unresolved_only or op.unresolved)

    def save_fills(self, fills):
        with self.db:
            for fill in fills:
                previous = self.db.execute(
                    "SELECT data FROM fills WHERE symbol=? AND trade_id=?",
                    (fill.symbol, fill.trade_id),
                ).fetchone()
                previous = loads(previous[0]) if previous is not None else None
                if previous is not None and replace(
                    previous, strategy_id=fill.strategy_id,
                    quote_qty=fill.quote_qty
                    if previous.quote_qty is None else previous.quote_qty,
                ) != fill:
                    raise ValueError(
                        "Conflicting trade data for the same unique fill ID"
                    )
                self.db.execute(
                    "INSERT INTO fills VALUES (?,?,?,?) ON CONFLICT(symbol,trade_id) DO UPDATE SET data=excluded.data",
                    (fill.symbol, fill.trade_id, utc(fill.ts).timestamp(), dumps(fill)),
                )
            # Late reports also repair closed episode audit records by execution time.
            episodes = self.db.execute(
                "SELECT key,data FROM kv WHERE key LIKE 'episode:%'"
            ).fetchall()
            for key, data in episodes:
                episode = loads(data)
                matching = self.fills_since(episode.started_at)
                total = sum(
                    (
                        f.qty
                        for f in matching
                        if f.strategy_id is not None
                        and (
                            episode.ended_at is None
                            or utc(f.ts) < utc(episode.ended_at)
                        )
                    ),
                    Decimal("0"),
                )
                self.db.execute(
                    "UPDATE kv SET data=? WHERE key=?",
                    (dumps(replace(episode, filled_bnb=total)), key),
                )

    def fill_cursor(self):
        return self.db.execute("SELECT COALESCE(MAX(rowid),0) FROM fills").fetchone()[0]

    def fills_after(self, cursor):
        return tuple(
            loads(row[0]) for row in self.db.execute(
                "SELECT data FROM fills WHERE rowid>? ORDER BY rowid", (cursor,)
            )
        )

    def fills_since(self, since):
        return tuple(
            loads(row[0])
            for row in self.db.execute(
                "SELECT data FROM fills WHERE ts>=? ORDER BY ts,trade_id",
                (utc(since).timestamp(),),
            )
        )

    def save_transfers(self, transfers):
        with self.db:
            for transfer in transfers:
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

    def pending_alerts(self):
        return tuple(
            (row[0], loads(row[1]))
            for row in self.db.execute(
                "SELECT id,data FROM outbox WHERE delivered=0 ORDER BY id"
            )
        )

    def mark_delivered(self, alert_id):
        with self.db:
            self.db.execute("UPDATE outbox SET delivered=1 WHERE id=?", (alert_id,))
