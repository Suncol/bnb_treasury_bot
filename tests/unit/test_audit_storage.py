import sqlite3
from dataclasses import replace
from datetime import timedelta

import pytest

from core.models import (
    CrashGuardState,
    Fill,
    Operation,
    OperationKind,
    OperationStatus,
    OrderPlan,
)
from storage.codec import dumps
from storage.repository import Repository
from tests.helpers import D, make_account, make_engine_inputs


@pytest.mark.parametrize("history", [3000, 36500])
def test_hot_operations_and_empty_fill_updates_do_not_scan_cold_history(
    history, monkeypatch
):
    repo = Repository(":memory:")
    now = make_engine_inputs().now
    payload = OrderPlan("BNBUSDT", "BUY", "LIMIT", D("1"), D("600"), "IOC", False)
    op = Operation(
        "closed",
        OperationKind.ORDER,
        "order:BNBUSDT",
        payload,
        now,
        status=OperationStatus.CONFIRMED,
    )
    data = dumps(op)
    with repo.db:
        repo.db.executemany(
            "INSERT INTO operations(client_id,scope,status,created,data,kind,active,unresolved) VALUES (?,?,?,?,?,?,0,0)",
            (
                (
                    str(i),
                    op.scope,
                    op.status.value,
                    now.timestamp(),
                    data,
                    op.kind.value,
                )
                for i in range(history)
            ),
        )
        for i in range(365):
            start = now - timedelta(days=400 - i)
            repo._save_episode(
                CrashGuardState(
                    active=False,
                    episode_id=str(i),
                    started_at=start,
                    ended_at=start + timedelta(minutes=5),
                )
            )
    active = replace(op, client_id="active", status=OperationStatus.PENDING)
    repo.record_intent(active)
    queries = []
    repo.db.set_trace_callback(queries.append)
    assert repo.operations(unresolved_only=True) == (active,)
    assert repo.operations(active_only=True) == (active,)
    repo.save_fills(())
    assert not any("episodes" in query or "FROM fills" in query for query in queries)
    for query in queries:
        if query.startswith("SELECT"):
            plan = " ".join(
                row[3] for row in repo.db.execute("EXPLAIN QUERY PLAN " + query)
            )
            assert "SCAN operations" not in plan or "USING INDEX" in plan
    repo.close()


def test_legacy_migration_preserves_fill_rowids_and_pending_intents(tmp_path):
    path = tmp_path / "old.db"
    now = make_engine_inputs().now
    fill = Fill("BNBUSDT", "old-fill", "order", now, D("1"), quote_qty=D("600"))
    operation = Operation(
        "pending",
        OperationKind.ORDER,
        "order:BNBUSDT",
        OrderPlan("BNBUSDT", "BUY", "LIMIT", D("1"), D("600"), "IOC", False),
        now,
    )
    with sqlite3.connect(path) as db:
        db.executescript("""CREATE TABLE fills(symbol TEXT,trade_id TEXT,ts REAL,data TEXT,PRIMARY KEY(symbol,trade_id));
            CREATE TABLE operations(client_id TEXT PRIMARY KEY,scope TEXT,status TEXT,created REAL,data TEXT);""")
        db.execute(
            "INSERT INTO fills(rowid,symbol,trade_id,ts,data) VALUES (41,?,?,?,?)",
            (fill.symbol, fill.trade_id, now.timestamp(), dumps(fill)),
        )
        db.execute(
            "INSERT INTO operations VALUES (?,?,?,?,?)",
            (
                operation.client_id,
                operation.scope,
                operation.status.value,
                now.timestamp(),
                dumps(operation),
            ),
        )
    repo = Repository(path)
    assert repo.db.execute("SELECT rowid FROM fills").fetchone()[0] == 41
    assert repo.operations(unresolved_only=True) == (operation,)
    assert repo.fills_since(now) == (fill,)
    assert repo.wallet_changes(0)[0][1] == "trade:BNBUSDT:old-fill"
    repo.close()
    reopened = Repository(path)
    assert reopened.db.execute("PRAGMA user_version").fetchone()[0] == 1
    assert len(reopened.wallet_changes(0)) == 1
    reopened.close()


def test_intent_checkpoint_and_runtime_are_one_transaction(tmp_path):
    repo = Repository(tmp_path / "atomic.db")
    now = make_engine_inputs().now
    repo.save("runtime", repo.runtime())
    operation = Operation(
        "first",
        OperationKind.ORDER,
        "order:BNBUSDT",
        OrderPlan("BNBUSDT", "BUY", "LIMIT", D("1"), D("600"), "IOC", False),
        now,
        balance_before=make_account(),
        balance_pending=True,
    )
    repo.db.execute(
        "CREATE TRIGGER fail_runtime BEFORE UPDATE ON kv WHEN NEW.key='runtime' BEGIN SELECT RAISE(ABORT,'injected'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="injected"):
        repo.record_intent(operation, repo.runtime())
    assert repo.load("spot_checkpoint") is None
    assert repo.operations() == ()
    repo.close()


def test_late_fill_only_repairs_its_closed_episode_and_sell_is_not_acquisition():
    repo = Repository(":memory:")
    now = make_engine_inputs().now
    for index in range(100):
        start = now + timedelta(hours=index)
        repo.save(
            "episode:" + str(index),
            CrashGuardState(
                active=False,
                episode_id=str(index),
                started_at=start,
                ended_at=start + timedelta(minutes=30),
            ),
        )
    fill = Fill(
        "BNBUSDT",
        "late",
        "order",
        now + timedelta(hours=42, minutes=1),
        D("2"),
        quote_qty=D("1200"),
        strategy_id="bnb-treasury",
    )
    repo.save_fills((fill, fill, replace(fill, trade_id="sell", side="SELL")))
    assert repo.load("episode:42").filled_bnb == D("2")
    assert repo.load("episode:41").filled_bnb == repo.load("episode:43").filled_bnb == 0
    repo.close()


def test_legacy_closed_order_archive_does_not_block_initial_balance_checkpoint(
    tmp_path,
):
    from core.models import OrderView
    from services.reconciliation import Reconciler
    from tests.fake_exchange import FakeExchange
    from tests.helpers import make_strategy_config

    path = tmp_path / "settled-old.db"
    exchange = FakeExchange()
    now = exchange.now
    payload = OrderPlan("BNBUSDT", "BUY", "LIMIT", D("1"), D("600"), "GTC", False)
    op = Operation(
        "closed",
        OperationKind.ORDER,
        "order:BNBUSDT",
        payload,
        now,
        status=OperationStatus.CONFIRMED,
        exchange_id="1",
    )
    order = OrderView(
        "BNBUSDT",
        "1",
        "closed",
        "bnb-treasury",
        D("600"),
        D("1"),
        D("0"),
        now,
        status="CANCELED",
    )
    with sqlite3.connect(path) as db:
        db.executescript("""CREATE TABLE operations(client_id TEXT PRIMARY KEY,scope TEXT,status TEXT,created REAL,data TEXT);
            CREATE TABLE kv(key TEXT PRIMARY KEY,data TEXT);""")
        db.execute(
            "INSERT INTO operations VALUES (?,?,?,?,?)",
            (op.client_id, op.scope, op.status.value, now.timestamp(), dumps(op)),
        )
        db.execute(
            "INSERT INTO kv VALUES (?,?)", ("tracked_orders", dumps({"closed": order}))
        )
    repo = Repository(path)
    assert repo.operations() == (op,)  # Audit history remains intact.
    assert not repo.operations(active_only=True)
    assert (
        not Reconciler(exchange, repo, make_strategy_config())
        .refresh(now)[2]
        .unresolved
    )
    assert repo.load("spot_checkpoint") is not None
    assert not exchange.writes
    repo.close()


def test_account_and_strategy_binding_survive_restart_and_reject_reuse(tmp_path):
    path = tmp_path / "identity.db"
    now = make_engine_inputs().now
    identity = {
        "account": "uid-fingerprint",
        "environment": "production-usds-m",
        "symbol": "BNBUSDT",
        "quote_asset": "USDT",
        "strategy_id": "bnb-treasury",
    }
    repo = Repository(path)
    repo.bind_identity(identity, "config-a", now)
    repo.close()
    repo = Repository(path)
    for field in identity:
        with pytest.raises(ValueError, match="identity mismatch"):
            repo.bind_identity({**identity, field: "different"}, "config-b", now)
        assert repo.load("account_identity") == identity
        assert repo.load("config_digest") == "config-a"
    repo.bind_identity(identity, "config-b", now)
    assert repo.load("config_digest") == "config-b"
    assert (
        repo.db.execute(
            "SELECT COUNT(*) FROM events WHERE kind='CONFIGURATION'"
        ).fetchone()[0]
        == 2
    )
    repo.close()
