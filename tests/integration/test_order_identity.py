from dataclasses import replace
from datetime import timedelta

import pytest

from core.models import (
    OperationKind,
    OperationResult,
    OperationStatus,
    OrderPlan,
    ReplenishmentState,
)
from services.binance_adapter import BinanceAdapter
from services.executor import Executor
from services.reconciliation import Reconciler
from services.runner import Runner
from storage.repository import Repository
from tests.helpers import D, make_strategy_config
from tests.integration.test_execution import run
from tests.integration.test_execution_boundaries import accumulate_setup, seed_bid


@pytest.mark.parametrize("degraded", [False, True])
def test_lost_ack_then_cancel_recovers_identity_across_restart(tmp_path, degraded):
    exchange, repo, runner = accumulate_setup(tmp_path)
    cfg = make_strategy_config()
    exchange.lose_response = OperationKind.ORDER
    original = Executor(exchange, repo, cfg).execute(
        OrderPlan(cfg.symbol, "BUY", "LIMIT", D("1"), D("590"), "GTC", False),
        exchange.now,
        ReplenishmentState.ACCUMULATE,
    )
    adapter = BinanceAdapter("", "", cfg, clients=(None, None, None))
    adapter.fetch_order = exchange.fetch_order
    query = exchange.query_operation
    inconclusive = True

    def query_operation(op):
        if op.kind == OperationKind.ORDER:
            if inconclusive:
                return OperationResult(OperationStatus.UNKNOWN)
            return adapter.query_operation(op)
        return query(op)

    submit = exchange.submit

    def cancel_renames_client(op):
        result = submit(op)
        if op.kind == OperationKind.CANCEL:
            order = exchange.orders[op.payload.order_id]
            exchange.orders[order.order_id] = replace(
                order, client_id="cancel-generated-id"
            )
        return result

    exchange.query_operation = query_operation
    exchange.submit = cancel_renames_client
    exchange.cancel_fill_qty = D("0.25")
    open_orders = exchange.fetch_open_orders
    if degraded:

        def unavailable(_):
            raise TimeoutError()

        exchange.fetch_open_orders = unavailable
    run(runner, exchange, inventory=False)
    assert exchange.orders["1"].status == "CANCELED"
    saved = next(op for op in repo.operations() if op.client_id == original.client_id)
    assert saved.status == OperationStatus.UNKNOWN and saved.exchange_id == "1"
    repo.close()

    inconclusive = False
    exchange.now += timedelta(seconds=10)  # Honor the persisted read retry window.
    exchange.fetch_open_orders = open_orders
    repo = Repository(tmp_path / "test.sqlite3")
    runner = Runner(exchange, repo, cfg, clock=lambda: exchange.now)
    plan = run(runner, exchange, inventory=False)
    assert plan is not None and not repo.operations(unresolved_only=True)
    assert sum(op.kind == OperationKind.ORDER for op in exchange.writes) == 1
    assert sum(op.kind == OperationKind.CANCEL for op in exchange.writes) == 1
    assert repo.fills_since(original.created_at)[0].qty == D("0.25")
    repo.close()


def test_legacy_tracked_identity_is_used_without_resubmitting(tmp_path):
    exchange, repo, runner = accumulate_setup(tmp_path)
    cfg = make_strategy_config()
    op = Executor(exchange, repo, cfg).execute(
        OrderPlan(cfg.symbol, "BUY", "LIMIT", D("1"), D("590"), "GTC", False),
        exchange.now,
        ReplenishmentState.ACCUMULATE,
    )
    order = replace(
        exchange.orders[op.exchange_id], client_id="cancel-id", status="CANCELED"
    )
    exchange.orders[order.order_id] = order
    repo.save(
        "tracked_orders", {op.client_id: replace(order, strategy_id=cfg.strategy_id)}
    )
    repo.update_operation(replace(op, status=OperationStatus.UNKNOWN, exchange_id=None))
    adapter = BinanceAdapter("", "", cfg, clients=(None, None, None))
    adapter.fetch_order = exchange.fetch_order
    exchange.query_operation = adapter.query_operation
    _, _, pending, consistent = Reconciler(exchange, repo, cfg).refresh(exchange.now)
    assert consistent and not pending.unresolved
    assert repo.operations()[0].exchange_id == order.order_id
    assert len(exchange.writes) == 1
    repo.close()


def test_reused_client_id_does_not_reassign_an_external_order(tmp_path):
    exchange, repo, runner = accumulate_setup(tmp_path)
    cfg = make_strategy_config()
    op = Executor(exchange, repo, cfg).execute(
        OrderPlan(cfg.symbol, "BUY", "LIMIT", D("1"), D("590"), "GTC", False),
        exchange.now,
        ReplenishmentState.ACCUMULATE,
    )
    external = replace(exchange.orders[op.exchange_id], order_id="external")
    exchange.orders[external.order_id] = external
    exchange.fill(op.exchange_id, D("1"))
    _, orders, pending, consistent = Reconciler(exchange, repo, cfg).refresh(
        exchange.now
    )
    assert consistent and not pending.unresolved
    assert len(orders) == 1 and orders[0].strategy_id is None
    assert repo.fills_since(op.created_at)[0].strategy_id == cfg.strategy_id
    assert len(exchange.writes) == 1
    repo.close()


@pytest.mark.parametrize("degraded", [False, True])
def test_amended_quantity_keeps_ownership_and_protective_cancel_after_restart(
    tmp_path, degraded
):
    exchange, repo, runner = accumulate_setup(tmp_path)
    cfg = make_strategy_config()
    op = seed_bid(exchange, repo, qty="2")
    runner.reconciler.refresh(exchange.now)
    exchange.fill(op.exchange_id, D("0.25"))
    exchange.orders[op.exchange_id] = replace(
        exchange.orders[op.exchange_id],
        qty=D("1"),
        client_id="amended-client-id",
    )
    repo.close()
    repo = Repository(tmp_path / "test.sqlite3")
    runner = Runner(exchange, repo, cfg, clock=lambda: exchange.now)
    read_market = exchange.fetch_market_snapshot
    exchange.fetch_market_snapshot = lambda symbol: replace(
        read_market(symbol), drawdown_1m=D("0.02")
    )
    if degraded:
        read_account = exchange.fetch_account_snapshot
        exchange.fetch_account_snapshot = lambda: (_ for _ in ()).throw(TimeoutError())
    runner.run_once(inventory_cycle=False)
    assert not exchange.orders[op.exchange_id].is_open
    assert sum(o.kind == OperationKind.CANCEL for o in exchange.writes) == 1
    if degraded:
        exchange.fetch_account_snapshot = read_account
    adapter = BinanceAdapter("", "", cfg, clients=(None, None, None))
    adapter.fetch_order = exchange.fetch_order
    exchange.query_operation = adapter.query_operation
    saved = next(o for o in repo.operations() if o.client_id == op.client_id)
    repo.update_operation(replace(saved, status=OperationStatus.UNKNOWN))
    _, _, pending, consistent = runner.reconciler.refresh(exchange.now)
    assert consistent and not pending.unresolved
    tracked = repo.load("tracked_orders")[op.client_id]
    assert tracked.qty == D("1") and tracked.filled_qty == D("0.25")
    assert repo.fills_since(op.created_at)[0].strategy_id == cfg.strategy_id
    assert sum(o.kind == OperationKind.ORDER for o in exchange.writes) == 1
    assert sum(o.kind == OperationKind.CANCEL for o in exchange.writes) == 1
    repo.close()
