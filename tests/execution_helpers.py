"""Shared fake-exchange execution setup for integration tests."""

from dataclasses import replace
from datetime import timedelta

from core.models import OrderPlan, ReplenishmentState
from services.executor import Executor
from services.runner import Runner
from storage.repository import Repository
from tests.fake_exchange import FakeExchange
from tests.helpers import D, make_account, make_strategy_config


def setup(tmp_path, exchange=None, state=ReplenishmentState.URGENT):
    exchange = exchange or FakeExchange()
    repo = Repository(tmp_path / "test.sqlite3")
    repo.initialize_spot_checkpoint(exchange.fetch_account_snapshot(), {"operator": "test", "reason": "quiescent fixture"})
    repo.save("runtime", replace(repo.runtime(), state=state))
    runner = Runner(exchange, repo, make_strategy_config(), clock=lambda: exchange.now)
    return exchange, repo, runner


def run(runner, exchange, inventory=True):
    return runner.run_once(
        market=exchange.fetch_market_snapshot("BNBUSDT"), inventory_cycle=inventory
    )


def accumulate_setup(tmp_path):
    return setup(
        tmp_path,
        FakeExchange(make_account(contract_bnb=D("27"), spot_usd=D("5000"))),
        ReplenishmentState.ACCUMULATE,
    )


def seed_bid(exchange, repo, *, age_hours=0, qty="1"):
    cfg = make_strategy_config()
    # Each seeded submission must cover the previous order's wallet changes.
    from services.reconciliation import Reconciler

    Reconciler(exchange, repo, cfg).refresh(exchange.now)
    op = Executor(exchange, repo, cfg, clock=lambda: exchange.now).execute(
        OrderPlan(cfg.symbol, "BUY", "LIMIT_MAKER", D(qty), D("598"), "GTC", True),
        exchange.now,
        ReplenishmentState.ACCUMULATE,
    )
    # Model an existing aged order explicitly; preparation now refreshes idle
    # account observations and correctly timestamps a real new intent at send.
    op = replace(op, created_at=exchange.now - timedelta(hours=age_hours))
    repo.update_operation(op)
    with repo.db:
        repo.db.execute("UPDATE operations SET created=? WHERE client_id=?",
                        (op.created_at.timestamp(), op.client_id))
    exchange.orders[op.exchange_id] = replace(
        exchange.orders[op.exchange_id], created_at=op.created_at
    )
    return op


def legacy_open_bid(exchange, repo, *, created_at=None):
    """A pre-upgrade accepted GTC, with no new-policy preparation/rebase."""
    from core.models import Operation, OperationKind, OperationStatus, OrderView
    cfg = make_strategy_config()
    created_at = created_at or exchange.now
    baseline = repo.load('spot_checkpoint')
    op = Operation('legacy-open', OperationKind.ORDER, 'order:BNBUSDT',
                   OrderPlan('BNBUSDT', 'BUY', 'LIMIT_MAKER', D('0.05'), D('598'), 'GTC', True),
                   created_at, status=OperationStatus.CONFIRMED, exchange_id='legacy-open',
                   state=ReplenishmentState.ACCUMULATE,
                   balance_before=replace(baseline['account'], wallet_event_cursor=baseline['cursor']),
                   balance_pending=False)
    repo.record_intent(replace(op, status=OperationStatus.PENDING), repo.runtime())
    repo.update_operation(op)
    observed = OrderView('BNBUSDT', op.exchange_id, op.client_id, cfg.strategy_id,
                         D('598'), D('0.05'), D('0'), created_at, state=op.state)
    exchange.orders[op.exchange_id] = observed
    repo.save_tracked_orders({op.client_id: observed})
    return op
