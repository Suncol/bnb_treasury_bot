from dataclasses import replace
from datetime import timedelta

import pytest

from core.models import OperationKind, OrderPlan, ReplenishmentState
from services.qualification import reconciliation_status
from tests.execution_helpers import setup, seed_bid
from tests.helpers import D, make_account
from tests.fake_exchange import FakeExchange


@pytest.mark.parametrize("cause", ["spot", "history", "observation"])
def test_distinct_durable_blockers_protect_runner_and_direct_execution(tmp_path, monkeypatch, cause):
    exchange, repo, runner = setup(tmp_path, FakeExchange(make_account(
        contract_bnb=D("27"), spot_usd=D("5000"))), ReplenishmentState.ACCUMULATE)
    if cause == "spot":
        seed_bid(exchange, repo)
        runner.reconciler.refresh(exchange.now)
        exchange.account = replace(exchange.account, spot_usd=D("5100"))
    elif cause == "history":
        monkeypatch.setattr("services.reconciliation.historical_metrics",
                            lambda *args, **kwargs: (D("0"), None, {"reason": "missing historical evidence"}))
    else:
        read = exchange.fetch_account_snapshot
        exchange.fetch_account_snapshot = lambda: replace(read(), ts=exchange.now - timedelta(seconds=20))
    writes_before = len(exchange.writes)
    runner.run_once()
    status = reconciliation_status(repo)
    assert status.blocked
    assert bool(status.own_operations) == (cause == "spot")
    assert bool(status.spot_settlement) == (cause == "spot")
    assert bool(status.historical_diagnostics) == (cause == "history")
    assert bool(status.observation) == (cause == "observation")
    assert len(exchange.writes) == writes_before and repo.runtime().last_inventory_at is None
    with pytest.raises(RuntimeError):
        runner.executor.execute(OrderPlan("BNBUSDT", "BUY", "LIMIT_MAKER", D("1"), D("590"), "GTC", True),
                                exchange.now, ReplenishmentState.ACCUMULATE)
    assert len(exchange.writes) == writes_before
    repo.close()


def test_open_accepted_order_is_not_an_unknown_submission(tmp_path):
    exchange, repo, runner = setup(tmp_path, FakeExchange(make_account(spot_usd=D("5000"))))
    op = seed_bid(exchange, repo)
    _, orders, pending, consistent = runner.reconciler.refresh(exchange.now)
    assert consistent and orders and not pending.unresolved
    assert not reconciliation_status(repo).blocked
    assert repo.operation(op.client_id).kind == OperationKind.ORDER
    assert repo.operations(active_only=True)
    repo.close()
