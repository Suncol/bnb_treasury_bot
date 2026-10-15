from datetime import timedelta

from core.models import OperationKind, ReplenishmentState
from storage.codec import loads
from tests.fake_exchange import FakeExchange
from tests.helpers import D, make_account
from tests.execution_helpers import run, setup


def watch_setup(tmp_path, contract_bnb="31", spot_usd="1000"):
    return setup(tmp_path, FakeExchange(make_account(
        contract_bnb=D(contract_bnb), spot_usd=D(spot_usd)
    )), ReplenishmentState.WATCH)


def test_inventory_consumption_does_not_stack_occupied_price_layers(tmp_path):
    ex, repo, runner = watch_setup(tmp_path)
    try:
        run(runner, ex)
        original = tuple(ex.orders)
        ex.now += timedelta(minutes=30)
        ex.futures_change("BNB", D("-0.02"))
        run(runner, ex)
        assert tuple(ex.orders) == original
        assert not repo.operations(unresolved_only=True)
    finally:
        repo.close()


def test_same_price_reassessment_keeps_orders_until_original_ttl(tmp_path):
    ex, repo, runner = watch_setup(tmp_path)
    try:
        run(runner, ex)
        original = tuple(ex.orders)
        ex.now += timedelta(hours=6)
        run(runner, ex, inventory=False)
        assert tuple(ex.orders) == original
        assert not any(o.kind == OperationKind.CANCEL for o in ex.writes)
        ex.now += timedelta(hours=18)
        run(runner, ex, inventory=False)
        cancels = [o for o in ex.writes if o.kind == OperationKind.CANCEL]
        assert len(cancels) == 3
        assert all(o.payload.reason == "Order TTL expired" for o in cancels)
        assert not repo.operations(unresolved_only=True)
    finally:
        repo.close()


def test_queued_group_submits_valid_last_layer_without_resplitting(tmp_path):
    ex, repo, runner = watch_setup(tmp_path, contract_bnb="28.26", spot_usd="3000")
    try:
        run(runner, ex)
        plan = loads(repo.db.execute(
            "SELECT data FROM events WHERE kind='CYCLE' ORDER BY id LIMIT 1"
        ).fetchone()[0])
        expected = tuple(plan.buy_plan.orders)
        submitted = tuple(o.payload for o in ex.writes if o.kind == OperationKind.ORDER)
        assert [o.qty for o in expected] == [D("1.12"), D("1.49"), D("1.12")]
        assert submitted == expected
    finally:
        repo.close()


def test_simple_matching_shares_volume_across_small_orders(tmp_path):
    ex, repo, runner = watch_setup(tmp_path)
    try:
        run(runner, ex)
        ids = tuple(ex.orders)
        # Leave a small remainder on the first order; later orders can consume
        # the same tick's unused volume, but all fills share one .20 BNB cap.
        ex.fill(ids[0], D("0.28"))
        before = len(ex.fills)
        ex.fill_ordinary_orders(D("0.20"))
        fills = ex.fills[before:]
        assert [f.qty for f in fills] == [D("0.02"), D("0.18")]
        assert sum((f.qty for f in fills), D("0")) == D("0.20")
        assert ex.orders[ids[1]].remaining_qty == D("0.22")
    finally:
        repo.close()
