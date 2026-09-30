"""Safety regressions derived from the supplied audit, asserting repaired behavior."""

from dataclasses import replace
from datetime import timedelta

import pytest

from core.models import GateStatus, OperationKind, ReplenishmentState
from core.replenishment_engine import build_cycle_plan
from tests.fake_exchange import FakeExchange
from tests.helpers import D, make_account, make_engine_inputs, make_strategy_config
from tests.integration.test_execution import setup


@pytest.mark.parametrize(
    "invalid", ["stale", "future", "nan", "infinite", "inconsistent", "unsettled"]
)
def test_invalid_inventory_freezes_decision_and_slice(invalid):
    inputs = make_engine_inputs(
        account=make_account(contract_bnb=D("24")),
        previous_candidate_state=ReplenishmentState.ACCUMULATE,
        candidate_streak=1,
    )
    if invalid == "stale":
        inputs = replace(
            inputs,
            account=replace(inputs.account, ts=inputs.now - timedelta(seconds=20)),
        )
    elif invalid == "future":
        inputs = replace(
            inputs,
            account=replace(inputs.account, ts=inputs.now + timedelta(seconds=20)),
        )
    elif invalid in {"nan", "infinite"}:
        inputs = replace(
            inputs,
            account=replace(
                inputs.account,
                contract_bnb=D("NaN" if invalid == "nan" else "Infinity"),
            ),
        )
    elif invalid == "inconsistent":
        inputs = replace(inputs, snapshot_consistent=False)
    else:
        inputs = replace(
            inputs, pending=replace(inputs.pending, has_pending_orders=True)
        )
    plan = build_cycle_plan(inputs, make_strategy_config())
    assert not plan.inventory_valid
    assert plan.state_decision.confirmed_state == inputs.previous_state
    assert plan.state_decision.candidate_state == inputs.previous_candidate_state
    assert plan.state_decision.candidate_streak == 1
    assert plan.slice_state == inputs.slice_state
    assert not plan.buy_plan.orders and not plan.transfer_decision.allow


def test_stale_urgent_snapshot_cannot_cause_later_ioc(tmp_path):
    exchange, repo, runner = setup(
        tmp_path,
        FakeExchange(make_account(contract_bnb=D("30"), spot_usd=D("2000"))),
        ReplenishmentState.IDLE,
    )
    original = exchange.fetch_account_snapshot
    exchange.fetch_account_snapshot = lambda: replace(
        original(), contract_bnb=D("24"), ts=exchange.now - timedelta(seconds=20)
    )
    first = runner.tick()
    assert first.gates.data_gate == GateStatus.BLOCKED
    assert repo.runtime().state == ReplenishmentState.IDLE
    assert repo.runtime().last_inventory_at is None
    assert repo.runtime().last_inventory_attempt_at == exchange.now
    assert not exchange.writes
    exchange.fetch_account_snapshot = original
    exchange.now += timedelta(seconds=11)
    assert runner.tick() is not None
    assert not any(op.kind == OperationKind.ORDER for op in exchange.writes)
    assert repo.runtime().state == ReplenishmentState.IDLE
    repo.close()


@pytest.mark.parametrize("entry", ["tick", "run_once"])
@pytest.mark.parametrize(
    "lagged", [("spot_bnb",), ("spot_usd",), ("spot_bnb", "spot_usd")]
)
def test_known_fill_cannot_recreate_demand_while_either_wallet_lags(
    tmp_path, entry, lagged
):
    from services.runner import Runner
    from storage.repository import Repository

    exchange, repo, runner = setup(
        tmp_path, FakeExchange(make_account(contract_bnb=D("24"), spot_usd=D("20000")))
    )
    read = exchange.fetch_account_snapshot
    before = read()
    exchange.fetch_account_snapshot = lambda: replace(
        read(), **{field: getattr(before, field) for field in lagged}
    )
    for index in range(4):
        getattr(runner, entry)()
        buys = [op for op in exchange.writes if op.kind == OperationKind.ORDER]
        assert len(buys) == 1 and buys[0].payload.qty == D("8")
        assert repo.operations(unresolved_only=True)
        assert repo.load("spot_settlement_issue")["differences"]
        if index == 1:
            repo.close()
            repo = Repository(tmp_path / "test.sqlite3")
            runner = Runner(
                exchange, repo, make_strategy_config(), clock=lambda: exchange.now
            )
        exchange.now += timedelta(seconds=11)
    exchange.fetch_account_snapshot = read
    getattr(runner, entry)()
    assert not repo.operations(unresolved_only=True)
    assert len([op for op in exchange.writes if op.kind == OperationKind.ORDER]) == 1
    assert exchange.account.contract_bnb == D("32")
    assert not repo.load("spot_settlement_issue")
    repo.close()


@pytest.mark.parametrize("asset", ["USDT", "BNB"])
def test_external_trade_evidence_explains_transfer_difference_without_reset(
    tmp_path, asset
):
    from core.models import AssetTransferPlan, Fill
    from services.runner import Runner
    from storage.repository import Repository

    exchange, repo, runner = setup(
        tmp_path,
        FakeExchange(
            make_account(contract_bnb=D("24"), spot_bnb=D("3.5"), spot_usd=D("1000"))
        ),
    )
    if asset == "USDT":
        transfer = AssetTransferPlan(asset, D("500"), "USDⓈ-M Futures", "SPOT", "test")
    else:
        transfer = AssetTransferPlan(asset, D("2"), "SPOT", "USDⓈ-M Futures", "test")
    runner.executor.execute(transfer, exchange.now, repo.runtime().state)
    exchange.now += timedelta(seconds=2)
    # A different pair spends quote or pays fees in BNB. Only verified receipts
    # may explain the difference; new timestamps and restarts cannot release it.
    symbol = "ETHUSDT" if asset == "USDT" else "ETHBTC"
    receipt = Fill(
        symbol,
        "outside",
        "external-order",
        exchange.now,
        D("0.01"),
        quote_qty=D("100") if asset == "USDT" else D("0.001"),
        base_asset="ETH",
        quote_asset="USDT" if asset == "USDT" else "BTC",
        commission=D("0.001") if asset == "BNB" else D("0"),
        commission_asset="BNB",
    )
    exchange.fills.append(receipt)
    field, change = (
        ("spot_usd", D("100")) if asset == "USDT" else ("spot_bnb", D("0.001"))
    )
    exchange.account = replace(
        exchange.account, **{field: getattr(exchange.account, field) - change}
    )
    assert runner.reconciler.refresh(exchange.now)[2].unresolved
    assert repo.load("spot_settlement_issue")["differences"] == {asset: -change}
    repo.close()
    repo = Repository(tmp_path / "test.sqlite3")
    runner = Runner(exchange, repo, make_strategy_config(), clock=lambda: exchange.now)
    assert runner.reconciler.refresh(exchange.now)[2].unresolved
    for _ in range(2):
        result = runner.reconciler.import_spot_trades(
            (symbol,),
            operator="operator-a",
            reason="Verified shared-wallet trade",
            now=exchange.now,
        )
        assert not result[2].unresolved
    assert len(exchange.writes) == 1
    assert not repo.load("spot_settlement_issue")
    assert not repo.operations(unresolved_only=True)
    assert len(repo.fills_since(receipt.ts)) == 1
    repo.close()


def test_partial_fill_reopens_balance_gate_for_a_previously_settled_order(tmp_path):
    from core.models import OrderPlan
    from services.runner import Runner
    from storage.repository import Repository

    exchange, repo, runner = setup(
        tmp_path,
        FakeExchange(make_account(contract_bnb=D("27"), spot_usd=D("10000"))),
        ReplenishmentState.ACCUMULATE,
    )
    op = runner.executor.execute(
        OrderPlan("BNBUSDT", "BUY", "LIMIT", D("2"), D("590"), "GTC", False),
        exchange.now,
        repo.runtime().state,
    )
    assert not runner.reconciler.refresh(exchange.now)[2].unresolved
    assert not repo.operation(op.client_id).balance_pending
    read = exchange.fetch_account_snapshot
    before = read()
    exchange.now += timedelta(seconds=2)
    exchange.fill(op.exchange_id, D("1"))
    exchange.fetch_account_snapshot = lambda: replace(
        read(), spot_bnb=before.spot_bnb, spot_usd=before.spot_usd
    )
    assert runner.reconciler.refresh(exchange.now)[2].unresolved
    assert repo.operation(op.client_id).balance_pending
    repo.close()
    repo = Repository(tmp_path / "test.sqlite3")
    runner = Runner(exchange, repo, make_strategy_config(), clock=lambda: exchange.now)
    runner.run_once()
    assert len([w for w in exchange.writes if w.kind != OperationKind.CANCEL]) == 1
    assert repo.load("spot_settlement_issue")
    exchange.fetch_account_snapshot = read
    assert not runner.reconciler.refresh(exchange.now)[2].unresolved
    assert not repo.operation(op.client_id).balance_pending
    repo.close()


def test_accepted_ioc_with_failed_result_commit_and_lagged_wallet_never_repeats(
    tmp_path,
):
    import sqlite3

    from services.runner import Runner
    from storage.repository import Repository

    exchange, repo, runner = setup(
        tmp_path, FakeExchange(make_account(contract_bnb=D("24"), spot_usd=D("20000")))
    )
    read = exchange.fetch_account_snapshot
    before = read()
    repo.db.execute(
        "CREATE TRIGGER fail_result BEFORE UPDATE ON operations BEGIN SELECT RAISE(ABORT,'disk failure'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="disk failure"):
        runner.run_once()
    assert len(exchange.writes) == 1
    assert repo.load("spot_checkpoint")["account"].spot_bnb == before.spot_bnb
    repo.close()
    repo = Repository(tmp_path / "test.sqlite3")
    repo.db.execute("DROP TRIGGER fail_result")
    runner = Runner(exchange, repo, make_strategy_config(), clock=lambda: exchange.now)
    exchange.fetch_account_snapshot = lambda: replace(
        read(), spot_bnb=before.spot_bnb, spot_usd=before.spot_usd
    )
    runner.run_once()
    assert len(exchange.writes) == 1 and repo.operations(unresolved_only=True)
    exchange.fetch_account_snapshot = read
    runner.run_once()
    assert len([w for w in exchange.writes if w.kind == OperationKind.ORDER]) == 1
    assert not repo.operations(unresolved_only=True)
    assert exchange.account.contract_bnb == D("32")
    repo.close()


def test_concurrent_sell_and_quote_fee_are_covered_during_transfer(tmp_path):
    from core.models import AssetTransferPlan, Fill

    exchange, repo, runner = setup(
        tmp_path,
        FakeExchange(
            make_account(contract_bnb=D("24"), spot_bnb=D("3.5"), spot_usd=D("1000"))
        ),
    )
    op = runner.executor.execute(
        AssetTransferPlan("USDT", D("500"), "USDⓈ-M Futures", "SPOT", "fund"),
        exchange.now,
        repo.runtime().state,
    )
    exchange.now += timedelta(seconds=2)
    exchange.fills.append(
        Fill(
            "BNBUSDT",
            "external-sell",
            "external-order",
            exchange.now,
            D("1"),
            commission=D(".6"),
            commission_asset="USDT",
            quote_qty=D("600"),
            side="SELL",
            quote_asset="USDT",
        )
    )
    exchange.account = replace(
        exchange.account,
        spot_bnb=exchange.account.spot_bnb - 1,
        spot_usd=exchange.account.spot_usd + D("599.4"),
    )
    assert not runner.reconciler.refresh(exchange.now)[2].unresolved
    assert not repo.operation(op.client_id).balance_pending
    assert not repo.load("spot_settlement_issue")
    assert len(exchange.writes) == 1
    repo.close()


@pytest.mark.parametrize(
    "constraint",
    ["min_price", "max_price", "position", "symbol_orders", "exchange_orders", "stale"],
)
def test_non_executable_exchange_limits_prevent_funding_and_buying(constraint):
    inputs = make_engine_inputs(
        account=make_account(
            contract_bnb=D("24"),
            contract_max_withdraw_amount=D("100000"),
            spot_usd=D("0"),
        )
    )
    filters = inputs.filters
    changes = {
        "min_price": {"min_price": D("700")},
        "max_price": {"max_price": D("590")},
        "position": {"max_position": inputs.account.spot_bnb},
        "symbol_orders": {"max_open_orders": 0},
        "exchange_orders": {"exchange_order_slots": 0},
        "stale": {"observed_at": inputs.now - timedelta(seconds=6)},
    }
    plan = build_cycle_plan(
        replace(inputs, filters=replace(filters, **changes[constraint])),
        make_strategy_config(),
    )
    assert not plan.transfer_decision.allow and not plan.buy_plan.orders


def test_position_limit_caps_urgent_order_and_order_slots_cap_layer_group():
    inputs = make_engine_inputs(
        account=make_account(contract_bnb=D("24"), spot_usd=D("20000"))
    )
    plan = build_cycle_plan(
        replace(inputs, filters=replace(inputs.filters, max_position=D("1.5"))),
        make_strategy_config(),
    )
    assert sum(o.qty for o in plan.buy_plan.orders) == D("1")
    inputs = replace(
        inputs,
        account=replace(inputs.account, contract_bnb=D("27")),
        previous_state=ReplenishmentState.ACCUMULATE,
        filters=replace(inputs.filters, max_open_orders=1),
    )
    plan = build_cycle_plan(inputs, make_strategy_config())
    assert len(plan.buy_plan.orders) == 1
