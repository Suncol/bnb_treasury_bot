from dataclasses import replace
from datetime import timedelta

from core.models import (
    CrashGuardState,
    OperationKind,
    OperationStatus,
    ReplenishmentState,
    RunMode,
)
from services.executor import Executor
from services.runner import Runner
from storage.repository import Repository
from tests.fake_exchange import FakeExchange
from tests.helpers import D, make_account, make_strategy_config
from tests.unit.test_order_planner import make_state_decision
from core.order_planner import plan_buy_orders


def setup(tmp_path, exchange=None, state=ReplenishmentState.URGENT):
    exchange = exchange or FakeExchange()
    repo = Repository(tmp_path / "test.sqlite3")
    repo.save("runtime", replace(repo.runtime(), state=state))
    runner = Runner(exchange, repo, make_strategy_config(), clock=lambda: exchange.now)
    return exchange, repo, runner


def run(runner, exchange, inventory=True):
    return runner.run_once(
        market=exchange.fetch_market_snapshot("BNBUSDT"), inventory_cycle=inventory
    )


def test_transfer_buy_and_bnb_return_use_refreshed_confirmed_balances(tmp_path):
    exchange, repo, runner = setup(tmp_path)
    plan = run(runner, exchange)
    assert plan is not None
    assert [o.kind for o in exchange.writes] == [
        OperationKind.TRANSFER,
        OperationKind.ORDER,
        OperationKind.TRANSFER,
    ]
    assert exchange.writes[0].payload.to_account == "SPOT"
    assert exchange.writes[-1].payload.asset == "BNB"
    assert repo.budget_used(exchange.now, "USDT") == D("3000")
    assert not repo.operations(unresolved_only=True)
    assert exchange.account.contract_bnb > D("24")


def test_pending_funding_waits_then_continues_on_reconciliation_tick(tmp_path):
    exchange, repo, runner = setup(tmp_path)
    exchange.hold_transfers = True
    run(runner, exchange)
    assert len(exchange.writes) == 1
    run(runner, exchange, inventory=False)
    assert len(exchange.writes) == 1
    exchange.hold_transfers = False
    exchange.settle("1")
    run(runner, exchange, inventory=False)
    assert any(op.kind == OperationKind.ORDER for op in exchange.writes)
    assert (
        sum(
            op.payload.to_account == "SPOT"
            for op in exchange.writes
            if op.kind == OperationKind.TRANSFER
        )
        == 1
    )


def test_transfer_timeout_and_restart_never_resubmit_even_if_balance_changed(tmp_path):
    exchange, repo, runner = setup(tmp_path)
    exchange.lose_response = OperationKind.TRANSFER
    run(runner, exchange)
    assert repo.operations()[0].status == OperationStatus.UNKNOWN
    repo.close()
    recovered = Repository(tmp_path / "test.sqlite3")
    runner = Runner(
        exchange, recovered, make_strategy_config(), clock=lambda: exchange.now
    )
    run(runner, exchange)
    assert len(exchange.writes) == 1
    assert recovered.operations()[0].status == OperationStatus.UNKNOWN


def test_order_timeout_reconciles_same_client_id_before_any_new_buy(tmp_path):
    exchange = FakeExchange(
        make_account(
            contract_bnb=D("24"),
            spot_usd=D("5000"),
            contract_max_withdraw_amount=D("4000"),
        )
    )
    exchange, repo, runner = setup(tmp_path, exchange)
    exchange.lose_response = OperationKind.ORDER
    run(runner, exchange)
    first = exchange.writes[0]
    assert first.kind == OperationKind.ORDER
    run(runner, exchange, inventory=False)
    assert repo.operations()[0].status == OperationStatus.CONFIRMED
    assert sum(op.client_id == first.client_id for op in exchange.writes) == 1
    assert sum(op.kind == OperationKind.ORDER for op in exchange.writes) == 1


def test_partial_cancel_race_counts_fills_once_and_survives_restart(tmp_path):
    exchange = FakeExchange(make_account(contract_bnb=D("27"), spot_usd=D("10000")))
    exchange, repo, runner = setup(tmp_path, exchange, ReplenishmentState.ACCUMULATE)
    cfg = make_strategy_config()
    buy = plan_buy_orders(
        make_state_decision(ReplenishmentState.ACCUMULATE, "5"),
        exchange.fetch_market_snapshot("BNBUSDT"),
        exchange.fetch_symbol_filters("BNBUSDT"),
        cfg,
        D("10000"),
    ).orders[0]
    Executor(exchange, repo, cfg).execute(
        buy, exchange.now - timedelta(minutes=1), ReplenishmentState.ACCUMULATE
    )
    guard = CrashGuardState(
        True, "episode", exchange.now, exchange.now, keep_price_ceiling=D("580")
    )
    repo.save("runtime", replace(repo.runtime(), guard=guard))
    exchange.cancel_fill_qty = D("0.5")
    run(runner, exchange)
    assert repo.runtime().guard.filled_bnb == D("0.5")
    repo.close()
    recovered = Repository(tmp_path / "test.sqlite3")
    runner = Runner(exchange, recovered, cfg, clock=lambda: exchange.now)
    run(runner, exchange, inventory=False)
    assert recovered.runtime().guard.filled_bnb == D("0.5")
    assert recovered.runtime().guard.episode_id == "episode"
    assert len(recovered.fills_since(guard.started_at)) == 1


def test_missing_trade_report_after_ioc_blocks_followup_until_reconciled(tmp_path):
    exchange = FakeExchange(
        make_account(
            contract_bnb=D("24"),
            spot_usd=D("1500"),
            contract_max_withdraw_amount=D("4000"),
        )
    )
    exchange, repo, runner = setup(tmp_path, exchange)
    exchange.hide_fills = True
    plan = run(runner, exchange)
    assert len(exchange.writes) == 1
    assert plan.gates.data_gate.value == "BLOCKED"
    exchange.hide_fills = False
    run(runner, exchange, inventory=False)
    assert any(
        op.kind == OperationKind.TRANSFER and op.payload.asset == "BNB"
        for op in exchange.writes
    )


def test_slice_wait_is_durable_across_restart(tmp_path):
    exchange = FakeExchange(make_account(contract_bnb=D("19"), spot_usd=D("30000")))
    exchange, repo, runner = setup(tmp_path, exchange)
    run(runner, exchange)
    assert sum(
        op.payload.qty for op in exchange.writes if op.kind == OperationKind.ORDER
    ) <= D("3")
    next_at = repo.runtime().slice_state.next_at
    assert next_at is not None
    repo.close()
    recovered = Repository(tmp_path / "test.sqlite3")
    runner = Runner(
        exchange, recovered, make_strategy_config(), clock=lambda: exchange.now
    )
    count = len(exchange.writes)
    run(runner, exchange)
    assert len(exchange.writes) == count
    assert recovered.runtime().slice_state.next_at == next_at


def test_consecutive_failures_pause_and_survive_restart(tmp_path):
    exchange, repo, runner = setup(tmp_path)
    exchange.read_failure = True
    for _ in range(3):
        run(runner, exchange)
    assert repo.runtime().run_mode == RunMode.PAUSED
    assert repo.runtime().pause_reason
    Runner(exchange, repo, make_strategy_config(), clock=lambda: exchange.now)
    assert repo.runtime().run_mode == RunMode.PAUSED
    assert not exchange.writes


def test_dry_run_does_not_create_failed_operations(tmp_path):
    exchange, repo, _ = setup(tmp_path)
    runner = Runner(
        exchange,
        repo,
        make_strategy_config(),
        clock=lambda: exchange.now,
        execute=False,
    )
    run(runner, exchange)
    assert not repo.operations() and not exchange.writes
    assert repo.runtime().run_mode == RunMode.AUTO


def test_confirmed_cancel_remains_owned_when_exchange_changes_client_id(tmp_path):
    exchange = FakeExchange(make_account(contract_bnb=D("42"), spot_usd=D("10000")))
    exchange, repo, runner = setup(tmp_path, exchange, ReplenishmentState.IDLE)
    cfg = make_strategy_config()
    buy = plan_buy_orders(
        make_state_decision(ReplenishmentState.WATCH, "1"),
        exchange.fetch_market_snapshot("BNBUSDT"),
        exchange.fetch_symbol_filters("BNBUSDT"),
        cfg,
        D("10000"),
    ).orders[0]
    Executor(exchange, repo, cfg).execute(buy, exchange.now, ReplenishmentState.WATCH)
    run(runner, exchange)
    exchange.orders["1"] = replace(exchange.orders["1"], client_id="exchange-cancel-id")
    repo.save("tracked_orders", {})
    run(runner, exchange)
    assert not repo.operations(unresolved_only=True)
    assert len([op for op in exchange.writes if op.kind == OperationKind.CANCEL]) == 1


def test_guard_change_between_layers_discards_remaining_old_group(tmp_path):
    exchange = FakeExchange(make_account(contract_bnb=D("27"), spot_usd=D("5000")))
    exchange, repo, runner = setup(tmp_path, exchange, ReplenishmentState.ACCUMULATE)
    exchange.feed = object()
    original = exchange.fetch_market_snapshot

    def market(symbol):
        value = original(symbol)
        return replace(value, drawdown_1m=D("0.02")) if exchange.writes else value

    exchange.fetch_market_snapshot = market
    run(runner, exchange)
    assert len([op for op in exchange.writes if op.kind == OperationKind.ORDER]) == 1
    assert any(op.kind == OperationKind.CANCEL for op in exchange.writes)
    assert repo.runtime().guard.active


def test_market_failure_still_reconciles_and_cancels_known_orders(tmp_path):
    exchange = FakeExchange(make_account(contract_bnb=D("27"), spot_usd=D("5000")))
    exchange, repo, runner = setup(tmp_path, exchange, ReplenishmentState.ACCUMULATE)
    cfg = make_strategy_config()
    buy = plan_buy_orders(
        make_state_decision(ReplenishmentState.ACCUMULATE, "5"),
        exchange.fetch_market_snapshot("BNBUSDT"),
        exchange.fetch_symbol_filters("BNBUSDT"),
        cfg,
        D("5000"),
    ).orders[0]
    Executor(exchange, repo, cfg).execute(
        buy, exchange.now, ReplenishmentState.ACCUMULATE
    )
    exchange.fetch_market_snapshot = lambda symbol: (_ for _ in ()).throw(
        TimeoutError()
    )
    runner.tick()
    assert any(op.kind == OperationKind.CANCEL for op in exchange.writes)
    assert len([op for op in exchange.writes if op.kind == OperationKind.ORDER]) == 1


def test_fast_ticks_do_not_advance_hysteresis_confirmation(tmp_path):
    exchange = FakeExchange(make_account(contract_bnb=D("27")))
    exchange, repo, runner = setup(tmp_path, exchange, ReplenishmentState.IDLE)
    runner.tick()
    assert (
        repo.runtime().candidate_streak == 1
        and repo.runtime().state == ReplenishmentState.IDLE
    )
    for _ in range(5):
        exchange.now += timedelta(seconds=1)
        runner.tick()
    assert (
        repo.runtime().candidate_streak == 1
        and repo.runtime().state == ReplenishmentState.IDLE
    )
    exchange.now += timedelta(minutes=30)
    runner.tick()
    assert repo.runtime().state == ReplenishmentState.ACCUMULATE


def test_operator_can_bind_timeout_only_with_terminal_exchange_evidence(tmp_path):
    exchange, repo, runner = setup(tmp_path)
    exchange.lose_response = OperationKind.TRANSFER
    run(runner, exchange)
    op = repo.operations()[0]
    runner.reconciler.bind_transfer_id(op.client_id, "1", exchange.now)
    assert repo.operations()[0].status == OperationStatus.CONFIRMED
    assert len(exchange.writes) == 1


def test_two_definitively_failed_funding_attempts_pause(tmp_path):
    exchange, repo, runner = setup(tmp_path)
    exchange.reject_kind = OperationKind.TRANSFER
    run(runner, exchange)
    run(runner, exchange)
    assert len(exchange.writes) == 2
    assert repo.runtime().run_mode == RunMode.PAUSED
    assert repo.runtime().transfer_failures == 2


def test_failed_unknown_query_does_not_skip_known_protective_cancels(tmp_path):
    from core.models import AssetTransferPlan, Operation

    exchange = FakeExchange(make_account(contract_bnb=D("27"), spot_usd=D("5000")))
    exchange, repo, runner = setup(tmp_path, exchange, ReplenishmentState.ACCUMULATE)
    cfg = make_strategy_config()
    buy = plan_buy_orders(
        make_state_decision(ReplenishmentState.ACCUMULATE, "5"),
        exchange.fetch_market_snapshot("BNBUSDT"),
        exchange.fetch_symbol_filters("BNBUSDT"),
        cfg,
        D("5000"),
    ).orders[0]
    Executor(exchange, repo, cfg).execute(
        buy, exchange.now, ReplenishmentState.ACCUMULATE
    )
    repo.record_intent(
        Operation(
            "lost-funding",
            OperationKind.TRANSFER,
            "transfer:fund",
            AssetTransferPlan("USDT", D("500"), "USDⓈ-M Futures", "SPOT", "test"),
            exchange.now,
        )
    )
    query = exchange.query_operation

    def failing_query(op):
        if op.kind == OperationKind.TRANSFER:
            raise TimeoutError()
        return query(op)

    exchange.query_operation = failing_query
    market = replace(exchange.fetch_market_snapshot("BNBUSDT"), drawdown_1m=D("0.02"))
    plan = runner.run_once(market=market)
    assert plan is not None
    assert any(op.kind == OperationKind.CANCEL for op in exchange.writes)
    assert len([op for op in exchange.writes if op.kind == OperationKind.ORDER]) == 1
    assert repo.operations(unresolved_only=True)[0].status == OperationStatus.UNKNOWN
