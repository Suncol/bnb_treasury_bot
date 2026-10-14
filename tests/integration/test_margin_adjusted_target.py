from dataclasses import replace
from datetime import timedelta

import pytest

from core.models import AssetTransferPlan, FuturesIncome, OperationKind, OperationStatus, OrderPlan, OrderView, ReplenishmentState, TransferRecord
from services.exchange_adapter import ExchangeError
from services.history import INCOME_RECHECK_INTERVAL
from services.runner import Runner
from storage.repository import Repository
from tests.fake_exchange import FakeExchange
from tests.helpers import D, make_account, make_strategy_config
from tests.integration.test_execution import setup


def margin_setup(tmp_path, *, spot_usd="0", baseline_hours=24):
    exchange = FakeExchange(make_account(
        contract_bnb=D("27"), contract_max_withdraw_amount=D("97000"),
        spot_usd=D(spot_usd),
    ))
    baseline_at = exchange.now - timedelta(hours=baseline_hours)
    exchange.account = replace(
        exchange.account, contract_quote_updated_at=baseline_at - timedelta(seconds=1),
    )
    read = exchange.fetch_account_snapshot

    def snapshot():
        account = read()
        return replace(account, contract_total_margin_balance=account.contract_quote_balance, contract_margin_asset="USDT")

    exchange.fetch_account_snapshot = snapshot
    exchange, repo, runner = setup(tmp_path, exchange, ReplenishmentState.ACCUMULATE)
    repo.save_snapshot(replace(snapshot(), ts=baseline_at))
    return exchange, repo, runner


@pytest.mark.parametrize("loss", ["-501", "-10000"])
def test_reconciled_loss_keeps_full_spot_funded_replenishment(tmp_path, loss):
    exchange, repo, runner = margin_setup(tmp_path, spot_usd="10000")
    exchange.futures_change("USDT", D(loss), "REALIZED_PNL")
    plan = runner.run_once()
    assert plan.inventory_valid and plan.active_buy_target == D("32")
    assert runner.cached_inputs.margin_change_24h == D(loss)
    assert len(exchange.writes) == 3
    assert all(op.kind == OperationKind.ORDER for op in exchange.writes)
    assert sum((op.payload.qty for op in exchange.writes), D("0")) == D("5")
    assert not repo.operations(unresolved_only=True)
    assert not repo.load("futures_settlement_issue")
    repo.close()


@pytest.mark.parametrize("warm_cache", [False, True])
def test_normal_funding_keeps_target_32_and_real_money_limits(tmp_path, warm_cache):
    exchange, repo, runner = margin_setup(tmp_path)
    calls, read = [], exchange.fetch_futures_income

    def income(since, until):
        calls.append((since, until))
        return read(since, until)

    exchange.fetch_futures_income = income
    if warm_cache:
        exchange.futures_change("USDT", D("-1"), "COMMISSION")
        runner.execute_enabled = False
        assert runner.run_once(inventory_cycle=False).inventory_valid and len(calls) == 1
        exchange.now += timedelta(seconds=1)
        runner.execute_enabled = True
    batch_started = exchange.now
    plan = runner.run_once()
    assert plan is not None and plan.inventory_valid
    assert plan.active_buy_target == D("32")
    assert runner.cached_inputs.margin_change_24h == (-D("1") if warm_cache else D("0"))
    assert runner.cached_inputs.account.contract_max_withdraw_amount == D("94000")
    assert repo.budget_used(exchange.now, "USDT") == D("3000")
    funding = [op for op in exchange.writes if op.kind == OperationKind.TRANSFER]
    assert len(funding) == 1 and funding[0].payload.amount == D("3000")
    orders = [op.payload for op in exchange.writes if op.kind == OperationKind.ORDER]
    assert orders and sum((o.qty * o.price for o in orders), D("0")) <= D("3000")
    assert sum((o.qty for o in orders), D("0")) <= D("5")
    assert not repo.operations(unresolved_only=True)
    assert len(calls) == (3 if warm_cache else 1)
    if warm_cache:
        assert calls[1] == calls[0]  # One full read for the new batch.
        assert calls[2][0] == calls[1][1] < calls[2][1]  # Funding only adds an increment.
        assert repo.load("futures_income_coverage")["checked_at"] == batch_started
    repo.close()


@pytest.mark.parametrize("net_transfer,loss,expected", [
    ("-3000", "0", "0"),
    ("-3000", "-1000", "-1000"),
    ("3000", "-1000", "-1000"),
])
def test_manual_settled_transfers_do_not_change_loss_signal_across_restart(
    tmp_path, net_transfer, loss, expected
):
    exchange, repo, runner = margin_setup(tmp_path, spot_usd="5000")
    if D(loss):
        exchange.futures_change("USDT", D(loss), "REALIZED_PNL")
    source, destination = (
        ("USDⓈ-M Futures", "SPOT") if D(net_transfer) < 0
        else ("SPOT", "USDⓈ-M Futures")
    )
    # Request time precedes the baseline; the income records the later settlement.
    exchange.transfers["manual"] = TransferRecord(
        "manual", "USDT", abs(D(net_transfer)), source, destination,
        exchange.now - timedelta(hours=25), OperationStatus.PENDING,
    )
    exchange.settle("manual")
    path = repo.path
    repo.close()
    repo = Repository(path)
    runner = Runner(exchange, repo, make_strategy_config(), clock=lambda: exchange.now, execute=False)
    for _ in range(2):
        plan = runner.run_once(inventory_cycle=False)
        assert plan is not None and plan.inventory_valid
        assert runner.cached_inputs.margin_change_24h == D(expected)
        assert plan.active_buy_target == D("32")
    assert not exchange.writes
    assert len(repo.futures_income_since(exchange.now - timedelta(hours=24))) == (2 if D(loss) else 1)
    repo.close()


def external_transfer(exchange, amount, loss="0"):
    if D(loss):
        exchange.futures_change("USDT", D(loss), "REALIZED_PNL")
    source, destination = (
        ("SPOT", "USDⓈ-M Futures") if D(amount) > 0
        else ("USDⓈ-M Futures", "SPOT")
    )
    identity = str(len(exchange.transfers) + 1)
    exchange.transfers[identity] = TransferRecord(
        identity, "USDT", abs(D(amount)), source, destination,
        exchange.now, OperationStatus.PENDING,
    )
    exchange.settle(identity)


@pytest.mark.parametrize("amount,loss", [("-3000", "0"), ("3000", "-1000")])
def test_split_margin_and_quote_observations_block_then_recover(tmp_path, amount, loss):
    exchange, repo, runner = margin_setup(tmp_path, spot_usd="5000")
    before = exchange.fetch_account_snapshot()
    external_transfer(exchange, amount, loss)
    read = exchange.fetch_account_snapshot
    exchange.fetch_account_snapshot = lambda: replace(
        read(), contract_quote_balance=before.contract_quote_balance,
        contract_quote_updated_at=before.contract_quote_updated_at,
    )
    runner.execute_enabled = False
    plan = runner.run_once()
    assert not plan.inventory_valid and not plan.transfer_decision.allow
    assert not plan.buy_plan.orders and repo.load("futures_settlement_issue")
    assert repo.runtime().last_inventory_at is None
    assert len(repo.snapshots_since(before.ts - timedelta(hours=25))) == 1
    exchange.fetch_account_snapshot = read
    recovered = runner.run_once()
    assert recovered.inventory_valid
    assert runner.cached_inputs.margin_change_24h == D(loss)
    assert recovered.active_buy_target == D("32")
    assert not exchange.writes
    repo.close()


@pytest.mark.parametrize("loss,missing", [("-1000", "loss"), ("-3000", "all")])
def test_missing_income_freezes_checkpoint_and_retries_after_restart(tmp_path, loss, missing):
    exchange, repo, runner = margin_setup(tmp_path, spot_usd="5000")
    checkpoint = repo.load("spot_checkpoint")
    external_transfer(exchange, "3000", loss)
    receipts = exchange.income[:]
    exchange.income = [r for r in receipts if missing != "all" and r.income_type != "REALIZED_PNL"]
    runner.execute_enabled = False
    for restart in (False, True):
        if restart:
            path = repo.path
            repo.close()
            repo = Repository(path)
            runner = Runner(exchange, repo, make_strategy_config(), clock=lambda: exchange.now, execute=False)
        plan = runner.run_once()
        assert not plan.inventory_valid and not plan.transfer_decision.allow
        assert not plan.buy_plan.orders and plan.bnb_transfer_plan is None
        assert repo.load("spot_checkpoint") == checkpoint
        assert repo.load("futures_income_coverage") is None
        assert repo.runtime().last_inventory_at is None
        with pytest.raises(RuntimeError, match="reconciled"):
            runner.executor.execute(
                AssetTransferPlan("USDT", D("500"), "USDⓈ-M Futures", "SPOT", "test"),
                exchange.now, repo.runtime().state,
            )
    exchange.income = receipts
    plan = runner.run_once()
    assert plan.inventory_valid and runner.cached_inputs.margin_change_24h == D(loss)
    assert plan.active_buy_target == D("32")
    assert repo.wallet_cursor() == 1
    assert len(repo.futures_income_since(checkpoint["account"].ts)) == 2
    assert not repo.load("futures_settlement_issue") and not exchange.writes
    repo.close()


def test_verified_income_window_is_reused_and_extended_across_restart(tmp_path):
    exchange, repo, runner = margin_setup(tmp_path, spot_usd="5000")
    external_transfer(exchange, "3000", "-1000")
    calls, read = [], exchange.fetch_futures_income

    def income(since, until):
        calls.append((since, until))
        return read(since, until)

    exchange.fetch_futures_income = income
    runner.execute_enabled = False
    for _ in range(3):
        assert runner.run_once(inventory_cycle=False).inventory_valid
    assert len(calls) == 1
    first = calls[0]
    exchange.now += timedelta(seconds=2)
    path = repo.path
    repo.close()
    repo = Repository(path)
    runner = Runner(exchange, repo, make_strategy_config(), clock=lambda: exchange.now, execute=False)
    assert runner.run_once(inventory_cycle=False).inventory_valid
    assert len(calls) == 1  # Wall time changed; the observed wallet version did not.
    exchange.futures_change("USDT", D("-1"), "COMMISSION")
    assert runner.run_once(inventory_cycle=False).inventory_valid
    assert len(calls) == 2 and calls[1][0] == first[1]
    assert calls[1][1] - calls[1][0] < timedelta(seconds=3)
    assert runner.cached_inputs.margin_change_24h == D("-1001")
    repo.close()


def test_income_read_failure_does_not_advance_verified_coverage(tmp_path):
    exchange, repo, runner = margin_setup(tmp_path, spot_usd="5000")
    external_transfer(exchange, "3000", "-1000")
    runner.execute_enabled = False
    assert runner.run_once(inventory_cycle=False).inventory_valid
    coverage = repo.load("futures_income_coverage")
    read = exchange.fetch_futures_income
    exchange.futures_change("USDT", D("-1"), "COMMISSION")

    def failed(*args):
        raise ExchangeError("incomplete income pagination")

    exchange.fetch_futures_income = failed
    with pytest.raises(ExchangeError, match="pagination"):
        runner.reconciler.refresh(exchange.now)
    assert repo.load("futures_income_coverage") == coverage
    exchange.fetch_futures_income = read
    assert runner.reconciler.refresh(exchange.now)[3]
    assert runner.reconciler.history[1] == D("-1001")
    repo.close()


def test_verified_futures_income_is_reused_while_spot_balance_lags(tmp_path):
    exchange, repo, runner = margin_setup(tmp_path, spot_usd="5000")
    before = exchange.fetch_account_snapshot()
    runner.executor.execute(
        AssetTransferPlan("USDT", D("500"), "USDⓈ-M Futures", "SPOT", "test"),
        exchange.now, repo.runtime().state, account=before,
    )
    read_account = exchange.fetch_account_snapshot
    exchange.fetch_account_snapshot = lambda: replace(read_account(), spot_usd=before.spot_usd)
    calls, read_income = [], exchange.fetch_futures_income

    def income(since, until):
        calls.append((since, until))
        return read_income(since, until)

    exchange.fetch_futures_income = income
    for _ in range(3):
        assert runner.reconciler.refresh(exchange.now)[2].unresolved
    assert len(calls) == 1 and len(exchange.writes) == 1
    exchange.fetch_account_snapshot = read_account
    assert not runner.reconciler.refresh(exchange.now)[2].unresolved
    assert len(calls) == 1
    repo.close()


def test_missing_tail_receipt_preserves_verified_prefix_and_repairs_only_tail(tmp_path):
    exchange, repo, runner = margin_setup(tmp_path, spot_usd="5000")
    external_transfer(exchange, "3000", "-1000")
    calls, read = [], exchange.fetch_futures_income

    def income(since, until):
        calls.append((since, until))
        return read(since, until)

    exchange.fetch_futures_income = income
    runner.execute_enabled = False
    assert runner.run_once(inventory_cycle=False).inventory_valid
    coverage = repo.load("futures_income_coverage")
    exchange.futures_change("USDT", D("-1"), "COMMISSION")
    missing = exchange.income.pop()
    for _ in range(2):
        assert not runner.run_once(inventory_cycle=False).inventory_valid
        prefix = repo.load("futures_income_coverage")
        assert prefix["since"] == coverage["since"]
        assert coverage["until"] - prefix["until"] <= timedelta(seconds=1)
    exchange.income.append(missing)
    assert runner.run_once(inventory_cycle=False).inventory_valid
    assert runner.cached_inputs.margin_change_24h == D("-1001")
    assert all(until - since < timedelta(seconds=2) for since, until in calls[1:])
    repo.close()


def test_unsupported_multi_asset_margin_cannot_hide_loss_or_fund_new_buys(tmp_path):
    exchange, repo, runner = margin_setup(tmp_path, spot_usd="5000")
    read = exchange.fetch_account_snapshot
    exchange.fetch_account_snapshot = lambda: replace(
        read(), contract_total_margin_balance=D("99000"), contract_margin_asset="USD",
    )
    exchange.income.append(FuturesIncome("TRANSFER", "usdc-credit", "USDC", D("3000"), exchange.now))
    runner.execute_enabled = False
    plan = runner.run_once()
    assert not plan.inventory_valid and not plan.transfer_decision.allow
    assert not plan.buy_plan.orders and repo.load("futures_settlement_issue")["margin"]
    assert not exchange.writes
    repo.close()


def test_history_warmup_does_not_masquerade_as_incomplete_settlement(tmp_path):
    exchange, repo, runner = margin_setup(tmp_path, spot_usd="5000", baseline_hours=0)
    runner.execute_enabled = False
    plan = runner.run_once()
    assert plan.inventory_valid and runner.cached_inputs.margin_change_24h is None
    assert not repo.load("futures_settlement_issue")
    repo.close()


def test_unknown_funding_remains_unknown_with_income_coverage_after_restart(tmp_path):
    exchange, repo, runner = margin_setup(tmp_path)
    exchange.lose_response = OperationKind.TRANSFER
    runner.run_once()
    assert len(exchange.writes) == 1
    assert repo.operations()[0].status == OperationStatus.UNKNOWN
    path = repo.path
    repo.close()
    repo = Repository(path)
    runner = Runner(exchange, repo, make_strategy_config(), clock=lambda: exchange.now)
    for _ in range(2):
        runner.run_once(inventory_cycle=False)
    assert len(exchange.writes) == 1 and repo.operations()[0].status == OperationStatus.UNKNOWN
    assert not any(op.kind == OperationKind.ORDER for op in exchange.writes)
    repo.close()


@pytest.mark.parametrize("invalid", ["stale", "nonfinite", "orders"])
def test_invalid_observation_preserves_futures_gate_and_blocks_direct_writes(tmp_path, invalid):
    exchange, repo, runner = margin_setup(tmp_path, spot_usd="5000")
    external_transfer(exchange, "3000", "-1000")
    receipts = exchange.income[:]
    exchange.income = [r for r in receipts if r.income_type != "REALIZED_PNL"]
    runner.reconciler.refresh(exchange.now)
    issue, checkpoint = repo.load("futures_settlement_issue"), repo.load("spot_checkpoint")
    assert issue
    read_account, read_orders = exchange.fetch_account_snapshot, exchange.fetch_open_orders
    if invalid == "stale":
        exchange.fetch_account_snapshot = lambda: replace(read_account(), ts=exchange.now - timedelta(minutes=1))
    elif invalid == "nonfinite":
        exchange.fetch_account_snapshot = lambda: replace(read_account(), contract_quote_balance=D("NaN"))
    else:
        calls = []

        def inconsistent_orders(symbol):
            calls.append(symbol)
            return () if len(calls) % 2 else (OrderView(
                symbol, "external", "external", None, D("500"), D("1"), D("0"), exchange.now,
            ),)

        exchange.fetch_open_orders = inconsistent_orders
    # Even receipts arriving during an invalid observation cannot reopen execution.
    exchange.income = receipts
    runner.reconciler.refresh(exchange.now)
    assert repo.load("spot_checkpoint") == checkpoint
    assert all(repo.load("futures_settlement_issue")[key] == value for key, value in issue.items())
    assert "observation" in repo.load("futures_settlement_issue")
    path = repo.path
    repo.close()
    repo = Repository(path)
    runner = Runner(exchange, repo, make_strategy_config(), clock=lambda: exchange.now, execute=False)
    exchange.fetch_account_snapshot, exchange.fetch_open_orders = read_account, read_orders
    for payload in (
        AssetTransferPlan("USDT", D("500"), "USDⓈ-M Futures", "SPOT", "test"),
        OrderPlan("BNBUSDT", "BUY", "LIMIT", D("1"), D("500"), "GTC", False),
    ):
        with pytest.raises(RuntimeError, match="reconciled"):
            runner.executor.execute(payload, exchange.now, repo.runtime().state, account=read_account())
    plan = runner.run_once(inventory_cycle=False)
    assert plan.inventory_valid and runner.cached_inputs.margin_change_24h == D("-1000")
    assert not repo.load("futures_settlement_issue") and not exchange.writes
    repo.close()


@pytest.mark.parametrize("currency,stale", [("USD", False), ("USDT", True)])
def test_direct_execution_validates_its_account_without_an_extra_read(tmp_path, currency, stale):
    exchange, repo, runner = margin_setup(tmp_path, spot_usd="5000")
    account = replace(exchange.fetch_account_snapshot(), contract_margin_asset=currency)
    if stale:
        account = replace(account, ts=exchange.now - timedelta(minutes=1))

    def unexpected_read():
        pytest.fail("A supplied snapshot must not cause another account query")

    exchange.fetch_account_snapshot = unexpected_read
    with pytest.raises(RuntimeError, match="observation"):
        runner.executor.execute(
            AssetTransferPlan("USDT", D("500"), "USDⓈ-M Futures", "SPOT", "test"),
            exchange.now, repo.runtime().state, account=account,
        )
    assert not exchange.writes and not repo.operations()
    repo.close()


@pytest.mark.parametrize("kind", [
    "INTERNAL_TRANSFER", "CROSS_COLLATERAL_TRANSFER", "STRATEGY_UMFUTURES_TRANSFER",
    "AUTO_EXCHANGE", "COIN_SWAP_DEPOSIT", "COIN_SWAP_WITHDRAW", "NEW_CAPITAL_TYPE",
])
@pytest.mark.parametrize("baseline_hours", [0, 24])
def test_other_capital_income_blocks_buys_and_reuses_coverage_after_restart(tmp_path, kind, baseline_hours):
    exchange, repo, runner = margin_setup(tmp_path, spot_usd="5000", baseline_hours=baseline_hours)
    checkpoint = repo.load("spot_checkpoint")
    exchange.futures_change("USDT", D("3000"), kind)
    exchange.futures_change("USDT", D("-1000"), "REALIZED_PNL")
    calls, read = [], exchange.fetch_futures_income

    def income(since, until):
        calls.append((since, until))
        return read(since, until)

    exchange.fetch_futures_income = income
    runner.execute_enabled = False
    for restart in (False, False, True):
        if restart:
            path = repo.path
            repo.close()
            repo = Repository(path)
            runner = Runner(exchange, repo, make_strategy_config(), clock=lambda: exchange.now, execute=False)
        plan = runner.run_once(inventory_cycle=False)
        assert not plan.inventory_valid and not plan.buy_plan.orders
        assert not plan.transfer_decision.allow and plan.bnb_transfer_plan is None
        assert repo.load("spot_checkpoint") == checkpoint
        assert repo.load("futures_settlement_issue")["quote"]["income_types"] == (kind,)
        with pytest.raises(RuntimeError, match="reconciled"):
            runner.executor.execute(
                AssetTransferPlan("USDT", D("500"), "USDⓈ-M Futures", "SPOT", "test"),
                exchange.now, repo.runtime().state,
            )
    assert len(calls) == 1 and repo.load("futures_income_coverage")
    assert len(repo.futures_income_since(calls[0][0])) == 2 and not exchange.writes
    repo.close()


@pytest.mark.parametrize("baseline_hours", [0, 24])
@pytest.mark.parametrize("missing", ["both", "income", "transfers"])
def test_late_round_trip_receipts_replay_earlier_checkpoint_after_restart(tmp_path, baseline_hours, missing):
    exchange, repo, runner = margin_setup(tmp_path, spot_usd="5000", baseline_hours=baseline_hours)
    initial = exchange.fetch_account_snapshot()
    external_transfer(exchange, "3000")
    external_transfer(exchange, "-3000")
    transfers, receipts = exchange.transfers.copy(), exchange.income[:]
    if missing == "both":
        # A published fee can explain the wallet while the round trip is late.
        exchange.futures_change("USDT", D("-1"), "COMMISSION")
        receipts = exchange.income[:]
    if missing != "income":
        exchange.transfers = {}
    if missing != "transfers":
        exchange.income = [r for r in receipts if r.income_type == "COMMISSION"]
    calls, read = [], exchange.fetch_futures_income

    def income(since, until):
        calls.append((since, until))
        return read(since, until)

    exchange.fetch_futures_income = income
    runner.execute_enabled = False
    runner.run_once(inventory_cycle=False)
    if missing == "both":
        # The known fee agrees before either transfer history publishes.
        assert repo.load("spot_checkpoint")["account"].contract_quote_updated_at == exchange.account.contract_quote_updated_at
        assert any(s.contract_quote_updated_at == initial.contract_quote_updated_at
                   for s in repo.snapshots_since(initial.ts - timedelta(hours=25)))
    path = repo.path
    repo.close()
    repo = Repository(path)
    runner = Runner(exchange, repo, make_strategy_config(), clock=lambda: exchange.now, execute=False)
    exchange.transfers, exchange.income = transfers, receipts
    # The first pass may invalidate a previously verified empty prefix.
    for _ in range(2):
        plan = runner.run_once(inventory_cycle=False)
    assert plan.inventory_valid
    expected = D("-1") if missing == "both" else D("0")
    assert runner.cached_inputs.margin_change_24h == (expected if baseline_hours == 24 else None)
    assert not repo.load("futures_settlement_issue") and repo.wallet_cursor() == 2
    assert repo.load("spot_checkpoint")["cursor"] == 2
    assert len(repo.futures_income_since(calls[0][0])) == len(receipts)
    assert repo.budget_used(exchange.now, "USDT") == D("3000")
    settled_calls = len(calls)
    for _ in range(3):
        assert runner.run_once(inventory_cycle=False).inventory_valid
    assert len(calls) == settled_calls and settled_calls <= 2
    assert not exchange.writes
    repo.close()


def test_foreign_currency_policy_keeps_verified_income_coverage_across_restart(tmp_path):
    exchange, repo, runner = margin_setup(tmp_path, spot_usd="5000")
    exchange.futures_change("USDT", D("-1000"), "REALIZED_PNL")
    exchange.transfers["usdc"] = TransferRecord(
        "usdc", "USDC", D("3000"), "SPOT", "USDⓈ-M Futures", exchange.now, OperationStatus.CONFIRMED,
    )
    exchange.income.append(FuturesIncome("TRANSFER", "usdc", "USDC", D("3000"), exchange.now))
    calls, read = [], exchange.fetch_futures_income

    def income(since, until):
        calls.append((since, until))
        return read(since, until)

    exchange.fetch_futures_income = income
    runner.execute_enabled = False
    for restart in (False, False, True):
        if restart:
            path = repo.path
            repo.close()
            repo = Repository(path)
            runner = Runner(exchange, repo, make_strategy_config(), clock=lambda: exchange.now, execute=False)
        plan = runner.run_once(inventory_cycle=False)
        assert not plan.inventory_valid and not plan.buy_plan.orders and not plan.transfer_decision.allow
        assert repo.load("futures_settlement_issue")
    assert len(calls) == 1 and repo.load("futures_income_coverage")
    exchange.futures_change("USDT", D("-1"), "COMMISSION")
    assert not runner.run_once(inventory_cycle=False).inventory_valid
    assert len(calls) == 2 and calls[1][0] == calls[0][1]
    assert calls[1][1] - calls[1][0] < timedelta(seconds=1)
    assert not exchange.writes
    repo.close()


def test_late_old_foreign_round_trip_recovers_without_current_conversion_gate(tmp_path):
    exchange, repo, runner = margin_setup(tmp_path, spot_usd="5000")
    original = exchange.fetch_account_snapshot()
    repo.save_snapshot(replace(original, ts=exchange.now - timedelta(hours=48),
                               contract_quote_updated_at=exchange.now - timedelta(hours=48)))
    exchange.futures_change("USDT", D("-1"), "COMMISSION")
    calls, read = [], exchange.fetch_futures_income

    def income(since, until):
        calls.append((since, until))
        return read(since, until)

    exchange.fetch_futures_income = income
    runner.execute_enabled = False
    assert runner.run_once(inventory_cycle=False).inventory_valid
    credit = TransferRecord("late-usdc-in", "USDC", D("3000"), "SPOT", "USDⓈ-M Futures",
                            original.ts - timedelta(hours=27), OperationStatus.CONFIRMED)
    debit = replace(credit, transfer_id="late-usdc-out", from_account="USDⓈ-M Futures",
                    to_account="SPOT", ts=original.ts - timedelta(hours=26))
    exchange.transfers.update({credit.transfer_id: credit, debit.transfer_id: debit})
    exchange.income.extend((
        FuturesIncome("TRANSFER", credit.transfer_id, "USDC", credit.amount, credit.ts),
        FuturesIncome("TRANSFER", debit.transfer_id, "USDC", -debit.amount, debit.ts),
    ))
    assert not runner.run_once(inventory_cycle=False).inventory_valid
    path = repo.path
    repo.close()
    repo = Repository(path)
    runner = Runner(exchange, repo, make_strategy_config(), clock=lambda: exchange.now, execute=False)
    for _ in range(3):
        plan = runner.run_once(inventory_cycle=False)
        assert plan.inventory_valid and runner.cached_inputs.margin_change_24h == D("-1")
        assert not repo.load("futures_settlement_issue")
    assert repo.load("spot_checkpoint")["cursor"] == repo.wallet_cursor() == 2
    assert len(calls) == 2 and calls[1][0] < credit.ts
    assert not exchange.writes
    repo.close()


def partial_capital_setup(tmp_path, *, baseline_hours=24):
    exchange, repo, runner = margin_setup(tmp_path, spot_usd="5000", baseline_hours=baseline_hours)
    exchange.futures_change("USDT", D("3000"), "INTERNAL_TRANSFER")
    exchange.futures_change("USDT", D("-3000"), "REALIZED_PNL")
    exchange.futures_change("USDT", D("-1"), "COMMISSION")
    receipts = exchange.income[:]
    exchange.income = receipts[-1:]
    calls, read = [], exchange.fetch_futures_income

    def income(since, until):
        calls.append((since, until))
        return read(since, until)

    exchange.fetch_futures_income = income
    runner.execute_enabled = False
    assert runner.run_once(inventory_cycle=False).inventory_valid
    assert len(calls) == 1
    return exchange, repo, runner, receipts, calls


@pytest.mark.parametrize("baseline_hours", [0, 24])
def test_partial_income_is_rechecked_on_persisted_deadline_after_restart(tmp_path, baseline_hours):
    exchange, repo, runner, receipts, calls = partial_capital_setup(tmp_path, baseline_hours=baseline_hours)
    coverage = repo.load("futures_income_coverage")
    # An incremental read must not postpone the full interval's deadline.
    exchange.futures_change("USDT", D("-1"), "COMMISSION")
    extra = exchange.income[-1]
    assert runner.run_once(inventory_cycle=False).inventory_valid
    assert len(calls) == 2 and calls[1][0] == calls[0][1]
    assert repo.load("futures_income_coverage")["checked_at"] == coverage["checked_at"]
    exchange.income = receipts + [extra]
    exchange.now = coverage["checked_at"] + INCOME_RECHECK_INTERVAL - timedelta(microseconds=1)
    assert runner.run_once(inventory_cycle=False).inventory_valid
    assert len(calls) == 2
    path = repo.path
    repo.close()
    repo = Repository(path)
    runner = Runner(exchange, repo, make_strategy_config(), clock=lambda: exchange.now, execute=False)
    exchange.now += timedelta(microseconds=1)
    plan = runner.run_once(inventory_cycle=False)
    assert not plan.inventory_valid and not plan.buy_plan.orders and not plan.transfer_decision.allow
    assert len(calls) == 3 and calls[2][0] == calls[0][0]
    assert len(repo.futures_income_since(calls[0][0])) == 4
    assert repo.load("futures_settlement_issue")["margin"]["income_types"] == ("INTERNAL_TRANSFER",)
    for _ in range(3):
        assert not runner.run_once(inventory_cycle=False).inventory_valid
    assert len(calls) == 3 and not exchange.writes
    repo.close()


@pytest.mark.parametrize("baseline_hours", [0, 24])
@pytest.mark.parametrize("new_wallet_version", [False, True])
def test_new_inventory_batch_rechecks_partial_income_before_advancing_state(
    tmp_path, baseline_hours, new_wallet_version
):
    exchange, repo, runner, receipts, calls = partial_capital_setup(tmp_path, baseline_hours=baseline_hours)
    exchange.income = receipts
    if new_wallet_version:
        exchange.futures_change("USDT", D("-1"), "COMMISSION")
    snapshots = repo.snapshots_since(exchange.now - timedelta(hours=49))
    account_reads, read = [], exchange.fetch_account_snapshot

    def account():
        account_reads.append(1)
        return read()

    exchange.fetch_account_snapshot = account
    plan = runner.run_once()
    assert not plan.inventory_valid and not plan.buy_plan.orders and not plan.transfer_decision.allow
    assert len(calls) == 2 and len(account_reads) == 1
    assert calls[1][0] == calls[0][0]
    retained = repo.snapshots_since(exchange.now - timedelta(hours=49))
    assert all(s in retained for s in snapshots)
    assert all(s.ts < exchange.now for s in retained)
    assert repo.runtime().last_inventory_at is None
    assert repo.load("futures_settlement_issue") and not repo.operations() and not exchange.writes
    repo.close()


@pytest.mark.parametrize("payload", [
    AssetTransferPlan("USDT", D("500"), "USDⓈ-M Futures", "SPOT", "test"),
    OrderPlan("BNBUSDT", "BUY", "LIMIT", D("1"), D("500"), "GTC", False),
])
def test_direct_execution_rechecks_partial_income_without_an_account_query(tmp_path, payload):
    exchange, repo, runner, receipts, calls = partial_capital_setup(tmp_path)
    exchange.income = receipts
    account = exchange.fetch_account_snapshot()

    def unexpected_account():
        pytest.fail("Income audit must reuse the supplied account")

    exchange.fetch_account_snapshot = unexpected_account
    with pytest.raises(RuntimeError, match="reconciled futures income"):
        runner.executor.execute(payload, exchange.now, repo.runtime().state, account=account)
    assert len(calls) == 2 and repo.load("futures_settlement_issue")
    assert not repo.operations() and not exchange.writes
    repo.close()


def test_failed_income_recheck_cannot_extend_lease_or_create_an_intent(tmp_path):
    exchange, repo, runner, receipts, calls = partial_capital_setup(tmp_path)
    coverage = repo.load("futures_income_coverage")
    exchange.now = coverage["checked_at"] + INCOME_RECHECK_INTERVAL
    read = exchange.fetch_futures_income

    def failed(*args):
        raise ExchangeError("incomplete income pagination")

    exchange.fetch_futures_income = failed
    with pytest.raises(ExchangeError, match="pagination"):
        runner.reconciler.refresh(exchange.now)
    assert repo.load("futures_income_coverage") == coverage
    with pytest.raises(ExchangeError, match="pagination"):
        runner.executor.execute(
            AssetTransferPlan("USDT", D("500"), "USDⓈ-M Futures", "SPOT", "test"),
            exchange.now, repo.runtime().state,
        )
    assert repo.load("futures_income_coverage") == coverage and not repo.operations()
    exchange.fetch_futures_income = read
    exchange.income = receipts
    assert not runner.run_once(inventory_cycle=False).inventory_valid
    assert len(calls) == 2 and not exchange.writes
    repo.close()


def test_old_coverage_without_a_recheck_timestamp_is_not_permanently_trusted(tmp_path):
    exchange, repo, runner, receipts, calls = partial_capital_setup(tmp_path)
    coverage = repo.load("futures_income_coverage")
    repo.save("futures_income_coverage", {k: coverage[k] for k in ("since", "until")})
    path = repo.path
    repo.close()
    repo = Repository(path)
    runner = Runner(exchange, repo, make_strategy_config(), clock=lambda: exchange.now, execute=False)
    exchange.income = receipts
    assert not runner.run_once(inventory_cycle=False).inventory_valid
    assert len(calls) == 2 and repo.load("futures_income_coverage")["checked_at"] == exchange.now
    assert not runner.run_once(inventory_cycle=False).inventory_valid
    assert len(calls) == 2 and not exchange.writes
    repo.close()


@pytest.mark.parametrize("new_wallet_version", [False, True])
@pytest.mark.parametrize("inventory_cycle", [False, True])
def test_new_execution_batch_audits_cached_income_once_for_all_orders(
    tmp_path, new_wallet_version, inventory_cycle
):
    # Fund the full target from spot even after the manual transfer to futures.
    exchange, repo, runner = margin_setup(tmp_path, spot_usd="10000")
    external_transfer(exchange, "3000", "-1000")
    calls, read = [], exchange.fetch_futures_income

    def income(since, until):
        calls.append((since, until))
        return read(since, until)

    exchange.fetch_futures_income = income
    runner.execute_enabled = False
    assert runner.run_once(inventory_cycle=False).inventory_valid and len(calls) == 1
    if new_wallet_version:
        exchange.futures_change("USDT", D("-1"), "COMMISSION")
    if not inventory_cycle:
        repo.save("runtime", replace(repo.runtime(), resume_replenishment=True))
    runner.execute_enabled = True
    plan = runner.run_once(inventory_cycle=inventory_cycle)
    assert plan.inventory_valid and plan.active_buy_target == D("32")
    assert len(exchange.writes) == 3 and all(op.kind == OperationKind.ORDER for op in exchange.writes)
    assert len(calls) == 2 and calls[1] == (calls[0][0], exchange.account.contract_quote_updated_at)
    assert not repo.operations(unresolved_only=True)
    repo.close()


def test_new_wallet_version_without_income_cannot_verify_a_hidden_capital_offset(tmp_path):
    exchange, repo, runner = margin_setup(tmp_path, spot_usd="5000")
    checkpoint = repo.load("spot_checkpoint")
    exchange.futures_change("USDT", D("3000"), "INTERNAL_TRANSFER")
    exchange.futures_change("USDT", D("-3000"), "REALIZED_PNL")
    receipts, exchange.income = exchange.income[:], []
    runner.execute_enabled = False
    assert not runner.run_once(inventory_cycle=False).inventory_valid
    assert repo.load("spot_checkpoint") == checkpoint
    assert repo.load("futures_income_coverage") is None
    path = repo.path
    repo.close()
    repo = Repository(path)
    runner = Runner(exchange, repo, make_strategy_config(), clock=lambda: exchange.now, execute=False)
    exchange.income = receipts
    assert not runner.run_once(inventory_cycle=False).inventory_valid
    assert repo.load("futures_settlement_issue")["quote"]["income_types"] == ("INTERNAL_TRANSFER",)
    assert repo.load("futures_income_coverage") and not exchange.writes
    repo.close()
