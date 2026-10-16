"""Income scan completion and final account freshness are separate evidence."""

import sqlite3
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace

import pytest

from core.models import (
    AssetTransferPlan, CrashGuardState, FuturesIncome, OperationKind, OperationStatus,
    OrderPlan, ReplenishmentState, RunMode, SliceState, TransferRecord,
)
from services.binance_adapter import BinanceAdapter, millis
from services.exchange_adapter import ExchangeError
from services.executor import ExecutionBlocked
from tests.helpers import (
    D, make_account, make_engine_inputs, make_market, make_strategy_config, order,
)
from tests.fake_exchange import FakeExchange
from tests.execution_helpers import setup, seed_bid
from tests.integration.test_margin_adjusted_target import margin_setup, partial_capital_setup


def slow_income_setup(tmp_path, *, with_bid=False, contract_bnb="27", spot_bnb="0.5"):
    exchange = FakeExchange(make_account(
        contract_bnb=D(contract_bnb), contract_max_withdraw_amount=D("97000"),
        spot_bnb=D(spot_bnb), spot_usd=D("10000"),
    ))
    started = exchange.now
    exchange.now -= timedelta(hours=24)
    exchange.account = replace(
        exchange.account, contract_quote_updated_at=exchange.now - timedelta(seconds=1),
        contract_bnb_updated_at=exchange.now - timedelta(seconds=1),
    )
    read_account = exchange.fetch_account_snapshot

    def margin_account():
        observed = read_account()
        return replace(observed, contract_total_margin_balance=observed.contract_quote_balance,
                       contract_margin_asset="USDT")

    exchange.fetch_account_snapshot = margin_account
    exchange, repo, runner = setup(tmp_path, exchange, ReplenishmentState.ACCUMULATE)
    if with_bid:
        seed_bid(exchange, repo)
    repo.save_snapshot(exchange.fetch_account_snapshot())
    exchange.now = started
    first_income_at = started - timedelta(hours=23)
    exchange.income = [
        FuturesIncome("COMMISSION", str(index + 1), "USDT", D("-0.001"),
                      first_income_at + timedelta(milliseconds=33120 * index))
        for index in range(2501)
    ]
    exchange.account = replace(
        exchange.account,
        contract_quote_balance=exchange.account.contract_quote_balance - D("2.501"),
        contract_quote_updated_at=started,
    )
    queries, pages, account_reads = [], [], []

    class FuturesPages:
        def get_income_history(self, **params):
            pages.append((params["start_time"], params["end_time"], params["page"]))
            rows = [{"incomeType": row.income_type, "tranId": row.transaction_id,
                     "asset": row.asset, "income": str(row.amount), "time": millis(row.ts)}
                    for row in exchange.income
                    if params["start_time"] <= millis(row.ts) <= params["end_time"]]
            offset = (params["page"] - 1) * params["limit"]
            page = rows[offset:offset + params["limit"]]
            exchange.now += timedelta(seconds=4)
            hook = getattr(exchange, "income_page_hook", None)
            if hook is not None:
                page = hook(params, page)
            return SimpleNamespace(data=lambda: page, headers={})

    # Exercise the real adapter's retention, seven-day segmentation, page loop,
    # duplicate validation and SDK transport checkpoints without network I/O.
    adapter = BinanceAdapter(
        "fixture-key", "fixture-secret", make_strategy_config(),
        clients=(SimpleNamespace(), FuturesPages(), SimpleNamespace()),
        clock=lambda: exchange.now,
    )
    adapter.read_checkpoint = runner.reconciler.read_checkpoint

    def income(since, until):
        queries.append((since, until))
        return adapter.fetch_futures_income(since, until)

    snapshot = exchange.fetch_account_snapshot

    def account():
        observed = snapshot()
        account_reads.append(observed)
        return observed

    exchange.fetch_futures_income = income
    exchange.fetch_account_snapshot = account
    return exchange, repo, runner, queries, pages, account_reads


def test_slow_full_income_scan_reuses_completion_with_a_fresh_final_account(tmp_path):
    exchange, repo, runner, queries, pages, account_reads = slow_income_setup(tmp_path)
    try:
        plan = runner.run_once()
        full_since = queries[0][0]
        metrics = {
            "full_queries": sum(since == full_since for since, _ in queries),
            "increment_queries": sum(since > full_since for since, _ in queries),
            "pages": len(pages),
            "original_account_age_seconds": (exchange.now - account_reads[0].ts).total_seconds(),
            "final_account_age_seconds": (exchange.now - runner.cached_inputs.account.ts).total_seconds(),
            "legal_plan": bool(plan is not None and plan.inventory_valid),
            "writes": len(exchange.writes),
            "coverage_saved": repo.load("futures_income_coverage") is not None,
        }
        assert metrics["legal_plan"], metrics
        assert metrics == {
            "full_queries": 1, "increment_queries": 0, "pages": 3,
            "original_account_age_seconds": 12.0, "final_account_age_seconds": 0.0,
            "legal_plan": True, "writes": 3, "coverage_saved": True,
        }
        assert account_reads[-1].ts > account_reads[0].ts
        assert runner.cached_inputs.margin_change_24h == D("-2.501")
        assert all(op.kind == OperationKind.ORDER for op in exchange.writes)
        assert sum(op.payload.qty for op in exchange.writes) == D("5")
        assert not repo.operations(unresolved_only=True)
    finally:
        repo.close()


@pytest.mark.parametrize("execute", [False, True])
def test_idle_maintenance_reuses_finite_income_coverage_without_full_audits(tmp_path, execute):
    exchange, repo, runner = margin_setup(tmp_path)
    exchange.account = replace(exchange.account, contract_bnb=D("32"))
    repo.save("runtime", replace(repo.runtime(), state=ReplenishmentState.IDLE))
    runner.execute_enabled = execute
    exchange.futures_change("USDT", D("-1"), "COMMISSION")
    requests, read = [], exchange.fetch_futures_income

    def income(since, until):
        requests.append((since, until))
        return read(since, until)

    exchange.fetch_futures_income = income
    try:
        assert runner.run_once(inventory_cycle=False).inventory_valid
        checked = repo.load("futures_income_coverage")["checked_at"]
        for _ in range(3):
            exchange.now += timedelta(seconds=10)
            assert runner.run_once(inventory_cycle=False).inventory_valid
        assert len(requests) == 1
        assert not exchange.writes
        assert repo.runtime().last_inventory_at is None
        exchange.now = checked + timedelta(minutes=5)
        assert runner.run_once(inventory_cycle=False).inventory_valid
        assert len(requests) == 2 and requests[1] == requests[0]
    finally:
        repo.close()


@pytest.mark.parametrize("execute", [False, True])
@pytest.mark.parametrize("change", ["urgent", "idle", "slice"])
def test_maintenance_audits_before_inventory_state_or_slice_changes(tmp_path, execute, change):
    exchange, repo, runner, receipts, calls = partial_capital_setup(tmp_path)
    exchange.income = receipts
    runner.execute_enabled = execute
    if change == "slice":
        runner.cfg = replace(runner.cfg, execution=replace(runner.cfg.execution, slice_threshold_bnb=D("4")))
    else:
        # Threshold changes model a new decision on an otherwise unchanged,
        # already verified account, so the hidden income is the only defect.
        thresholds = (replace(runner.cfg.thresholds, b_low=D("28"), b_alert=D("29"))
                      if change == "urgent" else replace(runner.cfg.thresholds, b_target=D("27"), b_alert=D("26")))
        runner.cfg = replace(runner.cfg, thresholds=thresholds)
    before = repo.runtime()
    try:
        plan = runner.run_once(inventory_cycle=False)
        assert len(calls) == 2, (change, plan)
        assert not plan.inventory_valid
        after = repo.runtime()
        assert (after.state, after.candidate, after.candidate_streak, after.slice_state) == (
            before.state, before.candidate, before.candidate_streak, before.slice_state)
        assert after.last_inventory_at is None
        assert not repo.operations() and not exchange.writes
    finally:
        repo.close()


def test_cached_tick_routes_new_state_decision_through_full_audit(tmp_path):
    exchange, repo, runner, receipts, calls = partial_capital_setup(tmp_path)
    exchange.income = receipts
    repo.save("runtime", replace(repo.runtime(), last_inventory_at=exchange.now))
    runner.cfg = replace(runner.cfg, thresholds=replace(
        runner.cfg.thresholds, b_low=D("28"), b_alert=D("29")))
    before = repo.runtime()
    try:
        plan = runner.tick()
        assert len(calls) == 2
        assert not plan.inventory_valid
        assert repo.runtime().state == before.state
        assert repo.runtime().candidate == before.candidate
        assert repo.runtime().last_inventory_at == before.last_inventory_at
        assert not repo.operations() and not exchange.writes
    finally:
        repo.close()


def test_direct_order_finishes_slow_audit_against_new_account(tmp_path):
    exchange, repo, runner, queries, pages, account_reads = slow_income_setup(tmp_path)
    supplied = exchange.fetch_account_snapshot()
    try:
        operation = runner.executor.execute(
            OrderPlan("BNBUSDT", "BUY", "LIMIT", D("1"), D("500"), "GTC", False),
            exchange.now, repo.runtime().state, account=supplied,
        )
        assert operation.status == OperationStatus.CONFIRMED
        assert operation.balance_before.ts > supplied.ts
        assert operation.created_at == operation.balance_before.ts == exchange.now
        assert len(queries) == 1 and len(pages) == 3 and len(exchange.writes) == 1
        assert repo.load("futures_income_coverage") is not None
        assert not runner.executor.income_audited(exchange.now)
    finally:
        repo.close()


@pytest.mark.parametrize("filled", [False, True])
def test_direct_order_discards_old_demand_when_slow_audit_observes_external_buy(tmp_path, filled):
    exchange, repo, runner, queries, pages, accounts = slow_income_setup(tmp_path)
    supplied = exchange.fetch_account_snapshot()

    def external_buy(params, rows):
        if len(pages) == 1:
            exchange.orders["manual"] = order(
                "manual", price="500", qty="5", strategy_id=None, created_at=exchange.now,
            )
            if filled:
                exchange.fill("manual", D("5"))
        return rows

    exchange.income_page_hook = external_buy
    try:
        with pytest.raises(ExecutionBlocked, match="balances or orders changed"):
            runner.executor.execute(
                OrderPlan("BNBUSDT", "BUY", "LIMIT", D("5"), D("500"), "GTC", False),
                exchange.now, repo.runtime().state, account=supplied,
            )
        assert accounts[-1].reserved_spot_usd == D("0" if filled else "2500")
        assert accounts[-1].spot_bnb == D("5.5" if filled else "0.5")
        assert len(queries) == 1 and len(pages) == 3
        assert not repo.operations() and not exchange.writes
        assert repo.runtime().api_errors == repo.runtime().transfer_failures == 0
    finally:
        repo.close()


@pytest.mark.parametrize("source", ["market", "filters"])
@pytest.mark.parametrize("age,allow", [(5, True), (5.000001, False)])
def test_direct_audit_preserves_supplied_market_and_filter_lifetime(tmp_path, source, age, allow):
    exchange, repo, runner = margin_setup(tmp_path, spot_usd="10000")
    exchange.futures_change("USDT", D("-1"), "COMMISSION")
    supplied = exchange.fetch_account_snapshot()
    inputs = make_engine_inputs(account=supplied)
    inputs = replace(
        inputs, market=replace(inputs.market, ts=exchange.now),
        filters=replace(inputs.filters, observed_at=(
            exchange.now - timedelta(seconds=age - 1) if source == "filters" else None
        )),
    )
    fetch = exchange.fetch_futures_income

    def delayed_income(since, until):
        rows = fetch(since, until)
        exchange.now += timedelta(seconds=age if source == "market" else 1)
        return rows

    def unexpected_account_read():
        pytest.fail("A still-fresh supplied account must not be queried again")

    exchange.fetch_futures_income = delayed_income
    exchange.fetch_account_snapshot = unexpected_account_read
    candidate = OrderPlan("BNBUSDT", "BUY", "LIMIT", D("1"), D("500"), "GTC", False)
    try:
        if allow:
            op = runner.executor.execute(candidate, exchange.now, repo.runtime().state,
                                         account=supplied, inputs=inputs)
            assert op.status == OperationStatus.CONFIRMED and len(exchange.writes) == 1
        else:
            with pytest.raises(ExecutionBlocked, match="expired"):
                runner.executor.execute(candidate, exchange.now, repo.runtime().state,
                                         account=supplied, inputs=inputs)
            assert not repo.operations() and not exchange.writes
    finally:
        repo.close()


@pytest.mark.parametrize("with_inputs", [False, True])
def test_slow_audit_rejects_changed_order_exposure_even_when_balances_are_unchanged(tmp_path, with_inputs):
    exchange, repo, runner, queries, pages, accounts = slow_income_setup(tmp_path)
    original_order = order("manual", price="500", qty="1", strategy_id=None, created_at=exchange.now)
    exchange.orders["manual"] = original_order
    supplied = exchange.fetch_account_snapshot()
    inputs = replace(make_engine_inputs(account=supplied), orders=(original_order,)) if with_inputs else None

    def replace_external_buy(params, rows):
        if len(pages) == 1:
            exchange.orders["manual"] = replace(original_order, status="CANCELED")
            exchange.orders["replacement"] = order(
                "replacement", price="100", qty="5", strategy_id=None, created_at=exchange.now,
            )
        return rows

    exchange.income_page_hook = replace_external_buy
    try:
        with pytest.raises(ExecutionBlocked, match="orders.*replan|reconciled.*account"):
            runner.executor.execute(
                OrderPlan("BNBUSDT", "BUY", "LIMIT", D("4"), D("500"), "GTC", False),
                exchange.now, repo.runtime().state, account=supplied, inputs=inputs,
            )
        assert accounts[-1].reserved_spot_usd == supplied.reserved_spot_usd == D("500")
        assert accounts[-1].spot_bnb == supplied.spot_bnb == D("0.5")
        assert len(queries) == 1 and len(pages) == 3
        assert not repo.operations() and not exchange.writes
    finally:
        repo.close()


@pytest.mark.parametrize("kind", ["bnb", "sweep"])
@pytest.mark.parametrize("changed", [False, True])
def test_slow_audit_keeps_transfer_candidate_only_if_financial_demand_is_unchanged(tmp_path, kind, changed):
    exchange, repo, runner, queries, pages, accounts = slow_income_setup(
        tmp_path, spot_bnb="5.5" if kind == "bnb" else "0.5",
        contract_bnb="27" if kind == "bnb" else "32",
    )
    if kind == "sweep":
        repo.save("runtime", replace(repo.runtime(), state=ReplenishmentState.IDLE))
    supplied = exchange.fetch_account_snapshot()
    candidate = AssetTransferPlan(
        "BNB" if kind == "bnb" else "USDT", D("5" if kind == "bnb" else "9500"),
        "SPOT", "USDⓈ-M Futures", "replenish" if kind == "bnb" else "sweep idle funds",
    )
    # BNB returns do not need fresh market data; their static quantity filters
    # and all balance/settlement constraints still apply.
    inputs = make_engine_inputs(account=supplied) if kind == "bnb" else None

    def external_activity(params, rows):
        if changed and len(pages) == 1:
            if kind == "bnb":
                exchange.transfers["manual"] = TransferRecord(
                    "manual", "BNB", D("4"), "SPOT", "USDⓈ-M Futures",
                    exchange.now, OperationStatus.PENDING,
                )
                exchange.settle("manual")
            else:
                exchange.orders["manual"] = order(
                    "manual", price="500", qty="5", strategy_id=None, created_at=exchange.now,
                )
        return rows

    exchange.income_page_hook = external_activity
    try:
        if changed:
            with pytest.raises(ExecutionBlocked, match="balances or orders changed"):
                runner.executor.execute(candidate, exchange.now, repo.runtime().state,
                                         account=supplied, inputs=inputs)
            assert not repo.operations() and not exchange.writes
        else:
            operation = runner.executor.execute(candidate, exchange.now, repo.runtime().state,
                                                account=supplied, inputs=inputs)
            assert operation.status == OperationStatus.PENDING
            assert operation.balance_before.ts > supplied.ts
            assert len(exchange.writes) == 1 and exchange.writes[0].payload == candidate
        assert not repo.load("spot_settlement_issue") and not repo.load("futures_settlement_issue")
    finally:
        repo.close()


@pytest.mark.parametrize("kind", ["order", "bnb", "sweep"])
def test_fresh_account_after_slow_audit_does_not_renew_other_candidate_inputs(tmp_path, kind):
    exchange, repo, runner, queries, pages, accounts = slow_income_setup(
        tmp_path, spot_bnb="5.5" if kind == "bnb" else "0.5",
        contract_bnb="32" if kind == "sweep" else "27",
    )
    supplied = exchange.fetch_account_snapshot()
    inputs = make_engine_inputs(account=supplied)
    inputs = replace(inputs, filters=replace(inputs.filters, observed_at=exchange.now))
    candidate = (OrderPlan("BNBUSDT", "BUY", "LIMIT", D("1"), D("500"), "GTC", False)
                 if kind == "order" else AssetTransferPlan(
                     "BNB" if kind == "bnb" else "USDT", D("5" if kind == "bnb" else "9500"),
                     "SPOT", "USDⓈ-M Futures", "return available assets",
                 ))
    try:
        with pytest.raises(ExecutionBlocked, match="expired"):
            runner.executor.execute(candidate, exchange.now, repo.runtime().state,
                                     account=supplied, inputs=inputs)
        assert accounts[-1].ts > supplied.ts and accounts[-1].ts == exchange.now
        assert len(queries) == 1 and len(pages) == 3
        assert not repo.operations() and not exchange.writes
    finally:
        repo.close()


@pytest.mark.parametrize("constraint", ["guard", "run_mode"])
def test_slow_direct_audit_discards_candidate_when_execution_constraints_change(tmp_path, constraint):
    exchange, repo, runner, queries, pages, accounts = slow_income_setup(tmp_path)
    supplied = exchange.fetch_account_snapshot()

    def change_constraint(params, rows):
        if len(pages) == 1:
            change = {"run_mode": RunMode.URGENT_ONLY} if constraint == "run_mode" else {
                "guard": CrashGuardState(active=True, episode_id="new-protection", started_at=exchange.now),
            }
            repo.save("runtime", replace(repo.runtime(), **change))
        return rows

    exchange.income_page_hook = change_constraint
    try:
        with pytest.raises(ExecutionBlocked, match="Risk protection changed|run mode"):
            runner.executor.execute(
                OrderPlan("BNBUSDT", "BUY", "LIMIT", D("1"), D("500"), "GTC", False),
                exchange.now, repo.runtime().state, account=supplied,
            )
        assert not repo.operations() and not exchange.writes
        assert repo.runtime().api_errors == repo.runtime().transfer_failures == 0
    finally:
        repo.close()


@pytest.mark.parametrize("waiting", ["paused", "slice", "chase", "market"])
def test_waiting_continuation_remains_maintenance_until_a_write_is_possible(tmp_path, waiting):
    exchange, repo, runner = margin_setup(tmp_path, spot_usd="2500")
    exchange.futures_change("USDT", D("-1"), "COMMISSION")
    runner.execute_enabled = False
    assert runner.run_once(inventory_cycle=False).inventory_valid
    runtime = replace(repo.runtime(), resume_replenishment=True)
    if waiting == "paused":
        runtime = replace(runtime, run_mode=RunMode.PAUSED)
    elif waiting == "slice":
        runtime = replace(runtime, slice_state=SliceState(True, exchange.now + timedelta(minutes=1)))
    else:
        market = exchange.fetch_market_snapshot

        def blocked_market(symbol):
            observed = market(symbol)
            return (replace(observed, return_1h=D("0.1")) if waiting == "chase"
                    else replace(observed, drawdown_15m=None))

        exchange.fetch_market_snapshot = blocked_market
    repo.save("runtime", runtime)
    runner.execute_enabled = True
    calls, income = [], exchange.fetch_futures_income

    def read(since, until):
        calls.append((since, until))
        return income(since, until)

    exchange.fetch_futures_income = read
    try:
        for _ in range(3):
            exchange.now += timedelta(seconds=2)
            assert runner.run_once(inventory_cycle=False).inventory_valid
        assert not calls and not exchange.writes
        assert repo.runtime().resume_replenishment
    finally:
        repo.close()


def test_slow_scan_extends_only_new_wallet_version_and_rechecks_final_account(tmp_path):
    exchange, repo, runner, queries, pages, account_reads = slow_income_setup(tmp_path)
    read = exchange.fetch_futures_income

    def publish_tail(since, until):
        rows = read(since, until)
        if len(queries) == 1:
            exchange.futures_change("USDT", D("-1"), "COMMISSION")
        return rows

    exchange.fetch_futures_income = publish_tail
    try:
        plan = runner.run_once()
        assert plan.inventory_valid and len(exchange.writes) == 3
        assert len(queries) == 2 and len(pages) == 4
        assert queries[1] == (queries[0][1], exchange.account.contract_quote_updated_at)
        assert runner.cached_inputs.margin_change_24h == D("-3.501")
        assert (account_reads[1].ts - account_reads[0].ts).total_seconds() > 12
        assert repo.load("futures_income_coverage")["checked_at"] == account_reads[0].ts
    finally:
        repo.close()


@pytest.mark.parametrize("slow", ["account", "tail"])
def test_slow_final_observation_blocks_with_bounded_reads_and_no_intent(tmp_path, slow):
    exchange, repo, runner, queries, pages, accounts = slow_income_setup(tmp_path)
    if slow == "account":
        read_account = exchange.fetch_account_snapshot

        def delayed_account():
            observed = read_account()
            if len(accounts) > 1:
                exchange.now += timedelta(seconds=11)
            return observed

        exchange.fetch_account_snapshot = delayed_account
    else:
        read = exchange.fetch_futures_income

        def delayed_tail(since, until):
            rows = read(since, until)
            if len(queries) == 1:
                exchange.futures_change("USDT", D("-1"), "COMMISSION")
            else:
                exchange.now += timedelta(seconds=7)
            return rows

        exchange.fetch_futures_income = delayed_tail
    before = repo.runtime()
    try:
        plan = runner.run_once()
        assert not plan.inventory_valid
        assert len(accounts) == 2 and len(queries) == (1 if slow == "account" else 2)
        assert len(pages) == (3 if slow == "account" else 4)
        assert not repo.operations() and not exchange.writes
        assert repo.load("futures_income_coverage") is None
        assert repo.runtime().state == before.state and repo.runtime().last_inventory_at is None
        assert not runner.executor.income_audited(exchange.now)
    finally:
        repo.close()


def test_slow_maintenance_scan_can_establish_finite_coverage(tmp_path):
    exchange, repo, runner, queries, pages, accounts = slow_income_setup(tmp_path)
    runner.execute_enabled = False
    try:
        assert runner.run_once(inventory_cycle=False).inventory_valid
        assert len(queries) == 1 and len(pages) == 3 and len(accounts) == 2
        assert runner.run_once(inventory_cycle=False).inventory_valid
        assert len(queries) == 1
        assert repo.load("futures_income_coverage") is not None
        assert not exchange.writes and repo.runtime().last_inventory_at is None
    finally:
        repo.close()


@pytest.mark.parametrize("failure", ["page", "duplicate", "conflict", "storage", "coverage"])
def test_incomplete_or_unstored_pagination_cannot_advance_coverage(tmp_path, failure):
    exchange, repo, runner, queries, pages, accounts = slow_income_setup(tmp_path)
    if failure == "storage":
        repo.db.execute("""CREATE TRIGGER fail_income BEFORE INSERT ON futures_income
                           WHEN NEW.transaction_id = '1001'
                           BEGIN SELECT RAISE(ABORT, 'income storage failure'); END""")
    elif failure == "coverage":
        repo.db.execute("""CREATE TRIGGER fail_coverage BEFORE INSERT ON kv
                           WHEN NEW.key = 'futures_income_coverage'
                           BEGIN SELECT RAISE(ABORT, 'income storage failure'); END""")
    elif failure == "conflict":
        repo.save_futures_income([replace(exchange.income[1000], amount=D("-2"))])
    else:
        first = []

        def broken_page(params, rows):
            if params["page"] == 1:
                first.extend(rows)
            elif failure == "page":
                raise ExchangeError("income page failed")
            else:
                return first
            return rows

        exchange.income_page_hook = broken_page
    try:
        if failure in {"storage", "coverage"}:
            with pytest.raises(sqlite3.IntegrityError, match="income storage failure"):
                runner.run_once()
        else:
            assert runner.run_once() is None
        assert len(queries) == 1 and len(pages) <= 3
        assert len(accounts) == (2 if failure == "coverage" else 1)
        assert repo.load("futures_income_coverage") is None
        assert len(repo.futures_income_since(queries[0][0])) == (
            2501 if failure == "coverage" else int(failure == "conflict"))
        assert not repo.operations() and not exchange.writes
        assert repo.runtime().last_inventory_at is None
        assert not runner.executor.income_audited(exchange.now)
    finally:
        repo.close()


@pytest.mark.parametrize("invalidate", ["rollback", "failed_proof"])
def test_same_batch_cannot_reuse_scan_after_rollback_or_failed_proof(tmp_path, invalidate):
    exchange, repo, runner, queries, pages, accounts = slow_income_setup(tmp_path, with_bid=True)
    try:
        with runner.executor.income_batch():
            first, _ = runner.reconciler.prepare_execution(exchange.now)
            assert runner.executor.income_audited(exchange.now, account=first)
            if invalidate == "rollback":
                exchange.now -= timedelta(seconds=1)
            else:
                original = exchange.account
                exchange.account = replace(original, spot_usd=original.spot_usd + D("1"))
                _, _, pending, _ = runner.reconciler.refresh(exchange.now, force_income=True)
                assert pending.unresolved
                assert not runner.executor.income_audited(exchange.now)
                exchange.account = original
            final, _ = runner.reconciler.prepare_execution(exchange.now)
            assert runner.executor.income_audited(exchange.now, account=final)
            assert len(queries) == 2 and queries[0] == queries[1] and len(pages) == 6
            assert final.ts > first.ts
        assert not runner.executor.income_audited(exchange.now)
        assert len(exchange.writes) == 1  # Existing open GTC keeps the strict window.
    finally:
        repo.close()


def test_execution_qualification_is_bound_to_the_audited_account_version(tmp_path):
    exchange, repo, runner, queries, pages, accounts = slow_income_setup(tmp_path)
    try:
        with runner.executor.income_batch():
            first, _ = runner.reconciler.prepare_execution(exchange.now)
            exchange.futures_change("USDT", D("-1"), "COMMISSION")
            current = exchange.fetch_account_snapshot()
            operation = runner.executor.execute(
                OrderPlan("BNBUSDT", "BUY", "LIMIT", D("1"), D("500"), "GTC", False),
                exchange.now, repo.runtime().state, account=current,
            )
            assert operation.status == OperationStatus.CONFIRMED
            assert operation.balance_before.contract_quote_updated_at > first.contract_quote_updated_at
            assert len(queries) == 2 and queries[1][0] == queries[0][1] and len(pages) == 4
            assert runner.reconciler.history[1] == D("-3.501")
    finally:
        repo.close()


def test_crash_during_actual_income_page_cancels_before_next_page_and_reaudits(tmp_path):
    exchange, repo, runner, queries, pages, accounts = slow_income_setup(tmp_path, with_bid=True)
    bid = repo.operations()[0]
    repo.save("runtime", replace(repo.runtime(), run_mode=RunMode.PAUSED))
    dropped = False
    started = exchange.now

    class Feed:
        def quote(self):
            return replace(make_market(), ts=exchange.now,
                           drawdown_1m=D(".02") if dropped else D("0"))

    exchange.feed = Feed()

    def page_boundary(params, rows):
        nonlocal dropped
        if len(pages) == 1:
            dropped = True
        elif len(pages) == 2:
            assert not exchange.orders[bid.exchange_id].is_open
        return rows

    exchange.income_page_hook = page_boundary
    submit, canceled = exchange.submit, []

    def capture(operation):
        if operation.kind == OperationKind.CANCEL:
            canceled.append((exchange.now - started, len(pages)))
        return submit(operation)

    exchange.submit = capture
    try:
        plan = runner.run_once()
        assert plan.inventory_valid and repo.runtime().guard.active
        assert canceled == [(timedelta(seconds=4), 1)]
        assert len(queries) == 2 and queries[0] == queries[1] and len(pages) == 6
        assert [op.kind for op in exchange.writes] == [OperationKind.ORDER, OperationKind.CANCEL]
    finally:
        repo.close()
