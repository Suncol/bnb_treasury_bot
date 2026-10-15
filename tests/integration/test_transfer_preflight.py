"""Final funding checks use new exchange evidence before journaling a transfer."""

from dataclasses import replace
from datetime import timedelta

import pytest

from core.models import (
    AssetTransferPlan, CrashGuardState, OperationKind, OperationStatus,
    ReplenishmentState, RunMode, TransferRecord,
)
from services.executor import ExecutionBlocked, Executor
from services.runner import Runner
from storage.repository import Repository
from tests.fake_exchange import FakeExchange
from tests.helpers import D, make_account, make_engine_inputs, make_filters, make_strategy_config


def funding(amount='3000'):
    return AssetTransferPlan('USDT', D(amount), 'USDⓈ-M Futures', 'SPOT', 'fund replenishment')


def external_funding(exchange, amount):
    exchange.now += timedelta(milliseconds=1)
    exchange.transfers["external"] = TransferRecord(
        "external", "USDT", D(amount), "USDⓈ-M Futures", "SPOT", exchange.now,
        OperationStatus.PENDING,
    )
    exchange.settle("external")


@pytest.fixture
def treasury(tmp_path):
    repositories = []

    def create(exchange=None, cfg=None, state=ReplenishmentState.URGENT):
        exchange = exchange or FakeExchange(make_account(
            contract_bnb=D('24'), contract_max_withdraw_amount=D('60000'),
        ))
        cfg = cfg or make_strategy_config()
        repo = Repository(tmp_path / f'preflight-{len(repositories)}.db')
        repositories.append(repo)
        repo.initialize_spot_checkpoint(exchange.fetch_account_snapshot(), {
            'operator': 'test', 'reason': 'quiescent fake exchange',
        })
        repo.save('runtime', replace(repo.runtime(), state=state))
        return exchange, repo, Runner(exchange, repo, cfg, clock=lambda: exchange.now)

    yield create
    for repo in repositories:
        repo.close()


@pytest.mark.parametrize('state', [ReplenishmentState.ACCUMULATE, ReplenishmentState.URGENT])
@pytest.mark.parametrize('latest,floor,amount,reason', [
    ('4000', '2000', '500', 'Minimum transfer unit'),
    ('25000', '2000', '3000', 'Transfer amount'),
    ('51000', '49000', '3000', 'absolute minimum'),
])
def test_direct_executor_rechecks_latest_withdrawal_risk_before_intent(
    treasury, state, latest, floor, amount, reason
):
    cfg = make_strategy_config()
    cfg = replace(cfg, risk=replace(cfg.risk, m_abs_min=D(floor)))
    exchange, repo, _ = treasury(cfg=cfg, state=state)
    approved_account = exchange.fetch_account_snapshot()
    exchange.account = replace(exchange.account, contract_max_withdraw_amount=D(latest))
    executor = Executor(exchange, repo, cfg, clock=lambda: exchange.now)
    with pytest.raises(ExecutionBlocked, match=reason):
        executor.execute(funding(amount), exchange.now, state, account=approved_account)
    assert not exchange.writes and not repo.operations()
    assert repo.runtime().api_errors == repo.runtime().transfer_failures == 0
    assert not repo.runtime().resume_replenishment


def test_runner_ends_changed_candidate_without_api_failure_or_immediate_retry(treasury):
    class MaximumFalls(FakeExchange):
        observe = False
        maxima = []

        def fetch_account_snapshot(self):
            if self.observe:
                maximum = D("60000") if not self.maxima else D("4000")
                self.maxima.append(maximum)
                self.account = replace(self.account, contract_max_withdraw_amount=maximum)
            return super().fetch_account_snapshot()

    exchange, repo, runner = treasury(MaximumFalls())
    exchange.observe = True
    repo.save("runtime", replace(repo.runtime(), api_errors=2))
    plan = runner.run_once(market=exchange.fetch_market_snapshot("BNBUSDT"))
    assert plan is not None
    assert not plan.transfer_decision.allow
    assert exchange.maxima == [D("60000"), D("4000")]
    assert not exchange.writes and not repo.operations()
    assert repo.runtime().api_errors == 2
    assert repo.runtime().read_failures == repo.runtime().transfer_failures == 0
    assert repo.runtime().run_mode == RunMode.AUTO
    assert runner.cached_inputs is None


@pytest.mark.parametrize("spent,adjusted", [("2500", "500"), ("3000", "0")])
def test_final_history_rechecks_external_budget_and_never_sends_clipped_candidate(treasury, spent, adjusted):
    exchange, repo, runner = treasury()
    original = exchange.fetch_account_snapshot()
    external_funding(exchange, spent)
    with pytest.raises(ExecutionBlocked) as blocked:
        runner.executor.execute(funding(), exchange.now, repo.runtime().state, account=original)
    assert blocked.value.decision.adjusted_amount == D(adjusted)
    assert repo.budget_used(exchange.now, "USDT") == D(spent)
    assert len(repo.transfers_since(original.ts)) == 1
    assert not repo.operations() and not exchange.writes


@pytest.mark.parametrize("amount,minimum", [("500", "1000"), ("501", "0")])
def test_changed_transfer_minimum_or_rounding_requires_new_candidate(treasury, amount, minimum):
    exchange, repo, runner = treasury()
    exchange.fetch_symbol_filters = lambda symbol: replace(make_filters(), min_transfer_usd=D(minimum))
    with pytest.raises(ExecutionBlocked) as blocked:
        runner.executor.execute(funding(amount), exchange.now, repo.runtime().state)
    assert blocked.value.decision.adjusted_amount == D("1000")
    assert not repo.operations() and not exchange.writes


def test_exact_remaining_budget_sends_once_using_final_observation_and_no_double_reservation(treasury):
    exchange, repo, runner = treasury()
    external_funding(exchange, "2500")
    runner.reconciler.refresh(exchange.now)
    original = exchange.fetch_account_snapshot()
    exchange.now += timedelta(seconds=1)
    exchange.account = replace(exchange.account, contract_max_withdraw_amount=D("55000"))
    final_time = exchange.now
    op = runner.executor.execute(funding("500"), original.ts, repo.runtime().state, account=original)
    assert len(exchange.writes) == 1 and exchange.writes[0].payload.amount == D("500")
    assert op.created_at == op.balance_before.ts == final_time
    assert op.balance_before.contract_max_withdraw_amount == D("55000")
    runner.reconciler.refresh(exchange.now)
    assert repo.budget_used(exchange.now, "USDT") == D("3000")
    assert not repo.operations(unresolved_only=True)


@pytest.mark.parametrize("evidence", ["past", "future", "nonfinite", "missing_income", "empty_new_version", "capital", "spot", "orders"])
def test_final_observation_must_have_valid_time_and_complete_financial_evidence(treasury, evidence):
    account = replace(
        make_account(contract_bnb=D("24"), contract_max_withdraw_amount=D("60000")),
        contract_total_margin_balance=D("60000"), contract_margin_asset="USDT",
    )
    exchange, repo, runner = treasury(FakeExchange(account))
    original = exchange.fetch_account_snapshot()
    fetch = exchange.fetch_account_snapshot
    if evidence in {"past", "future", "nonfinite"}:
        change = {"ts": exchange.now + timedelta(seconds=-11 if evidence == "past" else 1)}
        if evidence == "nonfinite":
            change = {"contract_max_withdraw_amount": D("NaN")}
        exchange.fetch_account_snapshot = lambda: replace(fetch(), **change)
    elif evidence in {"missing_income", "empty_new_version", "capital"}:
        exchange.futures_change("USDT", D("0") if evidence == "empty_new_version" else D("-1"),
                                "INTERNAL_TRANSFER" if evidence == "capital" else "COMMISSION")
        if evidence != "capital":
            exchange.income.clear()
    elif evidence == "spot":
        exchange.account = replace(exchange.account, spot_usd=D("500"))
    else:
        from tests.helpers import order
        reads = []

        def changing_orders(symbol):
            reads.append(symbol)
            return () if len(reads) == 1 else (order("external"),)

        exchange.fetch_open_orders = changing_orders
    with pytest.raises(ExecutionBlocked, match="reconciled"):
        runner.executor.execute(funding("500"), exchange.now, repo.runtime().state, account=original)
    assert not repo.operations() and not exchange.writes
    assert repo.runtime().api_errors == repo.runtime().transfer_failures == 0


@pytest.mark.parametrize("seconds,allow", [(5, True), (5.000001, False), (6, False)])
def test_slow_final_preparation_cannot_extend_runner_market_or_filter_lifetime(treasury, seconds, allow):
    class SlowFinalAccount(FakeExchange):
        slow = False

        def fetch_account_snapshot(self):
            observed = super().fetch_account_snapshot()
            if self.slow:
                self.now += timedelta(seconds=seconds)
            return observed

    exchange, repo, runner = treasury(SlowFinalAccount())
    account = exchange.fetch_account_snapshot()
    inputs = replace(
        make_engine_inputs(account=account), market=exchange.fetch_market_snapshot("BNBUSDT"),
        filters=replace(make_filters(), observed_at=exchange.now),
    )
    exchange.slow = True
    if allow:
        op = runner.executor.execute(funding("500"), exchange.now, repo.runtime().state,
                                     account=account, inputs=inputs)
        assert op.balance_before.ts == account.ts
        assert op.created_at == account.ts + timedelta(seconds=seconds)
        assert len(exchange.writes) == 1
    else:
        with pytest.raises(ExecutionBlocked, match="expired"):
            runner.executor.execute(funding("500"), exchange.now, repo.runtime().state,
                                     account=account, inputs=inputs)
        assert not repo.operations() and not exchange.writes


@pytest.mark.parametrize("mode", [RunMode.PAUSED, RunMode.URGENT_ONLY])
def test_runtime_mode_changed_during_preparation_blocks_old_funding(treasury, mode):
    exchange, repo, runner = treasury(state=ReplenishmentState.ACCUMULATE)
    original = exchange.fetch_account_snapshot()
    fetch = exchange.fetch_account_snapshot

    def changed_mode():
        repo.save("runtime", replace(repo.runtime(), run_mode=mode))
        return fetch()

    exchange.fetch_account_snapshot = changed_mode
    with pytest.raises(ExecutionBlocked, match="run mode"):
        runner.executor.execute(funding("500"), exchange.now, ReplenishmentState.URGENT, account=original)
    assert repo.runtime().run_mode == mode
    assert not repo.operations() and not exchange.writes


def test_pending_external_transfer_discovered_at_final_history_blocks_new_funding(treasury):
    exchange, repo, runner = treasury()
    original = exchange.fetch_account_snapshot()
    exchange.transfers["external"] = TransferRecord(
        "external", "USDT", D("2500"), "USDⓈ-M Futures", "SPOT", exchange.now,
        OperationStatus.PENDING,
    )
    with pytest.raises(ExecutionBlocked, match="reconciled"):
        runner.executor.execute(funding("500"), exchange.now, repo.runtime().state, account=original)
    assert repo.budget_used(exchange.now, "USDT") == D("2500")
    assert not repo.operations() and not exchange.writes


def test_priority_risk_change_during_final_reads_invalidates_funding_candidate(treasury):
    exchange, repo, runner = treasury()
    original = exchange.fetch_account_snapshot()
    fetch = exchange.fetch_account_snapshot

    def activate_protection():
        repo.save("runtime", replace(repo.runtime(), guard=CrashGuardState(
            active=True, episode_id="new-risk", started_at=exchange.now,
            keep_price_ceiling=D("580"),
        )))
        return fetch()

    exchange.fetch_account_snapshot = activate_protection
    with pytest.raises(ExecutionBlocked, match="Risk protection changed"):
        runner.executor.execute(funding("500"), exchange.now, repo.runtime().state, account=original)
    assert not repo.operations() and not exchange.writes
    assert repo.runtime().guard.active


def test_timeout_after_final_gate_keeps_one_unknown_identity_across_restart(treasury, tmp_path):
    exchange, repo, runner = treasury()
    exchange.lose_response = OperationKind.TRANSFER
    op = runner.executor.execute(funding("500"), exchange.now, repo.runtime().state)
    assert op.status == OperationStatus.UNKNOWN and not op.not_sent
    assert len(exchange.writes) == 1
    assert repo.budget_used(exchange.now, "USDT") == D("500")
    repo.close()
    restarted = Repository(tmp_path / 'preflight-0.db')
    try:
        executor = Executor(exchange, restarted, make_strategy_config(), clock=lambda: exchange.now)
        with pytest.raises(RuntimeError, match="reconciled"):
            executor.execute(funding("500"), exchange.now, ReplenishmentState.URGENT)
        assert restarted.operations(unresolved_only=True) == (op,)
        assert len(exchange.writes) == 1
    finally:
        restarted.close()
