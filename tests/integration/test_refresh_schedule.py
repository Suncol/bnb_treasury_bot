from dataclasses import replace
from datetime import timedelta

import pytest

from core.models import GateStatus, OperationKind, OperationStatus, ReplenishmentState
from core.time_utils import fresh
from services.exchange_adapter import ExchangeError
from services.runner import Runner
from storage.repository import Repository
from tests.fake_exchange import FakeExchange
from tests.helpers import D, make_account, make_filters, make_strategy_config


class ScheduledExchange(FakeExchange):
    """Timestamp real acquisitions and advance the clock during simulated reads."""

    def __init__(self, account=None, *, filter_delay=0.075, account_age=0):
        super().__init__(account or make_account(contract_bnb=D("32")))
        self.filter_delay = timedelta(seconds=filter_delay)
        self.account_age = timedelta(seconds=account_age)
        self.market_delay = timedelta(0)
        self.filter_age = timedelta(0)
        self.filter_reads = []
        self.filter_error = None

    def fetch_account_snapshot(self):
        account = super().fetch_account_snapshot()
        return replace(account, ts=account.ts - self.account_age,
                       futures_observed_until=account.ts, spot_observed_until=account.ts)

    def fetch_symbol_filters(self, symbol):
        observed = self.now
        self.filter_reads.append(observed)
        self.now += self.filter_delay
        if self.filter_error:
            raise self.filter_error
        return replace(make_filters(), observed_at=observed - self.filter_age)

    def fetch_market_snapshot(self, symbol):
        self.now += self.market_delay
        return super().fetch_market_snapshot(symbol)


@pytest.fixture
def controller(tmp_path):
    repositories = []

    def create(exchange=None, cfg=None):
        exchange = exchange or ScheduledExchange()
        repo = Repository(tmp_path / f"schedule-{len(repositories)}.sqlite3")
        repositories.append(repo)
        repo.initialize_spot_checkpoint(
            exchange.fetch_account_snapshot(),
            {"operator": "test", "reason": "quiescent fixture"},
        )
        runner = Runner(exchange, repo, cfg or make_strategy_config(), clock=lambda: exchange.now)
        return exchange, repo, runner

    yield create
    for repo in repositories:
        repo.close()


@pytest.mark.parametrize("filter_delay", [0.075, 1.25])
def test_continuous_ticks_keep_data_fresh_without_repeated_invalid_alerts(controller, filter_delay):
    exchange, repo, runner = controller(ScheduledExchange(filter_delay=filter_delay))
    start = exchange.now
    inventory_at = None
    for second in range(120):
        # Fractional wakeups and read latency must not turn a 5 s lifetime into
        # a 10/11 s refresh interval or advance the inventory confirmation clock.
        jitter = (0, 0.1, 0.025)[second % 3]
        exchange.now = max(exchange.now, start + timedelta(seconds=second + jitter))
        plan = runner.tick()
        assert plan is not None and plan.gates.data_gate == GateStatus.OPEN
        assert fresh(runner.cached_inputs.filters.observed_at, exchange.now, 5)
        assert fresh(runner.cached_inputs.account.ts, exchange.now, 10)
        if inventory_at is None:
            inventory_at = repo.runtime().last_inventory_at
        assert repo.runtime().last_inventory_at == inventory_at
    assert 2 < len(exchange.filter_reads) < 120
    assert all(alert.code != "DATA_INVALID" for _, alert in repo.pending_alerts())
    assert repo.runtime().api_errors == 0
    assert not exchange.writes


@pytest.mark.parametrize("account_age,account_lifetime", [(8, 10), (0, 3)])
def test_account_acquisition_deadline_can_precede_filter_deadline(
    controller, account_age, account_lifetime
):
    cfg = make_strategy_config()
    cfg = replace(cfg, risk=replace(cfg.risk, max_account_age_seconds=account_lifetime))
    exchange, _, runner = controller(ScheduledExchange(account_age=account_age), cfg)
    start = exchange.now
    for second in range(25):
        exchange.now = start + timedelta(seconds=second)
        plan = runner.tick()
        assert plan is not None and plan.gates.data_gate == GateStatus.OPEN
        assert fresh(runner.cached_inputs.account.ts, exchange.now, account_lifetime)
    gaps = [(b - a).total_seconds() for a, b in zip(exchange.filter_reads, exchange.filter_reads[1:])]
    assert gaps and max(gaps) <= account_lifetime - account_age


def test_due_check_uses_time_after_slow_market_read(controller):
    exchange, _, runner = controller()
    start = exchange.now
    runner.tick()
    exchange.now = start + timedelta(seconds=3)
    exchange.market_delay = timedelta(seconds=2.5)
    plan = runner.tick()
    assert len(exchange.filter_reads) == 2
    assert plan.gates.data_gate == GateStatus.OPEN
    assert fresh(runner.cached_inputs.filters.observed_at, exchange.now, 5)


@pytest.mark.parametrize("filter_age", [5.001, -60])
def test_expired_or_future_filters_still_block_every_tick(controller, filter_age):
    exchange = ScheduledExchange(
        make_account(contract_bnb=D("24"), contract_max_withdraw_amount=D("50000")),
        filter_delay=0,
    )
    exchange.filter_age = timedelta(seconds=filter_age)
    exchange, repo, runner = controller(exchange)
    for _ in range(20):
        plan = runner.tick()
        assert plan.gates.data_gate == GateStatus.BLOCKED
        assert not plan.buy_plan.orders and not plan.transfer_decision.allow
        exchange.now += timedelta(seconds=1)
    assert not exchange.writes
    assert repo.runtime().api_errors == 0


def test_slow_refresh_cannot_retimestamp_expired_filters(controller):
    exchange = ScheduledExchange(
        make_account(contract_bnb=D("24"), contract_max_withdraw_amount=D("50000")),
        filter_delay=5.001,
    )
    exchange, _, runner = controller(exchange)
    for _ in range(3):
        observed = exchange.now
        plan = runner.tick()
        assert runner.cached_inputs.filters.observed_at == observed
        assert plan.gates.data_gate == GateStatus.BLOCKED
        assert not plan.transfer_decision.allow and not plan.buy_plan.orders
    assert not exchange.writes


def test_early_refresh_failure_honors_retry_after(controller):
    exchange, repo, runner = controller()
    runner.tick()
    exchange.filter_error = ExchangeError("limited", http_status=429, retry_after=60)
    for _ in range(10):
        exchange.now += timedelta(seconds=1)
        if runner.tick() is None:
            break
    assert repo.runtime().read_retry_at is not None
    retry_at = repo.runtime().read_retry_at
    reads = len(exchange.filter_reads)
    assert reads == 2 and runner.cached_inputs is None
    while exchange.now + timedelta(seconds=1) < retry_at:
        exchange.now += timedelta(seconds=1)
        assert runner.tick() is None
        assert len(exchange.filter_reads) == reads
    exchange.filter_error = None
    exchange.now = retry_at
    assert runner.tick().gates.data_gate == GateStatus.OPEN
    assert len(exchange.filter_reads) == reads + 1
    assert not exchange.writes


def test_early_reconciliations_do_not_advance_state_confirmation(controller):
    exchange, repo, runner = controller(
        ScheduledExchange(make_account(contract_bnb=D("27")))
    )
    start = exchange.now
    for second in range(60):
        exchange.now = start + timedelta(seconds=second)
        runner.tick()
        assert repo.runtime().state == ReplenishmentState.IDLE
        assert repo.runtime().candidate_streak == 1
    assert len(exchange.filter_reads) > 2
    exchange.now = start + timedelta(minutes=30, seconds=1)
    runner.tick()
    assert repo.runtime().state == ReplenishmentState.ACCUMULATE


def test_continuous_ticks_reconcile_unknown_funding_before_any_new_action(controller):
    exchange = ScheduledExchange(
        make_account(contract_bnb=D("24"), contract_max_withdraw_amount=D("50000"))
    )
    exchange.hold_transfers = True
    exchange.lose_response = OperationKind.TRANSFER
    exchange, repo, runner = controller(exchange)
    runner.tick()
    operation = repo.operations()[0]
    assert operation.status == OperationStatus.UNKNOWN
    # Hide the receipt to keep execution unknown across several early refreshes.
    transfers = exchange.fetch_recent_transfers
    exchange.fetch_recent_transfers = lambda *args: ()
    for _ in range(25):
        exchange.now += timedelta(seconds=1)
        runner.tick()
        assert len(exchange.writes) == 1
        assert repo.operations()[0].status == OperationStatus.UNKNOWN
    assert len(exchange.filter_reads) > 2
    exchange.fetch_recent_transfers = transfers
    exchange.hold_transfers = False
    exchange.settle("1")
    exchange.now += timedelta(seconds=5)
    runner.tick()
    assert len(exchange.writes) == 1
    assert repo.operations()[0].status == OperationStatus.UNKNOWN
    runner.reconciler.bind_transfer_id(operation.client_id, "1", exchange.now)
    for _ in range(15):
        exchange.now += timedelta(seconds=1)
        runner.tick()
    funding = [op for op in exchange.writes if op.kind == OperationKind.TRANSFER
               and op.payload.to_account == "SPOT"]
    assert len(funding) == 1
    assert any(op.kind == OperationKind.ORDER for op in exchange.writes)
    assert not repo.operations(unresolved_only=True)
