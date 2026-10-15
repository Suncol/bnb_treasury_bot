import time
from dataclasses import replace
from datetime import timedelta
from threading import Event, get_ident
from types import SimpleNamespace

import pytest

from binance_common.errors import TooManyRequestsError

from core.models import AssetTransferPlan, GateStatus, Operation, OperationKind, OperationStatus
from core.time_utils import fresh
from services.exchange_adapter import RequestDeferred
from services.price_refresh import PriceRefreshPending
from services.runner import Runner
from services.sdk_transport import millis
from services.spot_market import SpotMarketData
from storage.repository import Repository
from tests.fake_exchange import FakeExchange
from tests.helpers import D, make_account, make_market, make_strategy_config


class PublicSpot:
    def __init__(self, clock):
        self.clock = clock
        self.started, self.release = Event(), Event()
        self.block = False
        self.error = None
        self.calls = 0
        self.timestamp_at_response = False
        self.rule = {'filterType':'PERCENT_PRICE', 'avgPriceMins':5,
                     'multiplierDown':'.9', 'multiplierUp':'1.1'}

    @staticmethod
    def response(data):
        return SimpleNamespace(headers={}, data=lambda: data)

    def exchange_info(self, **kwargs):
        return self.response({'exchangeFilters':[], 'symbols':[
            {'status':'TRADING', 'baseAsset':'BNB', 'quoteAsset':'USDT', 'filters':[
                {'filterType':'LOT_SIZE','stepSize':'.001','minQty':'.001','maxQty':'100'},
                {'filterType':'PRICE_FILTER','tickSize':'.01'}, self.rule,
            ]}]})

    def reference_price(self, **kwargs):
        self.calls += 1
        stamp = millis(self.clock())
        if self.block:
            self.started.set()
            assert self.release.wait(5), 'test did not release public request'
        if self.error:
            raise self.error
        if self.timestamp_at_response:
            stamp = millis(self.clock())
        return self.response({'referencePrice':'600', 'timestamp':stamp})


@pytest.fixture
def controller(tmp_path):
    ex = FakeExchange(make_account(contract_bnb=D('32')))
    cfg = make_strategy_config()
    client = PublicSpot(lambda: ex.now)
    market = SpotMarketData(cfg, spot=client, clock=lambda: ex.now)
    market.start_price_refresh(client=client)
    refresh = market._price_refresh
    origin = ex.now
    refresh.monotonic = lambda: (ex.now-origin).total_seconds()
    repo = Repository(tmp_path/'refresh.sqlite3')
    repo.initialize_spot_checkpoint(ex.fetch_account_snapshot(), {'reason':'test'})
    ex.fetch_symbol_filters = market.fetch_symbol_filters
    original_quote = ex.fetch_market_snapshot
    ex.fetch_market_snapshot = lambda symbol: (refresh.kick(), original_quote(symbol))[1]
    runner = Runner(ex, repo, cfg, clock=lambda: ex.now)
    yield SimpleNamespace(ex=ex, cfg=cfg, client=client, market=market, refresh=refresh,
                          runner=runner, repo=repo)
    client.release.set()
    if refresh._thread:
        refresh._thread.join(1)
    market.close_price_refresh()
    repo.close()


def complete(s):
    assert s.refresh._thread is not None
    s.refresh._thread.join(1)
    assert not s.refresh._thread.is_alive()


def seed(s):
    try:
        s.market.fetch_symbol_filters(s.cfg.symbol)
    except PriceRefreshPending:
        pass
    complete(s)
    assert s.runner.tick().gates.data_gate == GateStatus.OPEN
    return s.market.fetch_symbol_filters(s.cfg.symbol)


def test_slow_reference_read_does_not_block_continuous_ticks_or_risk(controller):
    s = controller
    first = seed(s)
    checkpoint_threads = []
    s.market.read_checkpoint = lambda: checkpoint_threads.append(get_ident())
    s.client.block = True
    s.ex.now += timedelta(seconds=1)
    s.runner.tick()
    assert s.client.started.wait(1)
    inventory_at = s.repo.runtime().last_inventory_at
    for _ in range(15):
        s.ex.now += timedelta(seconds=.2)
        started = time.monotonic()
        plan = s.runner.tick()
        assert time.monotonic()-started < .25
        assert plan.gates.data_gate == GateStatus.OPEN
        assert s.repo.runtime().last_inventory_at == inventory_at
    assert s.client.calls == 2  # One seed and one in flight, never a task per tick.
    assert fresh(first.observed_at, s.ex.now, 5)
    s.ex.fetch_market_snapshot = lambda symbol: replace(make_market(), ts=s.ex.now, drawdown_1m=D('.02'))
    assert s.runner.tick().crash_guard.active
    assert not s.ex.writes
    assert not checkpoint_threads  # Public worker cannot call into the journal.
    assert s.repo.runtime().api_errors == 0


def test_repeated_early_background_refresh_keeps_five_second_gate_open(controller):
    s = controller
    seed(s)
    inventory_at = s.repo.runtime().last_inventory_at
    for _ in range(90):
        s.ex.now += timedelta(seconds=1)
        s.refresh.kick()
        complete(s)
        plan = s.runner.tick()
        assert plan.gates.data_gate == GateStatus.OPEN
        assert fresh(s.runner.cached_inputs.filters.observed_at, s.ex.now, 5)
    assert s.repo.runtime().last_inventory_at == inventory_at
    assert s.repo.runtime().api_errors == 0 and not s.ex.writes


@pytest.mark.parametrize('age,expected', [(5,GateStatus.OPEN),(5.000001,GateStatus.BLOCKED)])
def test_inflight_read_does_not_extend_exact_filter_lifetime(controller, age, expected):
    s = controller
    first = seed(s)
    s.client.block = True
    s.ex.now += timedelta(seconds=1)
    s.refresh.kick()
    assert s.client.started.wait(1)
    s.ex.now = first.observed_at+timedelta(seconds=age)
    plan = s.runner.tick()
    assert plan.gates.data_gate == expected
    assert s.runner.cached_inputs.filters.observed_at == first.observed_at
    assert not s.ex.writes


def test_expired_response_is_discarded_and_error_honors_controller_backoff(controller):
    s = controller
    first = seed(s)
    s.client.block = True
    s.client.timestamp_at_response = True
    s.ex.now += timedelta(seconds=1)
    s.refresh.kick()
    assert s.client.started.wait(1)
    s.ex.now += timedelta(seconds=5.102)
    s.client.release.set()
    complete(s)
    assert s.refresh._value[2] == first.observed_at
    assert s.runner.tick() is None
    assert s.repo.runtime().api_errors == 1
    deadline = s.repo.runtime().read_retry_at
    assert deadline > s.ex.now
    calls = s.client.calls
    s.ex.now = deadline-timedelta(microseconds=1)
    assert s.runner.tick() is None
    assert s.client.calls == calls
    s.ex.now = deadline
    s.client.block = False
    s.refresh.kick()
    complete(s)
    assert s.runner.tick().gates.data_gate == GateStatus.OPEN
    assert not s.ex.writes


def test_pending_initial_observation_never_funds_or_pauses_auto(controller):
    s = controller
    s.ex.account = replace(s.ex.account, contract_bnb=D('24'))
    s.client.block = True
    for _ in range(8):
        assert s.runner.tick() is None
        s.ex.now += timedelta(milliseconds=100)
    assert s.client.started.is_set() and s.client.calls == 1
    assert s.repo.runtime().api_errors == s.repo.runtime().read_failures == 0
    assert not s.repo.operations() and not s.ex.writes


def test_rule_change_discards_old_inflight_result(controller):
    s = controller
    seed(s)
    s.client.block = True
    s.ex.now += timedelta(seconds=1)
    s.refresh.kick()
    assert s.client.started.wait(1)
    s.client.rule = dict(s.client.rule, multiplierUp='1.05')
    s.market.cache.pop('filters:'+s.cfg.symbol)
    with pytest.raises(PriceRefreshPending):
        s.market.fetch_symbol_filters(s.cfg.symbol)
    s.client.release.set()
    complete(s)
    assert s.refresh._value is None and s.refresh.discarded == 1
    s.client.block = False
    s.refresh.kick()
    complete(s)
    assert s.market.fetch_symbol_filters(s.cfg.symbol).max_price == D('630')


def test_future_cache_is_not_reused_after_clock_rollback(controller):
    s = controller
    seed(s)
    s.ex.now -= timedelta(seconds=1)
    s.client.block = True
    with pytest.raises(PriceRefreshPending):
        s.market.fetch_symbol_filters(s.cfg.symbol)
    assert not s.refresh._value


def test_shutdown_does_not_wait_for_stuck_request_or_publish_late_result(controller):
    s = controller
    s.client.block = True
    with pytest.raises(PriceRefreshPending):
        s.market.fetch_symbol_filters(s.cfg.symbol)
    assert s.client.started.wait(1)
    start = time.monotonic()
    s.market.close_price_refresh()
    assert time.monotonic()-start < .25
    s.client.release.set()
    complete(s)
    assert s.refresh._value is None and s.refresh.discarded == 1


def test_public_rate_limit_is_shared_with_mutations_and_does_not_regress(controller):
    s = controller
    seed(s)
    s.market._observe_request_budget('spot', {'x-mbx-used-weight-1m':'950'},
        {'rateLimits':[{'rateLimitType':'REQUEST_WEIGHT','interval':'MINUTE','intervalNum':1,'limit':1000}]})
    s.market._observe_request_budget('spot', {'x-mbx-used-weight-1m':'100'}, {})
    with pytest.raises(RequestDeferred):
        s.market.ensure_request_allowed(scope='spot')
    s.market.ensure_request_allowed(protective=True, scope='spot')
    s.market.request_not_before = s.ex.now+timedelta(seconds=60)
    with pytest.raises(RequestDeferred):
        s.market.ensure_request_allowed(protective=True, scope='spot')


def test_background_429_persists_backoff_and_suspends_reads(controller):
    s = controller
    seed(s)
    s.client.error = TooManyRequestsError('limited', status_code=-1003, retry_after=60)
    s.ex.now += timedelta(seconds=1)
    s.refresh.kick()
    complete(s)
    assert s.market.request_not_before == s.ex.now+timedelta(seconds=60)
    with pytest.raises(RequestDeferred):
        s.market.ensure_request_allowed(protective=True)
    assert s.runner.tick() is None
    assert s.repo.runtime().read_retry_at >= s.ex.now+timedelta(seconds=60)
    calls = s.client.calls
    for _ in range(10):
        s.ex.now += timedelta(seconds=1)
        assert s.runner.tick() is None
    assert s.client.calls == calls


def test_explicit_invalidation_discards_late_result_even_for_same_rules(controller):
    s = controller
    seed(s)
    s.client.block = True
    s.ex.now += timedelta(seconds=1)
    s.refresh.kick()
    assert s.client.started.wait(1)
    s.market.invalidate_filters(s.cfg.symbol)
    with pytest.raises(PriceRefreshPending):
        s.market.fetch_symbol_filters(s.cfg.symbol)
    s.client.release.set()
    complete(s)
    assert s.refresh.discarded == 1 and s.refresh._value is None


def test_pending_refresh_preserves_unknown_transfer_without_resubmitting(controller):
    s = controller
    s.ex.account = replace(s.ex.account, contract_bnb=D('24'))
    op = Operation('unknown-funding', OperationKind.TRANSFER, 'transfer:USDT',
        AssetTransferPlan('USDT',D('500'),'USDⓈ-M Futures','SPOT','lost response'),
        s.ex.now, status=OperationStatus.UNKNOWN)
    s.repo.record_intent(replace(op, status=OperationStatus.PENDING))
    s.repo.update_operation(op)
    s.client.block = True
    for _ in range(4):
        assert s.runner.tick() is None
        s.ex.now += timedelta(milliseconds=100)
    s.client.release.set()
    complete(s)
    assert s.runner.tick() is not None
    assert len(s.repo.operations(unresolved_only=True)) == 1
    assert s.repo.operation(op.client_id).status == OperationStatus.UNKNOWN
    assert not s.ex.writes
