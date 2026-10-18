"""Legacy acceptance boundaries and SDK clock sampling survive recovery."""

from dataclasses import replace
from datetime import timedelta

import pytest

from core.models import (
    Fill, FuturesIncome, OperationStatus, OrderPlan, ReplenishmentState,
    RunMode, TransferRecord,
)
from core.time_utils import fresh
from core.validation import valid_account_snapshot
from services.exchange_adapter import ExchangeError
from services.runner import Runner
from services.spot_market import SpotMarketData
from storage.repository import Repository
from tests.execution_helpers import setup
from tests.fake_exchange import FakeExchange
from tests.helpers import D, make_account, make_market, make_strategy_config
from tests.integration.test_binance_sdk import (
    NOW, filter_responses, funding_account_responses, millis, sdk,
)
from tests.integration.test_read_interleavings import account_reader, legacy_observations


@pytest.mark.parametrize('fee', [D('0'), D('.00075')])
@pytest.mark.parametrize('legacy', [False, True])
@pytest.mark.parametrize('restart', [False, True])
def test_retiring_first_intent_does_not_reopen_original_acquisition(sdk, tmp_path, legacy, restart, fee):
    buy_qty, sell_qty = (D('4.003'), D('3.997')) if fee else (D('.1'), D('.1'))
    buy_price, sell_price = (D('599.55'), D('600.45')) if fee else (D('598'), D('598'))
    net = buy_qty * (1 - fee)
    assert net == sell_qty * (1 + fee)
    assert buy_qty * buy_price == sell_qty * sell_price
    first_qty = D('1') if fee else D('.1')
    ex, repo, runner = setup(tmp_path, FakeExchange(replace(make_account(
        contract_bnb=D('24'), spot_bnb=D('.5') + net, spot_usd=D('10000')),
        contract_total_margin_balance=D('10000'), contract_margin_asset='USDT')),
        state=ReplenishmentState.ACCUMULATE)
    start = ex.now
    captured = []
    frozen = []
    revealed = []
    original_fills = ex.fetch_recent_fills
    ex.fetch_recent_fills = lambda symbol, since, until: tuple(
        f for f in original_fills(symbol, since, until) if revealed or f.trade_id != 'external')

    def during_balance():
        if captured:
            return
        ex.now = start + timedelta(seconds=2.5)
        # This SELL is already reflected in the accepted wallet, but its trade
        # receipt is hidden until the second strategy BUY has filled.
        ex.fills.append(Fill('BNBUSDT', 'external', 'external-sell', ex.now, sell_qty,
                             quote_qty=sell_qty * sell_price, commission=sell_qty * fee,
                             commission_asset='BNB', side='SELL'))
        ex.account = replace(ex.account, spot_bnb=D('.5'), spot_usd=D('10000') + sell_qty * sell_price)
        ex.now = start + timedelta(seconds=3)
        captured.append(ex.account)
    account_reader(sdk, ex, during_balance=during_balance)
    _, _, responses = sdk

    def spot(_):
        a = frozen[0] if frozen else ex.account
        locked = sum((o.remaining_qty * o.price for o in ex.orders.values() if o.is_open), D('0'))
        return 200, {'balances': [
            {'asset': 'BNB', 'free': str(a.spot_bnb), 'locked': '0'},
            {'asset': 'USDT', 'free': str(a.spot_usd - locked), 'locked': str(locked)}]}
    responses['GET', '/api/v3/account'] = spot
    original_fill = ex.fill

    def fill(order_id, qty):
        original_fill(order_id, qty)
        ex.account = replace(ex.account, spot_bnb=ex.account.spot_bnb - qty * fee)
        ex.fills[-1] = replace(ex.fills[-1], commission=qty * fee, commission_asset='BNB')
    ex.fill = fill

    def create(qty):
        plan = OrderPlan('BNBUSDT', 'BUY', 'LIMIT_MAKER', qty, buy_price, 'GTC', True)
        with runner.executor.execution_batch():
            account, _, _, consistent = runner.reconciler.refresh(ex.now)
            assert consistent
            return runner.executor.execute(plan, ex.now, ReplenishmentState.ACCUMULATE, account=account)
    first = create(first_qty)
    acceptance = repo.load('spot_acceptance')
    assert first.balance_before.ts == start
    assert acceptance['account'].ts == start
    ex.now += timedelta(milliseconds=100)
    second = create(buy_qty)
    assert first.balance_before.ts != second.balance_before.ts
    assert repo.operation(first.client_id).balance_pending is False
    if legacy:
        legacy_observations(repo)
    original_boundary = repo.account_event_boundary(
        repo.load('spot_acceptance')['account'], 'spot', settlement_epoch=acceptance['epoch'])
    assert original_boundary > start + timedelta(seconds=2.5)
    if restart:
        path, cfg = repo.path, runner.cfg
        repo.close()
        repo = Repository(path)
        runner = Runner(ex, repo, cfg, clock=lambda: ex.now)
    ex.now += timedelta(seconds=2)
    ex.fill(first.exchange_id, first_qty)
    runner.reconciler.refresh(ex.now)
    assert repo.operation(first.client_id).balance_pending is False
    assert first.client_id not in {op.client_id for op in repo.operations(active_only=True)}
    assert second.client_id in {op.client_id for op in repo.operations(active_only=True)}
    retired_boundary = repo.account_event_boundary(
        repo.load('spot_acceptance')['account'], 'spot', settlement_epoch=acceptance['epoch'])
    assert retired_boundary == original_boundary
    assert repo.load('spot_acceptance')['epoch'] == acceptance['epoch']
    if restart:
        repo.close()
        repo = Repository(path)
        runner = Runner(ex, repo, cfg, clock=lambda: ex.now)
    frozen.append(ex.account)
    ex.now += timedelta(seconds=2)
    ex.fill(second.exchange_id, buy_qty)
    revealed.append(True)
    runner.reconciler.refresh(ex.now)
    op = repo.operation(second.client_id)
    assert ex.account.spot_bnb == frozen[0].spot_bnb + net
    assert op.balance_pending, 'Old absorbed SELL falsely netted new BUY and released its responsibility'
    assert repo.load('spot_settlement_issue')['ambiguous_receipts'] == ('trade:BNBUSDT:external',)
    with pytest.raises(RuntimeError, match='New operations require an unpaused, reconciled runtime'):
        create(buy_qty)
    assert len(ex.writes) == 2
    repo.close()


@pytest.mark.parametrize('legacy', [False, True])
@pytest.mark.parametrize('keep_completion', [False, True])
@pytest.mark.parametrize('restart', [False, True])
def test_preaccepted_foreign_round_trip_does_not_reblock_existing_order(sdk, tmp_path, legacy, keep_completion, restart):
    ex, repo, runner = setup(tmp_path, FakeExchange(replace(make_account(
        contract_bnb=D('27'), spot_bnb=D('.5'), spot_usd=D('10000')),
        contract_total_margin_balance=D('10000'), contract_margin_asset='USDT')),
        state=ReplenishmentState.ACCUMULATE)
    start = ex.now
    # Fully observed foreign-asset round trip before the idle acceptance.
    credit = TransferRecord('old-credit', 'USDC', D('3000'), 'SPOT', 'USDⓈ-M Futures',
                            start - timedelta(milliseconds=700), OperationStatus.CONFIRMED)
    debit = replace(credit, transfer_id='old-debit', from_account='USDⓈ-M Futures', to_account='SPOT',
                    ts=start - timedelta(milliseconds=500))
    ex.transfers = {credit.transfer_id: credit, debit.transfer_id: debit}
    ex.income = [FuturesIncome('TRANSFER', 'income-' + t.transfer_id, 'USDC', amount, t.ts)
                 for t, amount in ((credit, D('3000')), (debit, D('-3000')))]
    repo.save_transfers((credit, debit))
    repo.save_futures_income(ex.income)
    account_reader(sdk, ex)
    with runner.executor.execution_batch():
        account, _, pending, consistent = runner.reconciler.refresh(ex.now)
        assert consistent and not pending.unresolved
        first = runner.executor.execute(
            OrderPlan('BNBUSDT', 'BUY', 'LIMIT_MAKER', D('.1'), D('598'), 'GTC', True),
            ex.now, ReplenishmentState.ACCUMULATE, account=account)
    accepted = repo.load('spot_acceptance')
    assert accepted['account'].ts == start
    assert accepted['income_cursor'] == 2
    assert repo.ambiguous_income(ex.income) == ()
    assert accepted['cursor'] == repo.wallet_cursor() == 2
    assert not repo.load('futures_settlement_issue')
    if legacy:
        legacy_observations(repo, keep_completion=keep_completion)
    original_baseline = repo.operation(first.client_id).balance_before
    ex.now += timedelta(seconds=2)
    ex.fill(first.exchange_id, D('.1'))
    observations = []
    for index in range(3):
        if restart and index == 1:
            path, cfg = repo.path, runner.cfg
            repo.close()
            repo = Repository(path)
            runner = Runner(ex, repo, cfg, clock=lambda: ex.now)
        account, _, pending, consistent = runner.reconciler.refresh(ex.now)
        observations.append(repo.operation(first.client_id).balance_pending)
        assert not repo.load('futures_settlement_issue')
        assert not repo.load('spot_settlement_issue')
        assert consistent and repo.ambiguous_income(ex.income) == ()
        assert account.spot_bnb == ex.account.spot_bnb == D('.6')
        ex.now += timedelta(hours=1)
    assert not any(observations), 'Known pre-acceptance USDC history reentered the strict window'
    assert repo.operation(first.client_id).balance_before == original_baseline
    assert len(ex.writes) == 1 and not repo.operations(active_only=True)
    repo.close()


@pytest.mark.parametrize('metadata_seconds', [3, 6, 11])
def test_metadata_refresh_cannot_pause_a_runner_with_synchronized_clocks(
    sdk, tmp_path, monkeypatch, metadata_seconds,
):
    adapter, calls, responses = sdk
    current, monotonic = [NOW], [0.0]
    adapter.clock = lambda: current[0]
    monkeypatch.setattr('services.sdk_transport.time.monotonic', lambda: monotonic[0])
    baseline = funding_account_responses(sdk)
    filter_responses(responses)
    time_samples = []

    def synchronized_time(_):
        value = millis(current[0])
        time_samples.append((value, millis(adapter.clock())))
        return 200, {'serverTime': value}

    for path in ('/api/v3/time', '/fapi/v1/time'):
        responses['GET', path] = synchronized_time
    path = tmp_path / 'metadata-runner.db'
    repo = Repository(path)
    repo.initialize_spot_checkpoint(baseline, {'reason': 'quiescent fixture'})
    repo.save('runtime', replace(repo.runtime(), state=ReplenishmentState.URGENT, api_errors=2))
    cfg = make_strategy_config()
    current[0] += timedelta(seconds=299.99)
    monotonic[0] = 299.99
    orders = []

    def open_orders(_):
        orders.append(current[0])
        # Initial planning reads two snapshots. Final funding preparation reads
        # two more. Cross the 300-second metadata TTL only after its account read.
        if len(orders) == 4:
            current[0] += timedelta(milliseconds=20)
            monotonic[0] += 0.02
        return 200, []

    metadata = responses['GET', '/fapi/v1/exchangeInfo']
    metadata_reads = []

    def metadata_refresh(_):
        metadata_reads.append(current[0])
        current[0] += timedelta(seconds=metadata_seconds)
        monotonic[0] += metadata_seconds
        return metadata

    responses['GET', '/api/v3/openOrders'] = open_orders
    responses['GET', '/fapi/v1/exchangeInfo'] = metadata_refresh
    responses['POST', '/sapi/v1/asset/transfer'] = (200, {'tranId': 999})
    accounts, filters = [], []
    fetch_account = adapter.fetch_account_snapshot
    fetch_filters = adapter.fetch_symbol_filters

    def captured_account():
        value = fetch_account()
        accounts.append(value)
        return value

    def captured_filters(symbol):
        value = fetch_filters(symbol)
        filters.append(value)
        return value

    monkeypatch.setattr(adapter, 'fetch_account_snapshot', captured_account)
    monkeypatch.setattr(adapter, 'fetch_symbol_filters', captured_filters)
    market = replace(make_market(), ts=current[0])
    runner = Runner(adapter, repo, cfg, clock=adapter.clock)
    runner.run_once(market=market)
    runtime = repo.runtime()
    assert accounts
    assert valid_account_snapshot(accounts[-1], current[0], cfg) == (metadata_seconds < 10)
    assert fresh(market.ts, current[0], cfg.crash_guard.max_market_age_seconds) == (metadata_seconds < 5)
    assert filters and all(
        value.observed_at is None
        or fresh(value.observed_at, current[0], cfg.crash_guard.max_market_age_seconds)
        for value in filters
    ) == (metadata_seconds < 5)
    assert all(server == local for server, local in time_samples)
    assert len(metadata_reads) == 1
    assert len(repo.operations()) == (1 if metadata_seconds < 5 else 0)
    assert runtime.api_errors == 2 and runtime.read_failures == 0
    persisted = (runtime.run_mode, runtime.api_errors, runtime.read_failures,
                 runtime.pause_reason, runtime.read_retry_at)
    repo.close()
    reopened = Repository(path)
    try:
        Runner(adapter, reopened, cfg, clock=adapter.clock)
        after = reopened.runtime()
        assert (after.run_mode, after.api_errors, after.read_failures,
                after.pause_reason, after.read_retry_at) == persisted
        assert after.run_mode == RunMode.AUTO
    finally:
        reopened.close()


@pytest.mark.parametrize('service', ['spot', 'futures', 'paper'])
@pytest.mark.parametrize('skew_ms', [-1001, -1000, 0, 1000, 1001, 'rollback'])
def test_actual_time_request_enforces_clock_bounds_after_slow_preparation(sdk, service, skew_ms):
    adapter, _, responses = sdk
    current = [NOW]
    if service == 'paper':
        adapter = SpotMarketData(adapter.cfg, spot=adapter.spot)
    adapter.clock = lambda: current[0]

    def checkpoint():
        current[0] += timedelta(seconds=3)

    adapter.read_checkpoint = checkpoint
    metadata = responses['GET', '/fapi/v1/exchangeInfo']

    def metadata_refresh(_):
        current[0] += timedelta(seconds=3)
        return metadata

    sampled = []

    def server_time(_):
        sampled.append(current[0] + timedelta(milliseconds=0 if skew_ms == 'rollback' else 1 + skew_ms))
        current[0] += timedelta(milliseconds=-1 if skew_ms == 'rollback' else 2)
        return 200, {'serverTime': millis(sampled[-1])}

    responses['GET', '/fapi/v1/exchangeInfo'] = metadata_refresh
    path, method = (('/api/v3/time', adapter.spot.time) if service != 'futures'
                    else ('/fapi/v1/time', adapter.futures.check_server_time))
    responses['GET', path] = server_time
    read_time = adapter.check_clock if service == 'paper' else lambda: adapter._server_time(method)
    if skew_ms == 'rollback' or abs(skew_ms) > 1000:
        with pytest.raises(ExchangeError, match='System clock differs'):
            read_time()
    else:
        observed = read_time()
        if service == 'paper':
            read_time()
            assert len(sampled) == 1  # Paper retains its successful-read cache.
        else:
            assert observed == sampled[-1]


@pytest.mark.parametrize('service', ['spot', 'futures'])
def test_clock_sample_excludes_budget_lock_waits(sdk, service):
    adapter, _, responses = sdk
    current, samples = [NOW], []
    adapter.clock = lambda: current[0]
    original = adapter._budget_lock

    class SlowLock:
        def __enter__(self):
            current[0] += timedelta(seconds=3)
            return original.__enter__()

        def __exit__(self, *args):
            return original.__exit__(*args)

    adapter._budget_lock = SlowLock()
    path, method = (('/api/v3/time', adapter.spot.time) if service == 'spot'
                    else ('/fapi/v1/time', adapter.futures.check_server_time))

    def server_time(_):
        samples.append(current[0])
        return 200, {'serverTime': millis(current[0])}

    responses['GET', path] = server_time
    assert adapter._server_time(method) == samples[-1]
    assert adapter._weight_inflight == {}


@pytest.mark.parametrize('service', ['spot', 'futures'])
@pytest.mark.parametrize('status', [429, 503])
def test_clock_error_keeps_transport_metadata_and_releases_reservation(sdk, service, status):
    adapter, _, responses = sdk
    path, method, endpoint = (
        ('/api/v3/time', adapter.spot.time, 'time') if service == 'spot'
        else ('/fapi/v1/time', adapter.futures.check_server_time, 'check_server_time')
    )
    responses['GET', path] = (status, {'code': -1003 if status == 429 else -1000, 'msg': 'read failed'})
    with pytest.raises(ExchangeError) as raised:
        adapter._server_time(method)
    assert raised.value.endpoint == endpoint
    assert raised.value.http_status == status
    assert adapter._weight_inflight == {}
    if status == 429:
        assert adapter.request_not_before is not None
