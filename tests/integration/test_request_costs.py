"""SDK admission must cover actual endpoint cost, including concurrent reads."""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Event

import pytest

from services.exchange_adapter import ExchangeError, RequestDeferred
from tests.integration.test_binance_sdk import NOW, account_responses, sdk
from tests.integration.test_request_budgets import observe


def test_futures_usage_without_metadata_is_bounded_before_income_http(sdk):
    adapter, calls, responses = sdk
    adapter._observe_request_budget('futures', {'x-mbx-used-weight-1m': '2390'}, {})
    responses['GET', '/fapi/v1/income'] = (429, {'code': -1003, 'msg': 'limited'})
    with pytest.raises(RequestDeferred):
        adapter.fetch_futures_income(NOW - timedelta(seconds=1), NOW)
    assert not calls
    adapter.ensure_request_allowed(scope='spot', protective=True, request_weight=1)
    assert adapter.request_not_before is None


@pytest.mark.parametrize('limits', [[], [{'rateLimitType': 'REQUEST_WEIGHT', 'interval': 'MINUTE',
                                        'intervalNum': 1, 'limit': 1200}]])
def test_futures_account_initializes_limits_and_rejects_missing_metadata(sdk, limits):
    adapter, calls, responses = account_responses(sdk)
    responses['GET', '/fapi/v1/exchangeInfo'] = (200, {'rateLimits': limits})
    if not limits:
        with pytest.raises(ExchangeError, match='request weight'):
            adapter.fetch_account_snapshot()
        assert not any('/fapi/v3/balance' in call['url'] for call in calls)
    else:
        adapter.fetch_account_snapshot()
        assert adapter.weight_limits['futures', 'x-mbx-used-weight-1m'] == 1200
        assert sum('/fapi/v1/exchangeInfo' in call['url'] for call in calls) == 1


@pytest.mark.parametrize('scope,endpoint,params,cost', [
    ('spot', 'depth', {'symbol': 'BNBUSDT', 'limit': 100}, 5),
    ('spot', 'depth', {'symbol': 'BNBUSDT', 'limit': 500}, 25),
    ('spot', 'depth', {'symbol': 'BNBUSDT', 'limit': 1000}, 50),
    ('spot', 'depth', {'symbol': 'BNBUSDT', 'limit': 5000}, 250),
    ('spot', 'get_open_orders', {'symbol': 'BNBUSDT'}, 6),
    ('spot', 'get_open_orders', {}, 80),
    ('spot', 'get_account', {}, 20),
    ('spot', 'my_filters', {'symbol': 'BNBUSDT'}, 40),
    ('spot', 'my_trades', {'symbol': 'BNBUSDT'}, 20),
    ('futures', 'futures_account_balance_v3', {}, 5),
    ('futures', 'account_information_v3', {}, 5),
    ('futures', 'get_income_history', {}, 30),
])
def test_read_cost_cannot_cross_protection_reserve(sdk, scope, endpoint, params, cost):
    adapter, calls, _ = sdk
    observe(adapter, 'REQUEST_WEIGHT', 5400 - cost + 1, limit=6000, interval='MINUTE', scope=scope)
    with pytest.raises(RequestDeferred):
        adapter._call(getattr(getattr(adapter, scope), endpoint), **params)
    assert not calls


def test_two_depth_requests_cannot_spend_the_same_capacity(sdk):
    adapter, calls, responses = sdk
    observe(adapter, 'REQUEST_WEIGHT', 5000, limit=6000, interval='MINUTE')
    started, finish = Event(), Event()
    def slow_depth(request):
        started.set()
        assert finish.wait(5)
        return 200, {'lastUpdateId': 1, 'bids': [], 'asks': []}, {'X-MBX-USED-WEIGHT-1M': '5250'}
    responses['GET', '/api/v3/depth'] = slow_depth
    with ThreadPoolExecutor(max_workers=1) as pool:
        first = pool.submit(adapter._call, adapter.spot.depth, symbol='BNBUSDT', limit=5000)
        assert started.wait(5)
        try:
            with pytest.raises(RequestDeferred):
                adapter._call(adapter.spot.depth, symbol='BNBUSDT', limit=5000)
        finally:
            finish.set()
        first.result(timeout=5)
    assert len(calls) == 1
    assert adapter.weight_usage['spot', 'x-mbx-used-weight-1m'][0] == 5250


def test_missing_response_headers_do_not_refund_dispatched_cost(sdk):
    adapter, calls, responses = sdk
    observe(adapter, 'REQUEST_WEIGHT', 5150, limit=6000, interval='MINUTE')
    responses['GET', '/api/v3/depth'] = TimeoutError('unknown response')
    with pytest.raises(ExchangeError):
        adapter._call(adapter.spot.depth, symbol='BNBUSDT', limit=5000)
    with pytest.raises(RequestDeferred):
        adapter._call(adapter.spot.depth, symbol='BNBUSDT', limit=5000)
    assert len(calls) == 1
    adapter.clock = lambda: NOW + timedelta(seconds=62)
    adapter.ensure_request_allowed(scope='spot', request_weight=250)


@pytest.mark.parametrize('timeout', [False, True])
def test_headerless_calls_at_window_boundary_cannot_spend_the_minute_twice(sdk, timeout):
    adapter, calls, responses = sdk
    responses['GET', '/api/v3/depth'] = (
        TimeoutError('unknown response') if timeout else
        (200, {'lastUpdateId': 1, 'bids': [], 'asks': []})
    )
    def request_depth():
        try:
            adapter._call(adapter.spot.depth, symbol='BNBUSDT', limit=5000)
        except RequestDeferred:
            raise
        except ExchangeError:
            if not timeout:
                raise
    adapter.clock = lambda: NOW + timedelta(seconds=59)
    request_depth()
    previous_calls = len(calls)
    # A request in the one-second expiry margin belongs to the new minute.
    adapter.clock = lambda: NOW + timedelta(seconds=60)
    request_depth()
    adapter.clock = lambda: NOW + timedelta(seconds=61)
    for _ in range(30):
        try:
            request_depth()
        except RequestDeferred:
            break
    assert (len(calls) - previous_calls) * 250 <= 5400
    with pytest.raises(RequestDeferred):
        request_depth()
    adapter.clock = lambda: NOW + timedelta(seconds=122)
    request_depth()


def test_clock_rollback_response_cannot_erase_dispatched_weight(sdk):
    adapter, calls, responses = sdk
    adapter.clock = lambda: NOW + timedelta(seconds=61)
    observe(adapter, 'REQUEST_WEIGHT', 5100, limit=6000, interval='MINUTE')
    def rollback(_):
        adapter.clock = lambda: NOW
        return 200, {'lastUpdateId': 1, 'bids': [], 'asks': []}, {'X-MBX-USED-WEIGHT-1M': '250'}
    responses['GET', '/api/v3/depth'] = rollback
    adapter._call(adapter.spot.depth, symbol='BNBUSDT', limit=5000)
    with pytest.raises(RequestDeferred):
        adapter._call(adapter.spot.depth, symbol='BNBUSDT', limit=5000)
    assert len(calls) == 1
    adapter.clock = lambda: NOW + timedelta(seconds=62)
    with pytest.raises(RequestDeferred):
        adapter.ensure_request_allowed(scope='spot', request_weight=250)


@pytest.mark.parametrize('with_header', [False, True])
def test_protective_reads_cannot_keep_previous_windows_exhausted_forever(sdk, with_header):
    adapter, _, responses = sdk
    observe(adapter, 'REQUEST_WEIGHT', 5400, limit=6000, interval='MINUTE')
    responses['GET', '/api/v3/order'] = (200, {'orderId': 123, 'status': 'NEW'},
        {'X-MBX-USED-WEIGHT-1M': '4'} if with_header else {})
    for minute in range(1, 5):
        adapter.clock = lambda: NOW + timedelta(minutes=minute)
        adapter._call(adapter.spot.get_order, symbol='BNBUSDT', order_id=123, protective=True)
        adapter.clock = lambda: NOW + timedelta(minutes=minute, seconds=2)
        adapter.ensure_request_allowed(scope='spot', request_weight=250)


@pytest.mark.parametrize('rule', [
    {'rateLimitType': 'REQUEST_WEIGHT', 'interval': 'MINUTE', 'intervalNum': 1, 'limit': 0},
    {'rateLimitType': 'REQUEST_WEIGHT', 'interval': 'WEEK', 'intervalNum': 1, 'limit': 2400},
])
def test_invalid_futures_limit_never_admits_account_or_income_reads(sdk, rule):
    adapter, calls, responses = sdk
    responses['GET', '/fapi/v1/exchangeInfo'] = (200, {'rateLimits': [rule]})
    with pytest.raises(ExchangeError):
        adapter.fetch_futures_income(NOW - timedelta(seconds=1), NOW)
    assert len(calls) == 1 and '/fapi/v1/exchangeInfo' in calls[0]['url']


def test_unknown_endpoint_cost_is_rejected_before_dispatch(sdk):
    adapter, calls, _ = sdk
    def unregistered_endpoint():
        pytest.fail('Unknown cost must not enter SDK')
    with pytest.raises(ExchangeError, match='Unknown SDK request weight'):
        adapter._call(unregistered_endpoint)
    assert not calls


def test_wallet_uid_limit_accounts_for_transfer_cost_and_keeps_spot_protection(sdk):
    from services.exchange_adapter import RequestNotSent

    adapter, calls, _ = sdk
    adapter._observe_request_budget('wallet:user_universal_transfer',
        {'x-sapi-used-uid-weight-1m': '161701'}, {})
    with pytest.raises(RequestNotSent):
        adapter._call(adapter.wallet.user_universal_transfer, type='MAIN_UMFUTURE', asset='BNB', amount='1')
    assert not calls
    adapter.ensure_request_allowed(scope='spot', protective=True, request_weight=4)


def test_unknown_weight_interval_blocks_until_its_observation_expires(sdk):
    adapter, calls, responses = sdk
    adapter._observe_request_budget('futures', {'x-mbx-used-weight-5m': '1'}, {})
    with pytest.raises(RequestDeferred, match='limit unavailable'):
        adapter.ensure_request_allowed(scope='futures', request_weight=1)
    adapter.clock = lambda: NOW + timedelta(minutes=5, seconds=1)
    adapter.ensure_request_allowed(scope='futures', request_weight=1)
    responses['GET', '/fapi/v1/income'] = (200, [])
    assert adapter.fetch_futures_income(NOW, adapter.clock()) == ()
    assert len(calls) == 2  # Metadata and income must not revive the unknown budget.


def test_real_futures_429_keeps_cross_service_server_cooldown(sdk):
    from services.exchange_adapter import RequestNotSent

    adapter, calls, responses = sdk
    responses['GET', '/fapi/v1/income'] = (429, {'code': -1003, 'msg': 'limited'}, {'Retry-After': '60'})
    with pytest.raises(ExchangeError):
        adapter.fetch_futures_income(NOW - timedelta(seconds=1), NOW)
    assert adapter.request_not_before == NOW + timedelta(seconds=60)
    with pytest.raises(RequestNotSent):
        adapter._call(adapter.spot.delete_order, symbol='BNBUSDT', order_id=123)
    assert all(call['method'] == 'GET' for call in calls)
