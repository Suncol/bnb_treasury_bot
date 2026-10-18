"""Offline regressions for request classes, quota observations and cancellation recovery."""

from dataclasses import replace
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from urllib.parse import parse_qs

import pytest

from core.models import (
    AssetTransferPlan, CancelPlan, CrashGuardState, Operation, OperationKind, OperationStatus,
    OrderPlan, ReplenishmentState,
)
from services.exchange_adapter import ExchangeError, RequestDeferred, RequestNotSent
from services.executor import ExecutionBlocked, Executor
from services.reconciliation import Reconciler
from storage.repository import Repository
from tests.fake_exchange import FakeExchange
from tests.helpers import D, make_account, make_strategy_config
from tests.integration.test_binance_sdk import NOW, millis, sdk, sdk_executor, operation




def observe(adapter, kind, used, *, limit=100, interval='DAY', scope='spot'):
    suffix = {'DAY': '1d', 'MINUTE': '1m'}[interval]
    prefix = 'x-mbx-order-count-' if kind == 'ORDERS' else 'x-mbx-used-weight-'
    header = prefix + suffix
    adapter._observe_request_budget(scope, {header: str(used)}, {
        'rateLimits': [{'rateLimitType': kind, 'interval': interval,
                        'intervalNum': 1, 'limit': limit}],
    })
    return header


def order_response(client_id='bt-review-owner'):
    return {
        'symbol': 'BNBUSDT', 'orderId': 123, 'clientOrderId': client_id,
        'side': 'BUY', 'type': 'LIMIT', 'price': '590', 'origQty': '1',
        'executedQty': '0', 'status': 'NEW', 'timeInForce': 'GTC',
        'time': millis(NOW),
    }


def seed_owner(repo):
    op = Operation(
        'bt-review-owner', OperationKind.ORDER, 'order:BNBUSDT',
        OrderPlan('BNBUSDT', 'BUY', 'LIMIT', D('1'), D('590'), 'GTC', False),
        NOW, state=ReplenishmentState.ACCUMULATE,
    )
    repo.record_intent(op)
    op = replace(op, status=OperationStatus.CONFIRMED, exchange_id='123')
    repo.update_operation(op)
    return op


@pytest.mark.parametrize('used', [89, 90])
def test_r1_orders_quota_does_not_block_order_query(sdk, used):
    adapter, calls, responses = sdk
    responses['GET', '/api/v3/order'] = (200, order_response())
    observe(adapter, 'ORDERS', used)
    assert adapter.request_not_before is None
    try:
        order = adapter.fetch_order('BNBUSDT', order_id='123')
    except RequestDeferred as exc:
        pytest.fail(f'ORDERS {used}/100 blocked GET; HTTP calls={len(calls)}; retry_after={exc.retry_after}')
    assert order.order_id == '123'
    assert [call['method'] for call in calls] == ['GET']


def test_r2_newer_order_count_may_decrease(sdk):
    adapter, _, _ = sdk
    header = observe(adapter, 'ORDERS', 90)
    adapter.clock = lambda: NOW + timedelta(seconds=1)
    observe(adapter, 'ORDERS', 1)
    assert adapter.weight_usage['spot', header][0] == 1, 'Later ORDERS=1 must replace 90'


def test_control_request_weight_keeps_high_water_mark(sdk):
    adapter, _, _ = sdk
    header = observe(adapter, 'REQUEST_WEIGHT', 90)
    observe(adapter, 'REQUEST_WEIGHT', 1)
    assert adapter.weight_usage['spot', header][0] == 90


@pytest.mark.parametrize('kind', [OperationKind.ORDER, OperationKind.CANCEL, OperationKind.TRANSFER])
def test_orders_quota_only_blocks_new_orders_at_final_sdk_boundary(sdk, kind):
    adapter, calls, responses = sdk
    observe(adapter, 'ORDERS', 90)
    responses['DELETE', '/api/v3/order'] = (200, {'orderId': 123, 'status': 'CANCELED'})
    responses['POST', '/sapi/v1/asset/transfer'] = (200, {'tranId': 456})
    payload = {
        OperationKind.ORDER: OrderPlan('BNBUSDT', 'BUY', 'LIMIT', D('1'), D('590'), 'GTC', False),
        OperationKind.CANCEL: CancelPlan('BNBUSDT', '123', 'protect'),
        OperationKind.TRANSFER: AssetTransferPlan('BNB', D('1'), 'SPOT', 'USDⓈ-M Futures', 'return'),
    }[kind]
    if kind == OperationKind.ORDER:
        with pytest.raises(RequestNotSent) as caught:
            adapter.submit(operation(kind, payload))
        assert caught.value.orders_only
        assert not calls
    else:
        assert adapter.submit(operation(kind, payload)).exchange_id is not None
        assert len(calls) == 1


@pytest.mark.parametrize('after_intent', [False, True])
def test_order_only_quota_does_not_persist_a_global_read_cooldown(sdk, tmp_path, after_intent):
    adapter, calls, responses = sdk
    responses['GET', '/api/v3/rateLimit/order'] = (200, [
        {'rateLimitType': 'ORDERS', 'interval': 'DAY', 'intervalNum': 1, 'limit': 100, 'count': 90},
    ])
    cfg = make_strategy_config()
    account = FakeExchange(make_account(contract_bnb=D('27'), spot_usd=D('2000'))).fetch_account_snapshot()
    path = tmp_path / 'order-quota.db'
    repo = Repository(path)
    repo.initialize_spot_checkpoint(account, {'reason': 'offline fixture'})
    runtime = replace(repo.runtime(), api_errors=1, transfer_failures=1,
                      resume_replenishment=True, resume_replanning=True)
    repo.save('runtime', runtime)
    payload = OrderPlan('BNBUSDT', 'BUY', 'LIMIT_MAKER', D('1'), D('590'), 'GTC', True)
    submit = adapter.submit

    def quota_after_journal(op):
        assert repo.operation(op.client_id).status == OperationStatus.PENDING
        observe(adapter, 'ORDERS', 90)
        return submit(op)

    executor = sdk_executor(adapter, repo, cfg)
    if after_intent:
        adapter.submit = quota_after_journal
        result = executor.execute(payload, NOW, ReplenishmentState.ACCUMULATE, account=account)
        assert result.status == OperationStatus.FAILED and result.not_sent
        assert not result.balance_pending and not result.failure_recorded
    else:
        observe(adapter, 'ORDERS', 90)
        with pytest.raises(ExecutionBlocked):
            executor.execute(payload, NOW, ReplenishmentState.ACCUMULATE, account=account)
        assert not repo.operations()
    assert [call['method'] for call in calls] == ([] if after_intent else ['GET'])
    assert not repo.operations(unresolved_only=True)
    assert repo.runtime() == runtime
    assert repo.budget_used(NOW, 'USDT') == 0
    assert repo.load('transport_backoff') is None
    repo.close()
    repo = Repository(path)
    assert repo.runtime() == runtime
    assert not repo.operations(unresolved_only=True)
    repo.close()


def test_fresh_response_with_decreased_orders_count_reopens_only_order_gate(sdk):
    adapter, calls, responses = sdk
    header = observe(adapter, 'ORDERS', 90)
    responses['GET', '/api/v3/order'] = (200, order_response(), {header: '1'})
    responses['POST', '/api/v3/order'] = (200, {'orderId': 456, 'status': 'NEW'})
    with pytest.raises(RequestDeferred):
        adapter.ensure_request_allowed(new_order=True)
    adapter.fetch_order('BNBUSDT', order_id='123')
    adapter.ensure_request_allowed(new_order=True)
    result = adapter.submit(operation(OperationKind.ORDER,
        OrderPlan('BNBUSDT', 'BUY', 'LIMIT', D('1'), D('590'), 'GTC', False)))
    assert result.status == OperationStatus.CONFIRMED
    assert [call['method'] for call in calls] == ['GET', 'POST']


def test_clock_rollback_during_request_does_not_lower_order_count(sdk):
    adapter, _, responses = sdk
    now = [NOW + timedelta(seconds=10)]
    adapter.clock = lambda: now[0]
    header = observe(adapter, 'ORDERS', 90)

    def rollback(_):
        now[0] -= timedelta(seconds=1)
        return 200, order_response(), {header: '1'}

    responses['GET', '/api/v3/order'] = rollback
    adapter.fetch_order('BNBUSDT', order_id='123')
    with pytest.raises(RequestDeferred):
        adapter.ensure_request_allowed(new_order=True)
    responses['GET', '/api/v3/order'] = (200, order_response(), {header: '1'})
    now[0] = NOW + timedelta(seconds=20)
    adapter.fetch_order('BNBUSDT', order_id='123')
    adapter.ensure_request_allowed(new_order=True)


@pytest.mark.parametrize('kind,older,newer,expected', [
    ('ORDERS', 90, 1, 1), ('ORDERS', 1, 90, 90),
    ('REQUEST_WEIGHT', 90, 1, 90), ('REQUEST_WEIGHT', 1, 90, 94),
])
def test_reordered_sdk_responses_respect_each_quota_semantics(sdk, kind, older, newer, expected):
    adapter, _, responses = sdk
    header = observe(adapter, kind, 0)
    started, release = Event(), Event()

    def response(request):
        old = parse_qs(request['params'])['orderId'] == ['1']
        if old:
            started.set()
            assert release.wait(5), 'older request was not released'
        return 200, order_response(), {header: str(older if old else newer)}

    responses['GET', '/api/v3/order'] = response
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(adapter.fetch_order, 'BNBUSDT', order_id='1')
        try:
            assert started.wait(3)
            adapter.fetch_order('BNBUSDT', order_id='123')
        finally:
            release.set()
        pending.result(timeout=3)
    assert adapter.weight_usage['spot', header][0] == expected
    if expected >= 90:
        with pytest.raises(RequestDeferred):
            adapter.ensure_request_allowed(new_order=True)
    else:
        adapter.ensure_request_allowed(new_order=True)


@pytest.mark.parametrize('kind,expected', [('ORDERS', 1), ('REQUEST_WEIGHT', 90)])
def test_window_reset_discards_old_orders_but_keeps_weight_conservative(sdk, kind, expected):
    adapter, _, responses = sdk
    now = [NOW + timedelta(seconds=59)]
    adapter.clock = lambda: now[0]
    header = observe(adapter, kind, 0, interval='MINUTE')
    started, release = Event(), Event()

    def response(request):
        old = parse_qs(request['params'])['orderId'] == ['1']
        if old:
            started.set()
            assert release.wait(5), 'older request was not released'
        return 200, order_response(), {header: '90' if old else '1'}

    responses['GET', '/api/v3/order'] = response
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(adapter.fetch_order, 'BNBUSDT', order_id='1')
        try:
            assert started.wait(3)
            now[0] = NOW + timedelta(seconds=61)
            adapter.fetch_order('BNBUSDT', order_id='123')
        finally:
            release.set()
        pending.result(timeout=3)
    assert adapter.weight_usage['spot', header][0] == expected
    if kind == 'ORDERS':
        adapter.ensure_request_allowed(new_order=True)
    else:
        with pytest.raises(RequestDeferred) as caught:
            adapter.ensure_request_allowed(new_order=True)
        assert not caught.value.orders_only


def test_order_quota_never_masks_a_general_request_cooldown(sdk):
    adapter, _, _ = sdk
    observe(adapter, 'ORDERS', 90)
    observe(adapter, 'REQUEST_WEIGHT', 5400, limit=6000, interval='MINUTE')
    with pytest.raises(RequestDeferred) as caught:
        adapter.ensure_request_allowed(new_order=True)
    assert not caught.value.orders_only
    assert caught.value.retry_after == 61


def test_restored_order_capacity_does_not_resubmit_an_unknown_order(sdk, tmp_path):
    adapter, calls, responses = sdk
    repo = Repository(tmp_path / 'unknown-order.db')
    try:
        account = FakeExchange(make_account(contract_bnb=D('27'), spot_usd=D('2000'))).fetch_account_snapshot()
        repo.initialize_spot_checkpoint(account, {'reason': 'offline fixture'})
        executor = sdk_executor(adapter, repo, make_strategy_config())
        responses['POST', '/api/v3/order'] = (503, {'msg': 'unknown execution'})
        responses['GET', '/api/v3/rateLimit/order'] = (200, [
            {'rateLimitType': 'ORDERS', 'interval': 'DAY', 'intervalNum': 1, 'limit': 100, 'count': 90},
        ])
        payload = OrderPlan('BNBUSDT', 'BUY', 'LIMIT', D('1'), D('590'), 'GTC', False)
        original = executor.execute(payload, NOW, ReplenishmentState.ACCUMULATE, account=account)
        assert original.status == OperationStatus.UNKNOWN and original.balance_pending
        for used in (90, 1):
            observe(adapter, 'ORDERS', used)
            expected = ExecutionBlocked if used == 90 else RuntimeError
            message = 'New order quota' if used == 90 else 'New operations require.*reconciled runtime'
            with pytest.raises(expected, match=message):
                executor.execute(payload, NOW, ReplenishmentState.ACCUMULATE, account=account)
        saved = repo.operation(original.client_id)
        assert saved.status == OperationStatus.UNKNOWN and saved.balance_pending
        assert not saved.not_sent
        assert len(repo.operations()) == 1
        assert [call['method'] for call in calls] == ['POST', 'GET']
    finally:
        repo.close()


@pytest.mark.parametrize('used,allowed', [(5996, True), (5997, False), (6000, False)])
def test_protective_query_has_a_finite_request_weight_reserve(sdk, used, allowed):
    adapter, calls, responses = sdk
    observe(adapter, 'REQUEST_WEIGHT', used, limit=6000, interval='MINUTE')
    responses['GET', '/api/v3/order'] = (200, order_response())
    if allowed:
        assert adapter.fetch_order('BNBUSDT', order_id='123', protective=True).order_id == '123'
        assert len(calls) == 1
        assert 'protective' not in parse_qs(calls[0]['params'])
    else:
        with pytest.raises(RequestDeferred):
            adapter.fetch_order('BNBUSDT', order_id='123', protective=True)
        assert not calls


def test_protection_does_not_leak_to_ordinary_queries_or_new_orders(sdk):
    adapter, calls, responses = sdk
    observe(adapter, 'REQUEST_WEIGHT', 5400, limit=6000, interval='MINUTE')
    responses['GET', '/api/v3/order'] = (200, order_response())
    adapter.fetch_order('BNBUSDT', order_id='123', protective=True)
    with pytest.raises(RequestDeferred):
        adapter.fetch_order('BNBUSDT', order_id='123')
    with pytest.raises(RequestNotSent):
        adapter._call(adapter.spot.new_order, protective=True, symbol='BNBUSDT')
    assert len(calls) == 1


@pytest.mark.parametrize('recover', [False, True])
@pytest.mark.parametrize('server_cooldown', [False, True])
def test_spot_cancellation_ignores_futures_quota_but_honors_server_ban(sdk, tmp_path, recover, server_cooldown):
    adapter, calls, responses = sdk
    now = [NOW]
    adapter.clock = lambda: now[0]
    repo = Repository(tmp_path / 'scoped-cancel.db')
    try:
        owner = seed_owner(repo)
        executor = Executor(adapter, repo, make_strategy_config())
        payload = CancelPlan('BNBUSDT', '123', 'protect')
        if recover:
            responses['DELETE', '/api/v3/order'] = (503, {'msg': 'unknown'})
            original = executor.execute(payload, now[0], ReplenishmentState.ACCUMULATE)
            assert original.status == OperationStatus.UNKNOWN
            now[0] += timedelta(seconds=3)
        responses['GET', '/api/v3/order'] = (200, order_response(owner.client_id))
        responses['DELETE', '/api/v3/order'] = (200, {'orderId': 123, 'status': 'CANCELED'})
        observe(adapter, 'REQUEST_WEIGHT', 100, limit=100, interval='MINUTE', scope='futures')
        observe(adapter, 'REQUEST_WEIGHT', 5400, limit=6000, interval='MINUTE')
        if server_cooldown:
            adapter.request_not_before = now[0] + timedelta(seconds=90)
            with pytest.raises(RequestDeferred):
                executor.execute(payload, now[0], ReplenishmentState.ACCUMULATE)
            assert len(calls) == int(recover)
        else:
            result = executor.execute(payload, now[0], ReplenishmentState.ACCUMULATE)
            assert result.status == OperationStatus.CONFIRMED
            assert [call['method'] for call in calls] == (
                ['DELETE', 'GET', 'DELETE'] if recover else ['DELETE'])
    finally:
        repo.close()


@pytest.mark.parametrize('initial_status', [OperationStatus.PENDING, OperationStatus.UNKNOWN])
@pytest.mark.parametrize('used', [0, 5400])
def test_r3_bound_cancel_recovery_can_use_protection_reserve(sdk, tmp_path, used, initial_status):
    adapter, calls, responses = sdk
    now = [NOW]
    adapter.clock = lambda: now[0]
    repo = Repository(tmp_path / 'recovery.db')
    try:
        owner = seed_owner(repo)
        executor = Executor(adapter, repo, make_strategy_config())
        responses['DELETE', '/api/v3/order'] = (
            (200, {'orderId': 123, 'status': 'PENDING_CANCEL'})
            if initial_status == OperationStatus.PENDING else (503, {'msg': 'unknown'})
        )
        payload = CancelPlan('BNBUSDT', '123', 'review protection')
        initial = executor.execute(payload, now[0], ReplenishmentState.ACCUMULATE)
        assert initial.status == initial_status
        assert initial.cancel_attempts == 1
        repo.close()
        repo = Repository(tmp_path / 'recovery.db')
        executor = Executor(adapter, repo, make_strategy_config())
        assert repo.cancel_operation('BNBUSDT', '123') == initial
        responses['GET', '/api/v3/order'] = (200, order_response(owner.client_id))
        responses['DELETE', '/api/v3/order'] = (200, {'orderId': 123, 'status': 'CANCELED'})
        now[0] += timedelta(seconds=3)
        observe(adapter, 'REQUEST_WEIGHT', used, limit=6000, interval='MINUTE')
        adapter.ensure_request_allowed(protective=True)
        try:
            result = executor.execute(payload, now[0], ReplenishmentState.ACCUMULATE)
        except RequestDeferred as exc:
            saved = repo.cancel_operation('BNBUSDT', '123')
            pytest.fail(f'Protective recovery blocked by GET at {used}/6000; '
                        f'HTTP={[call["method"] for call in calls]}; '
                        f'status={saved.status.value}; cancel_attempts={saved.cancel_attempts}; '
                        f'retry_after={exc.retry_after}')
        assert result.status == OperationStatus.CONFIRMED
        assert result.client_id == initial.client_id
        assert result.cancel_attempts == 2
        assert [call['method'] for call in calls] == ['DELETE', 'GET', 'DELETE']
    finally:
        repo.close()


@pytest.mark.parametrize('reply', ['missing', 'conflict', 'pending', 'filled', 'canceled', '429', '418', 'full_weight'])
def test_protected_cancel_query_preserves_unknown_and_retry_safety(sdk, tmp_path, reply):
    adapter, calls, responses = sdk
    now = [NOW]
    adapter.clock = lambda: now[0]
    repo = Repository(tmp_path / 'uncertain-cancel.db')
    try:
        owner = seed_owner(repo)
        executor = Executor(adapter, repo, make_strategy_config())
        responses['DELETE', '/api/v3/order'] = (503, {'msg': 'unknown'})
        payload = CancelPlan('BNBUSDT', '123', 'protect')
        original = executor.execute(payload, now[0], ReplenishmentState.ACCUMULATE)
        assert original.status == OperationStatus.UNKNOWN
        errors = repo.runtime().api_errors
        now[0] += timedelta(seconds=3)
        observe(adapter, 'REQUEST_WEIGHT', 5400, limit=6000, interval='MINUTE')
        row = order_response(owner.client_id)
        if reply == 'conflict':
            row['price'] = '591'
        elif reply in {'filled', 'canceled', 'pending'}:
            row['status'] = {'filled': 'FILLED', 'canceled': 'CANCELED', 'pending': 'PENDING_CANCEL'}[reply]
            row['executedQty'] = '1' if reply == 'filled' else '0'
        response = (200, row)
        if reply == 'missing':
            response = (400, {'code': -2013, 'msg': 'Order does not exist'})
        elif reply in {'429', '418'}:
            response = (int(reply), {'code': -1003, 'msg': 'limited'}, {'Retry-After': '90'})
        elif reply == 'full_weight':
            response = (200, row, {'X-MBX-USED-WEIGHT-1M': '6000'})
        responses['GET', '/api/v3/order'] = response
        if reply in {'conflict', '429', '418'}:
            with pytest.raises(ExchangeError):
                executor.execute(payload, now[0], ReplenishmentState.ACCUMULATE)
        else:
            executor.execute(payload, now[0], ReplenishmentState.ACCUMULATE)
        saved = repo.cancel_operation('BNBUSDT', '123')
        assert saved.client_id == original.client_id
        assert saved.cancel_attempts == 1 and not saved.not_sent
        assert repo.runtime().api_errors == errors
        assert [call['method'] for call in calls] == ['DELETE', 'GET']
        if reply in {'filled', 'canceled'}:
            assert saved.status == OperationStatus.CONFIRMED
        else:
            assert saved.status == OperationStatus.UNKNOWN and saved.unresolved
        if reply in {'429', '418'}:
            assert adapter.request_not_before == now[0] + timedelta(seconds=90)
            for protective in (False, True):
                with pytest.raises(RequestDeferred):
                    adapter.fetch_order('BNBUSDT', order_id='123', protective=protective)
            assert len(calls) == 2
        if reply == 'full_weight':
            assert repo.runtime().read_retry_at > now[0]
            assert saved.cancel_retry_at > now[0]
    finally:
        repo.close()


@pytest.mark.parametrize('crash_guard', [False, True])
@pytest.mark.parametrize('bound', [False, True])
def test_r3_degraded_protection_can_cancel_a_journaled_order(sdk, tmp_path, crash_guard, bound):
    adapter, calls, responses = sdk
    repo = Repository(tmp_path / 'degraded.db')
    try:
        owner = seed_owner(repo)
        if not bound:
            repo.update_operation(replace(owner, status=OperationStatus.UNKNOWN, exchange_id=None))
        responses['GET', '/api/v3/order'] = (200, order_response(owner.client_id))
        responses['DELETE', '/api/v3/order'] = (200, {'orderId': 123, 'status': 'CANCELED'})
        observe(adapter, 'REQUEST_WEIGHT', 5400, limit=6000, interval='MINUTE')
        reconciler = Reconciler(adapter, repo, make_strategy_config())
        guard = CrashGuardState(active=True, keep_price_ceiling=D('580')) if crash_guard else None
        try:
            plans = list(reconciler.protective_cancellations(now=NOW, guard=guard))
        except RequestDeferred:
            pytest.fail(f'Degraded protection yielded no cancellation; HTTP calls={len(calls)}')
        assert len(plans) == 1 and plans[0].order_id == '123'
        assert repo.operation(owner.client_id).exchange_id == '123'
        if not bound:
            assert repo.operation(owner.client_id).status == OperationStatus.UNKNOWN
        result = reconciler.executor.execute(plans[0], NOW, ReplenishmentState.ACCUMULATE)
        assert result.status == OperationStatus.CONFIRMED
        assert calls[-1]['method'] == 'DELETE'
    finally:
        repo.close()
