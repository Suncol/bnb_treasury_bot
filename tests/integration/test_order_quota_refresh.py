"""Recover ORDERS capacity from the documented read API, before journaling a write."""

from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import timedelta
from threading import Event
from urllib.parse import urlsplit

import pytest

from core.models import AssetTransferPlan, OperationKind, OperationStatus, OrderPlan, ReplenishmentState
from services.exchange_adapter import ExchangeError, RequestDeferred, RequestNotSent
from services.executor import ExecutionBlocked
from storage.repository import Repository
from tests.fake_exchange import FakeExchange
from tests.helpers import D, make_account, make_strategy_config
from tests.integration.test_binance_sdk import NOW, operation, sdk, sdk_executor
from tests.integration.test_request_budgets import observe, order_response


def counts(count, *, interval='DAY', limit=100, interval_num=1):
    return [{'rateLimitType': 'ORDERS', 'interval': interval,
             'intervalNum': interval_num, 'limit': limit, 'count': count}]


def paths(calls):
    return [(c['method'], urlsplit(c['url']).path) for c in calls]


def ready_executor(adapter, repo):
    account = FakeExchange(make_account(contract_bnb=D('27'), spot_usd=D('2000'))).fetch_account_snapshot()
    repo.initialize_spot_checkpoint(account, {'reason': 'offline fixture'})
    return sdk_executor(adapter, repo, make_strategy_config()), account


PAYLOAD = OrderPlan('BNBUSDT', 'BUY', 'LIMIT_MAKER', D('1'), D('590'), 'GTC', True)


def test_documented_count_body_can_release_order_gate(sdk):
    adapter, calls, responses = sdk
    header = observe(adapter, 'ORDERS', 90)
    responses['GET', '/api/v3/rateLimit/order'] = (
        200, counts(1), {'X-MBX-USED-WEIGHT-1M': '41'})
    adapter._call(adapter.spot.rate_limit_order, recv_window=5000)
    assert adapter.weight_usage['spot', header][0] == 1
    adapter.ensure_request_allowed(new_order=True)
    assert paths(calls) == [('GET', '/api/v3/rateLimit/order')]


def test_executor_refreshes_released_capacity_before_journaling(sdk, tmp_path):
    adapter, calls, responses = sdk
    header = observe(adapter, 'ORDERS', 90)
    responses['GET', '/api/v3/order'] = (
        200, dict(order_response(), status='FILLED', executedQty='1'),
        {'X-MBX-USED-WEIGHT-1M': '5'})
    adapter.fetch_order('BNBUSDT', order_id='123')
    assert adapter.weight_usage['spot', header][0] == 90
    with closing(Repository(tmp_path / 'released.db')) as repo:
        executor, account = ready_executor(adapter, repo)

        def refreshed(_):
            assert not repo.operations()  # No slow quota read after intent commit.
            return 200, counts(89), {'X-MBX-USED-WEIGHT-1M': '45'}

        responses['GET', '/api/v3/rateLimit/order'] = refreshed
        responses['POST', '/api/v3/order'] = (200, {'orderId': 456, 'status': 'NEW'})
        result = executor.execute(PAYLOAD, NOW, ReplenishmentState.ACCUMULATE, account=account)
        assert result.status == OperationStatus.CONFIRMED
        assert len(repo.operations()) == 1
        assert repo.runtime().read_retry_at is None
    assert paths(calls) == [('GET', '/api/v3/order'),
                            ('GET', '/api/v3/rateLimit/order'), ('POST', '/api/v3/order')]


def test_still_full_quota_refresh_is_bounded_without_global_cooldown(sdk, tmp_path, monkeypatch):
    adapter, calls, responses = sdk
    monotonic = [1000.0]
    monkeypatch.setattr('services.sdk_transport.time.monotonic', lambda: monotonic[0])
    observe(adapter, 'ORDERS', 90)
    responses['GET', '/api/v3/rateLimit/order'] = (200, counts(90))
    with closing(Repository(tmp_path / 'bounded.db')) as repo:
        executor, account = ready_executor(adapter, repo)
        runtime = repo.runtime()
        for offset in (0, 1, 29.999):
            monotonic[0] = 1000.0 + offset
            with pytest.raises(ExecutionBlocked):
                executor.execute(PAYLOAD, NOW, ReplenishmentState.ACCUMULATE, account=account)
        assert paths(calls) == [('GET', '/api/v3/rateLimit/order')]
        assert not repo.operations() and repo.runtime() == runtime
        assert repo.load('transport_backoff') is None
        responses['GET', '/api/v3/rateLimit/order'] = (200, counts(1))
        responses['POST', '/api/v3/order'] = (200, {'orderId': 456, 'status': 'NEW'})
        monotonic[0] = 1030.0
        result = executor.execute(PAYLOAD, NOW, ReplenishmentState.ACCUMULATE, account=account)
        assert result.status == OperationStatus.CONFIRMED
        assert paths(calls) == [('GET', '/api/v3/rateLimit/order')] * 2 + [('POST', '/api/v3/order')]


@pytest.mark.parametrize('status', [429, 418, 503])
def test_quota_read_failure_never_creates_an_order_intent(sdk, tmp_path, status):
    adapter, calls, responses = sdk
    header = observe(adapter, 'ORDERS', 90)
    responses['GET', '/api/v3/rateLimit/order'] = (
        status, {'code': -1003, 'msg': 'unavailable'}, {'Retry-After': '90'})
    with closing(Repository(tmp_path / 'failed-read.db')) as repo:
        executor, account = ready_executor(adapter, repo)
        with pytest.raises(ExchangeError):
            executor.execute(PAYLOAD, NOW, ReplenishmentState.ACCUMULATE, account=account)
        assert not repo.operations()
        assert adapter.weight_usage['spot', header][0] == 90
        if status in {429, 418}:
            assert adapter.request_not_before == NOW + timedelta(seconds=90)
            with pytest.raises(RequestDeferred):
                executor.execute(PAYLOAD, NOW, ReplenishmentState.ACCUMULATE, account=account)
        else:
            with pytest.raises(ExecutionBlocked):
                executor.execute(PAYLOAD, NOW, ReplenishmentState.ACCUMULATE, account=account)
        assert paths(calls) == [('GET', '/api/v3/rateLimit/order')]


def test_final_sdk_preflight_does_not_insert_quota_reads_after_intent(sdk):
    adapter, calls, _ = sdk
    observe(adapter, 'ORDERS', 90)
    with pytest.raises(RequestNotSent) as exc:
        adapter.submit(operation(OperationKind.ORDER, PAYLOAD))
    assert exc.value.orders_only
    assert not calls


@pytest.mark.parametrize('used,allowed', [(5360, True), (5361, False), (5400, False)])
def test_count_refresh_cannot_spend_protection_reserve(sdk, used, allowed):
    adapter, calls, responses = sdk
    observe(adapter, 'ORDERS', 90)
    observe(adapter, 'REQUEST_WEIGHT', used, limit=6000, interval='MINUTE')
    responses['GET', '/api/v3/rateLimit/order'] = (200, counts(1))
    if allowed:
        adapter.refresh_order_budget()
        assert paths(calls) == [('GET', '/api/v3/rateLimit/order')]
    else:
        with pytest.raises(RequestDeferred) as exc:
            adapter.refresh_order_budget()
        assert not exc.value.orders_only
        assert not calls


@pytest.mark.parametrize('scope', ['spot', 'futures'])
def test_general_cooldown_precedes_quota_refresh(sdk, scope):
    adapter, calls, _ = sdk
    observe(adapter, 'ORDERS', 90)
    observe(adapter, 'REQUEST_WEIGHT', 6000, limit=6000, interval='MINUTE', scope=scope)
    with pytest.raises(RequestDeferred) as exc:
        adapter.refresh_order_budget()
    assert not exc.value.orders_only and not calls


def test_count_refresh_is_single_flight_and_does_not_lock_out_other_reads(sdk):
    adapter, calls, responses = sdk
    header = observe(adapter, 'ORDERS', 90)
    started, release = Event(), Event()

    def slow(_):
        started.set()
        assert release.wait(5)
        return 200, counts(1)

    responses['GET', '/api/v3/rateLimit/order'] = slow
    responses['GET', '/api/v3/order'] = (200, order_response())
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(adapter.refresh_order_budget)
        try:
            assert started.wait(3)
            adapter.refresh_order_budget()
            with pytest.raises(RequestDeferred):
                adapter.ensure_request_allowed(new_order=True)
            assert adapter.fetch_order('BNBUSDT', order_id='123').order_id == '123'
        finally:
            release.set()
        pending.result(timeout=3)
    assert adapter.weight_usage['spot', header][0] == 1
    assert paths(calls) == [('GET', '/api/v3/rateLimit/order'), ('GET', '/api/v3/order')]


def test_slow_priority_checkpoint_cannot_cause_back_to_back_quota_reads(sdk, monkeypatch):
    adapter, calls, responses = sdk
    elapsed = [1000.0]
    monkeypatch.setattr('services.sdk_transport.time.monotonic', lambda: elapsed[0])
    observe(adapter, 'ORDERS', 90)
    responses['GET', '/api/v3/rateLimit/order'] = (200, counts(90))

    def checkpoint():
        elapsed[0] += 31
        adapter.read_checkpoint = None

    adapter.read_checkpoint = checkpoint
    adapter.refresh_order_budget()
    adapter.refresh_order_budget()
    assert paths(calls) == [('GET', '/api/v3/rateLimit/order')]
    elapsed[0] += 30
    adapter.refresh_order_budget()
    assert paths(calls) == [('GET', '/api/v3/rateLimit/order')] * 2


@pytest.mark.parametrize('older,newer', [(1, 90), (90, 1)])
def test_late_count_body_cannot_overwrite_newer_header_or_limit(sdk, older, newer):
    adapter, _, responses = sdk
    header = observe(adapter, 'ORDERS', 90)
    started, release = Event(), Event()

    def slow(_):
        started.set()
        assert release.wait(5)
        return 200, counts(older, limit=1000)

    responses['GET', '/api/v3/rateLimit/order'] = slow
    responses['GET', '/api/v3/order'] = (200, order_response(), {header: str(newer)})
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(adapter.refresh_order_budget)
        try:
            assert started.wait(3)
            adapter.fetch_order('BNBUSDT', order_id='123')
        finally:
            release.set()
        pending.result(timeout=3)
    assert adapter.weight_usage['spot', header][0] == newer
    assert adapter.weight_limits['spot', header] == 100


def test_count_windows_are_validated_individually_before_updating_limits(sdk):
    adapter, _, responses = sdk
    now = [NOW + timedelta(seconds=59)]
    adapter.clock = lambda: now[0]
    minute = observe(adapter, 'ORDERS', 90, interval='MINUTE')
    day = observe(adapter, 'ORDERS', 90)

    def crossed_window(_):
        now[0] += timedelta(seconds=2)
        return 200, counts(1, interval='MINUTE', limit=1000) + counts(1, limit=120)

    responses['GET', '/api/v3/rateLimit/order'] = crossed_window
    adapter.refresh_order_budget()
    assert adapter.weight_usage['spot', minute][0] == 90
    assert adapter.weight_limits['spot', minute] == 100
    assert adapter.weight_usage['spot', day][0] == 1
    assert adapter.weight_limits['spot', day] == 120


def test_count_body_observed_during_clock_rollback_cannot_lower_quota(sdk):
    adapter, _, responses = sdk
    now = [NOW + timedelta(seconds=10)]
    adapter.clock = lambda: now[0]
    header = observe(adapter, 'ORDERS', 90)

    def rollback(_):
        now[0] -= timedelta(seconds=1)
        return 200, counts(1, limit=1000)

    responses['GET', '/api/v3/rateLimit/order'] = rollback
    adapter.refresh_order_budget()
    assert adapter.weight_usage['spot', header][0] == 90
    assert adapter.weight_limits['spot', header] == 100


@pytest.mark.parametrize('invalid', [[], counts(-1), counts(1, limit=0),
                                    counts(1, interval_num=0), counts(1, interval='WEEK'),
                                    counts(1) + counts(90), counts(1) + counts(None, interval='MINUTE')])
def test_invalid_count_body_cannot_partially_release_quota(sdk, invalid):
    adapter, calls, responses = sdk
    header = observe(adapter, 'ORDERS', 90)
    responses['GET', '/api/v3/rateLimit/order'] = (
        200, invalid, {'X-MBX-USED-WEIGHT-1M': '41'})
    with pytest.raises(ExchangeError):
        adapter.refresh_order_budget()
    assert adapter.weight_usage['spot', header][0] == 90
    assert adapter.weight_usage['spot', 'x-mbx-used-weight-1m'][0] == 41
    adapter.refresh_order_budget()  # A failed read is bounded too.
    assert len(calls) == 1


def test_missing_or_conflicting_counts_cannot_release_a_full_window(sdk):
    adapter, _, responses = sdk
    day = observe(adapter, 'ORDERS', 90)
    minute = observe(adapter, 'ORDERS', 90, interval='MINUTE')
    responses['GET', '/api/v3/rateLimit/order'] = (200, counts(1), {day: '90'})
    adapter.refresh_order_budget()
    assert adapter.weight_usage['spot', day][0] == 90
    assert adapter.weight_usage['spot', minute][0] == 90


@pytest.mark.parametrize('kind', [OperationKind.ORDER, OperationKind.TRANSFER])
def test_refreshed_count_cannot_release_unknown_operations_after_restart(sdk, tmp_path, kind):
    adapter, calls, responses = sdk
    path = tmp_path / 'unknown.db'
    with closing(Repository(path)) as repo:
        _, account = ready_executor(adapter, repo)
        payload = PAYLOAD if kind == OperationKind.ORDER else AssetTransferPlan(
            'USDT', D('500'), 'USDⓈ-M Futures', 'SPOT', 'funding')
        op = replace(operation(kind, payload), balance_before=account, balance_pending=True)
        repo.record_intent(op)
        repo.update_operation(replace(op, status=OperationStatus.UNKNOWN))
    with closing(Repository(path)) as repo:
        observe(adapter, 'ORDERS', 90)
        responses['GET', '/api/v3/rateLimit/order'] = (200, counts(1))
        executor = sdk_executor(adapter, repo, make_strategy_config())
        with pytest.raises(RuntimeError, match='reconciled runtime'):
            executor.execute(PAYLOAD, NOW, ReplenishmentState.ACCUMULATE, account=account)
        saved = repo.operation(op.client_id)
        assert saved.status == OperationStatus.UNKNOWN and saved.balance_pending
        assert len(repo.operations()) == 1
        assert not saved.not_sent
    assert paths(calls) == [('GET', '/api/v3/rateLimit/order')]


def test_slow_count_refresh_cannot_extend_account_lifetime(sdk, tmp_path):
    adapter, calls, responses = sdk
    now = [NOW]
    adapter.clock = lambda: now[0]
    observe(adapter, 'ORDERS', 90)

    def slow(_):
        now[0] += timedelta(seconds=11)
        return 200, counts(1)

    responses['GET', '/api/v3/rateLimit/order'] = slow
    with closing(Repository(tmp_path / 'slow.db')) as repo:
        executor, account = ready_executor(adapter, repo)
        with pytest.raises(RuntimeError, match='valid, supported account observation'):
            executor.execute(PAYLOAD, NOW, ReplenishmentState.ACCUMULATE, account=account)
        assert not repo.operations()
    assert paths(calls) == [('GET', '/api/v3/rateLimit/order')]


def test_refreshed_counts_do_not_hide_new_request_weight_exhaustion(sdk, tmp_path):
    adapter, calls, responses = sdk
    observe(adapter, 'ORDERS', 90)
    observe(adapter, 'REQUEST_WEIGHT', 5360, limit=6000, interval='MINUTE')
    responses['GET', '/api/v3/rateLimit/order'] = (
        200, counts(1), {'X-MBX-USED-WEIGHT-1M': '5400'})
    with closing(Repository(tmp_path / 'weight-full.db')) as repo:
        executor, account = ready_executor(adapter, repo)
        with pytest.raises(RequestDeferred) as exc:
            executor.execute(PAYLOAD, NOW, ReplenishmentState.ACCUMULATE, account=account)
        assert not exc.value.orders_only
        assert not repo.operations()
    assert paths(calls) == [('GET', '/api/v3/rateLimit/order')]


def test_quota_lost_after_refresh_and_intent_uses_not_sent_without_another_read(sdk, tmp_path):
    adapter, calls, responses = sdk
    observe(adapter, 'ORDERS', 90)
    responses['GET', '/api/v3/rateLimit/order'] = (200, counts(1))
    submit = adapter.submit
    with closing(Repository(tmp_path / 'late-quota.db')) as repo:
        executor, account = ready_executor(adapter, repo)
        before = replace(repo.runtime(), resume_replenishment=True, resume_replanning=True)
        repo.save('runtime', before)

        def quota_after_intent(op):
            assert repo.operation(op.client_id).status == OperationStatus.PENDING
            observe(adapter, 'ORDERS', 90)
            return submit(op)

        adapter.submit = quota_after_intent
        result = executor.execute(PAYLOAD, NOW, ReplenishmentState.ACCUMULATE, account=account)
        assert result.status == OperationStatus.FAILED and result.not_sent
        assert not result.balance_pending and not result.failure_recorded
        assert repo.runtime() == before
        assert len(repo.operations()) == 1
    assert paths(calls) == [('GET', '/api/v3/rateLimit/order')]
