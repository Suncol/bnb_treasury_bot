"""Bound first cancellations retain DELETE capacity without weakening UNKNOWN recovery."""

from contextlib import closing
from dataclasses import replace
from datetime import timedelta

import pytest

from core.models import CancelPlan, OperationStatus, ReplenishmentState
from services.exchange_adapter import RequestDeferred
from services.executor import Executor
from services.reconciliation import Reconciler
from services.runner import Runner
from storage.repository import Repository
from tests.helpers import make_market, make_strategy_config
from tests.integration.test_binance_sdk import NOW, sdk
from tests.integration.test_request_budgets import observe, seed_owner


@pytest.mark.parametrize('used', [5996, 5997, 5998, 5999])
def test_known_identity_first_cancel_uses_remaining_delete_capacity(sdk, tmp_path, used):
    adapter, calls, responses = sdk
    responses['DELETE', '/api/v3/order'] = (200, {'orderId': 123, 'status': 'CANCELED'})
    with closing(Repository(tmp_path / 'bound.db')) as repo:
        seed_owner(repo)
        observe(adapter, 'REQUEST_WEIGHT', used, limit=6000, interval='MINUTE')
        reconciler = Reconciler(adapter, repo, make_strategy_config())
        try:
            plans = list(reconciler.protective_cancellations(now=NOW, guard=None))
        except RequestDeferred as exc:
            pytest.fail(f'Known exchange ID first cancellation withheld: used={used}; HTTP={calls}; {exc}')
        assert len(plans) == 1
        result = reconciler.executor.execute(plans[0], NOW, ReplenishmentState.ACCUMULATE)
        assert result.status == OperationStatus.CONFIRMED
        assert [c['method'] for c in calls] == ['DELETE']


def test_direct_first_delete_still_fits_same_budget(sdk, tmp_path):
    adapter, calls, responses = sdk
    responses['DELETE', '/api/v3/order'] = (200, {'orderId': 123, 'status': 'CANCELED'})
    with closing(Repository(tmp_path / 'direct.db')) as repo:
        seed_owner(repo)
        observe(adapter, 'REQUEST_WEIGHT', 5999, limit=6000, interval='MINUTE')
        executor = Executor(adapter, repo, make_strategy_config())
        result = executor.execute(CancelPlan('BNBUSDT', '123', 'protect'), NOW,
                                  ReplenishmentState.ACCUMULATE)
        assert result.status == OperationStatus.CONFIRMED
        assert [c['method'] for c in calls] == ['DELETE']


def test_real_ban_blocks_even_direct_delete(sdk, tmp_path):
    adapter, calls, _ = sdk
    with closing(Repository(tmp_path / 'banned.db')) as repo:
        seed_owner(repo)
        observe(adapter, 'REQUEST_WEIGHT', 5999, limit=6000, interval='MINUTE')
        adapter.request_not_before = NOW + timedelta(seconds=90)
        executor = Executor(adapter, repo, make_strategy_config())
        with pytest.raises(RequestDeferred):
            executor.execute(CancelPlan('BNBUSDT', '123', 'protect'), NOW,
                             ReplenishmentState.ACCUMULATE)
        assert not calls


def test_unknown_cancel_still_requires_verified_order_query(sdk, tmp_path):
    adapter, calls, responses = sdk
    now = [NOW]
    adapter.clock = lambda: now[0]
    responses['DELETE', '/api/v3/order'] = (503, {'msg': 'unknown execution'})
    with closing(Repository(tmp_path / 'unknown.db')) as repo:
        seed_owner(repo)
        executor = Executor(adapter, repo, make_strategy_config())
        plan = CancelPlan('BNBUSDT', '123', 'protect')
        first = executor.execute(plan, now[0], ReplenishmentState.ACCUMULATE)
        assert first.status == OperationStatus.UNKNOWN
        now[0] += timedelta(seconds=3)
        observe(adapter, 'REQUEST_WEIGHT', 5999, limit=6000, interval='MINUTE')
        with pytest.raises(RequestDeferred):
            executor.execute(plan, now[0], ReplenishmentState.ACCUMULATE)
        saved = repo.operation(first.client_id)
        assert saved.status == OperationStatus.UNKNOWN
        assert saved.cancel_attempts == 1
        assert [c['method'] for c in calls] == ['DELETE']


@pytest.mark.parametrize('blocked', ['full', 'server_ban', 'unbound'])
def test_fallback_cannot_bypass_budget_ban_or_missing_identity(sdk, tmp_path, blocked):
    adapter, calls, _ = sdk
    with closing(Repository(tmp_path / 'blocked.db')) as repo:
        owner = seed_owner(repo)
        if blocked == 'unbound':
            repo.update_operation(replace(owner, exchange_id=None, status=OperationStatus.UNKNOWN))
        observe(adapter, 'REQUEST_WEIGHT', 6000 if blocked == 'full' else 5999,
                limit=6000, interval='MINUTE')
        if blocked == 'server_ban':
            adapter.request_not_before = NOW + timedelta(seconds=90)
        reconciler = Reconciler(adapter, repo, make_strategy_config())
        with pytest.raises(RequestDeferred):
            list(reconciler.protective_cancellations(now=NOW))
        assert not calls
        assert repo.cancel_operation('BNBUSDT', '123') is None


@pytest.mark.parametrize('failure', ['ban', 'full'])
def test_new_limit_between_plan_and_first_delete_is_still_checked(sdk, tmp_path, failure):
    adapter, calls, _ = sdk
    with closing(Repository(tmp_path / 'race.db')) as repo:
        seed_owner(repo)
        observe(adapter, 'REQUEST_WEIGHT', 5999, limit=6000, interval='MINUTE')
        reconciler = Reconciler(adapter, repo, make_strategy_config())
        plans = list(reconciler.protective_cancellations(now=NOW))
        assert len(plans) == 1
        if failure == 'ban':
            adapter.request_not_before = NOW + timedelta(seconds=90)
        else:
            observe(adapter, 'REQUEST_WEIGHT', 6000, limit=6000, interval='MINUTE')
        with pytest.raises(RequestDeferred):
            reconciler.executor.execute(plans[0], NOW, ReplenishmentState.ACCUMULATE)
        assert not calls
        assert repo.cancel_operation('BNBUSDT', '123') is None


@pytest.mark.parametrize('execute', [True, False])
def test_runner_degraded_path_uses_remaining_capacity_only_in_execution_mode(sdk, tmp_path, execute):
    adapter, calls, responses = sdk
    responses['DELETE', '/api/v3/order'] = (200, {'orderId': 123, 'status': 'CANCELED'},
                                           {'X-MBX-USED-WEIGHT-1M': '6000'})
    with closing(Repository(tmp_path / 'runner.db')) as repo:
        seed_owner(repo)
        observe(adapter, 'REQUEST_WEIGHT', 5999, limit=6000, interval='MINUTE')
        runner = Runner(adapter, repo, make_strategy_config(), clock=lambda: NOW, execute=execute)
        market = replace(make_market(), ts=NOW)
        runner._failed_read(RequestDeferred('ordinary read capacity exhausted', retry_after=60), market)
        assert repo.runtime().read_retry_at > NOW
        assert repo.runtime().api_errors == 0
        if execute:
            assert repo.cancel_operation('BNBUSDT', '123').status == OperationStatus.CONFIRMED
            assert [c['method'] for c in calls] == ['DELETE']
        else:
            assert repo.cancel_operation('BNBUSDT', '123') is None
            assert not calls
