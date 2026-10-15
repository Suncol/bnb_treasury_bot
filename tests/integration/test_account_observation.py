"""Recoverable cross-endpoint account races through the official SDK and Runner."""

from dataclasses import replace
from datetime import timedelta

import pytest

from core.models import ReplenishmentState, RunMode, SliceState
from services.runner import Runner
from storage.repository import Repository
from tests.helpers import make_market, make_strategy_config
from tests.integration.test_binance_sdk import (
    NOW, account_responses, filter_responses, funding_account_responses, millis, sdk,
)


@pytest.fixture
def observed_account(sdk, tmp_path):
    adapter, calls, responses = account_responses(sdk)
    current = [NOW]
    adapter.clock = lambda: current[0]
    responses['GET', '/fapi/v3/balance'][1][0]['balance'] = '32'
    responses['GET', '/api/v3/account'][1]['balances'] = [
        {'asset': 'BNB', 'free': '0.5', 'locked': '0'},
        {'asset': 'USDT', 'free': '0', 'locked': '0'},
    ]
    responses['GET', '/api/v3/openOrders'] = (200, [])
    responses['GET', '/api/v3/myTrades'] = (200, [])
    responses['GET', '/sapi/v1/asset/transfer'] = (200, {'rows': [], 'total': 0})
    responses['GET', '/fapi/v1/income'] = (200, [])
    filter_responses(responses)
    repo = Repository(tmp_path / 'observation.db')
    repo.initialize_spot_checkpoint(adapter.fetch_account_snapshot(), {
        'operator': 'test', 'reason': 'quiescent observation fixture',
    })
    runner = Runner(adapter, repo, make_strategy_config(), clock=adapter.clock)
    yield adapter, repo, runner, current, calls, responses
    repo.close()


def observe(runner, current):
    return runner.run_once(market=replace(make_market(), ts=current[0]))


def test_withdrawal_limit_disagreement_cannot_return_the_earlier_high_limit(observed_account):
    from services.exchange_adapter import AccountObservationInconsistent

    adapter, repo, runner, current, calls, responses = observed_account
    balance = responses['GET', '/fapi/v3/balance'][1][1]
    margin = responses['GET', '/fapi/v3/account'][1]['assets'][0]
    balance['maxWithdrawAmount'] = '50000'
    margin['maxWithdrawAmount'] = '4000'
    with pytest.raises(AccountObservationInconsistent):
        adapter.fetch_account_snapshot()
    for _ in range(3):
        assert observe(runner, current) is None
        assert repo.runtime().run_mode == RunMode.AUTO
        assert repo.runtime().api_errors == 0
        current[0] = repo.runtime().read_retry_at
    margin['maxWithdrawAmount'] = '50000'
    plan = observe(runner, current)
    assert plan.inventory_valid
    assert runner.cached_inputs.account.contract_max_withdraw_amount == 50000
    assert not repo.operations()
    assert all(call['method'] == 'GET' for call in calls)


def test_real_sdk_final_preflight_vetoes_a_consistent_lower_limit(sdk, tmp_path):
    adapter, calls, responses = sdk
    account = funding_account_responses(sdk)
    filter_responses(responses)
    balance_response = responses['GET', '/fapi/v3/balance']
    account_asset = responses['GET', '/fapi/v3/account'][1]['assets'][0]
    observations = []

    def lower_limit_after_planning(request):
        maximum = '50000' if not observations else '4000'
        observations.append(maximum)
        balance_response[1][1]['maxWithdrawAmount'] = maximum
        account_asset['maxWithdrawAmount'] = maximum
        return balance_response

    responses['GET', '/fapi/v3/balance'] = lower_limit_after_planning
    repo = Repository(tmp_path / 'final-sdk-preflight.db')
    repo.initialize_spot_checkpoint(account, {'operator': 'test', 'reason': 'quiescent fixture'})
    repo.save('runtime', replace(repo.runtime(), state=ReplenishmentState.URGENT, api_errors=2))
    runner = Runner(adapter, repo, make_strategy_config(), clock=lambda: NOW)
    plan = runner.run_once(market=replace(make_market(), ts=NOW))
    assert plan is not None and not plan.transfer_decision.allow
    assert 'Minimum transfer unit' in plan.transfer_decision.message
    assert observations == ['50000', '4000']
    assert not repo.operations()
    assert repo.runtime().api_errors == 2 and repo.runtime().run_mode == RunMode.AUTO
    assert all(call['method'] == 'GET' for call in calls)
    repo.close()


@pytest.mark.parametrize('value', [None, 'NaN', '-1'])
def test_missing_or_invalid_account_maximum_remains_real_data_failure(sdk, value):
    from services.exchange_adapter import AccountObservationInconsistent, ExchangeError

    adapter, calls, responses = account_responses(sdk)
    asset = responses['GET', '/fapi/v3/account'][1]['assets'][0]
    if value is None:
        asset.pop('maxWithdrawAmount')
    else:
        asset['maxWithdrawAmount'] = value
    with pytest.raises(ExchangeError) as failure:
        adapter.fetch_account_snapshot()
    assert not isinstance(failure.value, AccountObservationInconsistent)


@pytest.mark.parametrize('field,value', [('walletBalance', '9999'), ('updateTime', millis(NOW) + 1)])
@pytest.mark.parametrize('previous_errors', [0, 2])
def test_successful_but_mixed_account_reads_freeze_and_recover_without_failure_pause(
    observed_account, field, value, previous_errors
):
    adapter, repo, runner, current, calls, responses = observed_account
    assert observe(runner, current).inventory_valid
    runtime = replace(
        repo.runtime(), state=ReplenishmentState.ACCUMULATE,
        candidate=ReplenishmentState.URGENT, candidate_streak=1,
        slice_state=SliceState(active=True, next_at=NOW + timedelta(minutes=2)),
        api_errors=previous_errors, read_failures=previous_errors, transfer_failures=1,
    )
    repo.save('runtime', runtime)
    before_checkpoint = repo.load('spot_checkpoint')
    before_history = repo.snapshots_since(NOW - timedelta(days=2))
    before_coverage = repo.load('futures_income_coverage')
    quote = responses['GET', '/fapi/v3/account'][1]['assets'][0]
    original = quote[field]
    quote[field] = value
    for _ in range(3):
        current[0] += timedelta(seconds=10)
        assert observe(runner, current) is None
        saved = repo.runtime()
        assert saved.run_mode == RunMode.AUTO
        assert saved.api_errors == previous_errors
        assert saved.read_failures == previous_errors
        assert saved.transfer_failures == 1
        assert (saved.state, saved.candidate, saved.candidate_streak, saved.slice_state,
                saved.last_inventory_at) == (
            runtime.state, runtime.candidate, runtime.candidate_streak,
            runtime.slice_state, runtime.last_inventory_at,
        )
        assert saved.read_retry_at > current[0]
        assert runner.cached_inputs is None
        assert repo.load('spot_checkpoint') == before_checkpoint
        assert repo.snapshots_since(NOW - timedelta(days=2)) == before_history
        assert repo.load('futures_income_coverage') == before_coverage
        assert not repo.operations()
        before_calls = len(calls)
        assert observe(runner, current) is None
        assert len(calls) == before_calls
    quote[field] = original
    current[0] = repo.runtime().read_retry_at
    plan = observe(runner, current)
    assert plan.inventory_valid
    assert repo.runtime().state == ReplenishmentState.IDLE
    assert repo.runtime().last_inventory_at == current[0]
    assert repo.runtime().run_mode == RunMode.AUTO
    assert repo.runtime().api_errors == repo.runtime().read_failures == 0
    assert all(call['method'] == 'GET' for call in calls)


@pytest.mark.parametrize('mode,execute,cooldown', [
    (RunMode.AUTO, True, False), (RunMode.PAUSED, True, False),
    (RunMode.AUTO, False, False), (RunMode.AUTO, True, True),
])
def test_inconsistent_account_still_protects_owned_order_and_preserves_prior_failures(
    observed_account, mode, execute, cooldown
):
    from core.models import OperationKind, OperationStatus, OrderPlan
    from tests.helpers import D

    adapter, repo, runner, current, calls, responses = observed_account
    responses['POST', '/api/v3/order'] = (200, {'orderId': 123, 'status': 'NEW'})
    op = runner.executor.execute(
        OrderPlan('BNBUSDT', 'BUY', 'LIMIT_MAKER', D('1'), D('598'), 'GTC', True),
        NOW, ReplenishmentState.ACCUMULATE, account=adapter.fetch_account_snapshot(),
    )
    repo.update_operation(replace(op, balance_pending=False))
    row = {
        'symbol': 'BNBUSDT', 'orderId': 123, 'clientOrderId': op.client_id,
        'price': '598', 'origQty': '1', 'executedQty': '0', 'time': millis(NOW),
        'side': 'BUY', 'status': 'NEW', 'timeInForce': 'GTC',
    }
    responses['GET', '/api/v3/openOrders'] = (200, [row])
    responses['GET', '/api/v3/order'] = (200, row)
    responses['DELETE', '/api/v3/order'] = (200, {'orderId': 123, 'status': 'CANCELED'})
    repo.save('runtime', replace(repo.runtime(), api_errors=2, read_failures=2, run_mode=mode))
    runner.execute_enabled = execute
    if cooldown:
        spot_response = responses['GET', '/api/v3/account']

        def server_disabled(call):
            adapter.request_not_before = NOW + timedelta(seconds=90)
            return spot_response

        responses['GET', '/api/v3/account'] = server_disabled
    responses['GET', '/fapi/v3/account'][1]['assets'][0]['walletBalance'] = '9999'
    assert observe(runner, current) is None
    cancelled = repo.cancel_operation('BNBUSDT', '123')
    if execute and not cooldown:
        assert cancelled.status == OperationStatus.CONFIRMED
        assert cancelled.cancel_attempts == 1
        assert sorted(operation.kind for operation in repo.operations()) == [OperationKind.CANCEL, OperationKind.ORDER]
    else:
        assert cancelled is None
        assert repo.operations() == (repo.operation(op.client_id),)
    assert repo.runtime().api_errors == repo.runtime().read_failures == 2
    assert repo.runtime().run_mode == mode
    assert repo.load('futures_settlement_issue')['observation']
    assert [call['method'] for call in calls if call['method'] != 'GET'] == (
        ['POST', 'DELETE'] if execute and not cooldown else ['POST']
    )
    if cooldown:
        assert repo.runtime().read_retry_at >= adapter.request_not_before


def test_authenticated_maintenance_preserves_api_failures_and_restart_retry(
    observed_account, tmp_path, monkeypatch
):
    from services import cli
    from services.exchange_adapter import AccountObservationInconsistent

    adapter, _, _, current, calls, responses = observed_account
    path = tmp_path / 'maintenance.db'
    repo = Repository(path)
    repo.save('runtime', replace(repo.runtime(), api_errors=2, read_failures=2, transfer_failures=1))
    repo.close()
    responses['GET', '/api/v3/account'][1]['uid'] = 123
    responses['GET', '/fapi/v3/account'][1]['assets'][0]['walletBalance'] = '9999'

    class Clock:
        @staticmethod
        def now(tz):
            return current[0]

    waits = []

    class Stop:
        def set(self):
            pass

        def wait(self, seconds):
            waits.append(seconds)
            return True

    monkeypatch.setattr(cli, 'datetime', Clock)
    monkeypatch.setattr(cli, 'Event', Stop)
    monkeypatch.setattr(cli.signal, 'signal', lambda *_: None)
    monkeypatch.setattr(cli, 'BinanceAdapter', lambda *args, **kwargs: adapter)
    monkeypatch.setattr(cli.os.sys, 'argv', [
        'cli', '--db', str(path), '--initialize-spot-baseline',
        '--operator', 'test', '--reason', 'quiescent fixture',
    ])
    with pytest.raises(AccountObservationInconsistent):
        cli.main()
    repo = Repository(path)
    runtime = repo.runtime()
    assert runtime.api_errors == runtime.read_failures == 2
    assert runtime.run_mode == RunMode.AUTO
    assert runtime.transfer_failures == 1
    assert repo.load('transport_backoff')['failures'] == 2
    assert repo.load('spot_checkpoint') is None
    assert repo.load('futures_settlement_issue')['observation']
    repo.close()
    before_calls = len(calls)
    cli.main()
    assert len(calls) == before_calls and len(waits) == 1


@pytest.mark.parametrize('failure', [401, 429, 418, 503, 'timeout', 'missing_asset', 'nonfinite'])
def test_real_account_failures_still_pause_and_respect_exchange_cooldown(observed_account, failure):
    import requests

    _, repo, runner, current, calls, responses = observed_account
    repo.save('runtime', replace(repo.runtime(), api_errors=2, read_failures=2))
    if failure == 'timeout':
        responses['GET', '/fapi/v3/account'] = requests.Timeout('read timeout')
    elif failure == 'missing_asset':
        responses['GET', '/fapi/v3/account'][1]['assets'] = []
    elif failure == 'nonfinite':
        responses['GET', '/fapi/v3/account'][1]['assets'][0]['walletBalance'] = 'NaN'
    else:
        responses['GET', '/fapi/v3/account'] = (
            failure, {'code': -1000, 'msg': 'account unavailable'}, {'Retry-After': '90'},
        )
    assert observe(runner, current) is None
    assert repo.runtime().api_errors == repo.runtime().read_failures == 3
    assert repo.runtime().run_mode == RunMode.PAUSED
    if failure in {429, 418}:
        assert repo.runtime().read_retry_at >= NOW + timedelta(seconds=90)
    assert not repo.operations()
    before_calls = len(calls)
    assert observe(runner, current) is None
    assert len(calls) == before_calls


def test_inconsistent_observation_retry_is_bounded_and_survives_restart(observed_account):
    from core.models import OrderPlan
    from tests.helpers import D

    adapter, repo, runner, current, calls, responses = observed_account
    old_account = adapter.fetch_account_snapshot()
    responses['GET', '/fapi/v3/account'][1]['assets'][0]['walletBalance'] = '9999'
    assert observe(runner, current) is None
    path, deadline = repo.path, repo.runtime().read_retry_at
    repo.close()
    repo = Repository(path)
    try:
        runner = Runner(adapter, repo, make_strategy_config(), clock=adapter.clock)
        before_calls = len(calls)
        with pytest.raises(RuntimeError, match='reconciled runtime'):
            runner.executor.execute(
                OrderPlan('BNBUSDT', 'BUY', 'LIMIT_MAKER', D('1'), D('598'), 'GTC', True),
                current[0], ReplenishmentState.ACCUMULATE, account=old_account,
            )
        current[0] = deadline - timedelta(microseconds=1)
        assert observe(runner, current) is None
        assert len(calls) == before_calls
        for _ in range(8):
            current[0] = repo.runtime().read_retry_at
            assert observe(runner, current) is None
            delay = (repo.runtime().read_retry_at - current[0]).total_seconds()
            assert 2 <= delay <= 61
            assert repo.runtime().api_errors == repo.runtime().read_failures == 0
            assert repo.runtime().run_mode == RunMode.AUTO
        assert 60 <= delay <= 61
        assert not repo.operations()
        assert not any(alert.code == 'API_ERROR' for _, alert in repo.pending_alerts())
        assert any(alert.code == 'ACCOUNT_INCONSISTENT' for _, alert in repo.pending_alerts())
    finally:
        repo.close()
