"""Real SDK observations interleaved with fills, risk changes and local warmup."""

from dataclasses import replace
from datetime import timedelta
import json
from urllib.parse import urlsplit

import pytest

from core.models import AssetTransferPlan, CancelPlan, Fill, OperationKind, OperationStatus, ReplenishmentState, RunMode
from core.validation import valid_account_snapshot
from services.market_stream import MarketStream
from services.exchange_adapter import ExchangeError
from services.executor import ExecutionBlocked, Executor
from services.runner import Runner
from storage.repository import Repository
from tests.execution_helpers import run, setup
from tests.fake_exchange import FakeExchange
from tests.helpers import D, make_account, make_market, make_strategy_config
from tests.integration.test_binance_sdk import filter_responses, millis, sdk
from tests.integration.test_order_quota_refresh import counts
from tests.integration.test_request_budgets import observe, seed_owner


def account_reader(sdk, ex, *, during_balance=lambda: None, spot_balance=None,
                   clock_skew_ms=0, time_read_ms=2):
    """Serialize the virtual wallets through the production SDK account reader."""
    adapter, _, responses = sdk
    adapter.clock = lambda: ex.now

    def server_time(_):
        # Sample at the midpoint of a short, nonzero round trip. The returned
        # server millisecond is not the response's local receive timestamp.
        sampled = ex.now + timedelta(milliseconds=time_read_ms / 2 + clock_skew_ms)
        ex.now += timedelta(milliseconds=time_read_ms)
        return 200, {'serverTime': millis(sampled)}

    for path in ('/api/v3/time', '/fapi/v1/time'):
        responses['GET', path] = server_time
    responses['GET', '/fapi/v1/multiAssetsMargin'] = (200, {'multiAssetsMargin': False})

    def balance(_):
        during_balance()
        a = ex.account
        return 200, [
            {'asset': 'BNB', 'balance': str(a.contract_bnb),
             'updateTime': millis(a.contract_bnb_updated_at) if a.contract_bnb_updated_at else 0},
            {'asset': 'USDT', 'balance': str(a.contract_quote_balance),
             'availableBalance': str(a.contract_max_withdraw_amount),
             'maxWithdrawAmount': str(a.contract_max_withdraw_amount),
             'updateTime': millis(a.contract_quote_updated_at) if a.contract_quote_updated_at else 0},
        ]

    def margin(_):
        a = ex.account
        return 200, {
            'totalWalletBalance': str(a.contract_quote_balance),
            'totalMarginBalance': str(a.contract_quote_balance),
            'assets': [{'asset': 'USDT', 'walletBalance': str(a.contract_quote_balance),
                        'marginBalance': str(a.contract_quote_balance),
                        'maxWithdrawAmount': str(a.contract_max_withdraw_amount),
                        'updateTime': millis(a.contract_quote_updated_at) if a.contract_quote_updated_at else 0}],
        }

    def spot(_):
        a = spot_balance() if spot_balance else ex.account
        return 200, {'balances': [
            {'asset': 'BNB', 'free': str(a.spot_bnb), 'locked': '0'},
            {'asset': 'USDT', 'free': str(a.spot_usd), 'locked': '0'},
        ]}

    responses['GET', '/fapi/v3/balance'] = balance
    responses['GET', '/fapi/v3/account'] = margin
    responses['GET', '/api/v3/account'] = spot
    ex.fetch_account_snapshot = adapter.fetch_account_snapshot
    ex.transfer_clock_ready = adapter.transfer_clock_ready
    return adapter


def legacy_observations(repo, *, keep_completion=False):
    """Restore the old schema-2 JSON shape without changing journal evidence."""
    def old_shape(value):
        if value.get('type') == 'AccountSnapshot':
            if not keep_completion:
                value['fields'].pop('futures_observed_until', None)
                value['fields'].pop('spot_observed_until', None)
            value['fields'].pop('futures_event_upper_bound', None)
            value['fields'].pop('spot_event_upper_bound', None)
        return value

    with repo.db:
        for table in ('kv', 'snapshots', 'operations'):
            rows = repo.db.execute(f'SELECT rowid,data FROM {table}').fetchall()
            repo.db.executemany(f'UPDATE {table} SET data=? WHERE rowid=?', [
                (json.dumps(json.loads(data, object_hook=old_shape)), rowid) for rowid, data in rows])


def test_acquisition_bounds_preserve_original_age_and_validate_versions(sdk):
    ex = FakeExchange()
    start = ex.now

    def balance():
        ex.now += timedelta(seconds=2)

    def spot():
        ex.now += timedelta(seconds=7)
        return ex.account

    adapter = account_reader(sdk, ex, during_balance=balance, spot_balance=spot)
    account = ex.fetch_account_snapshot()
    assert account.ts == start
    assert account.futures_observed_until == start + timedelta(seconds=2, milliseconds=2)
    assert account.spot_observed_until == start + timedelta(seconds=9, milliseconds=4)
    assert account.futures_event_upper_bound == start + timedelta(seconds=2, milliseconds=1)
    assert account.spot_event_upper_bound == start + timedelta(seconds=9, milliseconds=3)
    assert valid_account_snapshot(account, ex.now, adapter.cfg)
    assert not valid_account_snapshot(account, start + timedelta(seconds=10, microseconds=1), adapter.cfg)
    assert not valid_account_snapshot(account, start + timedelta(seconds=8), adapter.cfg)
    assert not valid_account_snapshot(replace(account, spot_observed_until=start), ex.now, adapter.cfg)
    assert not valid_account_snapshot(replace(
        account, contract_quote_updated_at=account.futures_event_upper_bound + timedelta(microseconds=1)), ex.now, adapter.cfg)
    assert valid_account_snapshot(replace(
        account, contract_quote_updated_at=account.futures_event_upper_bound), ex.now, adapter.cfg)


@pytest.mark.parametrize('restart', [False, True])
@pytest.mark.parametrize('offset_ms', [250, 500])
@pytest.mark.parametrize('fee', [D('0'), D('.00075')])
@pytest.mark.parametrize('legacy', [False, True])
@pytest.mark.parametrize('clock_skew_ms', [0, 500, 1000])
def test_sell_during_account_read_cannot_settle_a_new_buy(
    sdk, tmp_path, restart, offset_ms, fee, legacy, clock_skew_ms
):
    buy_qty, sell_qty = (D('8.006'), D('7.994')) if fee else (D('8'), D('8'))
    buy_price, sell_price = (D('599.55'), D('600.45')) if fee else (D('601.71'), D('601.71'))
    net = buy_qty * (1 - fee)
    assert net == sell_qty * (1 + fee)
    assert buy_qty * buy_price == sell_qty * sell_price
    ex, repo, runner = setup(tmp_path, FakeExchange(replace(make_account(
        contract_bnb=D('32') - buy_qty, spot_bnb=D('.5') + net,
        spot_usd=D('20000') - buy_qty * buy_price),
        contract_total_margin_balance=D('10000'), contract_margin_asset='USDT')))
    start = ex.now
    frozen = []

    def sell_during_read():
        if frozen:
            return
        ex.fills.append(Fill('BNBUSDT', 'external-sell', 'external',
                             start + timedelta(milliseconds=offset_ms + clock_skew_ms), sell_qty,
                             commission=sell_qty * fee, commission_asset='BNB',
                             quote_qty=sell_qty * sell_price, side='SELL'))
        ex.account = replace(ex.account, spot_bnb=D('.5'), spot_usd=D('20000'))
        frozen.append(ex.account)
        ex.now = start + timedelta(milliseconds=500)

    account_reader(sdk, ex, during_balance=sell_during_read, spot_balance=lambda: frozen[0],
                   clock_skew_ms=clock_skew_ms)
    fill = ex.fill

    def filled(order_id, qty):
        fill(order_id, qty)
        receipt = ex.fills[-1]
        ex.account = replace(ex.account, spot_bnb=ex.account.spot_bnb - qty * fee,
                             spot_usd=ex.account.spot_usd + receipt.quote_qty - qty * buy_price)
        ex.fills[-1] = replace(receipt, ts=receipt.ts + timedelta(milliseconds=clock_skew_ms),
                              commission=qty * fee, commission_asset='BNB',
                              quote_qty=qty * buy_price)

    ex.fill = filled
    filters = ex.fetch_symbol_filters('BNBUSDT')
    ex.fetch_symbol_filters = lambda _: replace(filters, qty_step=D('.001'), min_qty=D('.001'))
    run(runner, ex)
    buys = [op for op in ex.writes if op.kind == OperationKind.ORDER]
    assert len(buys) == 1 and buys[0].payload.qty == buy_qty
    if legacy:
        legacy_observations(repo)
    if restart:
        path, cfg = repo.path, runner.cfg
        repo.close()
        repo = Repository(path)
        runner = Runner(ex, repo, cfg, clock=lambda: ex.now)
    ex.now += timedelta(seconds=11)
    runner.tick()
    assert len([op for op in ex.writes if op.kind == OperationKind.ORDER]) == 1
    assert repo.operation(buys[0].client_id).balance_pending
    assert repo.load('spot_settlement_issue')['ambiguous_receipts'] == ('trade:BNBUSDT:external-sell',)
    repo.close()


@pytest.mark.parametrize('clock_skew_ms', [0, -500, -1000])
@pytest.mark.parametrize('restart', [False, True])
@pytest.mark.parametrize('missing_version', [False, True])
def test_owned_transfer_settles_with_negative_clock_skew(sdk, tmp_path, clock_skew_ms, restart, missing_version):
    exchange = FakeExchange(replace(make_account(
        contract_bnb=D('24'), spot_bnb=D('8.5')),
        contract_total_margin_balance=D('6000'), contract_margin_asset='USDT'))
    if missing_version:
        exchange.account = replace(exchange.account, contract_bnb_updated_at=None, contract_quote_updated_at=None)
    ex, repo, runner = setup(tmp_path, exchange)
    start = ex.now
    skew = timedelta(milliseconds=clock_skew_ms)

    def slow_balance():
        if ex.now == start:
            ex.now += timedelta(seconds=2)

    account_reader(sdk, ex, during_balance=slow_balance, clock_skew_ms=clock_skew_ms)
    original_settle = ex.settle

    def settle(transfer_id):
        original_settle(transfer_id)
        receipt = ex.transfers[transfer_id]
        ex.transfers[transfer_id] = replace(receipt, ts=receipt.ts + skew)
        ex.income[-1] = replace(ex.income[-1], ts=ex.income[-1].ts + skew)
        ex.account = replace(ex.account, contract_bnb_updated_at=(
            None if missing_version else ex.account.contract_bnb_updated_at + skew))

    ex.settle = settle
    run(runner, ex)
    transfers = [op for op in ex.writes if op.kind == OperationKind.TRANSFER]
    assert len(transfers) == 1 and ex.account.contract_bnb == 32
    before = repo.operation(transfers[0].client_id).balance_before
    if restart:
        path, cfg = repo.path, runner.cfg
        repo.close()
        repo = Repository(path)
        runner = Runner(ex, repo, cfg, clock=lambda: ex.now)
    ex.now += timedelta(seconds=11)
    run(runner, ex)
    assert not repo.operations(unresolved_only=True)
    assert repo.operation(transfers[0].client_id).balance_before == before
    assert not repo.load('futures_settlement_issue')
    assert len(ex.writes) == 1
    repo.close()


@pytest.mark.parametrize('legacy', [False, True])
@pytest.mark.parametrize('restart', [False, True])
def test_first_server_observation_cannot_retreat_past_old_sell(sdk, tmp_path, legacy, restart):
    ex, repo, runner = setup(tmp_path, FakeExchange(replace(make_account(
        contract_bnb=D('24'), spot_bnb=D('8.5'), spot_usd=D('20000') - D('8') * D('601.71')),
        contract_total_margin_balance=D('6000'), contract_margin_asset='USDT')))
    runner.execute_enabled = False
    start = ex.now
    frozen = []

    def sell():
        if frozen:
            return
        ex.fills.append(Fill('BNBUSDT', 'external', 'external', start + timedelta(milliseconds=750),
                             D('8'), quote_qty=D('8') * D('601.71'), side='SELL'))
        ex.account = replace(ex.account, spot_bnb=D('.5'), spot_usd=D('20000'))
        frozen.append(ex.account)
        ex.now = start + timedelta(milliseconds=500)

    account_reader(sdk, ex, during_balance=sell, spot_balance=lambda: frozen[0], clock_skew_ms=500)
    run(runner, ex)
    assert not ex.writes and repo.load('spot_acceptance') is not None
    if legacy:
        legacy_observations(repo, keep_completion=True)
    accepted = repo.load('spot_acceptance')
    if restart:
        path = repo.path
        repo.close()
        repo = Repository(path)
    # Both offsets are permitted, but the exchange event clock has regressed.
    ex.now += timedelta(milliseconds=100)
    account_reader(sdk, ex, spot_balance=lambda: frozen[0], clock_skew_ms=-500)
    original_fill = ex.fill

    def fill(order_id, qty):
        original_fill(order_id, qty)
        ex.fills[-1] = replace(ex.fills[-1], ts=ex.fills[-1].ts - timedelta(milliseconds=500))

    ex.fill = fill
    runner = Runner(ex, repo, runner.cfg, clock=lambda: ex.now)
    run(runner, ex)
    assert repo.load('spot_acceptance') == accepted
    assert not ex.writes and not repo.operations()
    assert repo.runtime().api_errors == 0
    ex.now += timedelta(seconds=11)
    runner.tick()
    buys = [op for op in ex.writes if op.kind == OperationKind.ORDER]
    assert len(buys) == 1
    current = repo.load('spot_acceptance')
    assert repo.account_event_boundary(
        current['account'], 'spot', settlement_epoch=current['epoch']) >= repo.account_event_boundary(
        accepted['account'], 'spot', settlement_epoch=accepted['epoch'])
    ex.now += timedelta(seconds=11)
    runner.tick()
    assert len([op for op in ex.writes if op.kind == OperationKind.ORDER]) == 1
    repo.close()


@pytest.mark.parametrize('direction', ['bnb', 'fund', 'sweep'])
@pytest.mark.parametrize('clock_skew_ms', [-1000, 1000])
def test_transfer_clock_must_cross_server_millisecond_before_intent(sdk, tmp_path, direction, clock_skew_ms):
    ex, repo, runner = setup(tmp_path, FakeExchange(replace(make_account(
        contract_bnb=D('24'), contract_max_withdraw_amount=D('50000'),
        spot_bnb=D('8.5'), spot_usd=D('5000')),
        contract_total_margin_balance=D('50000'), contract_margin_asset='USDT')))
    ex.now += timedelta(seconds=2)
    adapter = account_reader(sdk, ex, clock_skew_ms=clock_skew_ms, time_read_ms=0)
    plan = {
        'bnb': AssetTransferPlan('BNB', D('8'), 'SPOT', 'USDⓈ-M Futures', 'return'),
        'fund': AssetTransferPlan('USDT', D('500'), 'USDⓈ-M Futures', 'SPOT', 'fund'),
        'sweep': AssetTransferPlan('USDT', D('500'), 'SPOT', 'USDⓈ-M Futures', 'sweep'),
    }[direction]
    with pytest.raises(ExecutionBlocked, match='server time boundary'):
        runner.executor.execute(plan, ex.now, repo.runtime().state)
    assert not repo.operations() and not ex.writes
    assert repo.runtime().api_errors == 0
    check = adapter.transfer_clock_ready

    def crossed(account):
        ex.now += timedelta(milliseconds=1)
        return check(account)

    ex.transfer_clock_ready = crossed
    result = runner.executor.execute(plan, ex.now, repo.runtime().state)
    assert result.kind == OperationKind.TRANSFER and len(ex.writes) == 1
    repo.close()


@pytest.mark.parametrize('change', ['guard', 'qualification', 'account_age', 'filter_age', 'rollback', '503'])
def test_transfer_time_read_cannot_bypass_final_gates(sdk, tmp_path, change):
    ex, repo, runner = setup(tmp_path, FakeExchange(replace(make_account(
        contract_bnb=D('24'), spot_bnb=D('8.5')),
        contract_total_margin_balance=D('6000'), contract_margin_asset='USDT')))
    adapter = account_reader(sdk, ex)
    start = ex.now
    filters = ex.fetch_symbol_filters('BNBUSDT')
    ex.fetch_symbol_filters = lambda _: replace(filters, observed_at=start)
    read_time = sdk[2]['GET', '/fapi/v1/time']

    def check(account):
        def changed_read(request):
            if change == 'guard':
                runtime = repo.runtime()
                repo.save('runtime', replace(runtime, guard=replace(
                    runtime.guard, active=True, episode_id='during-clock-read', keep_price_ceiling=D('570'))))
            elif change == 'qualification':
                runner.executor.invalidate_income_audit()
            elif change in {'account_age', 'filter_age'}:
                delay = timedelta(seconds=11 if change == 'account_age' else 6)
                sampled = ex.now + delay / 2
                ex.now += delay
                return 200, {'serverTime': millis(sampled)}
            elif change == 'rollback':
                return 200, {'serverTime': millis(account.futures_event_upper_bound) - 1}
            elif change == '503':
                return 503, {'code': -1000, 'msg': 'time read failed'}
            return read_time(request)

        sdk[2]['GET', '/fapi/v1/time'] = changed_read
        try:
            return adapter.transfer_clock_ready(account)
        finally:
            sdk[2]['GET', '/fapi/v1/time'] = read_time

    ex.transfer_clock_ready = check
    run(runner, ex)
    assert not ex.writes and not repo.operations()
    assert repo.runtime().api_errors == (1 if change == '503' else 0)
    repo.close()


@pytest.mark.parametrize('funding', [False, True])
def test_transfer_time_checkpoint_invalidates_candidate_when_guard_starts(sdk, tmp_path, funding):
    ex, repo, runner = setup(tmp_path, FakeExchange(replace(make_account(
        contract_bnb=D('24'), spot_bnb=D('8.5'), spot_usd=D('10000')),
        contract_total_margin_balance=D('50000'), contract_margin_asset='USDT')))
    adapter = account_reader(sdk, ex)

    def checkpoint():
        runner._priority_market = replace(make_market(), ts=ex.now,
            best_bid=D('569.5'), best_ask=D('570.5'), mid_price=D('570'),
            vwap_5m=D('570'), smooth_price=D('570'), drawdown_1m=D('.05'))
        runner._priority_checkpoint()

    def check(account):
        adapter.read_checkpoint = checkpoint
        try:
            return adapter.transfer_clock_ready(account)
        finally:
            adapter.read_checkpoint = None

    ex.transfer_clock_ready = check
    plan = (AssetTransferPlan('USDT', D('500'), 'USDⓈ-M Futures', 'SPOT', 'fund') if funding else
            AssetTransferPlan('BNB', D('8'), 'SPOT', 'USDⓈ-M Futures', 'return'))
    with pytest.raises(ExecutionBlocked, match='Risk protection changed'):
        runner.executor.execute(plan, ex.now, repo.runtime().state)
    assert repo.runtime().guard.active
    assert not repo.operations() and not ex.writes
    repo.close()


@pytest.mark.parametrize('futures_ms,spot_ms', [(-1000, 1000), (1000, -1000)])
def test_server_boundaries_use_independent_wallet_clocks(sdk, tmp_path, futures_ms, spot_ms):
    ex = FakeExchange()
    adapter = account_reader(sdk, ex, time_read_ms=0)
    for path, skew in (('/fapi/v1/time', futures_ms), ('/api/v3/time', spot_ms)):
        sdk[2]['GET', path] = (200, {'serverTime': millis(ex.now) + skew})
    account = ex.fetch_account_snapshot()
    assert valid_account_snapshot(account, ex.now, adapter.cfg)
    ex, repo, runner = setup(tmp_path, ex)
    runner.execute_enabled = False
    run(runner, ex)
    accepted = repo.load('spot_acceptance')
    assert accepted is not None
    for field in ('futures_event_upper_bound', 'spot_event_upper_bound'):
        for value in (None, getattr(account, field) - timedelta(milliseconds=1)):
            ex.fetch_account_snapshot = lambda: replace(account, **{field: value})
            run(runner, ex)
            assert repo.load('spot_acceptance') == accepted
            assert not runner.executor.execution_qualified(ex.now)
    repo.close()


def test_server_time_checkpoint_delay_is_not_a_clock_offset(sdk):
    adapter, _, responses = sdk
    ex = FakeExchange()
    adapter.clock = lambda: ex.now
    adapter.read_checkpoint = lambda: setattr(ex, 'now', ex.now + timedelta(seconds=3))
    responses['GET', '/api/v3/time'] = lambda _: (200, {'serverTime': millis(ex.now)})
    assert adapter._server_time(adapter.spot.time) == ex.now


@pytest.mark.parametrize('failure', ['503', 'timeout', '429'])
@pytest.mark.parametrize('restart', [False, True])
def test_pending_quote_handles_protective_query_failure(sdk, tmp_path, failure, restart):
    import requests

    adapter, calls, responses = sdk
    now = [adapter.clock()]
    adapter.clock = lambda: now[0]
    cfg = make_strategy_config()
    repo = Repository(tmp_path / 'pending.sqlite3')
    seed_owner(repo)
    responses['DELETE', '/api/v3/order'] = (503, {'code': -1000, 'msg': 'unknown result'})
    op = Executor(adapter, repo, cfg, clock=adapter.clock).execute(
        CancelPlan('BNBUSDT', '123', 'protect'), now[0], ReplenishmentState.ACCUMULATE)
    assert op.status == OperationStatus.UNKNOWN
    if restart:
        path = repo.path
        repo.close()
        repo = Repository(path)
    now[0] += timedelta(seconds=3)
    responses['GET', '/api/v3/order'] = (
        requests.Timeout('temporary read failure') if failure == 'timeout' else
        (int(failure), {'code': -1003 if failure == '429' else -1000, 'msg': 'temporary read failure'}))
    adapter.feed = MarketStream('BNBUSDT', cfg.crash_guard)
    runner = Runner(adapter, repo, cfg, clock=adapter.clock)
    errors = repo.runtime().api_errors
    assert runner.tick() is None
    assert repo.runtime().api_errors == errors + 1
    assert repo.runtime().read_failures == 1
    assert repo.runtime().read_retry_at > now[0]
    if failure == '429':
        assert repo.runtime().read_retry_at >= now[0] + timedelta(seconds=60)
    assert repo.operation(op.client_id).status == OperationStatus.UNKNOWN
    assert [(c['method'], urlsplit(c['url']).path) for c in calls] == [
        ('DELETE', '/api/v3/order'), ('GET', '/api/v3/order')]
    assert runner.tick() is None
    assert len(calls) == 2
    repo.close()


@pytest.mark.parametrize('restart', [False, True])
@pytest.mark.parametrize('legacy', ['current', 'timed', 'untimed'])
def test_version_observed_during_slow_read_can_settle_and_return_bnb(sdk, tmp_path, restart, legacy):
    ex, repo, runner = setup(tmp_path, FakeExchange(replace(make_account(
        contract_bnb=D('24'), spot_bnb=D('.5'), spot_usd=D('20000')),
        contract_total_margin_balance=D('10000'), contract_margin_asset='USDT')))
    start = ex.now

    def income_during_read():
        if ex.now != start:
            return
        ex.now += timedelta(milliseconds=1499)
        ex.futures_change('USDT', D('1'), 'REALIZED_PNL')
        ex.now = start + timedelta(seconds=2)

    account_reader(sdk, ex, during_balance=income_during_read)
    ex.hide_fills = True
    run(runner, ex)
    buys = [op for op in ex.writes if op.kind == OperationKind.ORDER]
    assert len(buys) == 1
    before = repo.operation(buys[0].client_id).balance_before
    assert before.ts == start
    assert before.contract_quote_updated_at == start + timedelta(seconds=1.5)
    if legacy != 'current':
        legacy_observations(repo)
        before = repo.operation(buys[0].client_id).balance_before
        assert before.futures_observed_until is None
        if legacy == 'untimed':
            repo.update_operation(replace(repo.operation(buys[0].client_id), created_at=start))
    if restart:
        path, cfg = repo.path, runner.cfg
        repo.close()
        repo = Repository(path)
        runner = Runner(ex, repo, cfg, clock=lambda: ex.now)
    ex.hide_fills = False
    ex.now += timedelta(seconds=5)
    run(runner, ex)
    if legacy == 'untimed':
        assert ex.account.contract_bnb == 24
        assert repo.operation(buys[0].client_id).balance_pending
        ex.now += timedelta(hours=1)
        run(runner, ex)
        assert repo.operation(buys[0].client_id).balance_pending
    else:
        assert ex.account.contract_bnb == 32
        assert not repo.operations(unresolved_only=True)
    assert len([op for op in ex.writes if op.kind == OperationKind.ORDER]) == 1
    assert repo.operation(buys[0].client_id).balance_before == before
    repo.close()


def test_quota_checkpoint_discards_unguarded_ioc_before_intent(sdk, tmp_path):
    ex, repo, runner = setup(tmp_path, FakeExchange(make_account(
        contract_bnb=D('24'), spot_bnb=D('.5'), spot_usd=D('10000'))))
    adapter, calls, responses = sdk
    adapter.clock = lambda: ex.now
    observe(adapter, 'ORDERS', 90)
    responses['GET', '/api/v3/rateLimit/order'] = (200, counts(1))
    crash = replace(make_market(), ts=ex.now, best_bid=D('569.5'), best_ask=D('570.5'),
                    mid_price=D('570'), vwap_5m=D('570'), smooth_price=D('570'), drawdown_1m=D('.05'))

    def checkpoint():
        runner._priority_market = crash
        runner._priority_checkpoint()

    adapter.read_checkpoint = checkpoint
    ex.refresh_order_budget = adapter.refresh_order_budget
    run(runner, ex)
    assert repo.runtime().guard.active
    assert [(c['method'], urlsplit(c['url']).path) for c in calls] == [('GET', '/api/v3/rateLimit/order')]
    assert not ex.writes and not repo.operations()
    assert repo.runtime().api_errors == 0
    # A fresh plan under the guard remains executable and uses the 26 BNB target.
    ex.fetch_market_snapshot = lambda _: replace(crash, ts=ex.now)
    ex.now += timedelta(seconds=1)
    run(runner, ex)
    buys = [op for op in ex.writes if op.kind == OperationKind.ORDER]
    assert len(buys) == 1 and buys[0].payload.qty == 2
    assert repo.runtime().guard.filled_bnb == 2
    repo.close()


@pytest.mark.parametrize('mode', [RunMode.AUTO, RunMode.PAUSED])
@pytest.mark.parametrize('prior_errors', [0, 2])
def test_missing_local_quote_does_not_pause_and_recovers_bnb_return(sdk, tmp_path, mode, prior_errors):
    ex, repo, runner = setup(tmp_path, FakeExchange(make_account(
        contract_bnb=D('24'), spot_bnb=D('8.5'))))
    adapter, calls, responses = sdk
    adapter.clock = lambda: ex.now
    feed = MarketStream('BNBUSDT', runner.cfg.crash_guard)
    ex.feed = adapter.feed = feed
    ex.fetch_market_snapshot = adapter.fetch_market_snapshot
    repo.save('runtime', replace(repo.runtime(), run_mode=mode,
                                api_errors=prior_errors, read_failures=prior_errors))
    for _ in range(3):
        runner.tick()
        ex.now += timedelta(seconds=61)
    assert not calls
    assert repo.runtime().run_mode == mode
    assert repo.runtime().api_errors == repo.runtime().read_failures == prior_errors
    assert not repo.operations()
    feed._latest = replace(make_market(), ts=ex.now, smooth_price=None)
    responses['GET', '/api/v3/avgPrice'] = lambda _: (200, {
        'mins': 5, 'price': '600', 'closeTime': millis(ex.now)})
    responses['GET', '/api/v3/ticker'] = (200, {'priceChangePercent': '0'})
    responses['GET', '/api/v3/ticker/24hr'] = (200, {'priceChangePercent': '0'})
    runner.tick()
    if mode == RunMode.AUTO:
        assert ex.account.contract_bnb == 32
        assert len(ex.writes) == 1 and ex.writes[0].payload.asset == 'BNB'
    else:
        assert ex.account.contract_bnb == 24 and not ex.writes
        assert repo.runtime().run_mode == RunMode.PAUSED
    repo.close()


@pytest.mark.parametrize('minutes', [0, 5])
@pytest.mark.parametrize('missing', ['null', 'error'])
def test_missing_reference_price_uses_existing_fallback(sdk, minutes, missing):
    adapter, calls, responses = sdk
    filter_responses(responses, ({'filterType': 'PERCENT_PRICE', 'avgPriceMins': minutes,
                                  'multiplierDown': '.9', 'multiplierUp': '1.1'},))
    responses['GET', '/api/v3/referencePrice'] = (
        (200, {'referencePrice': None, 'timestamp': millis(adapter.clock())})
        if missing == 'null' else (400, {'code': -2043, 'msg': "This symbol doesn't have a reference price."}))
    responses['GET', '/api/v3/avgPrice'] = (200, {
        'mins': 5, 'price': '600', 'closeTime': millis(adapter.clock())})
    responses['GET', '/api/v3/ticker/price'] = (200, {'symbol': 'BNBUSDT', 'price': '600'})
    filters = adapter.fetch_symbol_filters('BNBUSDT')
    assert (filters.min_price, filters.max_price) == (D('540'), D('660'))
    assert any(urlsplit(c['url']).path == (
        '/api/v3/avgPrice' if minutes else '/api/v3/ticker/price') for c in calls)


@pytest.mark.parametrize('status,code', [(400, -1121), (401, -2043), (429, -2043),
                                       (418, -2043), (503, -2043)])
def test_reference_price_fallback_does_not_swallow_other_failures(sdk, status, code):
    adapter, calls, responses = sdk
    responses['GET', '/api/v3/referencePrice'] = (status, {'code': code, 'msg': 'unavailable'})
    with pytest.raises(ExchangeError):
        adapter._percentage_bounds('BNBUSDT', ({'avgPriceMins': 5},), D('0'), None,
                                   adapter.spot, checkpoint=True)
    assert len(calls) == 1


@pytest.mark.parametrize('stage', ['quota', 'preparation'])
def test_direct_executor_captures_guard_before_read_callbacks(sdk, tmp_path, stage):
    from tests.integration.test_order_quota_refresh import PAYLOAD, ready_executor

    adapter, calls, responses = sdk
    repo = Repository(tmp_path / 'direct.db')
    executor, account = ready_executor(adapter, repo)

    def change_guard():
        runtime = repo.runtime()
        repo.save('runtime', replace(runtime, guard=replace(
            runtime.guard, active=True, episode_id='new', started_at=adapter.clock(),
            keep_price_ceiling=D('570'))))

    if stage == 'quota':
        observe(adapter, 'ORDERS', 90)
        responses['GET', '/api/v3/rateLimit/order'] = (200, counts(1))
        adapter.read_checkpoint = change_guard
    else:
        executor.reconciler.read_checkpoint = change_guard
    with pytest.raises(ExecutionBlocked, match='Risk protection changed'):
        executor.execute(PAYLOAD, adapter.clock(), repo.runtime().state, account=account)
    assert not repo.operations()
    assert all(c['method'] == 'GET' for c in calls)
    repo.close()


def test_guard_started_by_plan_uses_its_own_risk_basis(tmp_path):
    ex, repo, runner = setup(tmp_path, FakeExchange(make_account(
        contract_bnb=D('24'), spot_bnb=D('.5'), spot_usd=D('10000'))))
    # No read checkpoint sees the drop; build_cycle_plan is its first consumer.
    runner._priority_checkpoint = runner.reconciler.read_checkpoint = lambda: None
    ex.fetch_market_snapshot = lambda _: replace(make_market(), ts=ex.now, drawdown_1m=D('.05'))
    run(runner, ex)
    buys = [op for op in ex.writes if op.kind == OperationKind.ORDER]
    assert len(buys) == 1 and buys[0].payload.qty == 2
    assert repo.runtime().guard.active and repo.runtime().guard.filled_bnb == 2
    repo.close()


@pytest.mark.parametrize('background', [False, True])
def test_reference_absence_keeps_runner_operating_in_both_refresh_paths(sdk, tmp_path, background):
    ex, repo, runner = setup(tmp_path, FakeExchange(make_account(contract_bnb=D('24'), spot_bnb=D('8.5'))))
    adapter, _, responses = sdk
    filter_responses(responses, ({'filterType': 'PERCENT_PRICE', 'avgPriceMins': 5,
                                  'multiplierDown': '.9', 'multiplierUp': '1.1'},))
    responses['GET', '/api/v3/referencePrice'] = (400, {'code': -2043, 'msg': 'No reference price'})
    responses['GET', '/api/v3/avgPrice'] = lambda _: (200, {
        'mins': 5, 'price': '600', 'closeTime': millis(ex.now)})
    adapter.clock = lambda: ex.now
    ex.fetch_symbol_filters = adapter.fetch_symbol_filters
    if background:
        adapter.start_price_refresh(client=adapter.spot)
        start = ex.now
        adapter._price_refresh.monotonic = lambda: (ex.now - start).total_seconds()
    try:
        for _ in range(4):
            run(runner, ex)
            if background:
                adapter._price_refresh._thread.join(1)
                assert not adapter._price_refresh._thread.is_alive()
            assert repo.runtime().api_errors == repo.runtime().read_failures == 0
            assert repo.runtime().run_mode == RunMode.AUTO
            ex.now += timedelta(seconds=1)
        assert ex.account.contract_bnb == 32
        assert len(ex.writes) == 1 and ex.writes[0].payload.asset == 'BNB'
    finally:
        adapter.close_price_refresh()
        repo.close()


@pytest.mark.parametrize('failure', ['worker', 'rest'])
def test_real_market_failure_still_pauses_after_three_attempts(sdk, tmp_path, failure):
    from types import SimpleNamespace

    ex, repo, runner = setup(tmp_path)
    adapter, calls, responses = sdk
    adapter.clock = lambda: ex.now
    feed = MarketStream('BNBUSDT', runner.cfg.crash_guard)
    ex.feed = adapter.feed = feed
    ex.fetch_market_snapshot = adapter.fetch_market_snapshot
    if failure == 'worker':
        feed._thread = SimpleNamespace(is_alive=lambda: False)
    else:
        responses['GET', '/api/v3/avgPrice'] = (503, {'code': -1000, 'msg': 'unavailable'})
    for _ in range(3):
        feed._latest = replace(make_market(), ts=ex.now)
        runner.tick()
        ex.now += timedelta(seconds=61)
    assert repo.runtime().run_mode == RunMode.PAUSED
    assert repo.runtime().api_errors == repo.runtime().read_failures == 3
    assert len(calls) == (3 if failure == 'rest' else 0)
    assert not ex.writes
    repo.close()
