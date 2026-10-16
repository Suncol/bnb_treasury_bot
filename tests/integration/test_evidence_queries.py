from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace

import pytest

from core.models import AssetTransferPlan, OperationStatus, OrderPlan, TransferRecord
from services.binance_adapter import BinanceAdapter
from services.exchange_adapter import ExchangeError
from services.reconciliation import Reconciler
from storage.repository import Repository
from tests.execution_helpers import seed_bid
from tests.helpers import D, make_strategy_config
from tests.integration.test_margin_adjusted_target import margin_setup


def record_reads(exchange):
    calls=[]
    for name in ('fetch_recent_fills','fetch_recent_transfers','fetch_futures_income'):
        method=getattr(exchange,name)
        def capture(*args, _name=name, _method=method):
            calls.append((_name,args))
            return _method(*args)
        setattr(exchange,name,capture)
    return calls


def test_idle_queries_only_current_orders_account_and_account_level_budget(tmp_path):
    ex,repo,runner=margin_setup(tmp_path,spot_usd='5000')
    runner.cfg=replace(runner.cfg,spot_activity_symbols=('ETHUSDT','BTCUSDT'))
    runner.reconciler.cfg=runner.cfg
    old=repo.load('spot_checkpoint')
    repo.save('spot_checkpoint',{**old,'account':replace(old['account'],ts=ex.now-timedelta(days=200))})
    def forbidden(*args,**kwargs): pytest.fail('Unrelated history read in idle observation')
    ex.fetch_recent_fills=ex.fetch_futures_income=forbidden
    repo.snapshots_since=forbidden
    calls=record_reads(ex)
    runner.execute_enabled=False
    assert runner.run_once().inventory_valid
    assert repo.runtime().last_inventory_at==ex.now
    assert calls==[('fetch_recent_transfers',(ex.now-timedelta(hours=24),ex.now))]
    runner.executor.execute(OrderPlan('BNBUSDT','BUY','LIMIT_MAKER',D('1'),D('598'),'GTC',True),ex.now,repo.runtime().state)
    assert len(ex.writes)==1
    assert len(calls)==2 and all(name=='fetch_recent_transfers' for name,_ in calls)
    repo.close()


def test_open_gtc_keeps_original_window_beyond_72_hours_and_repairs_late_income(tmp_path):
    ex,repo,runner=margin_setup(tmp_path,spot_usd='10000')
    cfg=replace(runner.cfg,spot_activity_symbols=('BTCUSDT','BTCUSDT'))
    runner.reconciler.cfg=cfg
    op=seed_bid(ex,repo)
    runner.reconciler.refresh(ex.now)
    assert not repo.operations(unresolved_only=True)
    repo.save_snapshot(op.balance_before)
    ex.now+=timedelta(hours=73)
    repo.save_snapshot(ex.fetch_account_snapshot())
    assert any(s.ts==op.balance_before.ts for s in repo.settlement_snapshots())
    ex.futures_change('USDT',D('-1'),'COMMISSION')
    receipts,ex.income=ex.income,[]
    calls=record_reads(ex)
    assert runner.reconciler.refresh(ex.now)[2].unresolved
    fill_calls=[args for name,args in calls if name=='fetch_recent_fills']
    assert {args[0] for args in fill_calls}=={'BNBUSDT','BTCUSDT'} and len(fill_calls)==2
    assert all(args[1]==op.balance_before.ts-timedelta(seconds=1) for args in fill_calls)
    income=[args for name,args in calls if name=='fetch_futures_income']
    assert income[0][0]==op.balance_before.ts-timedelta(seconds=1)
    path=repo.path;repo.close();repo=Repository(path)
    reconciler=Reconciler(ex,repo,cfg,clock=lambda:ex.now)
    ex.income=receipts
    assert not reconciler.refresh(ex.now)[2].unresolved
    assert repo.settlement_window_open() and len(ex.writes)==1
    repo.close()


def test_old_unknown_transfer_reserves_budget_and_queries_its_original_baseline(tmp_path):
    ex,repo,runner=margin_setup(tmp_path,spot_usd='5000')
    ex.hold_transfers=True
    op=runner.executor.execute(AssetTransferPlan('USDT',D('500'),'USDⓈ-M Futures','SPOT','fund'),ex.now,repo.runtime().state)
    ex.now+=timedelta(hours=97)
    calls=record_reads(ex)
    assert runner.reconciler.refresh(ex.now)[2].unresolved
    assert repo.budget_used(ex.now,'USDT')==D('500')
    assert all(args[0] <= op.balance_before.ts for name,args in calls if name=='fetch_recent_transfers')
    ex.settle(op.exchange_id)
    assert not runner.reconciler.refresh(ex.now)[2].unresolved
    assert any(args[0] <= op.balance_before.ts for name,args in calls if name=='fetch_futures_income')
    assert len(ex.writes)==1
    repo.close()


def test_known_external_pending_transfer_is_not_forgotten_when_history_omits_it(tmp_path):
    ex,repo,runner=margin_setup(tmp_path,spot_usd='5000')
    receipt=TransferRecord('old-external','USDT',D('500'),'USDⓈ-M Futures','SPOT',ex.now-timedelta(hours=73),OperationStatus.PENDING)
    repo.save_transfers((receipt,))
    calls=record_reads(ex)
    assert runner.reconciler.refresh(ex.now)[2].unresolved
    assert repo.settlement_epoch()==0
    transfers=[args for name,args in calls if name=='fetch_recent_transfers']
    assert transfers[0][0]==receipt.ts-timedelta(seconds=1)
    repo.close()


def test_idle_retired_order_receipt_watch_is_short_and_only_for_its_symbol(tmp_path):
    ex,repo,runner=margin_setup(tmp_path,spot_usd='5000')
    op=seed_bid(ex,repo)
    ex.fill(op.exchange_id,D('1'))
    runner.reconciler.refresh(ex.now)
    runner.reconciler.cfg=replace(runner.cfg,spot_activity_symbols=('BTCUSDT',))
    ex.now+=timedelta(hours=24)
    calls=record_reads(ex)
    assert not runner.reconciler.refresh(ex.now)[2].unresolved
    assert [args for name,args in calls if name=='fetch_recent_fills']==[('BNBUSDT',ex.now-timedelta(seconds=60),ex.now)]
    assert not any(name=='fetch_futures_income' for name,_ in calls)
    repo.close()


def test_retention_expiry_never_turns_a_long_open_window_into_idle_success(tmp_path):
    ex,repo,runner=margin_setup(tmp_path,spot_usd='5000')
    op=seed_bid(ex,repo)
    ex.now+=timedelta(days=100)
    ex.futures_change('USDT',D('-1'),'COMMISSION')
    adapter=BinanceAdapter('fixture-key','fixture-secret',make_strategy_config(),
        clients=(SimpleNamespace(),SimpleNamespace(),SimpleNamespace()),clock=lambda:ex.now)
    ex.fetch_futures_income=adapter.fetch_futures_income
    for _ in range(2):
        with pytest.raises(ExchangeError,match='retention.*archival'):
            runner.reconciler.refresh(ex.now)
        assert repo.settlement_window_open() and len(ex.writes)==1
        assert repo.operation(op.client_id).exchange_id==op.exchange_id
        assert repo.settlement_epoch()==op.settlement_epoch
    repo.close()


def test_direct_observation_and_execution_share_account_serialization(tmp_path):
    ex,repo,runner=margin_setup(tmp_path,spot_usd='5000')
    other=Repository(repo.path)
    payload=OrderPlan('BNBUSDT','BUY','LIMIT_MAKER',D('1'),D('598'),'GTC',True)
    with other.exclusive():
        with pytest.raises(BlockingIOError): runner.reconciler.refresh(ex.now)
        with pytest.raises(BlockingIOError): runner.executor.execute(payload,ex.now,repo.runtime().state)
        assert repo.settlement_epoch()==0 and not repo.operations() and not ex.writes
    runner.executor.execute(payload,ex.now,repo.runtime().state)
    assert len(ex.writes)==1
    other.close();repo.close()
