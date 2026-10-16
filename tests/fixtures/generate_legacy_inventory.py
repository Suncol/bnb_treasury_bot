"""Run with PYTHONPATH pointing at dc8f3f; export real legacy SQLite dumps."""
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
import json, tempfile
from core.models import AssetTransferPlan, CancelPlan, CrashGuardState, OperationKind, OrderPlan, ReplenishmentState, SliceState
from services.exchange_adapter import RequestNotSent
from tests.execution_helpers import setup
from tests.fake_exchange import FakeExchange
from tests.helpers import D, make_account
from storage.codec import dumps

from dataclasses import fields
from core.models import Operation
assert "settlement_epoch" not in {field.name for field in fields(Operation)}, "Use the archived baseline on PYTHONPATH"
cases = {}
for case in ('idle', 'urgent', 'open', 'partial', 'cancel_unknown', 'order_unknown', 'bnb', 'funding', 'sweep', 'funding_unknown', 'retired', 'not_sent'):
    folder = Path(tempfile.mkdtemp(prefix='bnb-legacy-'))
    ex, repo, runner = setup(folder, FakeExchange(make_account(
        contract_bnb=D('27'), spot_bnb=D('3.5'), spot_usd=D('5000'),
        contract_max_withdraw_amount=D('100000'))), ReplenishmentState.URGENT if case == 'urgent' else ReplenishmentState.ACCUMULATE)
    guard = CrashGuardState(active=False, episode_id='legacy-protection',
        started_at=ex.now-timedelta(hours=1), ended_at=ex.now, filled_bnb=D('0.5'))
    repo.save('episode:'+guard.episode_id, guard)
    repo.save('runtime', replace(repo.runtime(), guard=guard, slice_state=SliceState(True, ex.now+timedelta(minutes=30)),
        resume_replanning=True))
    if case in ('open','partial','cancel_unknown','order_unknown','retired','not_sent'):
        if case == 'order_unknown': ex.lose_response = OperationKind.ORDER
        if case == 'not_sent':
            def not_sent(op): raise RequestNotSent('fixture preflight', retry_after=60)
            ex.submit = not_sent
        op = runner.executor.execute(OrderPlan('BNBUSDT','BUY','LIMIT_MAKER',D('1'),D('598'),'GTC',True), ex.now, repo.runtime().state)
        if case in ('partial','retired'):
            ex.fill(op.exchange_id, D('0.2') if case == 'partial' else D('1'))
            runner.reconciler.refresh(ex.now)
        if case == 'cancel_unknown':
            ex.lose_response = OperationKind.CANCEL
            runner.executor.execute(CancelPlan('BNBUSDT', op.exchange_id, 'legacy cancel'), ex.now, repo.runtime().state)
    elif case not in {'idle', 'urgent'}:
        ex.hold_transfers = True
        if case == 'funding_unknown': ex.lose_response = OperationKind.TRANSFER
        asset = 'BNB' if case == 'bnb' else 'USDT'
        source, target = ('USDⓈ-M Futures','SPOT') if case.startswith('funding') else ('SPOT','USDⓈ-M Futures')
        runner.executor.execute(AssetTransferPlan(asset,D('1') if asset=='BNB' else D('500'),source,target,'legacy'), ex.now, repo.runtime().state)
        ex.settle(next(iter(ex.transfers)))
    else:
        ex.account = replace(ex.account, spot_usd=D('5123'))
        runner.reconciler.refresh(ex.now)
        repo.save('futures_settlement_issue', {'margin':{'reason':'legacy historical source gap'}})
    cases[case] = {'sql':'\n'.join(repo.db.iterdump())+'\nPRAGMA user_version=2;\n',
        'exchange':dumps({'account':ex.account, 'now':ex.now, 'orders':ex.orders,
                           'fills':ex.fills,'transfers':ex.transfers,'income':ex.income,
                           'writes':ex.writes})}
    repo.close()
output=Path(__file__).with_name('legacy_inventory.json')
output.parent.mkdir(parents=True,exist_ok=True)
output.write_text(json.dumps({'source_commit':'dc8f3f3438d47f0ea75748e799ea8e35ca7faaf6','cases':cases},ensure_ascii=False,indent=2)+'\n')
print(output,output.stat().st_size,'bytes',len(cases),'cases')
