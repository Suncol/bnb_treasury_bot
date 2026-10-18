"""A qualified ordinary group survives a temporary pre-intent quota veto."""

from dataclasses import replace
from datetime import timedelta

import pytest

from core.models import OperationKind, OperationStatus, ReplenishmentState, RunMode, SliceState
from services.exchange_adapter import RequestDeferred
from services.runner import Runner
from storage.repository import Repository
from tests.execution_helpers import run, setup
from tests.fake_exchange import FakeExchange
from tests.helpers import D, make_account


@pytest.mark.parametrize('restart', [False, True])
@pytest.mark.parametrize('orders_only', [False, True])
def test_first_group_replans_after_quota_recovery_without_sweeping(tmp_path, restart, orders_only):
    ex, repo, runner = setup(tmp_path, FakeExchange(make_account(
        contract_bnb=D('27'), spot_usd=D('5000'), contract_max_withdraw_amount=D('6000'))),
        ReplenishmentState.ACCUMULATE)
    started = ex.now
    def quota(**kwargs):
        if kwargs.get('new_order') and ex.now < started + timedelta(seconds=10):
            raise RequestDeferred('temporary capacity', orders_only=orders_only, retry_after=10)
    ex.ensure_request_allowed = quota
    run(runner, ex)
    assert not repo.operations() and not ex.writes
    assert repo.runtime().last_inventory_at == started
    assert repo.runtime().resume_replanning
    if restart:
        path, cfg = repo.path, runner.cfg
        repo.close()
        repo = Repository(path)
        runner = Runner(ex, repo, cfg, clock=lambda: ex.now)
    ex.now = started + timedelta(seconds=11)
    runner.tick()
    assert ex.writes and all(op.kind == OperationKind.ORDER for op in ex.writes)
    assert sum(op.payload.qty for op in ex.writes) == D('5')
    assert repo.runtime().last_inventory_at == started
    assert not repo.runtime().resume_replanning
    count = len(ex.writes)
    ex.now += timedelta(seconds=11)
    runner.tick()
    assert len(ex.writes) == count
    repo.close()


@pytest.mark.parametrize('gate', ['candidate', 'paused', 'observe', 'slice'])
def test_only_an_executable_confirmed_group_earns_replanning(tmp_path, gate):
    ex, repo, runner = setup(tmp_path, FakeExchange(make_account(contract_bnb=D('27'), spot_usd=D('5000'))),
                            ReplenishmentState.ACCUMULATE)
    if gate == 'candidate':
        repo.save('runtime', replace(repo.runtime(), state=ReplenishmentState.IDLE))
    elif gate == 'paused':
        repo.save('runtime', replace(repo.runtime(), run_mode=RunMode.PAUSED))
    elif gate == 'slice':
        repo.save('runtime', replace(repo.runtime(), slice_state=SliceState(True, ex.now + timedelta(minutes=30))))
    else:
        runner.execute_enabled = False
    run(runner, ex)
    assert not repo.runtime().resume_replanning
    assert not any(op.kind == OperationKind.ORDER for op in ex.writes)
    repo.close()


def test_unknown_first_order_consumes_replanning_and_slice_across_restart(tmp_path):
    ex, repo, runner = setup(tmp_path, FakeExchange(make_account(contract_bnb=D('27'), spot_usd=D('5000'))),
                            ReplenishmentState.ACCUMULATE)
    repo.save('runtime', replace(repo.runtime(), slice_state=SliceState(active=True)))
    ex.lose_response = OperationKind.ORDER
    run(runner, ex)
    assert len(ex.writes) == 1
    assert not repo.runtime().resume_replanning
    deadline, cfg, path = repo.runtime().slice_state.next_at, runner.cfg, repo.path
    repo.close()
    repo = Repository(path)
    runner = Runner(ex, repo, cfg, clock=lambda: ex.now)
    ex.now += timedelta(seconds=11)
    runner.tick()
    assert len(ex.writes) == 1
    assert repo.runtime().slice_state.next_at == deadline
    assert not repo.runtime().resume_replanning
    repo.close()


@pytest.mark.parametrize('phase', ['before_intent', 'after_intent', 'after_send'])
def test_first_group_process_interruption_preserves_exactly_one_execution_right(tmp_path, monkeypatch, phase):
    ex, repo, runner = setup(tmp_path, FakeExchange(make_account(
        contract_bnb=D('27'), spot_usd=D('5000'))), ReplenishmentState.ACCUMULATE)
    class ProcessInterrupted(BaseException):
        pass
    with monkeypatch.context() as patch:
        if phase == 'after_send':
            submit = ex.submit
            def interrupted_send(op):
                submit(op)
                raise ProcessInterrupted()
            patch.setattr(ex, 'submit', interrupted_send)
        else:
            record = repo.record_intent
            def interrupted_intent(op, runtime=None):
                if phase == 'after_intent':
                    record(op, runtime)
                raise ProcessInterrupted()
            patch.setattr(repo, 'record_intent', interrupted_intent)
        with pytest.raises(ProcessInterrupted):
            run(runner, ex)
    assert repo.runtime().resume_replanning == (phase == 'before_intent')
    identities = {op.client_id for op in repo.operations()}
    path, cfg, observed_at = repo.path, runner.cfg, repo.runtime().last_inventory_at
    repo.close()
    repo = Repository(path)
    runner = Runner(ex, repo, cfg, clock=lambda: ex.now)
    for _ in range(2):
        ex.now += timedelta(seconds=11)
        runner.tick()
    if phase == 'before_intent':
        assert ex.writes and all(op.kind == OperationKind.ORDER for op in ex.writes)
        assert sum(op.payload.qty for op in ex.writes) == D('5')
    else:
        assert {op.client_id for op in repo.operations()} == identities
        assert len(ex.writes) == (1 if phase == 'after_send' else 0)
        if phase == 'after_intent':
            assert repo.operations()[0].status == OperationStatus.UNKNOWN
            assert repo.operations()[0].balance_pending
    assert repo.runtime().last_inventory_at == observed_at
    assert not repo.runtime().resume_replanning
    repo.close()
