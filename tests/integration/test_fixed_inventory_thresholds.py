from dataclasses import replace
from datetime import timedelta

import pytest

from core.models import ReplenishmentState
from core.replenishment_engine import build_cycle_plan
from tests.helpers import D, make_account, make_engine_inputs, make_strategy_config
from tests.execution_helpers import setup
from tests.fake_exchange import FakeExchange
from services.runner import Runner
from storage.repository import Repository


@pytest.mark.parametrize("balance", ["25", "25.5", "26", "27", "28", "31.9", "31.900001", "32", "40"])
@pytest.mark.parametrize("state", list(ReplenishmentState))
def test_whole_plan_is_independent_of_consumption_history(balance, state):
    inputs = make_engine_inputs(account=make_account(contract_bnb=D(balance), spot_usd=D("5000")),
                                previous_state=state)
    plans = [build_cycle_plan(replace(inputs, consumption_rate=rate), make_strategy_config())
             for rate in (None, D("0"), D("100000"))]
    assert plans[0] == plans[1] == plans[2]
    assert plans[0].state_decision.t_depletion is None
    assert "DEPLETION" not in plans[0].state_decision.reason.value


def test_legacy_urgent_exits_with_valid_cycles_after_restart(tmp_path, monkeypatch):
    exchange, repo, runner = setup(tmp_path, FakeExchange(make_account(
        contract_bnb=D("27"), spot_usd=D("0"))), ReplenishmentState.URGENT)
    runner.execute_enabled = False
    monkeypatch.setattr("services.reconciliation.historical_metrics",
                        lambda *a, **kw: (D("100000"), None, None))
    runner.run_once()
    assert repo.runtime().state == ReplenishmentState.URGENT
    assert repo.runtime().candidate_streak == 1
    repo.close()
    repo = Repository(tmp_path / "test.sqlite3")
    runner = Runner(exchange, repo, make_strategy_config(), clock=lambda: exchange.now, execute=False)
    exchange.now += timedelta(minutes=30)
    runner.run_once()
    assert repo.runtime().state == ReplenishmentState.ACCUMULATE
    assert repo.runtime().candidate_streak == 0
    assert not exchange.writes
    repo.close()
