from datetime import timedelta

import pytest

from core.models import OperationKind
from services.runner import Runner
from storage.repository import Repository
from tests.execution_helpers import run
from tests.helpers import D
from tests.integration.test_replenishment_continuation import funded_setup, external_spend


@pytest.mark.parametrize('lose_response', [False, True])
def test_settled_continuation_buys_minimum_order_with_5_99_usdt(tmp_path, lose_response):
    ex, repo, runner, funding = funded_setup(tmp_path)
    assert not runner.reconciler.refresh(ex.now)[2].unresolved
    external_spend(ex, funding.payload.amount - D('5.99'), receipt=False)
    if lose_response:
        ex.lose_response = OperationKind.ORDER

    run(runner, ex, inventory=False)

    buys = [op for op in ex.writes if op.kind == OperationKind.ORDER]
    assert len(buys) == 1
    assert (buys[0].payload.qty, buys[0].payload.price) == (D('.01'), D('598.09'))
    assert buys[0].payload.qty * buys[0].payload.price * (1 + runner.cfg.risk.fee_reserve_rate) == D('5.9868809')
    assert repo.runtime().funding_exhausted_at is None
    assert len(ex.writes) == 2  # One settled funding, then one funded buy.

    path = repo.path
    repo.close()
    repo = Repository(path)
    runner = Runner(ex, repo, runner.cfg, clock=lambda: ex.now)
    ex.now += timedelta(seconds=1)
    run(runner, ex, inventory=False)
    assert len(ex.writes) == 2
    assert repo.runtime().funding_exhausted_at is None
    repo.close()


@pytest.mark.parametrize('cash,expected_price', [
    ('5.9868808', '595.70'),
    ('5.9868809', '598.09'),
    ('5.9868810', '598.09'),
    ('5.9629569', '590.90'),
    ('5.9629570', '595.70'),
    ('5.9629571', '595.70'),
    ('5.9149089', None),
    ('5.9149090', '590.90'),
    ('5.9149091', '590.90'),
    ('5.9090000', None),  # Nominal cost alone does not cover the fee reserve.
])
def test_funded_minimum_buy_uses_fee_inclusive_layer_boundaries(tmp_path, cash, expected_price):
    ex, repo, runner, funding = funded_setup(tmp_path)
    assert not runner.reconciler.refresh(ex.now)[2].unresolved
    external_spend(ex, funding.payload.amount - D(cash), receipt=False)

    run(runner, ex, inventory=False)

    buys = [op for op in ex.writes if op.kind == OperationKind.ORDER]
    if expected_price is not None:
        assert len(buys) == 1
        assert (buys[0].payload.qty, buys[0].payload.price) == (D('.01'), D(expected_price))
        assert buys[0].payload.qty * buys[0].payload.price * (1 + runner.cfg.risk.fee_reserve_rate) <= D(cash)
        assert repo.runtime().funding_exhausted_at is None
    else:
        assert not buys
        assert not repo.runtime().resume_replenishment
        assert repo.runtime().funding_exhausted_at == ex.now
    assert sum(op.kind == OperationKind.TRANSFER for op in ex.writes) == 1
    repo.close()
