from dataclasses import replace
from datetime import timedelta

import pytest

from core.models import OperationKind, OperationStatus, ReplenishmentState
from services.runner import Runner
from storage.repository import Repository
from tests.execution_helpers import run, seed_bid, setup
from tests.fake_exchange import FakeExchange
from tests.helpers import D, make_account, make_strategy_config
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


@pytest.mark.parametrize('cash,can_buy,restart,lose_response', [
    ('5.92', True, False, False),
    ('5.92', True, True, False),
    ('5.92', True, True, True),
    ('5.9149089', False, True, False),
    ('5.9149090', True, True, False),
    ('5.9149091', True, True, False),
    ('5.9090000', False, True, False),
])
def test_funded_minimum_buy_checks_unoccupied_prices_beyond_old_grid_slots(
    tmp_path, cash, can_buy, restart, lose_response,
):
    ex, repo, _ = setup(
        tmp_path, FakeExchange(make_account(
            contract_bnb=D('27'), spot_usd=D('100'),
            contract_max_withdraw_amount=D('100000'))),
        ReplenishmentState.ACCUMULATE,
    )
    cfg = replace(make_strategy_config(), spot_activity_symbols=('BTCUSDT',))
    runner = Runner(ex, repo, cfg, clock=lambda: ex.now)
    try:
        old = seed_bid(ex, repo, qty='.01')  # Valid GTC at 598, outside today's grid.
        ex.hold_transfers = True
        run(runner, ex)
        assert len(ex.writes) == 2
        funding = ex.writes[-1]
        assert funding.kind == OperationKind.TRANSFER
        assert repo.runtime().resume_replenishment
        ex.settle(repo.operation(funding.client_id).exchange_id)
        ex.hold_transfers = False
        assert not runner.reconciler.refresh(ex.now)[2].unresolved
        assert not repo.operation(funding.client_id).unresolved
        assert repo.settlement_window_open()  # The old GTC still needs strict evidence.

        # Supply a real receipt for the debit inside the old order's strict window.
        account = ex.fetch_account_snapshot()
        external_spend(ex, account.spot_usd - account.reserved_spot_usd - D(cash))
        account = ex.fetch_account_snapshot()
        assert account.reserved_spot_usd == D('5.98')
        assert account.spot_usd - account.reserved_spot_usd == D(cash)
        if restart:
            path = repo.path
            repo.close()
            repo = Repository(path)
            runner = Runner(ex, repo, cfg, clock=lambda: ex.now)
        assert repo.runtime().resume_replenishment
        if lose_response:
            ex.lose_response = OperationKind.ORDER

        run(runner, ex, inventory=False)

        new_buys = [op for op in ex.writes[2:] if op.kind == OperationKind.ORDER]
        if can_buy:
            assert len(new_buys) == 1
            buy = new_buys[0]
            assert (buy.payload.qty, buy.payload.price) == (D('.01'), D('590.90'))
            assert buy.payload.qty * buy.payload.price * (1 + cfg.risk.fee_reserve_rate) == D('5.914909')
            assert D('5.914909') <= D(cash)
            assert repo.runtime().funding_exhausted_at is None
            assert repo.operation(buy.client_id).status == (
                OperationStatus.UNKNOWN if lose_response else OperationStatus.CONFIRMED)
        else:
            assert not new_buys
            assert repo.runtime().funding_exhausted_at == ex.now
        assert not repo.runtime().resume_replenishment
        assert ex.orders[old.exchange_id].is_open
        assert all(op.kind != OperationKind.CANCEL for op in ex.writes)
        assert len(ex.writes) == 2 + int(can_buy)
        assert repo.budget_used(ex.now, 'USDT') == funding.payload.amount
        exhausted_at = repo.runtime().funding_exhausted_at

        # Restart after acceptance, UNKNOWN or shortfall: retain identity and the barrier.
        path = repo.path
        repo.close()
        repo = Repository(path)
        runner = Runner(ex, repo, cfg, clock=lambda: ex.now)
        for _ in range(2):
            ex.now += timedelta(seconds=1)
            run(runner, ex, inventory=False)
        assert len(ex.writes) == 2 + int(can_buy)
        assert not repo.operations(unresolved_only=True)
        assert ex.orders[old.exchange_id].is_open
        assert repo.runtime().funding_exhausted_at == exhausted_at
        assert repo.budget_used(ex.now, 'USDT') == funding.payload.amount
        assert sum(op.kind == OperationKind.TRANSFER for op in ex.writes) == 1
        if can_buy:
            recovered = repo.operation(buy.client_id)
            assert recovered.status == OperationStatus.CONFIRMED
            assert ex.orders[recovered.exchange_id].client_id == buy.client_id
    finally:
        repo.close()
