"""Shared account and transfer setup for recovery and storage tests."""

from core.models import AssetTransferPlan, ReplenishmentState
from services.executor import Executor
from tests.fake_exchange import FakeExchange
from tests.helpers import D, make_account, make_strategy_config


def funded(cls=FakeExchange):
    return cls(make_account(contract_bnb=D("24"), contract_max_withdraw_amount=D("50000"),
                            spot_bnb=D("10"), spot_usd=D("3000")))


def initialize(repo, ex):
    repo.initialize_spot_checkpoint(ex.fetch_account_snapshot(), {
        "operator": "offline-test", "reason": "quiescent simulation",
    })


def transfer(repo, ex, asset, to_account, amount):
    source = "SPOT" if to_account != "SPOT" else "USDⓈ-M Futures"
    return Executor(ex, repo, make_strategy_config()).execute(
        AssetTransferPlan(asset, D(amount), source, to_account, "recovery test"),
        ex.now, ReplenishmentState.URGENT, account=ex.fetch_account_snapshot(),
    )
