"""The review's 30-day deterministic probe, with explicit baseline and receipts."""

from collections import Counter
from dataclasses import replace
from datetime import timedelta
import json

from core.models import RunMode
from services.runner import Runner
from storage.repository import Repository
from tests.fake_exchange import FakeExchange
from tests.helpers import D, make_account, make_strategy_config


def test_thirty_virtual_days_preserve_inventory_budget_and_restart_invariants(tmp_path):
    class CommissionExchange(FakeExchange):
        def fill(self, order_id, qty):
            super().fill(order_id, qty)
            fee = self.fills[-1].qty * D("0.001")
            self.account = replace(self.account, spot_bnb=self.account.spot_bnb - fee)
            self.fills[-1] = replace(self.fills[-1], commission=fee, commission_asset="BNB")

    cfg = make_strategy_config()
    ex = CommissionExchange(make_account(contract_bnb=D("24"), contract_max_withdraw_amount=D("100000")))
    start = ex.now
    consumed = peak_supply = peak_budget = D("0")
    restarts = 0
    path = tmp_path / "month.db"
    repo = Repository(path)
    repo.initialize_spot_checkpoint(ex.fetch_account_snapshot(), {"operator": "simulation", "reason": "quiet initial state"})
    runner = Runner(ex, repo, cfg, clock=lambda: ex.now)
    try:
        for tick in range(1440):
            ex.now = start + timedelta(minutes=30 * tick)
            consumed += D("0.02")
            ex.futures_change("BNB", D("-0.02"))
            for order in tuple(ex.orders.values()):
                if order.is_open and order.time_in_force != "IOC":
                    ex.fill(order.order_id, min(order.remaining_qty, D("0.20")))
                    break
            if tick and tick % 144 == 0:
                repo.close()
                repo = Repository(path)
                runner = Runner(ex, repo, cfg, clock=lambda: ex.now)
                restarts += 1
            runner.tick()
            account = ex.fetch_account_snapshot()
            assert account.contract_bnb >= 0 and account.spot_bnb >= 0 and account.spot_usd >= 0
            assert account.reserved_spot_usd <= account.spot_usd
            supply = (account.contract_bnb + max(account.spot_bnb - cfg.thresholds.b_spot_reserve, D("0"))
                      + sum((o.remaining_qty for o in ex.orders.values() if o.side == "BUY"), D("0")))
            peak_supply = max(peak_supply, supply)
            assert supply <= cfg.thresholds.b_target, (tick, supply)
            budget = repo.budget_used(ex.now, cfg.quote_asset)
            peak_budget = max(peak_budget, budget)
            assert budget <= cfg.risk.u_budget_24h, (tick, budget)
            assert not repo.load("spot_settlement_issue"), tick
            assert not repo.load("futures_settlement_issue"), tick
            assert not repo.operations(unresolved_only=True), tick
            assert repo.runtime().run_mode != RunMode.PAUSED, tick
        bought = sum((f.qty for f in ex.fills), D("0"))
        fees = sum((f.commission for f in ex.fills), D("0"))
        assert account.contract_bnb + account.spot_bnb == D("24.5") + bought - consumed - fees
        result = {
            "virtual_days": 30, "ticks": 1440, "sqlite_reopens": restarts,
            "writes": dict(Counter(op.kind.value for op in ex.writes)),
            "gross_bought_bnb": str(bought), "spot_bnb_fees": str(fees),
            "futures_bnb_consumption": str(consumed),
            "final_contract_bnb": str(account.contract_bnb), "final_spot_bnb": str(account.spot_bnb),
            "peak_effective_supply_bnb": str(peak_supply), "peak_transfer_budget_usdt": str(peak_budget),
        }
        (tmp_path / "virtual_month_result.json").write_text(json.dumps(result, indent=2))
        print(json.dumps(result))
    finally:
        repo.close()
