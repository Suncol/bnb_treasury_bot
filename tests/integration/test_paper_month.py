"""Synthetic quotes and a virtual clock; this is not a historical market replay."""

from datetime import timedelta
from collections import Counter
import json

from core.models import OperationStatus, PaperConfig, RunMode
from services.runner import Runner
from tests.helpers import D
from tests.paper_helpers import paper_setup
from tests.month_metrics import inventory_metrics


def test_virtual_month_preserves_money_budget_and_restart_state(tmp_path):
    p = paper_setup(tmp_path / "month.db", config=PaperConfig())
    runner = Runner(p.ex, p.repo, p.cfg, clock=p.clock.utc)
    start = p.clock.now
    restarts, trace, max_open = 0, [], 0
    try:
        for tick in range(1440):
            boundary = start + timedelta(minutes=30 * (tick + 1))
            p.clock.elapse((boundary - p.clock.now).total_seconds())
            ask = "580" if tick % 4 == 1 else "600"
            p.feed.publish(ask=ask, qty="3")  # At most .3 BNB per observed sample.
            p.ex.advance()
            runner.run_once(market=p.feed.quote())
            for _ in range(6):
                if not p.repo.operations(unresolved_only=True):
                    break
                p.clock.elapse(2)
                p.feed.publish(ask=ask, qty="3")
                p.ex.advance()
                runner.run_once(market=p.feed.quote(), inventory_cycle=False)
            state = p.ex.ledger.state()
            account = p.ex.fetch_account_snapshot()
            assert account.spot_bnb + account.contract_bnb == (
                D("24.5") + state["gross_bought_bnb"] - state["spot_fee_bnb"] - state["consumed_bnb"])
            assert account.spot_usd + account.contract_quote_balance == D("100000") - state["quote_spent"]
            assert min(account.spot_bnb, account.contract_bnb, account.spot_usd, account.contract_quote_balance) >= 0
            assert 0 <= account.reserved_spot_usd <= account.spot_usd
            assert 0 <= account.reserved_spot_bnb <= account.spot_bnb
            orders = p.ex.fetch_open_orders(p.cfg.symbol)
            exposure = account.contract_bnb + max(account.spot_bnb - p.cfg.thresholds.b_spot_reserve, D("0"))
            exposure += sum((o.remaining_qty for o in orders), D("0"))
            assert exposure <= p.cfg.thresholds.b_target, (tick, exposure)
            assert p.repo.budget_used(p.clock.utc(), "USDT") <= p.cfg.risk.u_budget_24h
            assert not p.repo.load("spot_settlement_issue"), tick
            assert not p.repo.load("futures_settlement_issue"), tick
            assert not any(o.status == OperationStatus.UNKNOWN for o in p.repo.operations(unresolved_only=True))
            assert p.repo.runtime().run_mode != RunMode.PAUSED, tick
            if p.repo.runtime().state.value == "IDLE":
                assert not orders, tick
            trace.append({"at": p.clock.now, "contract_bnb": account.contract_bnb,
                          "state": p.repo.runtime().state.value})
            max_open = max(max_open, len(orders))
            if tick and tick % 144 == 0:
                identity = p.ex.account_identity()
                p.repo.close()
                p = paper_setup(p.path, config=p.config, clock=p.clock, feed=p.feed)
                assert p.ex.account_identity() == identity
                runner = Runner(p.ex, p.repo, p.cfg, clock=p.clock.utc)
                restarts += 1
        state = p.ex.ledger.state()
        assert restarts == 9
        assert state["gross_bought_bnb"] > 20 and state["spot_fee_bnb"] > 0
        assert state["consumed_bnb"] > 28
        assert state["next_transfer"] > 10 and state["next_order"] > 10
        assert p.repo.db.execute("SELECT COUNT(*) FROM paper_fills").fetchone()[0] == p.repo.db.execute("SELECT COUNT(*) FROM fills").fetchone()[0]
        tolerance = max(p.ex.min_transfer_bnb, state["qty_step"])
        inventory = inventory_metrics(trace, start, p.cfg, tolerance)
        assert inventory["first_observed_recovery_hours"]["25"] <= D("1")
        assert inventory["first_observed_recovery_hours"]["28"] <= D("2")
        assert inventory["below_floor_observations_after_initial_recovery"] == 0
        assert len(inventory["completed_rounds"]) >= 2, inventory
        assert inventory["completed_rounds"][0]["duration_hours"] <= D("96")
        assert all(p.cfg.thresholds.b_target-tolerance < round_["contract_bnb"] <= p.cfg.thresholds.b_target
                   for round_ in inventory["completed_rounds"])
        result = {
            "virtual_days": 30, "ticks": 1440, "sqlite_reopens": restarts,
            "writes": dict(Counter(o.kind.value for o in p.repo.operations())),
            "gross_bought_bnb": state["gross_bought_bnb"], "spot_bnb_fees": state["spot_fee_bnb"],
            "futures_bnb_consumption": state["consumed_bnb"],
            "final_contract_bnb": account.contract_bnb, "final_spot_bnb": account.spot_bnb,
            "maximum_open_orders": max_open, "inventory": inventory,
        }
        (tmp_path / "virtual_month_result.json").write_text(json.dumps(result, default=str, indent=2))
        (tmp_path / "inventory_trace.json").write_text(json.dumps(trace, default=str, indent=2))
    finally:
        p.repo.close()
