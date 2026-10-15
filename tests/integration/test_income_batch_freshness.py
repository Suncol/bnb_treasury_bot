"""Income scan completion and final account freshness are separate evidence."""

from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace

from core.models import FuturesIncome, OperationKind, ReplenishmentState
from services.binance_adapter import BinanceAdapter, millis
from tests.helpers import D, make_account, make_strategy_config
from tests.fake_exchange import FakeExchange
from tests.execution_helpers import setup


def slow_income_setup(tmp_path):
    exchange = FakeExchange(make_account(
        contract_bnb=D("27"), contract_max_withdraw_amount=D("97000"), spot_usd=D("10000"),
    ))
    started = exchange.now
    exchange.now -= timedelta(hours=24)
    exchange.account = replace(
        exchange.account, contract_quote_updated_at=exchange.now - timedelta(seconds=1),
        contract_bnb_updated_at=exchange.now - timedelta(seconds=1),
    )
    read_account = exchange.fetch_account_snapshot

    def margin_account():
        observed = read_account()
        return replace(observed, contract_total_margin_balance=observed.contract_quote_balance,
                       contract_margin_asset="USDT")

    exchange.fetch_account_snapshot = margin_account
    exchange, repo, runner = setup(tmp_path, exchange, ReplenishmentState.ACCUMULATE)
    repo.save_snapshot(exchange.fetch_account_snapshot())
    exchange.now = started
    first_income_at = started - timedelta(hours=23)
    exchange.income = [
        FuturesIncome("COMMISSION", str(index + 1), "USDT", D("-0.001"),
                      first_income_at + timedelta(milliseconds=33120 * index))
        for index in range(2501)
    ]
    exchange.account = replace(
        exchange.account,
        contract_quote_balance=exchange.account.contract_quote_balance - D("2.501"),
        contract_quote_updated_at=started,
    )
    queries, pages, account_reads = [], [], []

    class FuturesPages:
        def get_income_history(self, **params):
            pages.append((params["start_time"], params["end_time"], params["page"]))
            rows = [{"incomeType": row.income_type, "tranId": row.transaction_id,
                     "asset": row.asset, "income": str(row.amount), "time": millis(row.ts)}
                    for row in exchange.income
                    if params["start_time"] <= millis(row.ts) <= params["end_time"]]
            offset = (params["page"] - 1) * params["limit"]
            page = rows[offset:offset + params["limit"]]
            exchange.now += timedelta(seconds=4)
            return SimpleNamespace(data=lambda: page, headers={})

    # Exercise the real adapter's retention, seven-day segmentation, page loop,
    # duplicate validation and SDK transport checkpoints without network I/O.
    adapter = BinanceAdapter(
        "fixture-key", "fixture-secret", make_strategy_config(),
        clients=(SimpleNamespace(), FuturesPages(), SimpleNamespace()),
        clock=lambda: exchange.now,
    )
    adapter.read_checkpoint = runner.reconciler.read_checkpoint

    def income(since, until):
        queries.append((since, until))
        return adapter.fetch_futures_income(since, until)

    snapshot = exchange.fetch_account_snapshot

    def account():
        observed = snapshot()
        account_reads.append(observed)
        return observed

    exchange.fetch_futures_income = income
    exchange.fetch_account_snapshot = account
    return exchange, repo, runner, queries, pages, account_reads


def test_slow_full_income_scan_reuses_completion_with_a_fresh_final_account(tmp_path):
    exchange, repo, runner, queries, pages, account_reads = slow_income_setup(tmp_path)
    try:
        plan = runner.run_once()
        full_since = queries[0][0]
        metrics = {
            "full_queries": sum(since == full_since for since, _ in queries),
            "increment_queries": sum(since > full_since for since, _ in queries),
            "pages": len(pages),
            "original_account_age_seconds": (exchange.now - account_reads[0].ts).total_seconds(),
            "final_account_age_seconds": (exchange.now - runner.cached_inputs.account.ts).total_seconds(),
            "legal_plan": bool(plan is not None and plan.inventory_valid),
            "writes": len(exchange.writes),
            "coverage_saved": repo.load("futures_income_coverage") is not None,
        }
        assert metrics["legal_plan"], metrics
        assert metrics == {
            "full_queries": 1, "increment_queries": 0, "pages": 3,
            "original_account_age_seconds": 12.0, "final_account_age_seconds": 0.0,
            "legal_plan": True, "writes": 3, "coverage_saved": True,
        }
        assert account_reads[-1].ts > account_reads[0].ts
        assert runner.cached_inputs.margin_change_24h == D("-2.501")
        assert all(op.kind == OperationKind.ORDER for op in exchange.writes)
        assert sum(op.payload.qty for op in exchange.writes) == D("5")
        assert not repo.operations(unresolved_only=True)
    finally:
        repo.close()
