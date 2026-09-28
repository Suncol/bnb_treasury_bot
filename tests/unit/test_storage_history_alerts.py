from dataclasses import replace
from datetime import timedelta
import sqlite3

import pytest

from core.config import load_config
from core.models import (
    Alert,
    AssetTransferPlan,
    CrashGuardState,
    Fill,
    Operation,
    OperationKind,
    OperationStatus,
    ReplenishmentState,
    TransferRecord,
)
from core.replenishment_engine import build_cycle_plan
from services.alert_service import AlertService
from services.history import historical_metrics
from storage.repository import Repository
from tests.helpers import D, make_account, make_engine_inputs, make_strategy_config


def test_journal_reopens_exact_values_and_enforces_unresolved_scope(tmp_path):
    path = tmp_path / "journal.db"
    repo = Repository(path)
    now = make_engine_inputs().now
    payload = AssetTransferPlan(
        "USDT", D("500.00000001"), "USDⓈ-M Futures", "SPOT", "test"
    )
    op = Operation("one", OperationKind.TRANSFER, "fund", payload, now)
    repo.record_intent(op)
    with pytest.raises(sqlite3.IntegrityError):
        repo.record_intent(replace(op, client_id="two"))
    repo.close()
    repo = Repository(path)
    assert repo.operations() == (op,)
    repo.update_operation(replace(op, status=OperationStatus.CONFIRMED))
    repo.record_intent(replace(op, client_id="two"))


def test_independent_instances_cannot_execute_the_same_account_concurrently(tmp_path):
    first, second = (
        Repository(tmp_path / "journal.db"),
        Repository(tmp_path / "journal.db"),
    )
    with first.exclusive():
        with pytest.raises(BlockingIOError):
            with second.exclusive():
                pytest.fail("second writer acquired the same account")


def test_failed_operation_and_continuation_are_committed_together(tmp_path):
    path = tmp_path / "journal.db"
    repo = Repository(path)
    op = Operation(
        "rejected", OperationKind.TRANSFER, "fund",
        AssetTransferPlan("USDT", D("500"), "USDⓈ-M Futures", "SPOT", "test"),
        make_engine_inputs().now,
    )
    repo.record_intent(op, repo.runtime())
    failed = replace(op, status=OperationStatus.FAILED)
    resumed = replace(repo.runtime(), resume_repricing=True)
    repo.db.execute("""
        CREATE TEMP TRIGGER fail_runtime_update BEFORE UPDATE ON kv
        WHEN NEW.key = 'runtime'
        BEGIN SELECT RAISE(ABORT, 'runtime write failed'); END
    """)
    with pytest.raises(sqlite3.IntegrityError, match="runtime write failed"):
        repo.update_operation(failed, resumed)
    repo.close()
    repo = Repository(path)
    assert repo.operations(unresolved_only=True) == (op,)
    assert not repo.runtime().resume_repricing
    repo.update_operation(failed, resumed)
    repo.close()
    repo = Repository(path)
    assert repo.operations() == (failed,)
    assert repo.runtime().resume_repricing


def test_budget_uses_exchange_history_with_pending_reservation_and_no_duplicate(
    tmp_path,
):
    repo = Repository(tmp_path / "journal.db")
    now = make_engine_inputs().now
    old = TransferRecord(
        "old",
        "USDT",
        D("500"),
        "USDⓈ-M Futures",
        "SPOT",
        now - timedelta(hours=25),
        OperationStatus.CONFIRMED,
    )
    current = replace(old, transfer_id="current", amount=D("1000"), ts=now)
    repo.save_transfers((old, current, current))
    payload = AssetTransferPlan("USDT", D("500"), "USDⓈ-M Futures", "SPOT", "test")
    repo.record_intent(
        Operation("unknown", OperationKind.TRANSFER, "fund", payload, now)
    )
    assert repo.budget_used(now, "USDT") == D("1500")
    op = repo.operations()[0]
    repo.update_operation(
        replace(op, exchange_id="current", payload=replace(payload, amount=D("1000")))
    )
    assert repo.budget_used(now, "USDT") == D("1000")


def test_fill_dedup_and_late_report_repairs_closed_guard_episode(tmp_path):
    repo = Repository(tmp_path / "journal.db")
    now = make_engine_inputs().now
    episode = CrashGuardState(
        False, "old", now, now, ended_at=now + timedelta(minutes=5)
    )
    repo.save("episode:old", episode)
    fill = Fill(
        "BNBUSDT",
        "trade",
        "order",
        now + timedelta(seconds=1),
        D("1"),
        strategy_id="bnb-treasury",
    )
    repo.save_fills((fill, fill))
    assert repo.load("episode:old").filled_bnb == D("1")
    assert len(repo.fills_since(now)) == 1
    with pytest.raises(ValueError, match="Conflicting"):
        repo.save_fills((replace(fill, qty=D("2")),))


def test_consumption_estimate_removes_transfer_inflows(tmp_path):
    repo = Repository(tmp_path / "journal.db")
    now = make_engine_inputs().now
    for hour in range(7):
        account = replace(
            make_account(contract_bnb=D(30 - hour + (3 if hour >= 3 else 0))),
            ts=now - timedelta(hours=6 - hour),
        )
        repo.save_snapshot(account)
    transfer = TransferRecord(
        "credit",
        "BNB",
        D("3"),
        "SPOT",
        "USDⓈ-M Futures",
        now - timedelta(hours=3),
        OperationStatus.CONFIRMED,
    )
    repo.save_transfers((transfer,))
    rate, change = historical_metrics(repo, account, now)
    assert rate == D("1") and change is None


@pytest.mark.parametrize("minutes", [
    list(range(360, -1, -30)),
    [360, 359, 300, 120, 119, 1, 0],
])
def test_bursty_consumption_uses_window_average_and_triggers_early_replenishment(
    tmp_path, minutes
):
    repo = Repository(tmp_path / "journal.db")
    now = make_engine_inputs().now
    for minute in minutes:
        account = replace(
            make_account(contract_bnb=D("32") if minute > 90 else D("28.4")),
            ts=now - timedelta(minutes=minute),
        )
        repo.save_snapshot(account)
    rate, _ = historical_metrics(repo, account, now)
    assert rate == D("0.6")
    plan = build_cycle_plan(
        make_engine_inputs(account=account, consumption_rate=rate), make_strategy_config()
    )
    assert plan.state_decision.confirmed_state == ReplenishmentState.URGENT
    assert plan.state_decision.t_depletion == D("3.4") / D("0.6")


def test_consumption_combines_full_window_rates_and_preserves_margin_change(tmp_path):
    repo = Repository(tmp_path / "journal.db")
    now = make_engine_inputs().now
    for hours, balance, margin in [(24, 40, 10000), (6, 34, 9800), (0, 28, 9400)]:
        account = replace(
            make_account(contract_bnb=D(balance)),
            ts=now - timedelta(hours=hours),
            contract_total_margin_balance=D(margin),
        )
        repo.save_snapshot(account)
    assert historical_metrics(repo, account, now) == (D("0.75"), D("-600"))


def test_consumption_adjusts_only_confirmed_bnb_transfers_inside_sample_interval(tmp_path):
    repo = Repository(tmp_path / "journal.db")
    now = make_engine_inputs().now
    account = make_account(contract_bnb=D("27"))
    repo.save_snapshot(replace(account, ts=now - timedelta(hours=6), contract_bnb=D("30")))
    credit = TransferRecord(
        "credit", "BNB", D("5"), "SPOT", "USDⓈ-M Futures",
        now - timedelta(hours=5), OperationStatus.CONFIRMED,
    )
    repo.save_transfers((
        credit,
        replace(credit, transfer_id="debit", amount=D("2"), ts=now,
                from_account="USDⓈ-M Futures", to_account="SPOT"),
        replace(credit, transfer_id="at_start", ts=now - timedelta(hours=6)),
        replace(credit, transfer_id="after_end", ts=now + timedelta(seconds=1)),
        replace(credit, transfer_id="pending", status=OperationStatus.PENDING),
        replace(credit, transfer_id="failed", status=OperationStatus.FAILED),
        replace(credit, transfer_id="usd", asset="USDT"),
    ))
    assert historical_metrics(repo, account, now) == (D("1"), None)


@pytest.mark.parametrize("hours,balance,expected", [(5, "32", "0"), (7, "32.6", "0.6"), (6, "27", "0")])
def test_consumption_uses_actual_elapsed_time_and_requires_a_full_window(
    tmp_path, hours, balance, expected
):
    repo = Repository(tmp_path / "journal.db")
    now = make_engine_inputs().now
    account = make_account(contract_bnb=D("28.4"))
    repo.save_snapshot(replace(account, ts=now - timedelta(hours=hours), contract_bnb=D(balance)))
    assert historical_metrics(repo, account, now) == (D(expected), None)


def test_alert_outbox_retries_only_failed_sink_and_deduplicates_state(tmp_path):
    repo = Repository(tmp_path / "journal.db")

    class Sink:
        def __init__(self, name, fail=False):
            self.name, self.fail, self.keys = name, fail, []

        def send(self, alert, key):
            if self.fail:
                raise OSError()
            self.keys.append(key)

    first, second = Sink("first"), Sink("second", True)
    service = AlertService(repo, (first, second))
    alert = Alert("WARNING", "WAIT", "Waiting for reconciliation")
    for _ in range(3):
        repo.publish_alerts((alert,))
        service.deliver()
    assert len(first.keys) == 1 and not second.keys
    second.fail = False
    service.deliver()
    assert first.keys == second.keys and not repo.pending_alerts()
    repo.publish_alerts(())
    repo.publish_alerts((alert,))
    assert len(repo.pending_alerts()) == 1


def test_config_loads_decimal_and_rejects_typos(tmp_path):
    from pathlib import Path

    cfg = load_config("config/strategy.toml")
    assert cfg.risk.u_min == D("500") and cfg.crash_guard.drawdown_1m_enter == D(
        "0.008"
    )
    bad = tmp_path / "bad.toml"
    bad.write_text(
        Path("config/strategy.toml").read_text().replace("alpha_warn", "alpha_warm")
    )
    with pytest.raises(ValueError, match="Unknown"):
        load_config(bad)


def test_fill_quote_backfill_preserves_cursor_and_rejects_conflicting_cost(tmp_path):
    repo = Repository(tmp_path / "fills.db")
    now = make_engine_inputs().now
    legacy = Fill("BNBUSDT", "trade", "order", now, D("1"))
    repo.save_fills((legacy,))
    cursor = repo.fill_cursor()
    complete = replace(legacy, quote_qty=D("600"))
    repo.save_fills((complete, complete))
    assert repo.fill_cursor() == cursor and not repo.fills_after(cursor)
    with pytest.raises(ValueError, match="Conflicting"):
        repo.save_fills((replace(complete, quote_qty=D("599")),))
    newer = replace(complete, trade_id="next-trade")
    repo.save_fills((newer,))
    assert repo.fills_after(cursor) == (newer,)
    repo.close()
