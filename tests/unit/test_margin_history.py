from dataclasses import replace
from datetime import timedelta

import pytest

from core.models import FuturesIncome, OperationStatus, TransferRecord
from core.replenishment_engine import build_cycle_plan
from services.history import futures_settlement_evidence, historical_metrics, settlement_policy_issue
from storage.repository import Repository
from tests.helpers import D, make_account, make_engine_inputs, make_strategy_config


@pytest.mark.parametrize('legacy', [False, True])
@pytest.mark.parametrize('skew_ms', [-1000, -500, 0, 500, 1000])
def test_foreign_income_uses_the_original_futures_clock_boundary(tmp_path, legacy, skew_ms):
    repo = Repository(tmp_path / 'foreign.db')
    original = make_account()
    skew = timedelta(milliseconds=skew_ms)
    before = replace(original, wallet_event_cursor=0,
                     futures_event_upper_bound=None if legacy else original.ts + skew,
                     contract_quote_balance=D('10000'), contract_total_margin_balance=D('10000'),
                     contract_quote_updated_at=original.ts - timedelta(minutes=1),
                     contract_margin_asset='USDT')
    repo.save_futures_income((FuturesIncome(
        'TRANSFER', 'external-usdc', 'USDC', D('3000'),
        original.ts + timedelta(milliseconds=250) + skew),))
    after = replace(before, ts=original.ts + timedelta(seconds=1),
                    futures_event_upper_bound=original.ts + timedelta(seconds=1) + skew)
    valid, _, evidence = futures_settlement_evidence(repo, before, after, 'USDT')
    assert valid, evidence
    assert evidence['foreign_transfer_assets'] == ('USDC',)
    assert settlement_policy_issue(evidence) is not None
    repo.close()


@pytest.mark.parametrize("quote_asset", ["USDT", "USDC"])
@pytest.mark.parametrize("transfer,pnl,fee,funding,valuation,expected", [
    ("-3000", "0", "0", "0", "0", "0"),
    ("-3000", "-1000", "0", "0", "0", "-1000"),
    ("3000", "-1000", "0", "0", "0", "-1000"),
    ("3000", "-400", "-20", "-30", "-100", "-550"),
    ("-3000", "100", "-20", "30", "-10", "100"),
])
def test_margin_removes_only_settled_quote_transfers(
    tmp_path, quote_asset, transfer, pnl, fee, funding, valuation, expected
):
    repo = Repository(tmp_path / "margin.db")
    now = make_engine_inputs().now
    baseline = replace(
        make_account(), ts=now - timedelta(hours=24),
        contract_quote_balance=D("97000"), contract_total_margin_balance=D("97000"),
        contract_margin_asset=quote_asset,
    )
    repo.save_snapshot(baseline)
    repo.save_transfers((TransferRecord(
        "settled", quote_asset, abs(D(transfer)),
        "USDⓈ-M Futures" if D(transfer) < 0 else "SPOT",
        "SPOT" if D(transfer) < 0 else "USDⓈ-M Futures",
        now - timedelta(hours=1), OperationStatus.CONFIRMED,
    ),))
    income = tuple(
        FuturesIncome(kind, str(i), quote_asset, D(amount), now - timedelta(hours=1))
        for i, (kind, amount) in enumerate((
            ("TRANSFER", transfer), ("REALIZED_PNL", pnl),
            ("COMMISSION", fee), ("FUNDING_FEE", funding),
        ))
    )
    repo.save_futures_income(income + income + (
        FuturesIncome("TRANSFER", "other-asset", "BNB", D("5"), now),
    ))
    wallet = baseline.contract_quote_balance + sum((r.amount for r in income), D("0"))
    account = replace(
        baseline, ts=now, contract_quote_balance=wallet,
        contract_total_margin_balance=wallet + D(valuation),
    )
    _, change, issue = historical_metrics(repo, account, now, quote_asset)
    assert issue is None
    assert change == D(expected)
    plan = build_cycle_plan(
        replace(make_engine_inputs(account=account), margin_change_24h=change),
        make_strategy_config(),
    )
    assert plan.active_buy_target == D("32")
    repo.close()


def test_margin_uses_actual_baseline_and_wallet_event_boundaries_after_restart(tmp_path):
    path = tmp_path / "margin.db"
    repo = Repository(path)
    now = make_engine_inputs().now
    baseline = replace(
        make_account(), ts=now - timedelta(hours=25),
        contract_quote_balance=D("97000"), contract_total_margin_balance=D("97000"),
        contract_margin_asset="USDT",
    )
    repo.save_snapshot(baseline)
    # This snapshot is too recent to provide a complete 24h window.
    repo.save_snapshot(replace(baseline, ts=now - timedelta(hours=23)))
    repo.save_transfers((TransferRecord(
        "at-end", "USDT", D("3000"), "USDⓈ-M Futures", "SPOT", now - timedelta(seconds=3), OperationStatus.CONFIRMED,
    ),))
    account = replace(
        baseline, ts=now - timedelta(seconds=2),
        contract_quote_updated_at=now - timedelta(seconds=3),
        contract_quote_balance=D("93000"), contract_total_margin_balance=D("93000"),
    )
    repo.save_futures_income((
        FuturesIncome("TRANSFER", "included-in-baseline", "USDT", D("5000"), baseline.ts),
        FuturesIncome("REALIZED_PNL", "loss", "USDT", D("-1000"), baseline.ts + timedelta(minutes=1)),
        FuturesIncome("TRANSFER", "at-end", "USDT", D("-3000"), account.contract_quote_updated_at),
        FuturesIncome("TRANSFER", "not-in-wallet", "USDT", D("5000"), account.ts),
        FuturesIncome("TRANSFER", "future", "USDT", D("5000"), now + timedelta(seconds=1)),
    ))
    repo.close()
    repo = Repository(path)
    assert historical_metrics(repo, account, now, "USDT")[1] == D("-1000")
    repo.close()


@pytest.mark.parametrize("status", list(OperationStatus))
def test_transfer_status_cannot_replace_settled_income_evidence(tmp_path, status):
    repo = Repository(tmp_path / "margin.db")
    now = make_engine_inputs().now
    baseline = replace(
        make_account(), ts=now - timedelta(hours=24),
        contract_quote_balance=D("97000"), contract_total_margin_balance=D("97000"),
        contract_margin_asset="USDT",
    )
    repo.save_snapshot(baseline)
    repo.save_transfers((TransferRecord(
        "request", "USDT", D("3000"), "USDⓈ-M Futures", "SPOT",
        now - timedelta(hours=1), status,
    ),))
    account = replace(
        baseline, ts=now,
        contract_quote_balance=D("94000"), contract_total_margin_balance=D("94000"),
    )
    assert historical_metrics(repo, account, now, "USDT")[1] is None
    repo.save_futures_income((
        FuturesIncome("TRANSFER", "settled", "USDT", D("-3000"), now),
    ))
    # A receipt alone cannot adjust a wallet still showing its old balance.
    assert historical_metrics(repo, replace(account, contract_quote_balance=D("97000")), now, "USDT")[1] is None
    repo.close()


def test_confirmed_event_cursors_bound_the_snapshot_interval_without_request_times(tmp_path):
    repo = Repository(tmp_path / "margin.db")
    now = make_engine_inputs().now
    old = TransferRecord("old", "USDT", D("2000"), "SPOT", "USDⓈ-M Futures",
                         now - timedelta(hours=26), OperationStatus.CONFIRMED)
    repo.save_transfers((old,))
    baseline = replace(
        make_account(), ts=now - timedelta(hours=24),
        contract_quote_balance=D("97000"), contract_total_margin_balance=D("97000"),
        contract_margin_asset="USDT",
    )
    repo.save_snapshot(baseline)
    settled = replace(old, transfer_id="late-settlement", amount=D("3000"), ts=now - timedelta(hours=25))
    repo.save_transfers((settled, settled))
    account = replace(baseline, ts=now, wallet_event_cursor=repo.wallet_cursor(),
                      contract_quote_balance=D("99000"), contract_total_margin_balance=D("99000"))
    # A later observation must not extend the earlier snapshot's event cursor.
    repo.save_transfers((replace(old, transfer_id="future", ts=now + timedelta(seconds=1)),))
    repo.save_futures_income((
        FuturesIncome("TRANSFER", "old", "USDT", D("2000"), old.ts),
        FuturesIncome("TRANSFER", "settled", "USDT", D("3000"), now - timedelta(hours=2)),
        FuturesIncome("REALIZED_PNL", "loss", "USDT", D("-1000"), now - timedelta(hours=1)),
        FuturesIncome("TRANSFER", "future", "USDT", D("2000"), now + timedelta(seconds=1)),
    ))
    assert historical_metrics(repo, account, now, "USDT")[1:] == (D("-1000"), None)
    assert account.wallet_event_cursor == 2 and repo.wallet_cursor() == 3
    rolled_back = replace(account, wallet_event_cursor=0)
    assert historical_metrics(repo, rolled_back, now, "USDT")[2] is not None
    repo.close()


def test_foreign_capital_flows_cannot_disappear_when_margin_returns_to_usdt(tmp_path):
    repo = Repository(tmp_path / "margin.db")
    now = make_engine_inputs().now
    baseline = replace(make_account(), ts=now - timedelta(hours=24),
                       contract_quote_balance=D("97000"), contract_total_margin_balance=D("97000"),
                       contract_margin_asset="USDT")
    repo.save_snapshot(baseline)
    credit = TransferRecord("usdc-credit", "USDC", D("3000"), "SPOT", "USDⓈ-M Futures",
                            now - timedelta(hours=2), OperationStatus.CONFIRMED)
    repo.save_transfers((credit, replace(credit, transfer_id="usdc-debit", amount=D("2000"),
                                        from_account="USDⓈ-M Futures", to_account="SPOT", ts=now)))
    # USDC equity was credited, lost 1,000, then withdrawn; endpoints alone match USDT.
    account = replace(baseline, ts=now)
    _, change, issue = historical_metrics(repo, account, now, "USDT")
    assert change is None and "currency conversion" in issue["reason"]
    repo.close()


def test_foreign_income_still_blocks_when_the_quote_wallet_version_is_unchanged(tmp_path):
    repo = Repository(tmp_path / "margin.db")
    now = make_engine_inputs().now
    baseline = replace(
        make_account(), ts=now - timedelta(hours=24),
        contract_quote_updated_at=now - timedelta(hours=24),
        contract_quote_balance=D("97000"), contract_total_margin_balance=D("97000"),
        contract_margin_asset="USDT",
    )
    repo.save_snapshot(baseline)
    repo.save_futures_income((FuturesIncome("TRANSFER", "foreign-income", "USDC", D("3000"), now),))
    account = replace(baseline, ts=now, wallet_event_cursor=repo.wallet_cursor())
    _, change, issue = historical_metrics(repo, account, now, "USDT")
    assert change is None and "currency conversion" in issue["reason"]
    repo.close()


def test_missing_current_margin_is_distinct_from_history_warmup(tmp_path):
    repo = Repository(tmp_path / "margin.db")
    now = make_engine_inputs().now
    baseline = replace(make_account(), ts=now - timedelta(hours=24),
                       contract_quote_balance=D("97000"), contract_total_margin_balance=D("97000"),
                       contract_margin_asset="USDT")
    repo.save_snapshot(baseline)
    _, change, issue = historical_metrics(
        repo, replace(baseline, ts=now, contract_total_margin_balance=None), now, "USDT"
    )
    assert change is None and "missing" in issue["reason"]
    repo.close()


@pytest.mark.parametrize("corrupt_baseline", [False, True])
def test_late_capital_already_in_baseline_is_not_deducted_twice(tmp_path, corrupt_baseline):
    path = tmp_path / "margin.db"
    repo = Repository(path)
    now = make_engine_inputs().now
    anchor = replace(
        make_account(), ts=now - timedelta(hours=25),
        contract_quote_balance=D("97000"), contract_total_margin_balance=D("97000"),
        contract_quote_updated_at=now - timedelta(hours=25), contract_margin_asset="USDT",
    )
    baseline = replace(anchor, ts=now - timedelta(hours=24),
                       contract_quote_updated_at=now - timedelta(hours=24, seconds=1))
    if corrupt_baseline:
        baseline = replace(baseline, contract_quote_balance=D("97001"))
    repo.save_snapshot(anchor)
    repo.save_snapshot(baseline)
    # Both snapshots predate publication of the confirmed transfer journal.
    repo.save_transfers((TransferRecord(
        "late", "USDT", D("3000"), "SPOT", "USDⓈ-M Futures",
        anchor.ts + timedelta(minutes=1), OperationStatus.CONFIRMED,
    ),))
    repo.save_futures_income((
        FuturesIncome("TRANSFER", "late-income", "USDT", D("3000"), baseline.contract_quote_updated_at),
        FuturesIncome("REALIZED_PNL", "old-loss", "USDT", D("-3000"), baseline.contract_quote_updated_at),
        FuturesIncome("REALIZED_PNL", "new-loss", "USDT", D("-1000"), now),
    ))
    account = replace(baseline, ts=now, contract_quote_updated_at=now,
                      contract_quote_balance=baseline.contract_quote_balance - D("1000"),
                      contract_total_margin_balance=D("96000"))
    repo.close()
    repo = Repository(path)
    _, change, issue = historical_metrics(repo, account, now, "USDT")
    if corrupt_baseline:
        # The short wallet delta agrees, but the earlier checkpoint disproves it.
        assert change is None and issue is not None
    else:
        assert change == D("-1000") and issue is None
    repo.close()


@pytest.mark.parametrize("late_before_baseline", [False, True])
@pytest.mark.parametrize("late_foreign_receipts", [False, True])
def test_replay_proof_does_not_expand_the_strategy_currency_interval(
    tmp_path, late_before_baseline, late_foreign_receipts
):
    repo = Repository(tmp_path / "margin.db")
    now = make_engine_inputs().now
    old = replace(
        make_account(), ts=now - timedelta(hours=48),
        contract_quote_updated_at=now - timedelta(hours=48),
        contract_quote_balance=D("97000"), contract_total_margin_balance=D("97000"),
        contract_margin_asset="USDT",
    )
    repo.save_snapshot(old)
    credit = TransferRecord("old-usdc-in", "USDC", D("3000"), "SPOT", "USDⓈ-M Futures",
                            now - timedelta(hours=27), OperationStatus.CONFIRMED)
    debit = replace(credit, transfer_id="old-usdc-out", from_account="USDⓈ-M Futures",
                    to_account="SPOT", ts=now - timedelta(hours=26))
    if not late_foreign_receipts:
        repo.save_transfers((credit, debit))
    for hours in (25, 24):
        repo.save_snapshot(replace(old, ts=now - timedelta(hours=hours),
                                   contract_quote_updated_at=now - timedelta(hours=hours)))
    if late_foreign_receipts:
        repo.save_transfers((credit, debit))
    repo.save_transfers((TransferRecord(
        "new", "USDT", D("3000"), "SPOT", "USDⓈ-M Futures",
        now - timedelta(hours=25, minutes=30), OperationStatus.CONFIRMED,
    ),))
    receipts = [
        FuturesIncome("TRANSFER", "usdc-in", "USDC", D("3000"), credit.ts),
        FuturesIncome("TRANSFER", "usdc-out", "USDC", D("-3000"), debit.ts),
    ]
    for hours in (25, 24):
        receipts.extend((
            FuturesIncome("FUNDING_FEE", str(hours), "USDT", D("1"), now - timedelta(hours=hours)),
            FuturesIncome("COMMISSION", str(hours), "USDT", D("-1"), now - timedelta(hours=hours)),
        ))
    settled_at = now - timedelta(hours=24, seconds=1) if late_before_baseline else now
    receipts.append(FuturesIncome("TRANSFER", "new-income", "USDT", D("3000"), settled_at))
    if late_before_baseline:
        receipts.extend((
            FuturesIncome("REALIZED_PNL", "old-loss", "USDT", D("-3000"), settled_at),
            FuturesIncome("REALIZED_PNL", "loss", "USDT", D("-1000"), now),
        ))
    repo.save_futures_income(receipts)
    samples = repo.snapshots_since(now - timedelta(hours=49))
    before = samples[-1]
    balance = D("96000") if late_before_baseline else D("100000")
    account = replace(before, ts=now, contract_quote_updated_at=now,
                      contract_quote_balance=balance, contract_total_margin_balance=balance,
                      wallet_event_cursor=repo.wallet_cursor())
    valid, anchor, evidence = futures_settlement_evidence(repo, before, account, "USDT", samples=samples)
    assert valid and not evidence["foreign_transfer_assets"]
    anchor_hours = 48 if late_foreign_receipts else 25 if late_before_baseline else 24
    assert anchor.ts == now - timedelta(hours=anchor_hours)
    expected = D("-1000") if late_before_baseline else D("0")
    assert historical_metrics(repo, account, now, "USDT", samples=samples)[1:] == (expected, None)
    repo.close()


@pytest.mark.parametrize("proof", ["complete", "missing_anchor", "missing_credit", "missing_debit", "mismatched"])
def test_late_foreign_receipts_require_the_original_signed_flow_proof(tmp_path, proof):
    repo = Repository(tmp_path / "margin.db")
    now = make_engine_inputs().now
    baseline = replace(
        make_account(), ts=now - timedelta(hours=24),
        contract_quote_updated_at=now - timedelta(hours=24),
        contract_quote_balance=D("97000"), contract_total_margin_balance=D("97000"),
        contract_margin_asset="USDT",
    )
    if proof != "missing_anchor":
        repo.save_snapshot(replace(baseline, ts=now - timedelta(hours=48),
                                   contract_quote_updated_at=now - timedelta(hours=48)))
    repo.save_snapshot(baseline)
    credit = TransferRecord("late-in", "USDC", D("3000"), "SPOT", "USDⓈ-M Futures",
                            now - timedelta(hours=27), OperationStatus.CONFIRMED)
    debit = replace(credit, transfer_id="late-out", from_account="USDⓈ-M Futures",
                    to_account="SPOT", ts=now - timedelta(hours=26))
    repo.save_transfers((credit, debit))
    receipts = [
        FuturesIncome("FUNDING_FEE", "funding", "USDT", D("1"), now),
        FuturesIncome("COMMISSION", "fee", "USDT", D("-1"), now),
    ]
    if proof != "missing_credit":
        receipts.append(FuturesIncome("TRANSFER", "late-in", "USDC", D("3000"), credit.ts))
    if proof != "missing_debit":
        receipts.append(FuturesIncome("TRANSFER", "late-out", "USDC",
                                      D("-2000" if proof == "mismatched" else "-3000"), debit.ts))
    repo.save_futures_income(receipts)
    samples = repo.snapshots_since(now - timedelta(hours=49))
    before = samples[-1]
    account = replace(before, ts=now, contract_quote_updated_at=now,
                      wallet_event_cursor=repo.wallet_cursor())
    valid, _, _ = futures_settlement_evidence(repo, before, account, "USDT", samples=samples)
    _, change, issue = historical_metrics(repo, account, now, "USDT", samples=samples)
    assert valid == (proof == "complete")
    if proof == "complete":
        assert change == D("0") and issue is None
    else:
        assert change is None and issue is not None
    repo.close()
