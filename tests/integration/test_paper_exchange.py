from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

import pytest

from core.config import load_paper_config, validate_paper_config
from core.models import AssetTransferPlan, CancelPlan, Operation, OperationKind, OperationStatus, PaperConfig
from services.exchange_adapter import RequestRejected
from storage.paper_ledger import PAPER_ENVIRONMENT
from storage.repository import Repository
from tests.helpers import D
from tests.paper_helpers import order_operation, paper_setup


@pytest.fixture
def paper(tmp_path):
    p = paper_setup(tmp_path / "paper.db")
    yield p
    p.repo.close()


def submit_transfer(p, asset, source, destination, amount):
    op = Operation("bt-" + uuid4().hex, OperationKind.TRANSFER, "transfer", AssetTransferPlan(
        asset, D(amount), source, destination, "test"), p.clock.utc())
    return op, p.ex.submit(op)


def test_bootstrap_isolated_identity_and_explicit_defaults(paper):
    assert paper.ex.account_identity()["environment"] == PAPER_ENVIRONMENT
    assert paper.repo.load("spot_checkpoint")["account"].spot_usd == D("10000")
    assert paper.repo.db.execute("PRAGMA user_version").fetchone()[0] == 2
    assert load_paper_config("config/paper.toml") == PaperConfig()
    assert not paper.repo.operations()


@pytest.mark.parametrize("field,value", [("initial_spot_bnb", "NaN"), ("spot_fee_rate", "1"),
    ("liquidity_participation", "0"), ("transfer_delay_seconds", "-1"), ("bnb_consumption_per_hour", "Infinity")])
def test_invalid_paper_parameters_are_rejected(field, value):
    with pytest.raises(ValueError):
        validate_paper_config(replace(PaperConfig(), **{field: D(value)}))


def test_maker_crossing_rejected_without_financial_effect(paper):
    before = paper.ex.fetch_account_snapshot()
    with pytest.raises(RequestRejected, match="immediately match"):
        paper.ex.submit(order_operation(paper.clock, price="600"))
    after = paper.ex.fetch_account_snapshot()
    assert (after.spot_usd, after.spot_bnb, after.reserved_spot_usd) == (before.spot_usd, before.spot_bnb, D("0"))
    assert not paper.ex.fetch_open_orders("BNBUSDT")


def test_resting_orders_share_quote_by_price_priority_and_survive_restart(paper):
    low = paper.ex.submit(order_operation(paper.clock, price="590"))
    high = paper.ex.submit(order_operation(paper.clock, price="595"))
    paper.clock.elapse(1)
    paper.feed.publish(ask="580")  # 10 × .1 = one shared BNB.
    paper.ex.advance()
    assert paper.ex.fetch_order("BNBUSDT", order_id=high.exchange_id).filled_qty == 1
    assert paper.ex.fetch_order("BNBUSDT", order_id=low.exchange_id).filled_qty == 0
    paper.ex.advance()
    assert len(paper.ex.fetch_recent_fills("BNBUSDT", paper.clock.now - timedelta(hours=1), paper.clock.utc())) == 1
    paper.repo.close()
    recovered = paper_setup(paper.path, config=paper.config, clock=paper.clock, feed=paper.feed)
    try:
        assert recovered.ex.fetch_order("BNBUSDT", order_id=high.exchange_id).filled_qty == 1
        recovered.clock.elapse(1)
        recovered.feed.publish(ask="580")
        recovered.ex.advance()
        assert recovered.ex.fetch_order("BNBUSDT", order_id=high.exchange_id).status == "FILLED"
        assert recovered.ex.fetch_order("BNBUSDT", order_id=low.exchange_id).filled_qty == 0
    finally:
        recovered.repo.close()


@pytest.mark.parametrize("qty,price,expected,status", [("2", "601", "1", "EXPIRED"),
    ("1", "601", "1", "FILLED"), ("2", "590", "0", "EXPIRED")])
def test_ioc_uses_one_quote_and_keeps_terminal_filled_quantity(paper, qty, price, expected, status):
    result = paper.ex.submit(order_operation(paper.clock, qty=qty, price=price, order_type="LIMIT", tif="IOC"))
    order = paper.ex.fetch_order("BNBUSDT", order_id=result.exchange_id)
    assert result.status == OperationStatus.CONFIRMED
    assert order.status == status and order.filled_qty == D(expected)
    a = paper.ex.fetch_account_snapshot()
    assert a.reserved_spot_usd == 0 and a.spot_usd == D("10000") - D(expected) * 600
    assert a.spot_bnb == D("0.5") + D(expected) * D("0.999")
    second = paper.ex.submit(order_operation(paper.clock, price="601", order_type="LIMIT", tif="IOC"))
    # The first non-crossing IOC consumed nothing; crossing orders consume the shared remainder.
    assert paper.ex.fetch_order("BNBUSDT", order_id=second.exchange_id).filled_qty == (1 if expected == "0" else 0)


def test_gtc_immediate_price_improvement_then_resting_limit_fill(paper):
    op = order_operation(paper.clock, price="605", order_type="LIMIT")
    result = paper.ex.submit(op)
    a = paper.ex.fetch_account_snapshot()
    assert a.spot_usd == 9400 and a.reserved_spot_usd == 605
    paper.clock.elapse(1)
    paper.feed.publish(ask="590")
    paper.ex.advance()
    a = paper.ex.fetch_account_snapshot()
    fills = paper.ex.fetch_recent_fills("BNBUSDT", op.created_at, paper.clock.utc())
    assert tuple(f.quote_qty for f in fills) == (D("600"), D("605"))
    assert a.spot_usd == 8795 and a.spot_bnb == D("2.498") and a.reserved_spot_usd == 0


def test_partial_fill_below_order_minimum_and_cancel_race(paper):
    paper.feed.publish(ask="11", qty="100")
    result = paper.ex.submit(order_operation(paper.clock, price="10", qty="1"))
    paper.clock.elapse(1)
    paper.feed.publish(ask="9", qty="0.1")  # .01 × 10 = .1, below the order minimum notional.
    cancel = Operation("bt-cancel", OperationKind.CANCEL, "cancel", CancelPlan(
        "BNBUSDT", result.exchange_id, "test"), paper.clock.utc())
    paper.ex.submit(cancel)  # New quote is processed before cancellation.
    order = paper.ex.fetch_order("BNBUSDT", order_id=result.exchange_id)
    assert order.status == "CANCELED" and order.filled_qty == D("0.01")
    assert paper.ex.fetch_account_snapshot().reserved_spot_usd == 0


@pytest.mark.parametrize("bad", ["missing_qty", "nan", "future", "stale", "reversed", "disconnected"])
def test_invalid_or_missing_quote_never_fills_but_timers_advance(paper, bad):
    result = paper.ex.submit(order_operation(paper.clock))
    paper.clock.elapse(1)
    paper.feed.publish(ask="580")
    q = paper.feed.book
    q = {"missing_qty": replace(q, ask_qty=None), "nan": replace(q, ask_qty=D("NaN")),
         "future": replace(q, ts=paper.clock.now + timedelta(seconds=1)),
         "stale": replace(q, ts=paper.clock.now - timedelta(seconds=6)),
         "reversed": replace(q, best_bid=D("590")), "disconnected": None}[bad]
    paper.feed.book = q
    paper.ex.advance()
    assert paper.ex.fetch_order("BNBUSDT", order_id=result.exchange_id).filled_qty == 0
    assert paper.ex.ledger.state()["active_seconds"] == 1


@pytest.mark.parametrize("asset,source,dest,amount", [
    ("USDT", "USDⓈ-M Futures", "SPOT", "500"),
    ("USDT", "SPOT", "USDⓈ-M Futures", "500"),
    ("BNB", "SPOT", "USDⓈ-M Futures", "0.4"),
])
def test_transfer_reserves_then_settles_both_wallets_and_income(paper, asset, source, dest, amount):
    before = paper.ex.fetch_account_snapshot()
    op, result = submit_transfer(paper, asset, source, dest, amount)
    held = paper.ex.fetch_account_snapshot()
    assert result.status == OperationStatus.PENDING
    assert (held.spot_usd, held.spot_bnb, held.contract_bnb, held.contract_quote_balance) == (
        before.spot_usd, before.spot_bnb, before.contract_bnb, before.contract_quote_balance)
    if source != "SPOT":
        assert held.contract_max_withdraw_amount == before.contract_max_withdraw_amount - D(amount)
        assert held.contract_available_balance == before.contract_available_balance
    else:
        assert (held.reserved_spot_bnb if asset == "BNB" else held.reserved_spot_usd) == D(amount)
    paper.clock.elapse(1)
    paper.ex.advance()
    assert paper.ex.query_operation(replace(op, exchange_id=result.exchange_id)).status == OperationStatus.PENDING
    paper.clock.elapse(1)
    paper.ex.advance()
    receipt = paper.ex.query_operation(replace(op, exchange_id=result.exchange_id))
    assert receipt.status == OperationStatus.CONFIRMED and receipt.transfer.ts == result.transfer.ts
    after = paper.ex.fetch_account_snapshot()
    assert after.spot_usd + after.contract_quote_balance == before.spot_usd + before.contract_quote_balance
    assert after.spot_bnb + after.contract_bnb == before.spot_bnb + before.contract_bnb
    assert not after.reserved_spot_bnb and not after.reserved_spot_usd
    income = paper.ex.fetch_futures_income(op.created_at, paper.clock.utc())
    assert len(income) == 1 and income[0].income_type == "TRANSFER"
    assert income[0].transaction_id != result.exchange_id
    assert income[0].amount == (D(amount) if dest != "SPOT" else -D(amount))


def test_transfer_has_no_hidden_client_id_deduplication_or_unknown_attribution(paper):
    op, result = submit_transfer(paper, "USDT", "USDⓈ-M Futures", "SPOT", "500")
    duplicate = paper.ex.submit(op)
    assert result.exchange_id != duplicate.exchange_id
    paper.clock.elapse(2)
    paper.ex.advance()
    assert paper.ex.query_operation(op).status == OperationStatus.UNKNOWN
    assert paper.ex.fetch_account_snapshot().spot_usd == 11000


def test_restart_pauses_transfer_timer_and_does_not_reseed(paper):
    op, result = submit_transfer(paper, "USDT", "USDⓈ-M Futures", "SPOT", "500")
    identity = paper.ex.account_identity()
    paper.clock.elapse(1)
    paper.ex.advance()
    paper.repo.close()
    paper.clock.elapse(3600)
    recovered = paper_setup(paper.path, config=paper.config, clock=paper.clock, feed=paper.feed)
    try:
        assert recovered.ex.account_identity() == identity
        assert recovered.ex.query_operation(replace(op, exchange_id=result.exchange_id)).status == OperationStatus.PENDING
        recovered.clock.elapse(1)
        recovered.ex.advance()
        assert recovered.ex.fetch_account_snapshot().spot_usd == 10500
    finally:
        recovered.repo.close()


def test_queries_do_not_advance_time_quotes_or_consume(paper):
    paper.ex.submit(order_operation(paper.clock))
    state = paper.ex.ledger.state()
    paper.clock.elapse(60)
    paper.feed.publish(ask="580")
    for _ in range(3):
        paper.ex.fetch_account_snapshot()
        paper.ex.fetch_open_orders("BNBUSDT")
        paper.ex.fetch_recent_fills("BNBUSDT", state["advanced_at"], paper.clock.utc())
        paper.ex.fetch_recent_transfers(state["advanced_at"], paper.clock.utc())
        paper.ex.fetch_futures_income(state["advanced_at"], paper.clock.utc())
    assert paper.ex.ledger.state() == state


def test_minute_consumption_rounding_exhaustion_and_no_future_debt(tmp_path):
    p = paper_setup(tmp_path / "consume.db", config=replace(PaperConfig(), initial_contract_bnb=D("0.01"),
        initial_spot_bnb=D("1"), bnb_consumption_per_hour=D("1")))
    try:
        p.clock.elapse(59)
        p.ex.advance()
        assert p.ex.fetch_account_snapshot().contract_bnb == D("0.01")
        p.clock.elapse(1)
        p.ex.advance()
        assert p.ex.fetch_account_snapshot().contract_bnb == 0
        assert p.ex.ledger.state()["unmet_consumption_bnb"] == D("0.00666666")
        op, result = submit_transfer(p, "BNB", "SPOT", "USDⓈ-M Futures", "1")
        p.clock.elapse(2)
        p.ex.advance()
        assert p.ex.fetch_account_snapshot().contract_bnb == 1
        p.clock.elapse(58)
        p.ex.advance()
        assert p.ex.fetch_account_snapshot().contract_bnb == D("0.98333333")
        assert len(p.ex.fetch_futures_income(op.created_at - timedelta(hours=1), p.clock.utc())) == 3
    finally:
        p.repo.close()


def test_consumption_exact_hour_and_remainder_survive_restart(tmp_path):
    p = paper_setup(tmp_path / "hour.db", config=PaperConfig())
    try:
        for minute in range(60):
            p.clock.elapse(60)
            p.ex.advance()
            if minute == 29:
                p.repo.close()
                p = paper_setup(p.path, config=p.config, clock=p.clock, feed=p.feed)
        assert p.ex.ledger.state()["consumed_bnb"] == D("0.04")
        assert p.ex.fetch_account_snapshot().contract_bnb == D("23.96")
        assert p.ex.ledger.state()["consumption_remainder"] == 0
    finally:
        p.repo.close()


def test_parameter_change_and_missing_baseline_reject_without_repair(paper):
    paper.repo.close()
    with pytest.raises(ValueError, match="parameters changed"):
        paper_setup(paper.path, config=replace(paper.config, spot_fee_rate=D("0")), clock=paper.clock)
    repo = Repository(paper.path)
    repo.db.execute("DELETE FROM kv WHERE key='spot_checkpoint'")
    repo.db.commit()
    repo.close()
    with pytest.raises(ValueError, match="baseline"):
        paper_setup(paper.path, config=paper.config, clock=paper.clock)
