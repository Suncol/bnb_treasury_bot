from dataclasses import replace
from datetime import datetime, timezone
import json
from urllib.parse import parse_qs, urlsplit

import pytest
import requests

from core.models import (
    AssetTransferPlan,
    CancelPlan,
    Operation,
    OperationKind,
    OperationStatus,
    OrderPlan,
    ReplenishmentState,
    RunMode,
)
from services.binance_adapter import BinanceAdapter, millis
from services.exchange_adapter import ExchangeError, RequestRejected
from services.executor import Executor
from services.reconciliation import Reconciler
from storage.repository import Repository
from tests.fake_exchange import FakeExchange
from tests.helpers import D, make_strategy_config

NOW = datetime(2026, 3, 31, tzinfo=timezone.utc)


@pytest.fixture
def sdk(monkeypatch):
    calls = []
    responses = {}

    def request(self, **kwargs):
        calls.append(kwargs)
        key = (kwargs["method"], urlsplit(kwargs["url"]).path)
        value = responses[key]
        if callable(value):
            value = value(kwargs)
        if isinstance(value, Exception):
            raise value
        status, data = value
        response = requests.Response()
        response.status_code = status
        response.headers["Content-Type"] = "application/json"
        response._content = json.dumps(data).encode()
        return response

    monkeypatch.setattr(requests.Session, "request", request)
    adapter = BinanceAdapter(
        "fixture-key",
        "fixture-secret",
        make_strategy_config(),
        live=True,
        clock=lambda: NOW,
    )
    return adapter, calls, responses


def operation(kind, payload):
    return Operation("bt-01234567890123456789012345678901", kind, "scope", payload, NOW)


def test_official_sdk_serializes_decimal_strings_and_maker_omits_tif(sdk):
    adapter, calls, responses = sdk
    responses["POST", "/api/v3/order"] = (200, {"orderId": 123, "status": "NEW"})
    payload = OrderPlan(
        "BNBUSDT", "BUY", "LIMIT_MAKER", D("1.23000000"), D("594.01000000"), "GTC", True
    )
    result = adapter.submit(operation(OperationKind.ORDER, payload))
    assert result.status == OperationStatus.CONFIRMED and result.exchange_id == "123"
    params = parse_qs(calls[0]["params"])
    assert params["quantity"] == ["1.23000000"] and params["price"] == ["594.01000000"]
    assert "timeInForce" not in params
    assert params["newClientOrderId"] == ["bt-01234567890123456789012345678901"]
    assert (
        "signature" in params and calls[0]["headers"]["X-MBX-APIKEY"] == "fixture-key"
    )
    assert all(
        api.configuration.retries == 0
        for api in (adapter.spot, adapter.futures, adapter.wallet)
    )


@pytest.mark.parametrize(
    "kind,path,payload",
    [
        (
            OperationKind.ORDER,
            "/api/v3/order",
            OrderPlan("BNBUSDT", "BUY", "LIMIT", D("1"), D("600"), "IOC", False),
        ),
        (OperationKind.CANCEL, "/api/v3/order", CancelPlan("BNBUSDT", "123", "test")),
        (
            OperationKind.TRANSFER,
            "/sapi/v1/asset/transfer",
            AssetTransferPlan("USDT", D("500"), "USDⓈ-M Futures", "SPOT", "test"),
        ),
    ],
)
@pytest.mark.parametrize("failure", ["timeout", "5xx", "5xx_rejection", "unknown"])
def test_sdk_never_retries_mutations(sdk, kind, path, payload, failure):
    adapter, calls, responses = sdk
    method = "DELETE" if kind == OperationKind.CANCEL else "POST"
    responses[method, path] = {
        "timeout": requests.Timeout(),
        "5xx": (503, {"msg": "unknown"}),
        "5xx_rejection": (503, {
            "code": -2010, "msg": "Account has insufficient balance for requested action."
        }),
        "unknown": (400, {"code": -1007, "msg": "unknown execution"}),
    }[failure]
    with pytest.raises(ExchangeError) as error:
        adapter.submit(operation(kind, payload))
    assert not isinstance(error.value, RequestRejected)
    assert len(calls) == 1


def test_sdk_unknown_order_remains_unknown_and_transfer_never_sends_fake_client_id(sdk):
    adapter, calls, responses = sdk
    payload = OrderPlan("BNBUSDT", "BUY", "LIMIT", D("1"), D("600"), "IOC", False)
    responses["GET", "/api/v3/order"] = (
        400,
        {"code": -2013, "msg": "Order does not exist"},
    )
    assert (
        adapter.query_operation(operation(OperationKind.ORDER, payload)).status
        == OperationStatus.UNKNOWN
    )
    transfer = operation(
        OperationKind.TRANSFER,
        AssetTransferPlan("USDT", D("500.00"), "USDⓈ-M Futures", "SPOT", "test"),
    )
    responses["POST", "/sapi/v1/asset/transfer"] = (200, {"tranId": 456})
    result = adapter.submit(transfer)
    params = parse_qs(calls[-1]["params"])
    assert params["type"] == ["UMFUTURE_MAIN"] and params["amount"] == ["500.00"]
    assert "newClientOrderId" not in params and "clientTranId" not in params
    assert result.status == OperationStatus.PENDING
    count = len(calls)
    assert adapter.query_operation(transfer).status == OperationStatus.UNKNOWN
    assert len(calls) == count


def test_account_uses_asset_max_withdraw_and_total_spot_balances(sdk):
    adapter, calls, responses = sdk
    responses["GET", "/api/v3/time"] = responses["GET", "/fapi/v1/time"] = (
        200,
        {"serverTime": millis(NOW)},
    )
    responses["GET", "/fapi/v3/balance"] = (
        200,
        [
            {"asset": "BNB", "balance": "25.75", "updateTime": millis(NOW)},
            {
                "asset": "USDT",
                "balance": "10000",
                "availableBalance": "9999",
                "maxWithdrawAmount": "4567.89",
                "updateTime": millis(NOW),
            },
        ],
    )
    responses["GET", "/fapi/v3/account"] = (200, {"totalMarginBalance": "12345"})
    responses["GET", "/api/v3/account"] = (
        200,
        {
            "balances": [
                {"asset": "BNB", "free": "2", "locked": "1"},
                {"asset": "USDT", "free": "500", "locked": "100"},
            ]
        },
    )
    account = adapter.fetch_account_snapshot()
    assert account.contract_max_withdraw_amount == D("4567.89")
    assert account.contract_bnb == D("25.75")
    assert account.contract_quote_balance == D("10000")
    assert account.contract_bnb_updated_at == account.contract_quote_updated_at == NOW
    assert account.spot_usd == D("600") and account.reserved_spot_usd == D("100")
    assert account.spot_bnb == D("3") and account.reserved_spot_bnb == D("1")


def test_clock_skew_blocks_signed_reads_before_any_trade(sdk):
    adapter, calls, responses = sdk
    responses["GET", "/api/v3/time"] = (200, {"serverTime": millis(NOW) + 2000})
    with pytest.raises(ExchangeError, match="clock"):
        adapter.fetch_account_snapshot()
    assert len(calls) == 1


def test_transfer_history_is_paginated_and_confirmed_by_id(sdk):
    adapter, calls, responses = sdk

    def page(kwargs):
        params = parse_qs(kwargs["params"])
        if params["type"] == ["MAIN_UMFUTURE"]:
            return 200, {"rows": [], "total": 0}
        number = int(params["current"][0])
        return 200, {
            "total": 2,
            "rows": [
                {
                    "tranId": number,
                    "asset": "USDT",
                    "amount": "500",
                    "type": "UMFUTURE_MAIN",
                    "status": "CONFIRMED",
                    "timestamp": millis(NOW),
                }
            ],
        }

    responses["GET", "/sapi/v1/asset/transfer"] = page
    op = operation(
        OperationKind.TRANSFER,
        AssetTransferPlan("USDT", D("500"), "USDⓈ-M Futures", "SPOT", "test"),
    )
    assert (
        adapter.query_operation(replace(op, exchange_id="2")).status
        == OperationStatus.CONFIRMED
    )
    assert len(calls) == 3
    assert (
        adapter.query_operation(replace(op, exchange_id="3")).status
        == OperationStatus.UNKNOWN
    )


def test_sdk_filters_and_fee_reserve(sdk):
    adapter, calls, responses = sdk
    responses["GET", "/api/v3/exchangeInfo"] = (
        200,
        {
            "symbols": [
                {
                    "symbol": "BNBUSDT",
                    "baseAsset": "BNB",
                    "quoteAsset": "USDT",
                    "status": "TRADING",
                    "filters": [
                        {
                            "filterType": "LOT_SIZE",
                            "stepSize": "0.01",
                            "minQty": "0.01",
                            "maxQty": "1000",
                        },
                        {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                        {
                            "filterType": "NOTIONAL",
                            "minNotional": "5",
                            "maxNotional": "100000",
                        },
                    ],
                }
            ]
        },
    )
    responses["GET", "/sapi/v1/asset/tradeFee"] = (
        200,
        [{"symbol": "BNBUSDT", "makerCommission": "0.001", "takerCommission": "0.001"}],
    )
    filters = adapter.fetch_symbol_filters("BNBUSDT")
    assert filters.qty_step == D("0.01") and filters.max_notional == D("100000")


def test_oneof_market_responses_are_normalized(sdk):
    adapter, calls, responses = sdk
    responses["GET", "/api/v3/ticker/bookTicker"] = (
        200,
        {"symbol": "BNBUSDT", "bidPrice": "599", "askPrice": "601"},
    )
    responses["GET", "/api/v3/avgPrice"] = (
        200,
        {"mins": 5, "price": "600", "closeTime": millis(NOW)},
    )
    responses["GET", "/api/v3/ticker"] = (
        200,
        {"symbol": "BNBUSDT", "priceChangePercent": "10"},
    )
    responses["GET", "/api/v3/ticker/24hr"] = (
        200,
        {"symbol": "BNBUSDT", "priceChangePercent": "-8"},
    )
    market = adapter.fetch_market_snapshot("BNBUSDT")
    assert market.return_1h == D("0.10") and market.return_24h == D("-0.08")
    assert market.mid_price == D("600")


@pytest.mark.parametrize("order_type,tif,message", [
    ("LIMIT_MAKER", "GTC", "Order would immediately match and take."),
    ("LIMIT_MAKER", "GTC", "Account has insufficient balance for requested action."),
    ("LIMIT", "GTC", "Account has insufficient balance for requested action."),
    ("LIMIT", "IOC", "Account has insufficient balance for requested action."),
])
def test_sdk_definite_order_rejection_does_not_block_next_order(
    sdk, tmp_path, order_type, tif, message
):
    adapter, calls, responses = sdk
    responses["POST", "/api/v3/order"] = (400, {"code": -2010, "msg": message})
    payload = OrderPlan(
        "BNBUSDT", "BUY", order_type, D("1"), D("600"), tif, order_type == "LIMIT_MAKER"
    )
    repo = Repository(tmp_path / "sdk.sqlite3")
    executor = Executor(adapter, repo, make_strategy_config())
    rejected = executor.execute(payload, NOW, ReplenishmentState.ACCUMULATE)
    assert rejected.status == OperationStatus.FAILED
    assert "-2010" in rejected.error and message in rejected.error
    assert not repo.operations(unresolved_only=True)
    # A definitive failure is not queried back into UNKNOWN when it is absent.
    exchange = FakeExchange()
    exchange.fetch_order = adapter.fetch_order
    responses["GET", "/api/v3/order"] = (
        400, {"code": -2013, "msg": "Order does not exist."}
    )
    Reconciler(exchange, repo, make_strategy_config()).refresh(NOW)
    assert len(calls) == 1
    responses["POST", "/api/v3/order"] = (200, {"orderId": 123, "status": "NEW"})
    accepted = executor.execute(
        replace(payload, price=D("590")), NOW, ReplenishmentState.ACCUMULATE
    )
    assert accepted.status == OperationStatus.CONFIRMED
    assert accepted.client_id != rejected.client_id
    assert len(calls) == 2


@pytest.mark.parametrize("code,message", [
    (-2010, "Duplicate order sent."),
    (-2010, "Unknown rejection reason"),
    (-1007, "Timeout waiting for response from backend server. "
     "Send status unknown; execution status unknown."),
])
def test_ambiguous_sdk_rejections_and_absence_keep_journal_unresolved(
    sdk, tmp_path, code, message
):
    adapter, calls, responses = sdk
    responses["POST", "/api/v3/order"] = (400, {"code": code, "msg": message})
    responses["GET", "/api/v3/order"] = (
        400, {"code": -2013, "msg": "Order does not exist."}
    )
    payload = OrderPlan("BNBUSDT", "BUY", "LIMIT_MAKER", D("1"), D("600"), "GTC", True)
    repo = Repository(tmp_path / "sdk.sqlite3")
    executor = Executor(adapter, repo, make_strategy_config())
    unresolved = executor.execute(payload, NOW, ReplenishmentState.ACCUMULATE)
    assert unresolved.status == OperationStatus.UNKNOWN
    assert adapter.query_operation(unresolved).status == OperationStatus.UNKNOWN
    with pytest.raises(RuntimeError, match="reconciled"):
        executor.execute(payload, NOW, ReplenishmentState.ACCUMULATE)
    assert len(calls) == 2


def test_definite_transfer_limit_rejection_can_reconcile_and_counts_failures(sdk, tmp_path):
    adapter, calls, responses = sdk
    responses["POST", "/sapi/v1/asset/transfer"] = (
        400, {"code": -3020, "msg": "Transfer out amount exceeds max amount."}
    )
    repo = Repository(tmp_path / "sdk.sqlite3")
    executor = Executor(adapter, repo, make_strategy_config())
    payload = AssetTransferPlan("USDT", D("500"), "USDⓈ-M Futures", "SPOT", "fund")
    for attempt in range(2):
        op = executor.execute(
            payload, NOW, ReplenishmentState.URGENT, account=FakeExchange().fetch_account_snapshot()
        )
        assert op.status == OperationStatus.FAILED and "-3020" in op.error
        assert not repo.operations(unresolved_only=True)
        assert not repo.runtime().resume_replenishment
        Reconciler(FakeExchange(), repo, make_strategy_config()).refresh(NOW)
        assert len(calls) == attempt + 1  # No retry or lookup of a rejected transfer.
    assert repo.runtime().run_mode == RunMode.PAUSED
    repo.close()


@pytest.mark.parametrize("status,code", [(503, -3020), (400, -5012), (400, -3029)])
def test_uncertain_transfer_responses_keep_the_gate_closed(sdk, tmp_path, status, code):
    adapter, calls, responses = sdk
    responses["POST", "/sapi/v1/asset/transfer"] = (
        status, {"code": code, "msg": "transfer response"}
    )
    repo = Repository(tmp_path / "sdk.sqlite3")
    executor = Executor(adapter, repo, make_strategy_config())
    payload = AssetTransferPlan("USDT", D("500"), "USDⓈ-M Futures", "SPOT", "fund")
    op = executor.execute(
        payload, NOW, ReplenishmentState.URGENT, account=FakeExchange().fetch_account_snapshot()
    )
    assert op.status == OperationStatus.UNKNOWN
    assert adapter.query_operation(op).status == OperationStatus.UNKNOWN
    with pytest.raises(RuntimeError, match="reconciled"):
        executor.execute(payload, NOW, ReplenishmentState.URGENT)
    assert len(calls) == 1
    repo.close()


@pytest.mark.parametrize("changes", [
    {}, {"orderId": 124}, {"origQty": "2"}, {"price": "590"},
    {"origQty": "0"}, {"executedQty": "2"}, {"side": "SELL"},
])
def test_order_recovery_uses_known_exchange_id_and_checks_identity(sdk, changes):
    adapter, calls, responses = sdk
    payload = OrderPlan("BNBUSDT", "BUY", "LIMIT", D("1"), D("600"), "GTC", False)
    op = replace(operation(OperationKind.ORDER, payload), exchange_id="123")
    responses["GET", "/api/v3/order"] = (200, {
        "symbol": "BNBUSDT", "orderId": 123, "clientOrderId": "cancel-id",
        "price": "600", "origQty": "1", "executedQty": "0", "time": millis(NOW),
        "side": "BUY", "status": "CANCELED", "timeInForce": "GTC", **changes,
    })
    result = adapter.query_operation(op)
    assert result.status == (OperationStatus.UNKNOWN if changes else OperationStatus.CONFIRMED)
    params = parse_qs(calls[0]["params"])
    assert params["orderId"] == ["123"] and "origClientOrderId" not in params


def test_trade_ledger_preserves_actual_quote_cost_and_commission(sdk):
    adapter, calls, responses = sdk
    responses["GET", "/api/v3/myTrades"] = (200, [{
        "symbol": "BNBUSDT", "id": 7, "orderId": 123, "time": millis(NOW),
        "price": "599.99", "qty": "0.123", "quoteQty": "73.79877",
        "commission": "0.00009225", "commissionAsset": "BNB",
        "isBuyer": True, "isMaker": True, "isBestMatch": True,
    }])
    fill, = adapter.fetch_recent_fills("BNBUSDT", NOW, NOW)
    assert fill.qty == D("0.123") and fill.quote_qty == D("73.79877")
    assert fill.commission == D("0.00009225") and fill.commission_asset == "BNB"
    assert len(calls) == 1


def test_sdk_recovers_quantity_reduction_and_changed_client_id(sdk):
    adapter, calls, responses = sdk
    payload = OrderPlan("BNBUSDT", "BUY", "LIMIT", D("2"), D("600"), "GTC", False)
    op = replace(operation(OperationKind.ORDER, payload), exchange_id="123")
    responses["GET", "/api/v3/order"] = (200, {
        "symbol": "BNBUSDT", "orderId": 123, "clientOrderId": "amended-id",
        "price": "600", "origQty": "1", "executedQty": "0.25", "time": millis(NOW),
        "side": "BUY", "status": "PARTIALLY_FILLED", "timeInForce": "GTC",
    })
    assert adapter.query_operation(op).status == OperationStatus.CONFIRMED
    # Without a bound exchange identity, matching a reused client ID is insufficient.
    responses["GET", "/api/v3/order"][1]["clientOrderId"] = op.client_id
    assert adapter.query_operation(replace(op, exchange_id=None)).status == OperationStatus.UNKNOWN
