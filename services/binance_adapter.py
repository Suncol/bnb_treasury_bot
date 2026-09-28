from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import time
from binance_common.configuration import ConfigurationRestAPI
from binance_common.errors import BadRequestError, Error as SDKError, UnauthorizedError
from binance_sdk_spot.spot import Spot
from binance_sdk_wallet.wallet import Wallet
from binance_sdk_derivatives_trading_usds_futures.derivatives_trading_usds_futures import (
    DerivativesTradingUsdsFutures,
)

from core.models import (
    AccountSnapshot,
    Fill,
    MarketSnapshot,
    OperationKind,
    OperationResult,
    OperationStatus,
    OrderView,
    SymbolFilters,
    TransferRecord,
)
from core.time_utils import utc
from .exchange_adapter import ExchangeError, RequestRejected, order_matches_operation

D = Decimal


def millis(value):
    return int(utc(value).timestamp() * 1000)


def timestamp(value):
    return datetime.fromtimestamp(int(value) / 1000, timezone.utc)


class BinanceAPIError(ExchangeError):
    def __init__(self, code, *, message="", definitive=False):
        self.code, self.definitive = code, definitive
        self.message = message
        super().__init__(f"Binance SDK error code {code}: {message}")


def sdk_data(value):
    """Normalize SDK models (including oneOf/list responses) at this boundary."""
    if isinstance(value, (tuple, list)):
        return [sdk_data(item) for item in value]
    if hasattr(value, "to_dict"):
        return sdk_data(value.to_dict())
    if isinstance(value, dict):
        return {key: sdk_data(item) for key, item in value.items()}
    return value


class BinanceAdapter:
    """Official binance-connector-python modular SDKs, with retries disabled."""

    def __init__(
        self,
        api_key,
        api_secret,
        cfg,
        *,
        live=False,
        clients=None,
        clock=None,
        feed=None,
        min_transfer_bnb=D("0.1"),
        min_transfer_usd=D("0"),
    ):
        self.cfg, self.live, self.feed = cfg, live, feed
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.min_transfer_bnb, self.min_transfer_usd = (
            min_transfer_bnb,
            min_transfer_usd,
        )
        self.cache = {}
        if clients is None:

            def config():
                return ConfigurationRestAPI(
                    api_key=api_key,
                    api_secret=api_secret,
                    timeout=5000,
                    retries=0,
                    backoff=0,
                )

            self.spot = Spot(config_rest_api=config()).rest_api
            self.futures = DerivativesTradingUsdsFutures(
                config_rest_api=config()
            ).rest_api
            self.wallet = Wallet(config_rest_api=config()).rest_api
        else:
            self.spot, self.futures, self.wallet = clients

    @staticmethod
    def _call(method, **params):
        try:
            return sdk_data(method(**params).data())
        except SDKError as exc:
            # SDK status_code contains the Binance code on 4xx responses.
            raise BinanceAPIError(
                getattr(exc, "status_code", None),
                message=getattr(exc, "error_message", ""),
                definitive=isinstance(exc, (BadRequestError, UnauthorizedError)),
            ) from None
        except Exception as exc:
            raise ExchangeError(
                f"SDK transport or response failure: {type(exc).__name__}"
            ) from None

    def _check_clock(self):
        def check():
            for method in (self.spot.time, self.futures.check_server_time):
                before = millis(self.clock())
                server = self._call(method)["serverTime"]
                after = millis(self.clock())
                if abs(int(server) - (before + after) // 2) > 1000:
                    raise ExchangeError(
                        "System clock differs from Binance; synchronize it before trading"
                    )
            return True

        self._cached("clock", 1800, check)

    def _cached(self, key, seconds, fetch):
        cached = self.cache.get(key)
        if cached is None or time.monotonic() - cached[0] >= seconds:
            self.cache[key] = (time.monotonic(), fetch())
        return self.cache[key][1]

    def fetch_account_snapshot(self):
        self._check_clock()
        started = self.clock()
        futures = self._call(self.futures.futures_account_balance_v3, recv_window=5000)
        margin = self._call(self.futures.account_information_v3, recv_window=5000)
        spot = self._call(self.spot.get_account, recv_window=5000)
        assets = {a["asset"]: a for a in futures}
        balances = {a["asset"]: a for a in spot["balances"]}
        quote = assets.get(self.cfg.quote_asset)
        if quote is None or "maxWithdrawAmount" not in quote:
            raise ExchangeError("Quote asset does not expose futures maxWithdrawAmount")
        bnb = balances.get("BNB", {"free": "0", "locked": "0"})
        usd = balances.get(self.cfg.quote_asset, {"free": "0", "locked": "0"})
        return AccountSnapshot(
            started,
            D(assets.get("BNB", {}).get("balance", "0")),
            D(quote["maxWithdrawAmount"]),
            D(quote["availableBalance"]),
            D(bnb["free"]) + D(bnb["locked"]),
            D(usd["free"]) + D(usd["locked"]),
            D(usd["locked"]),
            D(margin["totalMarginBalance"]),
            D(bnb["locked"]),
            contract_quote_balance=D(quote["balance"]) if "balance" in quote else None,
            contract_bnb_updated_at=timestamp(assets["BNB"]["updateTime"])
            if assets.get("BNB", {}).get("updateTime") else None,
            contract_quote_updated_at=timestamp(quote["updateTime"])
            if quote.get("updateTime") else None,
        )

    def fetch_symbol_filters(self, symbol):
        def fetch():
            data = self._call(self.spot.exchange_info, symbol=symbol)["symbols"][0]
            if (
                data["status"] != "TRADING"
                or data["baseAsset"] != "BNB"
                or data["quoteAsset"] != self.cfg.quote_asset
            ):
                raise ExchangeError("Symbol is unavailable or has unexpected assets")
            filters = {f["filterType"]: f for f in data["filters"]}
            lot, price = filters["LOT_SIZE"], filters["PRICE_FILTER"]
            minimum = max(
                (
                    D(f["minNotional"])
                    for k, f in filters.items()
                    if k in {"MIN_NOTIONAL", "NOTIONAL"}
                ),
                default=D("0"),
            )
            maximum = D(filters.get("NOTIONAL", {}).get("maxNotional", "0")) or None
            if D(lot["stepSize"]) <= 0 or D(price["tickSize"]) <= 0:
                raise ExchangeError("Unsupported disabled lot or price filter")
            fees = self._call(self.wallet.trade_fee, symbol=symbol, recv_window=5000)
            if (
                max(D(fees[0]["makerCommission"]), D(fees[0]["takerCommission"]))
                > self.cfg.risk.fee_reserve_rate
            ):
                raise ExchangeError(
                    "Configured fee reserve is below the account's trading fee"
                )
            return SymbolFilters(
                D(lot["stepSize"]),
                D(price["tickSize"]),
                D(lot["minQty"]),
                minimum,
                self.min_transfer_bnb,
                self.min_transfer_usd,
                D(lot["maxQty"]),
                maximum,
            )

        return self._cached("filters:" + symbol, 300, fetch)

    def fetch_market_snapshot(self, symbol):
        if self.feed is not None:
            streamed = self.feed.quote()
            quote_ts, bid, ask = streamed.ts, streamed.best_bid, streamed.best_ask
        else:
            quote_ts = self.clock()
            book = self._call(self.spot.ticker_book_ticker, symbol=symbol)
            bid, ask = D(book["bidPrice"]), D(book["askPrice"])
        avg = self._cached(
            "avg:" + symbol, 2, lambda: self._call(self.spot.avg_price, symbol=symbol)
        )
        if avg["mins"] != 5:
            raise ExchangeError("Expected a 5 minute VWAP anchor")
        one = self._cached(
            "1h:" + symbol,
            10,
            lambda: self._call(self.spot.ticker, symbol=symbol, window_size="1h"),
        )
        day = self._cached(
            "24h:" + symbol, 10, lambda: self._call(self.spot.ticker24hr, symbol=symbol)
        )
        market = MarketSnapshot(
            min(utc(quote_ts), timestamp(avg["closeTime"])),
            symbol,
            bid,
            ask,
            (bid + ask) / 2,
            D(avg["price"]),
            D(one["priceChangePercent"]) / 100,
            D(day["priceChangePercent"]) / 100,
        )
        if self.feed is not None:
            market = replace(
                market,
                smooth_price=streamed.smooth_price,
                drawdown_1m=streamed.drawdown_1m,
                drawdown_5m=streamed.drawdown_5m,
                drawdown_15m=streamed.drawdown_15m,
            )
        return market

    @staticmethod
    def _order(row):
        return OrderView(
            row["symbol"],
            str(row["orderId"]),
            row["clientOrderId"],
            None,
            D(row["price"]),
            D(row["origQty"]),
            D(row["executedQty"]),
            timestamp(row["time"]),
            side=row["side"],
            status=row["status"],
            time_in_force=row["timeInForce"],
        )

    def fetch_open_orders(self, symbol):
        return tuple(
            self._order(row)
            for row in self._call(
                self.spot.get_open_orders, symbol=symbol, recv_window=5000
            )
        )

    def fetch_order(self, symbol, *, client_id=None, order_id=None):
        params = {
            "symbol": symbol,
            **(
                {"orig_client_order_id": client_id}
                if client_id
                else {"order_id": int(order_id)}
            ),
        }
        try:
            return self._order(
                self._call(self.spot.get_order, **params, recv_window=5000)
            )
        except BinanceAPIError as exc:
            if exc.code == -2013:
                return None  # Absence is NOT proof that a timed-out submission failed.
            raise

    def fetch_recent_fills(self, symbol, since, until):
        fills = []
        start, end = millis(since), millis(until)
        while start <= end:
            stop = min(start + 86_400_000 - 1, end)
            params = {
                "symbol": symbol,
                "start_time": start,
                "end_time": stop,
                "limit": 1000,
            }
            while True:
                rows = self._call(self.spot.my_trades, **params, recv_window=5000)
                for row in rows:
                    if int(row["time"]) <= stop and row["isBuyer"]:
                        fills.append(
                            Fill(
                                symbol,
                                str(row["id"]),
                                str(row["orderId"]),
                                timestamp(row["time"]),
                                D(row["qty"]),
                                D(row["commission"]),
                                row["commissionAsset"],
                                quote_qty=D(row["quoteQty"]),
                            )
                        )
                if len(rows) < 1000 or int(rows[-1]["time"]) > stop:
                    break
                # fromId pagination also handles >1000 trades in one millisecond.
                next_id = int(rows[-1]["id"]) + 1
                if next_id <= params.get("from_id", -1):
                    raise ExchangeError("Trade pagination made no progress")
                params = {"symbol": symbol, "from_id": next_id, "limit": 1000}
            start = stop + 1
        return tuple(fills)

    def fetch_recent_transfers(self, since, until):
        if utc(since) < utc(self.clock()) - timedelta(days=180):
            raise ExchangeError(
                "Transfer history exceeds exchange retention; reconciliation required"
            )
        transfers = []
        for direction, source, destination in (
            ("UMFUTURE_MAIN", "USDⓈ-M Futures", "SPOT"),
            ("MAIN_UMFUTURE", "SPOT", "USDⓈ-M Futures"),
        ):
            page, seen = 1, set()
            while True:
                result = self._call(
                    self.wallet.query_user_universal_transfer_history,
                    type=direction,
                    start_time=millis(since),
                    end_time=millis(until),
                    current=page,
                    size=100,
                    recv_window=5000,
                )
                rows = result["rows"]
                for row in rows:
                    key = str(row["tranId"])
                    if key in seen:
                        raise ExchangeError("Transfer pagination repeated a record")
                    seen.add(key)
                    status = {
                        "CONFIRMED": OperationStatus.CONFIRMED,
                        "FAILED": OperationStatus.FAILED,
                        "PENDING": OperationStatus.PENDING,
                    }.get(row["status"], OperationStatus.UNKNOWN)
                    transfers.append(
                        TransferRecord(
                            key,
                            row["asset"],
                            D(row["amount"]),
                            source,
                            destination,
                            timestamp(row["timestamp"]),
                            status,
                        )
                    )
                if len(seen) >= result["total"]:
                    break
                if not rows:
                    raise ExchangeError("Incomplete transfer history")
                page += 1
        return tuple(transfers)

    def submit(self, operation):
        if not self.live:
            raise RequestRejected("Live mutations are disabled")
        p = operation.payload
        try:
            if operation.kind == OperationKind.ORDER:
                params = {
                    "symbol": p.symbol,
                    "side": p.side,
                    "type": p.order_type,
                    "quantity": format(p.qty, "f"),
                    "price": format(p.price, "f"),
                    "new_client_order_id": operation.client_id,
                    "new_order_resp_type": "RESULT",
                }
                if p.order_type != "LIMIT_MAKER":
                    params["time_in_force"] = p.time_in_force
                result = self._call(self.spot.new_order, **params, recv_window=5000)
                status = (
                    OperationStatus.PENDING
                    if result["status"] == "PENDING_NEW"
                    else OperationStatus.CONFIRMED
                )
                return OperationResult(status, str(result["orderId"]))
            if operation.kind == OperationKind.CANCEL:
                result = self._call(
                    self.spot.delete_order,
                    symbol=p.symbol,
                    order_id=int(p.order_id),
                    recv_window=5000,
                )
                status = (
                    OperationStatus.CONFIRMED
                    if result["status"]
                    in {"CANCELED", "FILLED", "EXPIRED", "EXPIRED_IN_MATCH"}
                    else OperationStatus.PENDING
                )
                return OperationResult(status, p.order_id)
            direction = {
                ("SPOT", "USDⓈ-M Futures"): "MAIN_UMFUTURE",
                ("USDⓈ-M Futures", "SPOT"): "UMFUTURE_MAIN",
            }[(p.from_account, p.to_account)]
            result = self._call(
                self.wallet.user_universal_transfer,
                type=direction,
                asset=p.asset,
                amount=format(p.amount, "f"),
                recv_window=5000,
            )
            return OperationResult(OperationStatus.PENDING, str(result["tranId"]))
        except BinanceAPIError as exc:
            # A narrow allowlist: unknown codes, duplicate IDs, cancel races,
            # 429s and all 5xx remain UNKNOWN and are queried, never retried.
            definite = {
                -1013,
                -1021,
                -1022,
                -1100,
                -1101,
                -1102,
                -1111,
                -1121,
                -2014,
                -2015,
            }
            order_rejected = (
                operation.kind == OperationKind.ORDER
                and exc.code == -2010
                and (
                    exc.message == "Account has insufficient balance for requested action."
                    or (
                        p.order_type == "LIMIT_MAKER"
                        and exc.message == "Order would immediately match and take."
                    )
                )
            )
            transfer_rejected = (
                operation.kind == OperationKind.TRANSFER and exc.code == -3020
            )
            if exc.definitive and (
                exc.code in definite or order_rejected or transfer_rejected
            ):
                raise RequestRejected(str(exc)) from None
            raise

    def query_operation(self, operation):
        if operation.kind in {OperationKind.ORDER, OperationKind.CANCEL}:
            if operation.kind == OperationKind.ORDER:
                order = self.fetch_order(
                    operation.payload.symbol,
                    **(
                        {"order_id": operation.exchange_id}
                        if operation.exchange_id is not None
                        else {"client_id": operation.client_id}
                    ),
                )
            else:
                order = self.fetch_order(
                    operation.payload.symbol, order_id=operation.payload.order_id
                )
            if order is None:
                return OperationResult(OperationStatus.UNKNOWN, operation.exchange_id)
            if operation.kind == OperationKind.ORDER and not order_matches_operation(
                order, operation
            ):
                return OperationResult(OperationStatus.UNKNOWN, operation.exchange_id)
            if order.status in {"PENDING_NEW", "PENDING_CANCEL"}:
                return OperationResult(OperationStatus.PENDING, order.order_id)
            if operation.kind == OperationKind.ORDER or not order.is_open:
                return OperationResult(OperationStatus.CONFIRMED, order.order_id)
            return OperationResult(OperationStatus.UNKNOWN, order.order_id)
        if operation.exchange_id is None:
            # Universal transfer has no client ID field. Amount/time matching
            # cannot uniquely prove ownership; leave this intent unresolved.
            return OperationResult(OperationStatus.UNKNOWN)
        records = self.fetch_recent_transfers(
            operation.created_at - timedelta(minutes=1), self.clock()
        )
        matches = [r for r in records if r.transfer_id == operation.exchange_id]
        if len(matches) == 1:
            r, p = matches[0], operation.payload
            if (r.asset, r.amount, r.from_account, r.to_account) == (
                p.asset,
                p.amount,
                p.from_account,
                p.to_account,
            ):
                return OperationResult(r.status, r.transfer_id)
        return OperationResult(OperationStatus.UNKNOWN, operation.exchange_id)
