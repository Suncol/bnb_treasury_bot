from __future__ import annotations

from calendar import monthrange
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256

from binance_common.configuration import ConfigurationRestAPI
from binance_sdk_derivatives_trading_usds_futures.derivatives_trading_usds_futures import (
    DerivativesTradingUsdsFutures,
)
from binance_sdk_spot.spot import Spot
from binance_sdk_wallet.wallet import Wallet

from core.models import (
    AccountSnapshot,
    Fill,
    FuturesIncome,
    OperationKind,
    OperationResult,
    OperationStatus,
    OrderView,
    TransferRecord,
)
from core.time_utils import utc

from .exchange_adapter import (
    ExchangeError,
    RequestRejected,
    order_matches_operation,
)

from .sdk_transport import BinanceAPIError, millis, timestamp, sdk_data as sdk_data
from .spot_market import SpotMarketData

D = Decimal


class BinanceAdapter(SpotMarketData):
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
        if clients is None:

            def config():
                return ConfigurationRestAPI(
                    api_key=api_key,
                    api_secret=api_secret,
                    timeout=5000,
                    retries=0,
                    backoff=0,
                )

            spot = Spot(config_rest_api=config()).rest_api
            self.futures = DerivativesTradingUsdsFutures(
                config_rest_api=config()
            ).rest_api
            self.wallet = Wallet(config_rest_api=config()).rest_api
        else:
            spot, self.futures, self.wallet = clients

        super().__init__(cfg, spot=spot, clock=clock, feed=feed,
                         min_transfer_bnb=min_transfer_bnb, min_transfer_usd=min_transfer_usd)
        self.live = live

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

    def account_identity(self):
        account = self._call(self.spot.get_account, recv_window=5000)
        if account.get("uid") is None:
            raise ExchangeError("Account UID is required to bind the execution journal")
        return {
            "account": sha256(str(account["uid"]).encode()).hexdigest(),
            "environment": "binance-production-usds-m",
            "symbol": self.cfg.symbol,
            "quote_asset": self.cfg.quote_asset,
            "strategy_id": self.cfg.strategy_id,
        }

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
            if assets.get("BNB", {}).get("updateTime")
            else None,
            contract_quote_updated_at=timestamp(quote["updateTime"])
            if quote.get("updateTime")
            else None,
        )

    def _filter_overrides(self, symbol):
        return self._call(self.spot.my_filters, symbol=symbol, recv_window=5000)

    def _open_order_count(self):
        return len(self._call(self.spot.get_open_orders, recv_window=5000))

    def _check_commission(self, symbol):
        fees = self._call(self.spot.account_commission, symbol=symbol)
        rates = []
        for role in ("maker", "taker"):
            components = tuple(
                D(fees[section][field])
                for section in (
                    "standardCommission",
                    "taxCommission",
                    "specialCommission",
                )
                for field in (role, "buyer")
            )
            if any(not rate.is_finite() or rate < 0 for rate in components):
                raise ExchangeError("Unsupported BUY commission component")
            rates.append(sum(components, D("0")))
        if max(rates) > self.cfg.risk.fee_reserve_rate:
            raise ExchangeError(
                "Configured fee reserve is below complete BUY commission"
            )
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
        if symbol == self.cfg.symbol:
            base_asset, quote_asset = "BNB", self.cfg.quote_asset
        else:
            meta = self._cached(
                "assets:" + symbol,
                300,
                lambda: self._call(self.spot.exchange_info, symbol=symbol)["symbols"][
                    0
                ],
            )
            base_asset, quote_asset = meta["baseAsset"], meta["quoteAsset"]
            if meta["symbol"] != symbol or symbol != base_asset + quote_asset:
                raise ExchangeError("Unexpected activity symbol metadata")
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
                    if start <= int(row["time"]) <= stop:
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
                                side="BUY" if row["isBuyer"] else "SELL",
                                base_asset=base_asset,
                                quote_asset=quote_asset,
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

    def fetch_futures_income(self, since, until):
        # Respect the documented three-calendar-month retention. Do not accept
        # a successful but truncated response as complete reconciliation evidence.
        current = utc(self.clock())
        month_index = current.year * 12 + current.month - 1 - 3
        year, month = divmod(month_index, 12)
        month += 1
        cutoff = current.replace(
            year=year, month=month,
            day=min(current.day, monthrange(year, month)[1]),
        )
        if utc(since) < cutoff:
            raise ExchangeError(
                "Futures income exceeds supported retention; authenticated archival evidence required"
            )
        records = {}
        start, end = millis(since), millis(until)
        while start <= end:
            stop = min(start + 7 * 86_400_000 - 1, end)
            page = 1
            while True:
                rows = self._call(
                    self.futures.get_income_history,
                    start_time=start, end_time=stop, page=page, limit=1000,
                    recv_window=5000,
                )
                for row in rows:
                    if not start <= int(row["time"]) <= stop:
                        raise ExchangeError("Futures income outside requested interval")
                    record = FuturesIncome(
                        row["incomeType"], str(row["tranId"]), row["asset"],
                        D(row["income"]), timestamp(row["time"]),
                    )
                    key = record.income_type, record.transaction_id
                    if key in records:
                        raise ExchangeError("Futures income pagination repeated an identity")
                    if not record.amount.is_finite():
                        raise ExchangeError("Invalid futures income amount")
                    records[key] = record
                if len(rows) < 1000:
                    break
                page += 1
            start = stop + 1
        return tuple(records.values())

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
            if operation.kind == OperationKind.ORDER and exc.code in {-1013, -2010}:
                self.cache.pop("filters:" + p.symbol, None)
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
                    exc.message
                    == "Account has insufficient balance for requested action."
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
                return OperationResult(r.status, r.transfer_id, r)
        return OperationResult(OperationStatus.UNKNOWN, operation.exchange_id)
