from __future__ import annotations

import time
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256

from binance_common.configuration import ConfigurationRestAPI
from binance_common.errors import (
    BadRequestError,
    ForbiddenError,
    NotFoundError,
    RateLimitBanError,
    ServerError,
    TooManyRequestsError,
    UnauthorizedError,
)
from binance_common.errors import (
    Error as SDKError,
)
from binance_sdk_derivatives_trading_usds_futures.derivatives_trading_usds_futures import (
    DerivativesTradingUsdsFutures,
)
from binance_sdk_spot.spot import Spot
from binance_sdk_wallet.wallet import Wallet

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
from core.time_utils import fresh, utc

from .exchange_adapter import (
    ExchangeError,
    RequestDeferred,
    RequestRejected,
    order_matches_operation,
)

D = Decimal


def millis(value):
    return int(utc(value).timestamp() * 1000)


def timestamp(value):
    return datetime.fromtimestamp(int(value) / 1000, timezone.utc)


class BinanceAPIError(ExchangeError):
    def __init__(self, code, *, message="", definitive=False, **metadata):
        self.code, self.definitive = code, definitive
        self.message = message
        super().__init__(
            f"Binance SDK error code {code}: {message}", code=code, **metadata
        )


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
        if any(
            not value.is_finite() or value < 0
            for value in (min_transfer_bnb, min_transfer_usd)
        ):
            raise ValueError("Transfer minima must be finite and nonnegative")
        self.cache = {}
        self.request_not_before = None
        self.request_weights = {}
        self.weight_limits = {}
        self.weight_usage = {}
        self.read_checkpoint = None
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

    def ensure_request_allowed(self, *, protective=False, scope=None):
        now = self.clock()
        if self.request_not_before is not None and now < self.request_not_before:
            raise RequestDeferred(
                "Exchange request cooldown",
                retry_after=(self.request_not_before - now).total_seconds(),
            )
        if not protective:
            for (service, header), (used, expires) in self.weight_usage.items():
                limit = self.weight_limits.get((service, header))
                if (
                    (scope is None or service == scope)
                    and limit
                    and now < expires
                    and used >= limit * D("0.9")
                ):
                    raise RequestDeferred(
                        "Request budget reserved for protection",
                        endpoint=service,
                        retry_after=(expires - now).total_seconds(),
                        request_weights=self.request_weights,
                    )

    def _observe_request_budget(self, scope, headers, data):
        periods = {
            "SECOND": (1, "s"),
            "MINUTE": (60, "m"),
            "HOUR": (3600, "h"),
            "DAY": (86400, "d"),
        }
        if isinstance(data, dict):
            for rule in data.get("rateLimits", []) or []:
                kind, interval = rule.get("rateLimitType"), rule.get("interval")
                if kind not in {"REQUEST_WEIGHT", "ORDERS"} or interval not in periods:
                    continue
                prefix = (
                    "x-mbx-used-weight-"
                    if kind == "REQUEST_WEIGHT"
                    else "x-mbx-order-count-"
                )
                self.weight_limits[
                    scope, prefix + str(rule["intervalNum"]) + periods[interval][1]
                ] = int(rule["limit"])
        now = self.clock()
        for header, value in headers.items():
            if not header.startswith(("x-mbx-used-weight-", "x-mbx-order-count-")):
                continue
            suffix = header.rsplit("-", 1)[-1]
            try:
                seconds = (
                    int(suffix[:-1])
                    * {"s": 1, "m": 60, "h": 3600, "d": 86400}[suffix[-1]]
                )
                used = int(value)
                if seconds <= 0 or used < 0:
                    continue
            except (ValueError, KeyError):
                continue
            end = (int(now.timestamp()) // seconds + 1) * seconds + 1
            self.weight_usage[scope, header] = (
                used,
                datetime.fromtimestamp(end, timezone.utc),
            )

    def _call(self, method, **params):
        endpoint = getattr(method, "__name__", type(method).__name__)
        mutation = endpoint in {"new_order", "delete_order", "user_universal_transfer"}
        if not mutation and self.read_checkpoint is not None:
            self.read_checkpoint()
        owner = getattr(method, "__self__", None)
        scope = (
            "futures"
            if owner is self.futures
            else "wallet"
            if owner is self.wallet
            else "spot"
        )
        self.ensure_request_allowed(protective=endpoint == "delete_order", scope=scope)
        try:
            response = method(**params)
            self.request_weights = {
                k.lower(): v
                for k, v in getattr(response, "headers", {}).items()
                if k.lower().startswith(
                    ("x-mbx-used-weight", "x-mbx-order-count", "x-sapi-used")
                )
            }
            data = sdk_data(response.data())
            self._observe_request_budget(scope, self.request_weights, data)
            return data
        except SDKError as exc:
            status = next(
                (
                    http
                    for cls, http in (
                        (TooManyRequestsError, 429),
                        (RateLimitBanError, 418),
                        (BadRequestError, 400),
                        (UnauthorizedError, 401),
                        (ForbiddenError, 403),
                        (NotFoundError, 404),
                    )
                    if isinstance(exc, cls)
                ),
                None,
            )
            if isinstance(exc, ServerError):
                status = getattr(exc, "status_code", None)
            retry_after = getattr(exc, "retry_after", None)
            if status in {429, 418}:
                retry_after = (
                    max(float(retry_after or 0), 1)
                    if retry_after is not None
                    else (120 if status == 418 else 60)
                )
                self.request_not_before = self.clock() + timedelta(seconds=retry_after)
            raise BinanceAPIError(
                getattr(exc, "status_code", None),
                message=getattr(exc, "error_message", ""),
                definitive=isinstance(exc, (BadRequestError, UnauthorizedError)),
                endpoint=endpoint,
                http_status=status,
                retry_after=retry_after,
                request_weights=self.request_weights,
            ) from exc
        except Exception as exc:
            raise ExchangeError(
                f"SDK transport or response failure: {type(exc).__name__}",
                endpoint=endpoint,
            ) from exc

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

    def fetch_symbol_filters(self, symbol):
        def metadata():
            info = self._call(self.spot.exchange_info, symbol=symbol)
            data = info["symbols"][0]
            if (
                data["status"] != "TRADING"
                or data["baseAsset"] != "BNB"
                or data["quoteAsset"] != self.cfg.quote_asset
            ):
                raise ExchangeError("Symbol is unavailable or has unexpected assets")
            personal = self._call(self.spot.my_filters, symbol=symbol, recv_window=5000)
            filters = {f["filterType"]: f for f in data["filters"]}
            filters.update(
                {f["filterType"]: f for f in personal.get("symbolFilters", [])}
            )
            exchange_filters = {
                f["filterType"]: f for f in info.get("exchangeFilters", [])
            }
            exchange_filters.update(
                {f["filterType"]: f for f in personal.get("exchangeFilters", [])}
            )
            irrelevant = {
                "ICEBERG_PARTS",
                "MARKET_LOT_SIZE",
                "MAX_NUM_ALGO_ORDERS",
                "MAX_NUM_ICEBERG_ORDERS",
                "TRAILING_DELTA",
                "MAX_NUM_ORDER_LISTS",
                "MAX_NUM_ORDER_AMENDS",
                "T_PLUS_SELL",
            }
            supported = {
                "LOT_SIZE",
                "PRICE_FILTER",
                "MIN_NOTIONAL",
                "NOTIONAL",
                "PERCENT_PRICE",
                "PERCENT_PRICE_BY_SIDE",
                "MAX_NUM_ORDERS",
                "MAX_POSITION",
            }
            if set(filters) - supported - irrelevant:
                raise ExchangeError(
                    "Unsupported active symbol filter: "
                    + ",".join(sorted(set(filters) - supported - irrelevant))
                )
            if set(exchange_filters) - {
                "EXCHANGE_MAX_NUM_ORDERS",
                "EXCHANGE_MAX_NUM_ALGO_ORDERS",
                "EXCHANGE_MAX_NUM_ICEBERG_ORDERS",
                "EXCHANGE_MAX_NUM_ORDER_LISTS",
            }:
                raise ExchangeError("Unsupported active exchange filter")
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
            lot, price = filters["LOT_SIZE"], filters["PRICE_FILTER"]
            minimum = max(
                (
                    D(f["minNotional"])
                    for kind, f in filters.items()
                    if kind in {"MIN_NOTIONAL", "NOTIONAL"}
                ),
                default=D("0"),
            )
            maximum = D(filters.get("NOTIONAL", {}).get("maxNotional", "0")) or None
            max_qty = D(lot["maxQty"]) or None
            for rule in personal.get("assetFilters", []):
                if rule["filterType"] != "MAX_ASSET":
                    raise ExchangeError("Unsupported active asset filter")
                limit = D(rule["limit"])
                if limit > 0 and rule["asset"] == "BNB":
                    max_qty = min(max_qty, limit) if max_qty is not None else limit
                elif limit > 0 and rule["asset"] == self.cfg.quote_asset:
                    maximum = min(maximum, limit) if maximum is not None else limit
            normalized = SymbolFilters(
                D(lot["stepSize"]),
                D(price["tickSize"]),
                D(lot["minQty"]),
                minimum,
                self.min_transfer_bnb,
                self.min_transfer_usd,
                max_qty,
                maximum,
                min_price=D(price.get("minPrice", "0")),
                max_price=D(price.get("maxPrice", "0")) or None,
                max_position=D(filters.get("MAX_POSITION", {}).get("maxPosition", "0"))
                or None,
                max_open_orders=int(filters["MAX_NUM_ORDERS"]["maxNumOrders"])
                if "MAX_NUM_ORDERS" in filters
                else None,
            )
            amounts = (
                normalized.qty_step,
                normalized.price_tick,
                normalized.min_qty,
                normalized.min_notional,
                normalized.min_price,
                normalized.max_price,
                normalized.max_qty,
                normalized.max_notional,
                normalized.max_position,
            )
            if any(
                value is not None and (not value.is_finite() or value < 0)
                for value in amounts
            ):
                raise ExchangeError("Invalid exchange filter amounts")
            if normalized.qty_step <= 0 or normalized.price_tick <= 0:
                raise ExchangeError("Unsupported disabled lot or price step")
            return normalized, filters, exchange_filters

        normalized, rules, exchange_rules = self._cached(
            "filters:" + symbol, 300, metadata
        )
        # Dynamic reference bounds and account order counts are never cached with
        # five-minute metadata. Unknown active rules fail closed before funding.
        observed_at = self.clock()
        floor, ceiling = normalized.min_price, normalized.max_price
        percentage = [
            rules[k] for k in ("PERCENT_PRICE", "PERCENT_PRICE_BY_SIDE") if k in rules
        ]
        if percentage:
            reference = self._call(self.spot.reference_price, symbol=symbol)
            for rule in percentage:
                if reference.get("referencePrice") is not None:
                    anchor = D(reference["referencePrice"])
                    if not fresh(
                        timestamp(reference["timestamp"]),
                        self.clock(),
                        self.cfg.crash_guard.max_market_age_seconds,
                    ):
                        raise ExchangeError("Stale exchange reference price")
                elif int(rule["avgPriceMins"]) == 0:
                    anchor = D(
                        self._call(self.spot.ticker_price, symbol=symbol)["price"]
                    )
                else:
                    avg = self._call(self.spot.avg_price, symbol=symbol)
                    if int(avg["mins"]) != int(rule["avgPriceMins"]) or not fresh(
                        timestamp(avg["closeTime"]),
                        self.clock(),
                        self.cfg.crash_guard.max_market_age_seconds,
                    ):
                        raise ExchangeError(
                            "No matching fresh average for percent-price filter"
                        )
                    anchor = D(avg["price"])
                lower = (
                    D(rule.get("bidMultiplierDown", rule.get("multiplierDown", "0")))
                    * anchor
                )
                upper = (
                    D(rule.get("bidMultiplierUp", rule.get("multiplierUp", "0")))
                    * anchor
                )
                if (
                    not anchor.is_finite()
                    or anchor <= 0
                    or not lower.is_finite()
                    or not upper.is_finite()
                    or not 0 <= lower <= upper
                ):
                    raise ExchangeError("Invalid percent-price bounds")
                floor = max(floor, lower)
                ceiling = upper if ceiling is None else min(ceiling, upper)
        slots = None
        if "EXCHANGE_MAX_NUM_ORDERS" in exchange_rules:
            active = self._call(self.spot.get_open_orders, recv_window=5000)
            slots = max(
                0,
                int(exchange_rules["EXCHANGE_MAX_NUM_ORDERS"]["maxNumOrders"])
                - len(active),
            )
        return replace(
            normalized,
            min_price=floor,
            max_price=ceiling,
            exchange_order_slots=slots,
            observed_at=observed_at,
        )

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
                sample_continuity=streamed.sample_continuity,
                sample_stable_since=streamed.sample_stable_since,
                sampled_at=streamed.sampled_at,
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
                return OperationResult(r.status, r.transfer_id)
        return OperationResult(OperationStatus.UNKNOWN, operation.exchange_id)
