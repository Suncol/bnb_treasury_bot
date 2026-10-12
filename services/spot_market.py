"""Public Spot market data and shared filter normalization; no account credentials."""

from dataclasses import replace
from decimal import Decimal

from binance_common.configuration import ConfigurationRestAPI
from binance_sdk_spot.spot import Spot

from core.models import MarketSnapshot, SymbolFilters
from core.time_utils import fresh, utc
from .exchange_adapter import ExchangeError
from .sdk_transport import SDKTransport, millis, timestamp

D = Decimal


class SpotMarketData(SDKTransport):
    def __init__(self, cfg, *, spot=None, clock=None, feed=None,
                 min_transfer_bnb=D("0.1"), min_transfer_usd=D("0"), order_count=None):
        super().__init__(clock=clock)
        self.cfg, self.feed = cfg, feed
        self.min_transfer_bnb, self.min_transfer_usd = min_transfer_bnb, min_transfer_usd
        if any(not v.is_finite() or v < 0 for v in (min_transfer_bnb, min_transfer_usd)):
            raise ValueError("Transfer minima must be finite and nonnegative")
        self.spot = spot if spot is not None else Spot(config_rest_api=ConfigurationRestAPI(
            timeout=5000, retries=0, backoff=0,
        )).rest_api
        self._order_count = order_count

    def _filter_overrides(self, symbol):
        return {}

    def _check_commission(self, symbol):
        pass

    def _open_order_count(self):
        if self._order_count is None:
            raise ExchangeError("Local order count is required for public exchange filters")
        return self._order_count()

    def check_clock(self):
        def check():
            before = millis(self.clock())
            server = self._call(self.spot.time)["serverTime"]
            after = millis(self.clock())
            if abs(int(server) - (before + after) // 2) > 1000:
                raise ExchangeError("System clock differs from Binance; synchronize it before trading")
            return True
        self._cached("clock", 1800, check)

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
            personal = self._filter_overrides(symbol)
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
            self._check_commission(symbol)
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
            slots = max(
                0,
                int(exchange_rules["EXCHANGE_MAX_NUM_ORDERS"]["maxNumOrders"])
                - self._open_order_count(),
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
