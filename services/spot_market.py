"""Public Spot market data and shared filter normalization; no account credentials."""

import json
import logging
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal

from binance_common.configuration import ConfigurationRestAPI
from binance_sdk_spot.spot import Spot

from core.models import MarketSnapshot, SymbolFilters
from core.time_utils import fresh, utc
from .exchange_adapter import ExchangeError
from .sdk_transport import BinanceAPIError, SDKTransport, millis, timestamp
from .price_refresh import PriceRefresh

D = Decimal


@dataclass(frozen=True)
class _AveragePrice:
    price: Decimal
    minutes: int
    close_time: datetime
    requested_at: datetime
    received_at: datetime


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
        self._price_refresh = None

    def start_price_refresh(self, *, client=None):
        if self._price_refresh is not None:
            return
        # Public reads own their session and never execute a repository callback.
        client = client if client is not None else Spot(config_rest_api=ConfigurationRestAPI(
            timeout=1000, retries=0, backoff=0,
        )).rest_api
        self._price_client = client
        self._price_refresh = PriceRefresh(
            clock=self.clock, lifetime=self.cfg.crash_guard.max_market_age_seconds,
            close_client=lambda: client._session.close() if hasattr(client, "_session") else None,
        )

    def close_price_refresh(self):
        if self._price_refresh is not None:
            self._price_refresh.close()

    def invalidate_filters(self, symbol):
        self.cache.pop("filters:" + symbol, None)
        if self._price_refresh is not None:
            self._price_refresh.invalidate()

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
            if self._price_refresh is None:
                floor, ceiling, observed_at = self._percentage_bounds(
                    symbol, percentage, floor, ceiling, self.spot, checkpoint=True,
                )
            else:
                key = (symbol, json.dumps(percentage, sort_keys=True), floor, ceiling)
                floor, ceiling, observed_at = self._price_refresh.read(
                    key, lambda: self._percentage_bounds(
                        symbol, percentage, normalized.min_price, normalized.max_price,
                        self._price_client, checkpoint=False,
                    ),
                )
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

    def _percentage_bounds(self, symbol, percentage, floor, ceiling, client, *, checkpoint):
        observed_at = self.clock()
        try:
            reference = self._call(client.reference_price, checkpoint=checkpoint, symbol=symbol)
        except BinanceAPIError as exc:
            if (exc.endpoint, exc.http_status, exc.code) != ("reference_price", 400, -2043):
                raise
            reference = {}  # Documented absence; use the same fallback as null.
        for rule in percentage:
            if reference.get("referencePrice") is not None:
                anchor = D(reference["referencePrice"])
                if not fresh(
                    timestamp(reference["timestamp"]),
                    self.clock(),
                    self.cfg.crash_guard.max_market_age_seconds,
                ):
                    raise ExchangeError("Stale exchange reference price")
                observed_at = min(utc(observed_at), timestamp(reference["timestamp"]))
            elif int(rule["avgPriceMins"]) == 0:
                anchor = D(
                    self._call(client.ticker_price, checkpoint=checkpoint, symbol=symbol)["price"]
                )
            else:
                avg = self._call(client.avg_price, checkpoint=checkpoint, symbol=symbol)
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
        return floor, ceiling, observed_at

    def _average_price(self, symbol):
        key = "avg:" + symbol
        cached = self.cache.get(key)
        if cached is not None and not fresh(
            cached[1].requested_at, self.clock(),
            self.cfg.crash_guard.max_market_age_seconds,
        ):
            # A UTC jump must not turn a monotonic cache hit into fresh data.
            self.cache.pop(key)

        def fetch():
            requested_at = utc(self.clock())
            data = self._call(self.spot.avg_price, symbol=symbol)
            received_at = utc(self.clock())
            average = _AveragePrice(
                D(data["price"]), data["mins"], timestamp(data["closeTime"]),
                requested_at, received_at,
            )
            logging.getLogger("bnb_treasury.market").debug(
                "AVG_PRICE symbol=%s price=%s mins=%s closeTime=%s "
                "requested_at=%s received_at=%s",
                symbol, average.price, average.minutes, data["closeTime"],
                requested_at.isoformat(), received_at.isoformat(),
            )
            return average

        average = self._cached(
            key, min(2, self.cfg.crash_guard.max_market_age_seconds), fetch
        )
        return average, cached is not None and average is cached[1]

    def fetch_market_snapshot(self, symbol):
        if self._price_refresh is not None:
            self._price_refresh.kick()
        if self.feed is not None:
            streamed = self.feed.quote()
            quote_ts, bid, ask = streamed.ts, streamed.best_bid, streamed.best_ask
        else:
            quote_ts = self.clock()
            book = self._call(self.spot.ticker_book_ticker, symbol=symbol)
            bid, ask = D(book["bidPrice"]), D(book["askPrice"])
        avg, cache_hit = self._average_price(symbol)
        if avg.minutes != 5:
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
            # A successful read confirms the current average even when there
            # has been no trade recently. Count network time and cached reuse
            # against its lifetime; never retimestamp on receipt or cache hit.
            min(utc(quote_ts), avg.requested_at),
            symbol,
            bid,
            ask,
            (bid + ask) / 2,
            avg.price,
            D(one["priceChangePercent"]) / 100,
            D(day["priceChangePercent"]) / 100,
            quote_ts=utc(quote_ts),
            vwap_requested_at=avg.requested_at,
            vwap_received_at=avg.received_at,
            vwap_close_time=avg.close_time,
            vwap_cache_hit=cache_hit,
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
