from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

from core.models import ExecutionQuote, Operation, OperationKind, OrderPlan, PaperConfig
from services.exchange_adapter import RequestDeferred
from services.paper_exchange import PaperExchange
from storage.repository import Repository
from tests.helpers import D, make_filters, make_market, make_strategy_config


class PaperClock:
    def __init__(self):
        self.now = datetime(2026, 10, 12, tzinfo=timezone.utc)
        self.seconds = D("0")

    def utc(self):
        # Preserve event order even when a deterministic scenario has one wall tick.
        self.now += timedelta(microseconds=1)
        return self.now

    def monotonic(self):
        return self.seconds

    def elapse(self, seconds):
        self.seconds += D(str(seconds))
        self.now += timedelta(seconds=float(seconds))


class QuoteFeed:
    def __init__(self, clock):
        self.clock = clock
        self.book = None
        self.ready = True
        self.publish()

    def publish(self, *, ask="600", bid=None, qty="10"):
        ask = D(ask)
        self.book = ExecutionQuote(self.clock.utc(), "BNBUSDT", D(bid) if bid else ask - D("1"),
                                   ask, D(qty) if qty is not None else None)

    def execution_quote(self):
        return self.book

    def quote(self):
        return replace(make_market(), ts=self.clock.utc(), best_bid=self.book.best_bid,
                       best_ask=self.book.best_ask, mid_price=(self.book.best_bid + self.book.best_ask) / 2,
                       vwap_5m=self.book.best_ask, smooth_price=self.book.best_ask if self.ready else None,
                       drawdown_1m=D("0") if self.ready else None,
                       drawdown_5m=D("0") if self.ready else None,
                       drawdown_15m=D("0") if self.ready else None)


class PaperMarket:
    def __init__(self, clock):
        self.clock = clock
        self.feed = None
        self.request_not_before = None
        self.read_checkpoint = None
        self.filters = make_filters()

    def ensure_request_allowed(self, **kwargs):
        if self.request_not_before and self.clock.utc() < self.request_not_before:
            raise RequestDeferred("Public request cooldown")

    def check_clock(self):
        pass

    def fetch_market_snapshot(self, symbol):
        return self.feed.quote()

    def fetch_symbol_filters(self, symbol):
        return replace(self.filters, observed_at=self.clock.utc())


def paper_setup(path, *, config=None, cfg=None, clock=None, feed=None):
    clock = clock or PaperClock()
    feed = feed or QuoteFeed(clock)
    cfg = cfg or make_strategy_config()
    config = config or replace(PaperConfig(), initial_spot_usd=D("10000"), bnb_consumption_per_hour=D("0"))
    repo = Repository(path)
    market = PaperMarket(clock)
    try:
        ex = PaperExchange(repo, cfg, config, market_data=market, feed=feed,
                           clock=clock.utc, monotonic=clock.monotonic)
    except Exception:
        repo.close()
        raise
    ex.fetch_symbol_filters(cfg.symbol)
    ex.advance()
    return SimpleNamespace(ex=ex, repo=repo, market=market, cfg=cfg, config=config, clock=clock, feed=feed, path=path)


def order_operation(clock, *, price="590", qty="2", order_type="LIMIT_MAKER", tif="GTC", client_id=None):
    payload = OrderPlan("BNBUSDT", "BUY", order_type, D(qty), D(price), tif, order_type == "LIMIT_MAKER")
    return Operation(client_id or "bt-" + uuid4().hex, OperationKind.ORDER, "order:BNBUSDT", payload, clock.utc())
