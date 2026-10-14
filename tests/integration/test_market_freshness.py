import json
import logging
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace

import pytest

from core.crash_guard import market_invalid_reasons, valid_market_snapshot
from core.models import GateStatus
from services.exchange_adapter import ExchangeError
from services.runner import Runner
from services.sdk_transport import millis
from services.spot_market import SpotMarketData
from storage.codec import dumps, loads
from storage.repository import Repository
from tests.fake_exchange import FakeExchange
from tests.helpers import D, make_account, make_filters, make_market, make_strategy_config


class PublicPrices:
    """REST observations and last-trade timestamps advance independently."""

    def __init__(self, exchange):
        self.exchange = exchange
        self.seconds = 0
        self.average_reads = []
        self.last_trade = exchange.now - timedelta(seconds=10)
        self.price = "593.12345678"
        self.minutes = 5
        self.delay = 0.075
        self.ticker_delay = 0
        self.quote_age = 0
        self.failure = None

    def advance(self, seconds):
        self.seconds += seconds
        self.exchange.now += timedelta(seconds=seconds)

    def response(self, data):
        return SimpleNamespace(headers={}, data=lambda: data)

    def avg_price(self, **kwargs):
        self.average_reads.append(self.exchange.now)
        self.advance(self.delay)
        if self.failure:
            raise self.failure
        return self.response({"mins": self.minutes, "price": self.price,
                              "closeTime": millis(self.last_trade)})

    def ticker(self, **kwargs):
        self.advance(self.ticker_delay)
        return self.response({"priceChangePercent": "0"})

    ticker24hr = ticker

    def quote(self):
        return replace(make_market(), ts=self.exchange.now - timedelta(seconds=self.quote_age))


@pytest.fixture
def setup(monkeypatch, tmp_path):
    exchange = FakeExchange(make_account(contract_bnb=D("32")))
    prices = PublicPrices(exchange)
    monkeypatch.setattr("services.sdk_transport.time.monotonic", lambda: prices.seconds)
    cfg = make_strategy_config()
    feed = SimpleNamespace(quote=prices.quote)
    market = SpotMarketData(cfg, spot=prices, clock=lambda: exchange.now, feed=feed)
    exchange.fetch_market_snapshot = market.fetch_market_snapshot
    exchange.fetch_symbol_filters = lambda symbol: replace(make_filters(), observed_at=exchange.now)
    exchange.feed = feed
    repo = Repository(tmp_path / "freshness.sqlite3")
    repo.initialize_spot_checkpoint(exchange.fetch_account_snapshot(),
                                   {"operator": "test", "reason": "quiescent fixture"})
    runner = Runner(exchange, repo, cfg, clock=lambda: exchange.now)
    market.read_checkpoint = runner._priority_checkpoint
    yield SimpleNamespace(exchange=exchange, prices=prices, market=market, repo=repo,
                          runner=runner, cfg=cfg)
    repo.close()


def test_continuous_ticks_with_unchanged_last_trade_keep_fresh_observations_open(setup):
    s = setup
    for n in range(150):
        s.prices.advance((1, 1.001, 0.999)[n % 3])
        plan = s.runner.tick()
        assert plan is not None and plan.gates.data_gate == GateStatus.OPEN
        market = s.runner.cached_inputs.market
        assert market.vwap_5m == D(s.prices.price)
        assert market.vwap_close_time == s.prices.last_trade
        assert market.ts >= market.vwap_requested_at - timedelta(seconds=1)
    assert 20 < len(s.prices.average_reads) < 150
    assert not s.exchange.writes
    assert s.repo.runtime().api_errors == 0
    assert not any(a.code == "DATA_INVALID" for _, a in s.repo.pending_alerts())


def test_cache_hit_preserves_request_and_response_times_and_actual_average(setup):
    s = setup
    first = s.market.fetch_market_snapshot(s.cfg.symbol)
    s.prices.advance(1)
    second = s.market.fetch_market_snapshot(s.cfg.symbol)
    assert not first.vwap_cache_hit and second.vwap_cache_hit
    assert second.vwap_requested_at == first.vwap_requested_at
    assert second.vwap_received_at == first.vwap_received_at
    assert second.vwap_close_time == first.vwap_close_time
    assert second.ts == first.vwap_requested_at
    assert second.quote_ts > first.quote_ts
    assert second.vwap_5m != s.prices.quote().vwap_5m
    assert loads(dumps(second)) == second


@pytest.mark.parametrize("jump", [6, -1])
def test_wall_clock_jump_invalidates_average_cache_without_waiting_for_monotonic_ttl(setup, jump):
    s = setup
    first = s.market.fetch_market_snapshot(s.cfg.symbol)
    s.exchange.now += timedelta(seconds=jump)
    second = s.market.fetch_market_snapshot(s.cfg.symbol)
    assert len(s.prices.average_reads) == 2
    assert not second.vwap_cache_hit
    assert second.vwap_requested_at != first.vwap_requested_at


@pytest.mark.parametrize("elapsed,valid", [(5, True), (5.000001, False)])
def test_observation_retains_exact_five_second_lifetime(setup, elapsed, valid):
    s = setup
    market = s.market.fetch_market_snapshot(s.cfg.symbol)
    now = market.vwap_requested_at + timedelta(seconds=elapsed)
    # Isolate the average deadline from independently fresh book data.
    market = replace(market, ts=market.vwap_requested_at, quote_ts=now)
    assert valid_market_snapshot(market, now, s.cfg) is valid
    if not valid:
        assert "VWAP_OBSERVATION_TIMESTAMP" in market_invalid_reasons(market, now, s.cfg)


def test_slow_average_response_cannot_retimestamp_expired_observation(setup):
    s = setup
    s.prices.delay = 5.001
    start = s.exchange.now
    market = s.market.fetch_market_snapshot(s.cfg.symbol)
    assert market.vwap_requested_at == start
    assert market.vwap_received_at == s.exchange.now
    assert "VWAP_OBSERVATION_TIMESTAMP" in market_invalid_reasons(market, s.exchange.now, s.cfg)
    assert not valid_market_snapshot(market, s.exchange.now, s.cfg)


def test_slow_other_rest_reads_do_not_retimestamp_average_or_quote(setup):
    s = setup
    s.prices.ticker_delay = 3
    market = s.market.fetch_market_snapshot(s.cfg.symbol)
    assert "QUOTE_TIMESTAMP" in market_invalid_reasons(market, s.exchange.now, s.cfg)
    assert "VWAP_OBSERVATION_TIMESTAMP" in market_invalid_reasons(market, s.exchange.now, s.cfg)


def test_future_component_cannot_hide_behind_minimum_snapshot_timestamp(setup):
    s = setup
    market = s.market.fetch_market_snapshot(s.cfg.symbol)
    future = s.exchange.now + timedelta(seconds=1)
    assert not valid_market_snapshot(replace(market, quote_ts=future), s.exchange.now, s.cfg)
    assert not valid_market_snapshot(replace(market, vwap_requested_at=future), s.exchange.now, s.cfg)
    assert not valid_market_snapshot(replace(market, vwap_received_at=future), s.exchange.now, s.cfg)
    assert not valid_market_snapshot(replace(market, vwap_close_time=future), s.exchange.now, s.cfg)


@pytest.mark.parametrize("price", ["0", "-1", "NaN", "Infinity"])
def test_invalid_average_never_opens_gate_or_funds_account(setup, price):
    s = setup
    s.exchange.account = replace(s.exchange.account, contract_bnb=D("24"))
    s.prices.price = price
    plan = s.runner.tick()
    assert plan.gates.data_gate == GateStatus.BLOCKED
    assert not plan.transfer_decision.allow and not plan.buy_plan.orders
    assert not s.exchange.writes


def test_failed_average_refresh_never_uses_old_cached_response(setup):
    s = setup
    assert s.runner.tick().gates.data_gate == GateStatus.OPEN
    s.prices.advance(3)
    s.prices.failure = TimeoutError()
    assert s.runner.tick() is None
    assert s.runner.cached_inputs is None
    assert s.repo.runtime().read_retry_at is not None
    calls = len(s.prices.average_reads)
    assert s.runner.tick() is None
    assert len(s.prices.average_reads) == calls
    assert not s.exchange.writes


@pytest.mark.parametrize("minutes", [1, 5.5, "5"])
def test_average_window_mismatch_is_still_rejected(setup, minutes):
    s = setup
    s.prices.minutes = minutes
    with pytest.raises(ExchangeError, match="5 minute VWAP"):
        s.market.fetch_market_snapshot(s.cfg.symbol)


def test_gate_diagnostics_use_exact_planner_average_and_log_recovery(setup, caplog):
    s = setup
    caplog.set_level(logging.DEBUG, logger="bnb_treasury")
    s.prices.quote_age = 6
    blocked = s.runner.tick()
    assert blocked.gates.data_gate == GateStatus.BLOCKED
    records = [json.loads(r.getMessage().removeprefix("DATA_GATE "))
               for r in caplog.records if r.getMessage().startswith("DATA_GATE ")]
    bad = records[-1]
    assert bad["market"]["vwap_5m"] == s.prices.price
    assert bad["market"]["vwap_close_time"] == str(s.prices.last_trade)
    assert bad["vwap_observation_age_seconds"] < 1
    assert bad["vwap_last_trade_age_seconds"] > 10
    assert "QUOTE_TIMESTAMP" in bad["reasons"]
    assert any("closeTime=" + str(millis(s.prices.last_trade)) in r.getMessage()
               for r in caplog.records)
    s.prices.quote_age = 0
    s.prices.advance(1)
    assert s.runner.tick().gates.data_gate == GateStatus.OPEN
    records = [json.loads(r.getMessage().removeprefix("DATA_GATE "))
               for r in caplog.records if r.getMessage().startswith("DATA_GATE ")]
    assert records[-1]["data_gate"] == "OPEN" and not records[-1]["reasons"]
    count = len(records)
    s.prices.advance(1)
    s.runner.tick()
    assert sum(r.getMessage().startswith("DATA_GATE ") for r in caplog.records) == count
