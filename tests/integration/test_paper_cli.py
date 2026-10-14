import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlsplit

import pytest
import requests

from core.models import ExecutionQuote
from services import binance_adapter, cli
from services.exchange_adapter import ExchangeError
from services.market_stream import MarketStream
from services.paper_exchange import PaperExchange
from services.spot_market import SpotMarketData
from storage.codec import loads
from storage.repository import Repository
from tests.helpers import D, depth_messages, make_market, make_strategy_config
from tests.paper_helpers import paper_setup


@pytest.fixture
def public_http(monkeypatch):
    calls = []

    def request(self, **kwargs):
        calls.append(kwargs)
        path = urlsplit(kwargs["url"]).path
        assert kwargs["method"] == "GET"
        # The official SDK includes an empty API-key header on public clients.
        assert not any(k.lower() == "x-mbx-apikey" and v for k, v in kwargs.get("headers", {}).items())
        params = parse_qs(kwargs.get("params", ""))
        assert "signature" not in params
        now = int(datetime.now(timezone.utc).timestamp() * 1000)
        data = {
            "/api/v3/time": {"serverTime": now},
            "/api/v3/exchangeInfo": {
                "exchangeFilters": [{"filterType": "EXCHANGE_MAX_NUM_ORDERS", "maxNumOrders": 20}],
                "symbols": [{"symbol": "BNBUSDT", "status": "TRADING", "baseAsset": "BNB", "quoteAsset": "USDT",
                    "filters": [{"filterType": "LOT_SIZE", "stepSize": ".01", "minQty": ".01", "maxQty": "100"},
                                {"filterType": "PRICE_FILTER", "tickSize": ".01"},
                                {"filterType": "MIN_NOTIONAL", "minNotional": "5"},
                                {"filterType": "PERCENT_PRICE", "avgPriceMins": 5, "multiplierDown": ".9", "multiplierUp": "1.1"}]}]},
            "/api/v3/referencePrice": {"symbol": "BNBUSDT", "referencePrice": "600", "timestamp": now},
            "/api/v3/avgPrice": {"mins": 5, "price": "600", "closeTime": now},
            "/api/v3/ticker": {"symbol": "BNBUSDT", "priceChangePercent": "0"},
            "/api/v3/ticker/24hr": {"symbol": "BNBUSDT", "priceChangePercent": "0"},
        }
        assert path in data, f"Unexpected private or non-public request: {path}"
        response = requests.Response()
        response.status_code = 200
        response.headers["Content-Type"] = "application/json"
        response._content = json.dumps(data[path]).encode()
        return response

    monkeypatch.setattr(requests.Session, "request", request)
    return calls


class PublicFeed:
    def __init__(self, *args):
        self.book = ExecutionQuote(datetime.now(timezone.utc), "BNBUSDT", D("599"), D("601"), D("10"))

    def start(self):
        return self

    def close(self):
        pass

    def execution_quote(self):
        return self.book

    def quote(self):
        return replace(make_market(), ts=self.book.ts, smooth_price=None, drawdown_1m=None, drawdown_5m=None, drawdown_15m=None)


def test_cli_paper_uses_only_unsigned_public_sdk_with_credentials_present(tmp_path, monkeypatch, public_http, capsys):
    path = tmp_path / "paper.db"
    for key in ("BINANCE_API_KEY", "BINANCE_API_SECRET", "ALERT_WEBHOOK_URL"):
        monkeypatch.setenv(key, "test-value-must-not-be-read")
    original_get = cli.os.environ.get

    def get(key, *args):
        if key in {"BINANCE_API_KEY", "BINANCE_API_SECRET", "ALERT_WEBHOOK_URL"}:
            pytest.fail(f"Paper read production setting {key}")
        return original_get(key, *args)

    monkeypatch.setattr(cli.os.environ, "get", get)
    monkeypatch.setattr(cli, "BinanceAdapter", lambda *a, **k: pytest.fail("Paper constructed BinanceAdapter"))
    monkeypatch.setattr(binance_adapter, "DerivativesTradingUsdsFutures", lambda *a, **k: pytest.fail("Paper constructed futures client"))
    monkeypatch.setattr(binance_adapter, "Wallet", lambda *a, **k: pytest.fail("Paper constructed wallet client"))
    monkeypatch.setattr(cli, "MarketStream", PublicFeed)
    monkeypatch.setattr(cli.os.sys, "argv", ["cli", "--paper", "--once", "--db", str(path)])
    cli.main()
    assert public_http and all(c["method"] == "GET" for c in public_http)
    price_requests = [c for c in public_http if urlsplit(c['url']).path == '/api/v3/referencePrice']
    assert price_requests and all(c['timeout'] == 1 for c in price_requests)
    repo = Repository(path)
    assert repo.runtime().read_failures == repo.runtime().api_errors == 0
    assert not repo.load("transport_backoff")
    before = tuple(repo.db.iterdump())
    assert repo.db.execute("SELECT COUNT(*) FROM paper_orders").fetchone()[0] == 0  # Real warmup applies.
    repo.close()
    monkeypatch.setattr(cli, "MarketStream", lambda *a: pytest.fail("Status started a stream"))
    monkeypatch.setattr(cli, "PaperExchange", lambda *a, **k: pytest.fail("Status initialized an exchange"))
    monkeypatch.setattr(cli.os.sys, "argv", ["cli", "--paper", "--status", "--db", str(path), "--paper-config", "absent-file.toml"])
    count = len(public_http)
    cli.main()
    status = loads(capsys.readouterr().out)
    assert status["paper"]["account"].contract_bnb == 24
    assert len(public_http) == count
    repo = Repository(path, readonly=True)
    assert tuple(repo.db.iterdump()) == before
    repo.close()


@pytest.mark.parametrize("args", [["--paper", "--live"], ["--paper-config", "config/paper.toml"]])
def test_incompatible_flags_fail_before_database_or_network(tmp_path, monkeypatch, args):
    path = tmp_path / "never-created.db"
    monkeypatch.setattr(cli.os.sys, "argv", ["cli", "--db", str(path), *args])
    monkeypatch.setattr(cli, "BinanceAdapter", lambda *a, **k: pytest.fail("Unexpected authenticated client"))
    with pytest.raises(SystemExit):
        cli.main()
    assert not path.exists()


@pytest.mark.parametrize("paper_target", [True, False])
def test_cross_environment_database_rejected_before_sdk(tmp_path, monkeypatch, paper_target):
    path = tmp_path / "bound.db"
    if paper_target:
        p = paper_setup(path)
        p.repo.close()
        args = ["--live"]
    else:
        repo = Repository(path)
        repo.bind_identity({"account": "real", "environment": "binance-production-usds-m"}, "cfg", datetime.now(timezone.utc))
        repo.close()
        args = ["--paper"]
    monkeypatch.setattr(cli.os.sys, "argv", ["cli", "--db", str(path), *args])
    monkeypatch.setattr(cli, "BinanceAdapter", lambda *a, **k: pytest.fail("SDK constructed before environment check"))
    monkeypatch.setattr(cli, "PaperExchange", lambda *a, **k: pytest.fail("Paper constructed before environment check"))
    with pytest.raises(ValueError, match="database"):
        cli.main()


@pytest.mark.parametrize("paper", [True, False])
def test_default_database_selected_by_environment(monkeypatch, paper):
    paths = []

    def repository(path, **kwargs):
        paths.append(path)
        raise SystemExit()

    monkeypatch.setattr(cli, "Repository", repository)
    monkeypatch.setattr(cli.os.sys, "argv", ["cli", *( ["--paper"] if paper else [])])
    with pytest.raises(SystemExit):
        cli.main()
    assert paths == ["var/paper.sqlite3" if paper else "var/treasury.sqlite3"]


def test_paper_status_does_not_create_missing_database(tmp_path, monkeypatch):
    path = tmp_path / "missing.db"
    monkeypatch.setattr(cli.os.sys, "argv", ["cli", "--paper", "--status", "--db", str(path)])
    import sqlite3
    with pytest.raises(sqlite3.OperationalError):
        cli.main()
    assert not path.exists()


def test_market_stream_exposes_original_quote_and_invalidates_it(monkeypatch):
    stream = MarketStream("BNBUSDT", make_strategy_config().crash_guard, client=object())
    now = datetime.now(timezone.utc)
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now
    monkeypatch.setattr("services.market_stream.datetime", Clock)
    snapshot, event = depth_messages(now)
    stream.on_message(snapshot)
    stream.on_message(event)
    q = stream.execution_quote()
    assert q.ask_qty == D("3.5") and int(q.ts.timestamp() * 1000) == event["E"]
    # Distinct observed source times; replaying one timestamp cannot renew it.
    for i, qty in enumerate([None, "invalid"], 2):
        now += timedelta(seconds=1)
        snapshot, event = depth_messages(now, i, qty)
        stream.on_message(snapshot)
        stream.on_message(event)
        assert stream.execution_quote().ask_qty is None
    assert stream.execution_quote().ask_qty is None
    assert stream.execution_quote().ask_qty is None and stream.quote().best_ask == 601
    stream._invalidate()
    assert stream.execution_quote() is None


def test_public_filters_count_only_virtual_orders(tmp_path, public_http):
    from tests.paper_helpers import order_operation, PaperClock, QuoteFeed
    clock = PaperClock()
    clock.utc = lambda: datetime.now(timezone.utc)  # Match the public HTTP fixture's wall clock.
    cfg = make_strategy_config()
    provider = SpotMarketData(cfg, clock=clock.utc)
    repo = Repository(tmp_path / "public.db")
    feed = QuoteFeed(clock)
    from core.models import PaperConfig
    ex = PaperExchange(repo, cfg, replace(PaperConfig(), initial_spot_usd=D("10000")),
                       market_data=provider, feed=feed, clock=clock.utc, monotonic=clock.monotonic)
    try:
        assert ex.fetch_symbol_filters("BNBUSDT").exchange_order_slots == 20
        ex.submit(order_operation(clock))
        assert ex.fetch_symbol_filters("BNBUSDT").exchange_order_slots == 19
        assert all(urlsplit(c["url"]).path != "/api/v3/openOrders" for c in public_http)
    finally:
        repo.close()


def test_public_rate_limit_is_persisted_and_restart_waits(tmp_path, monkeypatch):
    from datetime import timedelta
    calls = []

    def limited(self, **kwargs):
        calls.append(kwargs)
        response = requests.Response()
        response.status_code = 429
        response.headers.update({"Content-Type": "application/json", "Retry-After": "120"})
        response._content = b'{"code":-1003,"msg":"limited"}'
        return response

    monkeypatch.setattr(requests.Session, "request", limited)
    monkeypatch.setattr(cli, "MarketStream", PublicFeed)
    path = tmp_path / "rate-limit.db"
    monkeypatch.setattr(cli.os.sys, "argv", ["cli", "--paper", "--once", "--db", str(path)])
    before = datetime.now(timezone.utc)
    cli.main()
    repo = Repository(path)
    deadline = repo.load("transport_backoff")["retry_at"]
    assert deadline >= before + timedelta(seconds=120)
    assert repo.runtime().read_failures == 1 and not repo.operations()
    repo.close()
    assert len(calls) == 1

    class Stop:
        def set(self):
            pass

        def wait(self, seconds):
            assert seconds > 0
            return True

    monkeypatch.setattr(cli, "Event", Stop)
    for _ in range(2):
        cli.main()
    assert len(calls) == 1


def test_paper_tick_lock_contention_defers_without_api_failure(tmp_path, monkeypatch, caplog):
    path = tmp_path / "contended.db"

    def factory(*args, **kwargs):
        exchange = PaperExchange(*args, **kwargs)
        def busy():
            raise BlockingIOError("cycle lock busy")
        exchange.advance = busy
        return exchange

    monkeypatch.setattr(cli, "PaperExchange", factory)
    monkeypatch.setattr(cli, "MarketStream", PublicFeed)
    monkeypatch.setattr(cli.os.sys, "argv", ["cli", "--paper", "--once", "--db", str(path)])
    cli.main()
    repo = Repository(path)
    assert repo.runtime().read_failures == repo.runtime().api_errors == 0
    assert not repo.operations() and "deferring paper tick" in caplog.text
    repo.close()


def test_status_pins_one_snapshot_during_transfer_settlement(tmp_path, monkeypatch, capsys):
    from core.models import AssetTransferPlan, Operation, OperationKind
    p = paper_setup(tmp_path / "view.db")
    op = Operation("local", OperationKind.TRANSFER, "transfer", AssetTransferPlan(
        "USDT", D("500"), "USDⓈ-M Futures", "SPOT", "test"), p.clock.utc())
    p.ex.submit(op)
    original_repository = Repository
    settled = []

    class Connection:
        def __init__(self, connection):
            self.connection = connection

        def __getattr__(self, name):
            return getattr(self.connection, name)

        def execute(self, sql, *args):
            cursor = self.connection.execute(sql, *args)
            if sql == "SELECT data FROM paper_state WHERE id=1" and not settled:
                p.clock.elapse(2)
                p.ex.advance()
                settled.append(True)
            return cursor

    def reader(*args, **kwargs):
        repo = original_repository(*args, **kwargs)
        repo.db = Connection(repo.db)
        return repo

    monkeypatch.setattr(cli, "Repository", reader)
    monkeypatch.setattr(cli.os.sys, "argv", ["cli", "--paper", "--status", "--db", str(p.path)])
    try:
        cli.main()
        status = loads(capsys.readouterr().out)["paper"]
        assert settled and p.ex.fetch_account_snapshot().spot_usd == 10500
        assert status["account"].spot_usd == 10000 and status["account"].contract_quote_balance == 100000
        assert status["account"].contract_max_withdraw_amount == 99500
        assert len(status["pending_transfers"]) == 1
    finally:
        p.repo.close()
