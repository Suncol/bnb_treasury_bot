import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import aiohttp
import pytest
import requests
from urllib.parse import parse_qs, urlsplit
from binance_common import websocket as sdk_ws

from services.exchange_adapter import ExchangeError
from services.market_data import MarketWindows
from services.market_stream import MarketStream
from tests.helpers import D, depth_messages, make_market, make_strategy_config


class Socket:
    def __init__(self, transport):
        self.transport = transport
        self.messages = asyncio.Queue()
        self.sent = []
        self.closed = False
        self.version = 100

    def __aiter__(self):
        return self

    async def __anext__(self):
        message = await self.messages.get()
        if message is None:
            raise StopAsyncIteration
        return message

    async def send_str(self, message):
        if self.transport.subscription_failures:
            self.transport.subscription_failures -= 1
            raise ConnectionError("subscription send failed")
        self.sent.append(json.loads(message))

    async def close(self):
        self.closed = True
        self.messages.put_nowait(None)

    def exception(self):
        return ConnectionError("disconnected")

    def ticker(self):
        now = datetime.now(timezone.utc)
        data = {"e": "depthUpdate", "s": "BNBUSDT", "E": int(now.timestamp() * 1000),
                "U": self.version, "u": self.version + 3,
                "b": [["599", "1"]], "a": [["601", "3.5"]]}
        self.version += 4
        self.messages.put_nowait(SimpleNamespace(type=aiohttp.WSMsgType.TEXT,
            data=json.dumps({"stream": "bnbusdt@depth@100ms", "data": data})))


@pytest.fixture
def network(monkeypatch):
    """Run the real SDK's connection and subscription code with no network IO."""
    transport = SimpleNamespace(
        attempts=0,
        failures=0,
        subscription_failures=0,
        sockets=[],
        sessions=[],
        handshake=None,
        rest_calls=[],
        depth_response=None,
    )

    def request(self, **kwargs):
        transport.rest_calls.append(kwargs)
        assert kwargs["method"] == "GET" and urlsplit(kwargs["url"]).path == "/api/v3/depth"
        assert parse_qs(kwargs["params"]) == {"symbol": ["BNBUSDT"], "limit": ["5000"]}
        assert kwargs["timeout"] == 1
        assert not kwargs["headers"].get("X-MBX-APIKEY")
        data = {"lastUpdateId": 101, "bids": [["599", "2"], ["500", "1"]],
                "asks": [["601", "5"], ["700", "1"]]}
        if transport.depth_response is not None:
            data = transport.depth_response(data)
        response = requests.Response()
        response.status_code = 200
        response.headers["Content-Type"] = "application/json"
        response.headers["X-MBX-USED-WEIGHT-1M"] = "250"
        response._content = json.dumps(data).encode()
        return response

    monkeypatch.setattr(requests.Session, "request", request)

    class Session:
        def __init__(self):
            self.closed = False
            self.sockets = []
            transport.sessions.append(self)

        async def ws_connect(self, *args, **kwargs):
            transport.attempts += 1
            if transport.handshake is not None:
                await transport.handshake.wait()
            if transport.failures:
                transport.failures -= 1
                raise ConnectionError("offline")
            socket = Socket(transport)
            self.sockets.append(socket)
            transport.sockets.append(socket)
            return socket

        async def close(self):
            self.closed = True
            for socket in self.sockets:
                await socket.close()

    monkeypatch.setattr(sdk_ws.aiohttp, "ClientSession", Session)
    monkeypatch.setattr(sdk_ws, "SUBSCRIBE_MESSAGE_DELAY_SECONDS", 0)
    monkeypatch.setattr(
        sdk_ws, "global_stream_connections", sdk_ws.StreamConnectionsMap()
    )
    return transport


async def wait_until(condition):
    async with asyncio.timeout(4):
        while not condition():
            await asyncio.sleep(0.01)


@asynccontextmanager
async def running_stream(network):
    stream = MarketStream("BNBUSDT", make_strategy_config().crash_guard)
    api = stream.client.websocket_streams
    api.configuration.reconnect_delay = 0
    task = asyncio.create_task(stream._run())
    try:
        yield stream, api
    finally:
        stream.close()
        await asyncio.wait_for(task, 3)
    assert all(s.closed for s in network.sessions)
    assert all(s.closed for s in network.sockets)
    assert not sdk_ws.global_stream_connections.stream_connections_map


async def received_quote(stream, network, socket_count):
    await wait_until(
        lambda: len(network.sockets) == socket_count
        and sum(len(m["params"]) for m in network.sockets[-1].sent) == 1
    )
    socket = network.sockets[-1]
    assert sorted(p for m in socket.sent for p in m["params"]) == ["bnbusdt@depth@100ms"]
    socket.ticker()
    await wait_until(lambda: stream._latest is not None)
    assert stream.quote().mid_price == D("600")
    return socket


@pytest.mark.parametrize("failure", ["eof", "error", "silent"])
def test_sdk_feed_recovers_repeated_disconnects_and_rewarms(network, failure):
    async def replay():
        async with running_stream(network) as (stream, api):
            socket = await received_quote(stream, network, 1)
            for socket_count in (2, 3):
                stream.windows = MarketWindows(stream.cfg)
                now = datetime.now(timezone.utc)
                for seconds in range(901, 0, -1):
                    ts = now - timedelta(seconds=seconds)
                    stream.windows.update(replace(make_market(), ts=ts), ts)
                # Recreate a fully warmed window before each interruption.
                warm = stream.windows.update(replace(make_market(), ts=now), now)
                assert warm.drawdown_15m is not None
                old_windows = stream.windows
                if failure == "silent":
                    stream._last_depth_at -= stream.cfg.max_market_age_seconds + 1
                elif failure == "eof":
                    socket.messages.put_nowait(None)
                else:
                    socket.messages.put_nowait(
                        SimpleNamespace(type=aiohttp.WSMsgType.ERROR)
                    )
                await wait_until(lambda: stream.windows is not old_windows)
                with pytest.raises(ExchangeError):
                    stream.quote()
                socket = await received_quote(stream, network, socket_count)
                assert stream.quote().drawdown_15m is None
                assert len(api.connections) == 1
                assert (
                    len(api.connections[0].stream_callback_map["bnbusdt@depth@100ms"]) == 1
                )
            assert network.attempts == 3

    asyncio.run(replay())


@pytest.mark.parametrize("close_old", [False, True])
def test_sdk_scheduled_reconnect_does_not_race_the_watchdog(network, close_old):
    async def replay():
        async with running_stream(network) as (stream, api):
            await received_quote(stream, network, 1)
            old_connection = api.connections[0]
            network.handshake = asyncio.Event()
            replacing = asyncio.create_task(
                api.schedule_reconnect(
                    old_connection,
                    api.configuration,
                    0,
                    close_old,
                )
            )
            await wait_until(lambda: network.attempts == 2)
            await asyncio.sleep(0.45)  # Span two application watchdog checks.
            assert network.attempts == 2 and api.reconnect_tasks
            network.handshake.set()
            await replacing
            if not close_old:
                await api.close_connection(old_connection, False)
            await received_quote(stream, network, 2)
            await asyncio.sleep(0.45)
            assert network.attempts == 2 and len(api.connections) == 1
            assert all(len(api.connections[0].stream_callback_map[name]) == 1
                       for name in ("bnbusdt@depth@100ms",))

    asyncio.run(replay())


@pytest.mark.parametrize("scheduled", [False, True])
def test_exhausted_sdk_retries_start_a_fresh_subscription(network, scheduled):
    async def replay():
        async with running_stream(network) as (stream, api):
            socket = await received_quote(stream, network, 1)
            network.failures = 3
            if scheduled:
                await api.schedule_reconnect(api.connections[0], api.configuration, 0)
            else:
                socket.messages.put_nowait(None)
            await wait_until(lambda: network.attempts >= 4)
            with pytest.raises(ExchangeError):
                stream.quote()
            await received_quote(stream, network, 2)
            assert network.attempts == 5
            assert len(api.connections[0].stream_callback_map["bnbusdt@depth@100ms"]) == 1

    asyncio.run(replay())


def test_initial_sdk_connection_failure_is_retried(network):
    network.failures = 1

    async def replay():
        async with running_stream(network) as (stream, api):
            await received_quote(stream, network, 1)
            assert network.attempts == 2

    asyncio.run(replay())


@pytest.mark.parametrize("scheduled", [False, True])
def test_failed_resubscription_does_not_leave_a_stale_sdk_stream(network, scheduled):
    async def replay():
        async with running_stream(network) as (stream, api):
            socket = await received_quote(stream, network, 1)
            network.subscription_failures = 1
            if scheduled:
                with pytest.raises(ConnectionError, match="subscription send failed"):
                    await api.schedule_reconnect(
                        api.connections[0], api.configuration, 0
                    )
            else:
                socket.messages.put_nowait(None)
            await received_quote(stream, network, 3)
            assert network.attempts == 3
            assert len(api.connections[0].stream_callback_map["bnbusdt@depth@100ms"]) == 1

    asyncio.run(replay())


@pytest.mark.parametrize("stage", ["close", "forget"])
def test_cleanup_failure_does_not_stop_stream_recovery(network, monkeypatch, stage):
    async def replay():
        async with running_stream(network) as (stream, api):
            await received_quote(stream, network, 1)
            failures = 0
            if stage == "close":
                original = api.close_connection

                async def faulty(*args, **kwargs):
                    nonlocal failures
                    await original(*args, **kwargs)
                    if failures == 0 and kwargs.get("close_session"):
                        failures += 1
                        raise RuntimeError("cleanup failed after transport close")

                monkeypatch.setattr(api, "close_connection", faulty)
            else:
                original = __import__(
                    "services.market_stream", fromlist=["_forget_connection_streams"]
                )._forget_connection_streams

                def faulty(connection):
                    nonlocal failures
                    original(connection)
                    if failures == 0:
                        failures += 1
                        raise RuntimeError("subscription cleanup failed")

                monkeypatch.setattr(
                    "services.market_stream._forget_connection_streams", faulty
                )
            # Resubscription fails while a connection is still known to finally.
            network.subscription_failures = 1
            network.sockets[-1].messages.put_nowait(None)
            await received_quote(stream, network, 3)
            assert failures == 1
            assert stream.last_error and "cleanup" in stream.last_error["stage"]

    asyncio.run(replay())


def test_real_cancellation_propagates_and_cleans_sdk_connections(network):
    async def replay():
        stream = MarketStream("BNBUSDT", make_strategy_config().crash_guard)
        task = asyncio.create_task(stream._run())
        await received_quote(stream, network, 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert all(session.closed for session in network.sessions)
        assert not sdk_ws.global_stream_connections.stream_connections_map

    asyncio.run(replay())


def test_rest_snapshot_and_diff_use_public_sdk_and_shared_request_budget(network):
    from services.spot_market import SpotMarketData
    cfg = make_strategy_config()
    shared = SpotMarketData(cfg)

    async def replay():
        stream = MarketStream("BNBUSDT", cfg.crash_guard, transport=shared)
        task = asyncio.create_task(stream._run())
        try:
            await received_quote(stream, network, 1)
            assert stream.execution_quote().ask_qty == D("3.5")
            assert shared.weight_usage["spot", "x-mbx-used-weight-1m"][0] == 250
            assert len(network.rest_calls) == 1
        finally:
            stream.close()
            await task
    asyncio.run(replay())


@pytest.mark.parametrize("reason", ["close", "reconnect", "overflow"])
def test_slow_snapshot_is_single_flight_and_late_results_cannot_republish(network, reason):
    from threading import Event
    blocked, release = Event(), Event()

    def held_snapshot(data):
        blocked.set()
        assert release.wait(3)
        return data

    network.depth_response = held_snapshot

    async def replay():
        stream = MarketStream("BNBUSDT", make_strategy_config().crash_guard)
        task = asyncio.create_task(stream._run())
        try:
            await wait_until(lambda: network.sockets and network.sockets[-1].sent)
            socket = network.sockets[-1]
            socket.ticker()
            await wait_until(blocked.is_set)
            # Neither risk consumer needs the outstanding network call's lock.
            assert stream.pending_risk() is None
            assert stream.execution_quote() is None
            for _ in range(10):
                socket.ticker()
            await asyncio.sleep(0.05)
            assert len(network.rest_calls) == 1
            if reason == "close":
                stream.close()
            elif reason == "reconnect":
                socket.messages.put_nowait(None)
                await wait_until(lambda: len(network.sockets) == 2)
            else:
                for update in range(104, 200):
                    event = depth_messages(datetime.now(timezone.utc), update)[1]
                    stream.on_message(event)
                assert stream.execution_quote() is None
            release.set()
            await wait_until(lambda: stream.health()["snapshot_discarded"] == 1)
            assert stream.execution_quote() is None
            assert len(network.rest_calls) == 1
        finally:
            release.set()
            stream.close()
            await task
    asyncio.run(replay())


def test_snapshot_failure_has_finite_retry_groups_and_does_not_reconnect_valid_diff(network, monkeypatch):
    virtual = [0.0]
    monkeypatch.setattr("services.market_stream.monotonic", lambda: virtual[0])
    def failed(data):
        raise requests.Timeout("depth timeout")
    network.depth_response = failed

    async def replay():
        stream = MarketStream("BNBUSDT", make_strategy_config().crash_guard)
        task = asyncio.create_task(stream._run())
        try:
            await wait_until(lambda: network.sockets and network.sockets[-1].sent)
            socket = network.sockets[-1]
            for attempt, at in enumerate((0, 1, 3), 1):
                virtual[0] = at
                socket.ticker()
                await wait_until(lambda: len(network.rest_calls) == attempt)
                await wait_until(lambda: stream.last_error is not None and not stream._snapshot_thread.is_alive())
                assert stream.execution_quote() is None
            for second in range(4, 33):
                virtual[0] = second
                socket.ticker()
                await asyncio.sleep(0.01)
            assert len(network.rest_calls) == 3
            assert len(network.sockets) == 1
            network.depth_response = lambda data: {**data, "lastUpdateId": socket.version - 3}
            virtual[0] = 33
            socket.ticker()
            await wait_until(lambda: stream.execution_quote() is not None)
            assert len(network.rest_calls) == 4
            assert not stream.quote().windows_ready
        finally:
            stream.close()
            await task
    asyncio.run(replay())


@pytest.mark.parametrize("gate", ["cooldown", "reserved_weight"])
def test_snapshot_requests_share_main_transport_cooldown_and_reserved_budget(network, monkeypatch, gate):
    from services.spot_market import SpotMarketData
    virtual = [0.0]
    start = datetime.now(timezone.utc)
    monkeypatch.setattr("services.market_stream.monotonic", lambda: virtual[0])
    cfg = make_strategy_config()
    shared = SpotMarketData(cfg, clock=lambda: start + timedelta(seconds=virtual[0]))
    if gate == "cooldown":
        shared.request_not_before = start + timedelta(seconds=10)
    else:
        key = ("spot", "x-mbx-used-weight-1m")
        shared.weight_limits[key] = 1000
        shared.weight_usage[key] = (950, start + timedelta(seconds=10))

    async def replay():
        stream = MarketStream("BNBUSDT", cfg.crash_guard, transport=shared)
        task = asyncio.create_task(stream._run())
        try:
            await wait_until(lambda: network.sockets and network.sockets[-1].sent)
            socket = network.sockets[-1]
            socket.ticker()
            await wait_until(lambda: stream.last_error is not None)
            assert stream.last_error["type"] == "RequestDeferred"
            for second in range(1, 10):
                virtual[0] = second
                socket.ticker()
                await asyncio.sleep(0.01)
            assert network.rest_calls == []
            assert stream.execution_quote() is None
            virtual[0] = 10
            network.depth_response = lambda data: {**data, "lastUpdateId": socket.version - 3}
            socket.ticker()
            await wait_until(lambda: stream.execution_quote() is not None)
            assert len(network.rest_calls) == 1
        finally:
            stream.close()
            await task
    asyncio.run(replay())
