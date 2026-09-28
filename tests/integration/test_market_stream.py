import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace

import aiohttp
from binance_common import websocket as sdk_ws
import pytest

from services.exchange_adapter import ExchangeError
from services.market_data import MarketWindows
from services.market_stream import MarketStream
from tests.helpers import D, make_market, make_strategy_config


class Socket:
    def __init__(self, transport):
        self.transport = transport
        self.messages = asyncio.Queue()
        self.sent = []
        self.closed = False

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
        self.messages.put_nowait(SimpleNamespace(
            type=aiohttp.WSMsgType.TEXT,
            data=json.dumps({
                "stream": "bnbusdt@ticker",
                "data": {
                    "e": "24hrTicker", "s": "BNBUSDT",
                    "E": int(datetime.now(timezone.utc).timestamp() * 1000),
                    "b": "599", "a": "601", "P": "0",
                },
            }),
        ))


@pytest.fixture
def network(monkeypatch):
    """Run the real SDK's connection and subscription code with no network IO."""
    transport = SimpleNamespace(
        attempts=0, failures=0, subscription_failures=0,
        sockets=[], sessions=[], handshake=None,
    )

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
    monkeypatch.setattr(sdk_ws, "global_stream_connections", sdk_ws.StreamConnectionsMap())
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
    await wait_until(lambda: (
        len(network.sockets) == socket_count and network.sockets[-1].sent
    ))
    socket = network.sockets[-1]
    assert [m["params"] for m in socket.sent] == [["bnbusdt@ticker"]]
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
                    stream._last_message_at -= stream.cfg.max_market_age_seconds + 1
                elif failure == "eof":
                    socket.messages.put_nowait(None)
                else:
                    socket.messages.put_nowait(SimpleNamespace(type=aiohttp.WSMsgType.ERROR))
                await wait_until(lambda: stream.windows is not old_windows)
                with pytest.raises(ExchangeError):
                    stream.quote()
                socket = await received_quote(stream, network, socket_count)
                assert stream.quote().drawdown_15m is None
                assert len(api.connections) == 1
                assert len(api.connections[0].stream_callback_map["bnbusdt@ticker"]) == 1
            assert network.attempts == 3
    asyncio.run(replay())


@pytest.mark.parametrize("close_old", [False, True])
def test_sdk_scheduled_reconnect_does_not_race_the_watchdog(network, close_old):
    async def replay():
        async with running_stream(network) as (stream, api):
            await received_quote(stream, network, 1)
            old_connection = api.connections[0]
            network.handshake = asyncio.Event()
            replacing = asyncio.create_task(api.schedule_reconnect(
                old_connection, api.configuration, 0, close_old,
            ))
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
            assert len(api.connections[0].stream_callback_map["bnbusdt@ticker"]) == 1
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
            assert len(api.connections[0].stream_callback_map["bnbusdt@ticker"]) == 1
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
                    await api.schedule_reconnect(api.connections[0], api.configuration, 0)
            else:
                socket.messages.put_nowait(None)
            await received_quote(stream, network, 3)
            assert network.attempts == 3
            assert len(api.connections[0].stream_callback_map["bnbusdt@ticker"]) == 1
    asyncio.run(replay())
