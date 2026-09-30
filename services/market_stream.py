import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal
from threading import Event, Lock, Thread
from time import monotonic

from binance_common.configuration import ConfigurationWebSocketStreams
from binance_common.websocket import _forget_connection_streams
from binance_sdk_spot.spot import Spot

from core.crash_guard import latch_guard_trigger
from core.models import CrashGuardState, MarketSnapshot

from .binance_adapter import sdk_data
from .exchange_adapter import ExchangeError
from .market_data import MarketWindows


class MarketStream:
    """Official SDK's 1s ticker feed, sampled independently of REST execution."""

    def __init__(self, symbol, cfg, *, client=None):
        self.symbol, self.cfg = symbol, cfg
        self.client = client or Spot(
            config_ws_streams=ConfigurationWebSocketStreams(reconnect_delay=1000)
        )
        self._stop, self._lock = Event(), Lock()
        self._latest = None
        # Two bounded slots: one being persisted, one collecting newer triggers.
        self._risk = self._leased_risk = None
        self._thread = None
        self.windows = MarketWindows(cfg)
        self._last_message_at = monotonic()
        self.last_error = None

    def start(self):
        self._thread = Thread(
            target=lambda: asyncio.run(self._supervise()), daemon=True
        )
        self._thread.start()
        return self

    def on_message(self, message):
        try:
            event = sdk_data(message)
            if event.get("s") != self.symbol or event.get("e") != "24hrTicker":
                return
            now = datetime.now(timezone.utc)
            bid, ask = Decimal(event["b"]), Decimal(event["a"])
            quote = MarketSnapshot(
                datetime.fromtimestamp(event["E"] / 1000, timezone.utc),
                self.symbol,
                bid,
                ask,
                (bid + ask) / 2,
                (bid + ask) / 2,
                Decimal("0"),
                Decimal(event["P"]) / 100,
            )
            if not quote.return_24h.is_finite():
                raise ValueError("Invalid ticker return")
            with self._lock:
                sampled = self.windows.update(quote, now)
                self._last_message_at = monotonic()
                self._latest = sampled
                risk = latch_guard_trigger(
                    self._risk or CrashGuardState(), sampled, now, self.cfg
                )
                self._risk = risk if risk.active else None
        except Exception as exc:
            self._record_error("ticker", exc)
            self._invalidate()

    def _record_error(self, stage, exc):
        self.last_error = {"stage": stage, "type": type(exc).__name__}
        logging.getLogger("bnb_treasury").warning(
            "Market stream %s failed", stage, exc_info=True
        )

    def _invalidate(self):
        with self._lock:
            self.windows = MarketWindows(self.cfg)
            self._latest = None

    async def _supervise(self):
        while not self._stop.is_set():
            try:
                await self._run()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._record_error("supervisor", exc)
            finally:
                self._invalidate()
            if not self._stop.is_set():
                await asyncio.sleep(1)

    async def _run(self):
        api = self.client.websocket_streams
        while not self._stop.is_set():
            connection = None
            self._invalidate()

            def opened():
                nonlocal connection
                connection = next(c for c in api.connections if c.is_open)
                self._last_message_at = monotonic()
                self._invalidate()

            try:
                await api.create_connection()
                if not api.connections:
                    raise ExchangeError("Market stream connection failed")
                api.on_connection("open", opened)
                for event in ("close", "error", "reconnect"):
                    api.on_connection(event, self._invalidate)
                stream = await api.ticker(symbol=self.symbol.lower())
                stream.on("message", self.on_message)
                while not self._stop.is_set():
                    # EOF is handled in an SDK background task, not by this try.
                    # Reuse SDK reconnection to preserve subscriptions/callbacks;
                    # do not race its scheduled 23h replacement.
                    if not api.reconnect_tasks and (
                        not connection.is_open
                        or monotonic() - self._last_message_at
                        > self.cfg.max_market_age_seconds
                    ):
                        self._invalidate()
                        if connection.reconnect_emitted:
                            break  # SDK's scheduled recovery exhausted its retries.
                        connection = await api.reconnect(connection, api.configuration)
                        if connection is None:
                            break  # SDK exhausted retries and removed the subscription.
                    await asyncio.sleep(0.2)
            except Exception as exc:
                self._record_error("connection", exc)
                self._invalidate()
            finally:
                # Cleanup operations fail independently. Cancellation remains a
                # BaseException and propagates, including during true shutdown.
                try:
                    if connection is not None:
                        _forget_connection_streams(connection)
                except Exception as exc:
                    self._record_error("subscription cleanup", exc)
                try:
                    await asyncio.wait_for(
                        api.close_connection(close_session=True), timeout=5
                    )
                except Exception as exc:
                    self._record_error("connection cleanup", exc)
            if not self._stop.is_set():
                await asyncio.sleep(1)

    def health(self):
        return {
            "running": self._thread is not None and self._thread.is_alive(),
            "last_message_age_seconds": monotonic() - self._last_message_at,
            "last_error": self.last_error,
        }

    def quote(self):
        if self._thread is not None and not self._thread.is_alive():
            raise ExchangeError("Market stream worker exited")
        with self._lock:
            if self._latest is None:
                raise ExchangeError("No current market stream snapshot")
            return self._latest

    def pending_risk(self):
        with self._lock:
            if self._leased_risk is None:
                self._leased_risk, self._risk = self._risk, None
            return self._leased_risk

    def acknowledge_risk(self, event):
        with self._lock:
            if self._leased_risk is event:
                self._leased_risk = None

    def close(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=8)
