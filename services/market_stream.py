import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from threading import Event, Lock, Thread
from time import monotonic

from binance_common.configuration import ConfigurationWebSocketStreams
from binance_common.websocket import _forget_connection_streams
from binance_sdk_spot.spot import Spot
from binance_sdk_spot.websocket_streams.models import (
    DiffBookDepthUpdateSpeedEnum, PartialBookDepthLevelsEnum,
    PartialBookDepthUpdateSpeedEnum,
)

from core.crash_guard import latch_guard_trigger
from core.models import CrashGuardState, MarketSnapshot

from .sdk_transport import sdk_data
from .exchange_adapter import ExchangeError
from .market_data import MarketWindows
from .market_depth import DepthSnapshots


class MarketStream:
    """Timestamped full depth observations, sampled independently of REST."""

    def __init__(self, symbol, cfg, *, client=None):
        self.symbol, self.cfg = symbol, cfg
        self.client = client or Spot(
            config_ws_streams=ConfigurationWebSocketStreams(reconnect_delay=1000)
        )
        self._stop, self._lock = Event(), Lock()
        self._latest = None
        self._execution_quote = None
        # Two bounded slots: one being persisted, one collecting newer triggers.
        self._risk = self._leased_risk = None
        self._thread = None
        self.windows = MarketWindows(cfg)
        self.depth = DepthSnapshots(symbol, cfg.max_market_age_seconds)
        self._last_message_at = monotonic()
        self._last_frame_at = self._last_message_at
        self._last_ping_at = None
        self._last_sample_source = None
        self._frames = self._matched_quotes = self._reconnects = 0
        self._last_health_log = 0
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
            now = datetime.now(timezone.utc)
            with self._lock:
                self._frames += 1
                self._last_frame_at = monotonic()
                execution = self.depth.update(event, now)
                if execution is None:
                    return
                self._last_message_at = monotonic()
                self._matched_quotes += 1
                self._execution_quote = execution
                # Matching runs at 100ms; retain the configured sampling cadence
                # for smoothing and drawdowns. Every sample has a new source E.
                if (self._last_sample_source is not None
                        and execution.ts - self._last_sample_source < timedelta(seconds=self.cfg.check_seconds)):
                    return
                quote = MarketSnapshot(
                    execution.ts, self.symbol, execution.best_bid, execution.best_ask,
                    (execution.best_bid + execution.best_ask) / 2,
                    (execution.best_bid + execution.best_ask) / 2,
                    Decimal("0"), Decimal("0"), quote_ts=execution.ts,
                )
                previous = self.windows.continuity
                previous_source = self.windows.last_source_ts
                previous_received = self.windows.raw[-1][0] if self.windows.raw else None
                sampled = self.windows.update(quote, now)
                self._last_sample_source = execution.ts
                self._latest = sampled
                risk = latch_guard_trigger(
                    self._risk or CrashGuardState(), sampled, now, self.cfg
                )
                self._risk = risk if risk.active else None
                if previous != self.windows.continuity:
                    self._diagnostic("SAMPLE_RESET", previous_source_at=previous_source,
                                     previous_received_at=previous_received, source_at=execution.ts,
                                     received_at=now, continuity=self.windows.continuity)
        except Exception as exc:
            self._record_error("depth", exc)
            self._invalidate("INVALID_DEPTH")

    def _diagnostic(self, reason, **fields):
        logging.getLogger("bnb_treasury.market").info(
            "MARKET_STREAM %s", json.dumps({"reason": reason, "symbol": self.symbol,
                "quote_silence_seconds": monotonic() - self._last_message_at,
                "frame_silence_seconds": monotonic() - self._last_frame_at,
                "ping_age_seconds": monotonic() - self._last_ping_at if self._last_ping_at is not None else None,
                "frames": self._frames, "matched_quotes": self._matched_quotes,
                "reconnects": self._reconnects, **fields}, default=str),
        )

    def _ping(self, *_):
        self._last_ping_at = monotonic()

    def _record_error(self, stage, exc):
        self.last_error = {"stage": stage, "type": type(exc).__name__}
        logging.getLogger("bnb_treasury").warning(
            "Market stream %s failed", stage, exc_info=True
        )

    def _invalidate(self, reason="RESET"):
        with self._lock:
            self._diagnostic(reason, last_source_at=self.depth.last_source_at,
                             last_update_id=self.depth.last_update_id)
            self.windows = MarketWindows(self.cfg)
            self.depth = DepthSnapshots(self.symbol, self.cfg.max_market_age_seconds)
            self._last_sample_source = None
            self._latest = None
            self._execution_quote = None

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
                self._invalidate("CONNECTED")

            try:
                await api.create_connection()
                if not api.connections:
                    raise ExchangeError("Market stream connection failed")
                api.on_connection("open", opened)
                for event in ("close", "error", "reconnect"):
                    api.on_connection(event, lambda *_, reason=event: self._invalidate(reason.upper()))
                api.on_connection("ping", self._ping)
                api.on_connection("pong", self._ping)
                snapshots = await api.partial_book_depth(
                    symbol=self.symbol.lower(), levels=PartialBookDepthLevelsEnum.LEVELS_5.value,
                    update_speed=PartialBookDepthUpdateSpeedEnum.UPDATE_SPEED_100ms.value,
                )
                snapshots.on("message", self.on_message)
                events = await api.diff_book_depth(
                    symbol=self.symbol.lower(), update_speed=DiffBookDepthUpdateSpeedEnum.UPDATE_SPEED_100ms.value,
                )
                events.on("message", self.on_message)
                while not self._stop.is_set():
                    # EOF is handled in an SDK background task, not by this try.
                    # Reuse SDK reconnection to preserve subscriptions/callbacks;
                    # do not race its scheduled 23h replacement.
                    if not api.reconnect_tasks and (
                        not connection.is_open
                        or monotonic() - self._last_message_at
                        > self.cfg.max_market_age_seconds
                    ):
                        self._reconnects += 1
                        self._invalidate("SOCKET_CLOSED" if not connection.is_open else "QUOTE_SILENCE")
                        if connection.reconnect_emitted:
                            break  # SDK's scheduled recovery exhausted its retries.
                        connection = await api.reconnect(connection, api.configuration)
                        if connection is None:
                            break  # SDK exhausted retries and removed the subscription.
                    if monotonic() - self._last_health_log >= 60:
                        self._last_health_log = monotonic()
                        self._diagnostic("HEALTH", last_source_at=self.depth.last_source_at,
                                         last_update_id=self.depth.last_update_id,
                                         pending_snapshots=len(self.depth.snapshots), pending_events=len(self.depth.events))
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
            "source": "depth5+depth@100ms",
            "frames": self._frames, "matched_quotes": self._matched_quotes,
            "reconnects": self._reconnects,
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

    def execution_quote(self):
        if self._thread is not None and not self._thread.is_alive():
            raise ExchangeError("Market stream worker exited")
        with self._lock:
            return self._execution_quote

    def acknowledge_risk(self, event):
        with self._lock:
            if self._leased_risk is event:
                self._leased_risk = None

    def close(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=8)
