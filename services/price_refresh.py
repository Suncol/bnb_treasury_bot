"""Single in-flight public-price read; the controller never waits for its I/O."""

import logging
import time
from threading import Lock, Thread

from core.time_utils import fresh
from .exchange_adapter import ExchangeError, MarketDataPending


class PriceRefresh:
    def __init__(self, *, clock, lifetime, close_client, monotonic=time.monotonic):
        self.clock, self.lifetime = clock, lifetime
        self.monotonic, self.close_client = monotonic, close_client
        self._lock = Lock()
        self._key = self._fetch = self._value = self._error = None
        self._thread = None
        self._started = None
        self._received_at = None
        self._generation = 0
        self._closed = False
        self.completed = self.discarded = 0

    def read(self, key, fetch):
        with self._lock:
            if key != self._key:
                self._generation += 1
                self._key, self._fetch = key, fetch
                self._value = self._error = self._started = None
                self._received_at = None
            if self._received_at is not None and self.clock() < self._received_at:
                self._generation += 1
                self._value = self._started = self._received_at = None
        self.kick()
        with self._lock:
            if self._value is None:
                raise MarketDataPending("Public price observation is pending")
            # Cache hits keep the acquisition timestamp, including after expiry.
            # The planner and execution gates still enforce the original TTL.
            return self._value

    def kick(self):
        with self._lock:
            if self._closed:
                raise MarketDataPending("Public price refresher is closed")
            if self._error is not None:
                error, self._error = self._error, None
                raise error  # Hand off once, on the controller's failure boundary.
            if self._fetch is None or (self._thread and self._thread.is_alive()):
                return
            if self._started is not None and self.monotonic() - self._started < min(1, self.lifetime / 3):
                return
            self._started = self.monotonic()
            self._thread = Thread(target=self._run, args=(self._generation, self._fetch),
                                  name="bnb-public-price", daemon=True)
            self._thread.start()

    def _run(self, generation, fetch):
        started = self.monotonic()
        try:
            value = fetch()
            if not fresh(value[2], self.clock(), self.lifetime):
                raise ExchangeError("Public price refresh expired before completion",
                                    endpoint="reference_price")
            error = None
        except Exception as exc:
            value, error = None, exc
        with self._lock:
            discarded = self._closed or generation != self._generation
            if discarded:
                self.discarded += 1
            elif error is not None:
                self._error = error
            else:
                self._value = value
                self._received_at = self.clock()
                self.completed += 1
            closed = self._closed
        elapsed = self.monotonic() - started
        logging.getLogger("bnb_treasury.market").log(
            logging.WARNING if error else logging.INFO if elapsed >= 1 else logging.DEBUG,
            "PUBLIC_PRICE_REFRESH elapsed_seconds=%.6f observed_at=%s discarded=%s error=%s",
            elapsed, value[2].isoformat() if value else None, discarded,
            type(error).__name__ if error else None,
        )
        if closed:
            self.close_client()

    def invalidate(self):
        with self._lock:
            self._generation += 1
            self._value = self._error = self._started = self._received_at = None

    def close(self):
        with self._lock:
            self._closed = True
            thread = self._thread
        if thread is not None:
            thread.join(timeout=0.05)
        if thread is None or not thread.is_alive():
            self.close_client()
