"""python -m services.cli --help. Does not trade unless --live is supplied."""

import argparse
from decimal import Decimal
import logging
import os
from threading import Event
import signal
import time
from datetime import datetime, timezone

from core.config import load_config
from core.models import RunMode
from storage.repository import Repository
from storage.codec import dumps
from .alert_service import AlertService, LogSink, WebhookSink
from .binance_adapter import BinanceAdapter
from .market_stream import MarketStream
from .runner import Runner
from .reconciliation import Reconciler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config/strategy.toml")
    parser.add_argument("--db", default="var/treasury.sqlite3")
    parser.add_argument(
        "--live",
        action="store_true",
        help="Enable actual Binance order, cancel and transfer requests",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Reconcile and evaluate once; market windows still require warmup",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Print persisted runtime and unresolved operations without contacting Binance",
    )
    parser.add_argument(
        "--mode",
        choices=[m.value for m in RunMode],
        help="Explicitly set/reset the persisted run mode",
    )
    parser.add_argument("--min-transfer-bnb", type=Decimal, default=Decimal("0.1"))
    parser.add_argument("--min-transfer-usd", type=Decimal, default=Decimal("0"))
    parser.add_argument(
        "--bind-transfer-id",
        nargs=2,
        metavar=("CLIENT_ID", "TRANSFER_ID"),
        help="Verify and bind an operator-identified transfer; exits without trading",
    )
    args = parser.parse_args()
    if args.min_transfer_bnb < 0 or args.min_transfer_usd < 0:
        parser.error("Transfer minima must be nonnegative")
    cfg = load_config(args.config)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    stop = Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    repository = Repository(args.db)
    if args.status:
        try:
            print(
                dumps(
                    {
                        "runtime": repository.runtime(),
                        "unresolved": repository.operations(unresolved_only=True),
                    }
                )
            )
        finally:
            repository.close()
        return
    feed = (
        None
        if args.bind_transfer_id
        else MarketStream(cfg.symbol, cfg.crash_guard).start()
    )
    exchange = BinanceAdapter(
        os.environ.get("BINANCE_API_KEY", ""),
        os.environ.get("BINANCE_API_SECRET", ""),
        cfg,
        live=args.live,
        feed=feed,
        min_transfer_bnb=args.min_transfer_bnb,
        min_transfer_usd=args.min_transfer_usd,
    )
    if args.bind_transfer_id:
        try:
            Reconciler(exchange, repository, cfg).bind_transfer_id(
                *args.bind_transfer_id, datetime.now(timezone.utc)
            )
        finally:
            repository.close()
        return
    sinks = [LogSink()]
    if os.environ.get("ALERT_WEBHOOK_URL"):
        sinks.append(WebhookSink("webhook", os.environ["ALERT_WEBHOOK_URL"]))
    alerts = AlertService(repository, sinks)
    runner = Runner(
        exchange,
        repository,
        cfg,
        alerts=alerts,
        execute=args.live,
    )
    if args.mode:
        runner.set_run_mode(RunMode(args.mode))
    try:
        # Wait for one stream quote; window warmup happens in normal ticks.
        for _ in range(50):
            try:
                feed.quote()
                break
            except Exception:
                if stop.wait(0.1):
                    return
        while not stop.is_set():
            started = time.monotonic()
            runner.tick()
            if args.once:
                break
            stop.wait(
                max(0, cfg.crash_guard.check_seconds - (time.monotonic() - started))
            )
    finally:
        feed.close()
        alerts.close()
        repository.close()


if __name__ == "__main__":
    main()
