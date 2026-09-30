"""python -m services.cli --help. Does not trade unless --live is supplied."""

import argparse
import logging
import os
import signal
import time
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
from threading import Event

from core.config import load_config
from core.models import RunMode
from storage.codec import dumps
from storage.repository import Repository

from .alert_service import AlertService, LogSink, WebhookSink
from .binance_adapter import BinanceAdapter
from .market_stream import MarketStream
from .reconciliation import Reconciler
from .runner import Runner


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
    parser.add_argument(
        "--import-spot-trades",
        nargs="+",
        metavar="SYMBOL",
        help="Verify external trade receipts since the frozen balance checkpoint; exits without trading",
    )
    parser.add_argument(
        "--operator", help="Operator identity recorded with evidence import"
    )
    parser.add_argument("--reason", help="Audit reason for evidence import")
    args = parser.parse_args()
    if any(
        not value.is_finite() or value < 0
        for value in (args.min_transfer_bnb, args.min_transfer_usd)
    ):
        parser.error("Transfer minima must be finite and nonnegative")
    if args.bind_transfer_id and args.import_spot_trades:
        parser.error("Use one maintenance action at a time")
    if args.import_spot_trades and (not args.operator or not args.reason):
        parser.error("Evidence import requires --operator and --reason")
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
                        "spot_settlement_issue": repository.load(
                            "spot_settlement_issue"
                        ),
                        "account_identity": repository.load("account_identity"),
                        "storage": repository.health(),
                    }
                )
            )
        finally:
            repository.close()
        return
    feed = alerts = None
    try:
        # Read-only status above does not participate in execution ownership.
        # Every writer/maintenance command takes the same lifetime lease.
        repository.acquire_controller()
        exchange = BinanceAdapter(
            os.environ.get("BINANCE_API_KEY", ""),
            os.environ.get("BINANCE_API_SECRET", ""),
            cfg,
            live=args.live,
            min_transfer_bnb=args.min_transfer_bnb,
            min_transfer_usd=args.min_transfer_usd,
        )
        exchange.request_not_before = repository.runtime().read_retry_at
        # Restart and mode changes must honor the saved cooldown before even
        # authenticating the database identity. Shutdown remains interruptible.
        if exchange.request_not_before is not None:
            delay = (
                exchange.request_not_before - datetime.now(timezone.utc)
            ).total_seconds()
            if delay > 0:
                logging.getLogger("bnb_treasury").warning(
                    "Waiting until %s before authenticating account identity",
                    exchange.request_not_before,
                )
                if stop.wait(delay):
                    return
        repository.bind_identity(
            exchange.account_identity(),
            sha256(dumps(cfg).encode()).hexdigest(),
            datetime.now(timezone.utc),
        )
        if args.bind_transfer_id:
            Reconciler(exchange, repository, cfg).bind_transfer_id(
                *args.bind_transfer_id, datetime.now(timezone.utc)
            )
            return
        if args.import_spot_trades:
            Reconciler(exchange, repository, cfg).import_spot_trades(
                args.import_spot_trades,
                operator=args.operator,
                reason=args.reason,
                now=datetime.now(timezone.utc),
            )
            print(
                dumps(
                    {
                        "remaining_issue": repository.load("spot_settlement_issue"),
                        "unresolved": repository.operations(unresolved_only=True),
                    }
                )
            )
            return
        feed = MarketStream(cfg.symbol, cfg.crash_guard).start()
        exchange.feed = feed
        sinks = [LogSink()]
        if os.environ.get("ALERT_WEBHOOK_URL"):
            sinks.append(WebhookSink("webhook", os.environ["ALERT_WEBHOOK_URL"]))
        alerts = AlertService(repository, sinks)
        runner = Runner(exchange, repository, cfg, alerts=alerts, execute=args.live)
        if args.mode:
            runner.set_run_mode(RunMode(args.mode))
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
        if feed is not None:
            feed.close()
        if alerts is not None:
            alerts.close()
        repository.close()


if __name__ == "__main__":
    main()
