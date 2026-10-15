"""python -m services.cli --help. --live trades on Binance; --paper executes virtually."""

import argparse
import logging
import os
import signal
import time
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
from threading import Event

from core.config import load_config, load_paper_config
from core.models import RunMode
from storage.codec import dumps
from storage.repository import Repository
from storage.paper_ledger import PaperLedger, check_environment

from .alert_service import AlertService, LogSink, WebhookSink
from .binance_adapter import BinanceAdapter
from .exchange_adapter import AccountObservationInconsistent, ExchangeError, RequestDeferred
from .executor import Executor
from .market_stream import MarketStream
from .paper_exchange import PaperExchange
from .read_backoff import clear_read_backoff, record_read_failure, retry_deadline
from .reconciliation import Reconciler
from .runner import Runner


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config/strategy.toml")
    parser.add_argument("--db", help="Execution journal; defaults to var/paper.sqlite3 in paper, var/treasury.sqlite3 otherwise")
    environment = parser.add_mutually_exclusive_group()
    environment.add_argument(
        "--live",
        action="store_true",
        help="Enable actual Binance order, cancel and transfer requests",
    )
    environment.add_argument("--paper", action="store_true", help="Execute using public quotes and a persistent virtual account")
    parser.add_argument("--paper-config", help="Paper parameters; defaults to config/paper.toml, requires --paper")
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
        "--operator", help="Operator identity recorded with evidence maintenance"
    )
    parser.add_argument("--reason", help="Audit reason for evidence maintenance")
    parser.add_argument(
        "--initialize-spot-baseline",
        action="store_true",
        help="Attest that external spot activity is stopped and initialize a new ledger; never resets a checkpoint",
    )
    args = parser.parse_args()
    if args.paper_config and not args.paper:
        parser.error("--paper-config requires --paper")
    if args.paper and (args.initialize_spot_baseline or args.import_spot_trades):
        parser.error("Paper accounts initialize atomically; external spot trade import is unsupported")
    args.db = args.db or ("var/paper.sqlite3" if args.paper else "var/treasury.sqlite3")
    if any(
        not value.is_finite() or value < 0
        for value in (args.min_transfer_bnb, args.min_transfer_usd)
    ):
        parser.error("Transfer minima must be finite and nonnegative")
    if sum(bool(x) for x in (
        args.bind_transfer_id, args.import_spot_trades, args.initialize_spot_baseline
    )) > 1:
        parser.error("Use one maintenance action at a time")
    if (args.import_spot_trades or args.initialize_spot_baseline) and (
        not args.operator or not args.reason
    ):
        parser.error("Evidence maintenance requires --operator and --reason")
    cfg = load_config(args.config)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    stop = Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    repository = Repository(args.db, readonly=args.paper and args.status)
    if args.status:
        try:
            if args.paper:
                repository.db.execute("BEGIN")  # One consistent WAL snapshot while the controller writes.
            check_environment(repository, paper=args.paper)
            status = {
                "runtime": repository.runtime(),
                "unresolved": repository.operations(unresolved_only=True),
                "spot_settlement_issue": repository.load("spot_settlement_issue"),
                "account_identity": repository.load("account_identity"),
                "transport_backoff": repository.load("transport_backoff"),
                "futures_settlement_issue": repository.load("futures_settlement_issue"),
                "storage": repository.health(),
            }
            if args.paper:
                status["paper"] = PaperLedger.status(repository)
            print(dumps(status))
        finally:
            repository.close()
        return
    feed = alerts = None
    exchange = None
    authenticated = False
    try:
        # Read-only status above does not participate in execution ownership.
        # Every writer/maintenance command takes the same lifetime lease.
        repository.acquire_controller()
        check_environment(repository, paper=args.paper)
        if args.paper:
            exchange = PaperExchange(
                repository, cfg, load_paper_config(args.paper_config or "config/paper.toml"),
                min_transfer_bnb=args.min_transfer_bnb, min_transfer_usd=args.min_transfer_usd,
            )
        else:
            exchange = BinanceAdapter(
                os.environ.get("BINANCE_API_KEY", ""),
                os.environ.get("BINANCE_API_SECRET", ""), cfg, live=args.live,
                min_transfer_bnb=args.min_transfer_bnb, min_transfer_usd=args.min_transfer_usd,
            )
        exchange.request_not_before = retry_deadline(repository)
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
        authenticated = True
        if args.initialize_spot_baseline:
            Reconciler(exchange, repository, cfg).initialize_spot_baseline(
                operator=args.operator,
                reason=args.reason,
                now=datetime.now(timezone.utc),
            )
            clear_read_backoff(repository)
            return
        if args.bind_transfer_id:
            Reconciler(exchange, repository, cfg).bind_transfer_id(
                *args.bind_transfer_id, datetime.now(timezone.utc)
            )
            clear_read_backoff(repository)
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
            clear_read_backoff(repository)
            return
        feed = MarketStream(cfg.symbol, cfg.crash_guard,
                            transport=exchange.market if args.paper else exchange).start()
        exchange.feed = feed
        if hasattr(exchange, "start_price_refresh"):
            exchange.start_price_refresh()
        sinks = [LogSink(prefix="PAPER " if args.paper else "")]
        if not args.paper and os.environ.get("ALERT_WEBHOOK_URL"):
            sinks.append(WebhookSink("webhook", os.environ["ALERT_WEBHOOK_URL"]))
        alerts = AlertService(repository, sinks)
        runner = Runner(exchange, repository, cfg, clock=exchange.clock,
                        alerts=alerts, execute=args.live or args.paper)
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
            try:
                if args.paper:
                    exchange.advance()
                runner.tick()
            except BlockingIOError:
                logging.getLogger("bnb_treasury").warning("Account lock busy; deferring paper tick")
            if args.once:
                break
            stop.wait(
                max(0, cfg.crash_guard.check_seconds - (time.monotonic() - started))
            )
    except (ExchangeError, TimeoutError, ConnectionError) as exc:
        record_read_failure(
            repository, exc, datetime.now(timezone.utc),
            authenticated=authenticated, exchange=exchange,
        )
        if authenticated and not isinstance(exc, (RequestDeferred, AccountObservationInconsistent)):
            Executor(exchange, repository, cfg).note_failure(reason=type(exc).__name__)
        raise
    finally:
        if exchange is not None and hasattr(exchange, "close_price_refresh"):
            exchange.close_price_refresh()
        if feed is not None:
            feed.close()
        if alerts is not None:
            alerts.close()
        repository.close()


if __name__ == "__main__":
    main()
