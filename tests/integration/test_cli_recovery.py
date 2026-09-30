from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from services import cli
from services.exchange_adapter import ExchangeError
from storage.repository import Repository


@pytest.mark.parametrize("phase", ["authentication", "binding", "import", "initialize"])
@pytest.mark.parametrize("failure", [429, 418, 503, "timeout"])
def test_startup_and_maintenance_persist_transport_backoff(tmp_path, monkeypatch, caplog, phase, failure):
    now = datetime(2026, 9, 30, tzinfo=timezone.utc)
    calls, waits = [], []
    path = tmp_path / "startup.db"

    class Clock:
        @staticmethod
        def now(tz):
            return now

    class Stop:
        def set(self):
            pass

        def wait(self, seconds):
            waits.append(seconds)
            return True  # Supervisor shutdown while waiting; no network.

    class Adapter:
        request_not_before = None

        def __init__(self, *args, **kwargs):
            pass

        def fail(self):
            if failure in {429, 418}:
                self.request_not_before = now + timedelta(seconds=120)
                raise ExchangeError("limited", http_status=failure, retry_after=120)
            if failure == 503:
                raise ExchangeError("unavailable", http_status=503)
            raise TimeoutError()

        def account_identity(self):
            calls.append("identity")
            if phase == "authentication":
                self.fail()
            return {"account": "test-account"}

    def reconciler(exchange, repo, cfg):
        def action(*args, **kwargs):
            calls.append("maintenance")
            exchange.fail()
        return SimpleNamespace(bind_transfer_id=action, import_spot_trades=action,
                               initialize_spot_baseline=action)

    monkeypatch.setattr(cli, "datetime", Clock)
    monkeypatch.setattr(cli, "Event", Stop)
    monkeypatch.setattr(cli.signal, "signal", lambda *_: None)
    monkeypatch.setattr(cli, "BinanceAdapter", Adapter)
    monkeypatch.setattr(cli, "Reconciler", reconciler)
    extra = {
        "authentication": [], "binding": ["--bind-transfer-id", "local", "exchange"],
        "import": ["--import-spot-trades", "ETHUSDT", "--operator", "test", "--reason", "repair"],
        "initialize": ["--initialize-spot-baseline", "--operator", "test", "--reason", "quiescence"],
    }[phase]
    monkeypatch.setattr(cli.os.sys, "argv", ["cli", "--db", str(path), "--once", *extra])
    with pytest.raises((ExchangeError, TimeoutError)):
        cli.main()
    with_repo = Repository(path)
    deadline = with_repo.load("transport_backoff")["retry_at"]
    assert deadline >= now + timedelta(seconds=120 if failure in {429, 418} else 2)
    if phase == "authentication":
        assert with_repo.load("account_identity") is None
        assert with_repo.runtime().read_failures == 0
    else:
        assert with_repo.runtime().read_retry_at == deadline
    with_repo.close()
    before_calls = list(calls)
    for _ in range(3):
        cli.main()
    assert calls == before_calls
    assert len(waits) == 3
    assert "retry at" in caplog.text


def test_status_reports_transport_failure_without_authentication(tmp_path, monkeypatch, capsys):
    path = tmp_path / "status.db"
    repo = Repository(path)
    repo.save("transport_backoff", {"failures": 1, "retry_at": datetime.now(timezone.utc)})
    repo.close()
    monkeypatch.setattr(cli.os.sys, "argv", ["cli", "--db", str(path), "--status"])
    monkeypatch.setattr(cli, "BinanceAdapter", lambda *a, **k: pytest.fail("offline status authenticated"))
    cli.main()
    assert "transport_backoff" in capsys.readouterr().out
