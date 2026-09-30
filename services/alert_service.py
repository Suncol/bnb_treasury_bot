import json
import logging
from threading import Event, Lock, Thread
from urllib.request import Request, urlopen

from storage.repository import Repository


class LogSink:
    name = "log"

    def send(self, alert, key):
        logging.getLogger("bnb_treasury").warning(
            "%s %s: %s [%s]", alert.level, alert.code, alert.message, key
        )


class WebhookSink:
    def __init__(self, name, url):
        if not url.startswith("https://"):
            raise ValueError("Alert webhooks require HTTPS")
        self.name, self.url = name, url

    def send(self, alert, key):
        body = json.dumps(
            {"level": alert.level, "code": alert.code, "message": alert.message}
        ).encode()
        request = Request(
            self.url,
            data=body,
            headers={"Content-Type": "application/json", "Idempotency-Key": key},
            method="POST",
        )
        with urlopen(request, timeout=5) as response:
            if not 200 <= response.status < 300:
                raise OSError("Alert delivery failed")


class AlertService:
    def __init__(self, repository, sinks):
        self.repository, self.sinks = repository, tuple(sinks)
        if len({s.name for s in self.sinks}) != len(self.sinks):
            raise ValueError("Alert sink names must be unique")
        self._outbox = (
            Repository(repository.path) if repository.path != ":memory:" else repository
        )
        self._wake, self._stop = Event(), Event()
        self._start_lock, self._delivery_lock = Lock(), Lock()
        self._thread = None

    def notify(self):
        """Wake a single outbox worker; notification I/O never runs on the caller."""
        with self._start_lock:
            if self._stop.is_set():
                return
            if self._thread is None:
                self._thread = Thread(
                    target=self._run, name="alert-outbox", daemon=True
                )
                self._thread.start()
        self._wake.set()

    def close(self):
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join()
        if self._outbox is not self.repository:
            self._outbox.close()

    def _run(self):
        while not self._stop.is_set():
            self._wake.wait(5)
            self._wake.clear()
            if self._stop.is_set():
                return
            try:
                self.deliver()
            except Exception:
                logging.getLogger("bnb_treasury").exception(
                    "Alert outbox processing failed"
                )

    def deliver(self):
        with self._delivery_lock:
            # This worker owns an independent connection for short local reads
            # and acknowledgements. Never take the account execution lock.
            with self._outbox.local_access():
                pending = self._outbox.pending_alerts()
            for alert_id, alert in pending:
                if self._stop.is_set():
                    return
                key = f"alert:{alert_id}"
                with self._outbox.local_access():
                    delivered = set(self._outbox.load(key, ()))
                for sink in self.sinks:
                    if self._stop.is_set():
                        return
                    if sink.name in delivered:
                        continue
                    try:
                        sink.send(alert, key)
                    except Exception:
                        logging.getLogger("bnb_treasury").error(
                            "Alert delivery failed for sink %s", sink.name
                        )
                        continue
                    delivered.add(sink.name)
                    with self._outbox.local_access():
                        self._outbox.save(key, tuple(sorted(delivered)))
                if self.sinks and all(s.name in delivered for s in self.sinks):
                    with self._outbox.local_access():
                        self._outbox.mark_delivered(alert_id)
