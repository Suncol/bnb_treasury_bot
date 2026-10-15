"""Shared REST failure boundary for startup, maintenance and the controller."""

import logging
import random
import traceback
from dataclasses import replace
from datetime import timedelta

from core.time_utils import utc

from .exchange_adapter import AccountObservationInconsistent, RequestDeferred


def retry_deadline(repository):
    values = (repository.runtime().read_retry_at,
              repository.load("transport_backoff", {}).get("retry_at"))
    return max((utc(value) for value in values if value is not None), default=None)


def record_read_failure(repository, exc, now, *, authenticated=True, exchange=None):
    runtime = repository.runtime()
    transport = repository.load("transport_backoff", {})
    inconsistent = isinstance(exc, AccountObservationInconsistent)
    failures = max(transport.get("failures", 0), runtime.read_failures)
    failures += not isinstance(exc, (RequestDeferred, AccountObservationInconsistent))
    observation_retries = transport.get("observation_retries", 0) + 1 if inconsistent else 0
    delay = max(float(getattr(exc, "retry_after", None) or 0),
                min(60, 2 ** min(max(failures, observation_retries), 6)))
    retry_at = utc(now) + timedelta(seconds=delay + random.uniform(0, 1))
    for deadline in (retry_deadline(repository), getattr(exchange, "request_not_before", None)):
        if deadline is not None:
            retry_at = max(retry_at, utc(deadline))
    # This key contains no account identity or trading state. Authentication can
    # fail before business-journal ownership has been verified.
    backoff = {"failures": failures, "retry_at": retry_at}
    if inconsistent:
        backoff["observation_retries"] = observation_retries
    repository.save("transport_backoff", backoff)
    if authenticated:
        repository.save("runtime", replace(runtime, read_failures=failures, read_retry_at=retry_at))
        if inconsistent:
            issues = dict(repository.load("futures_settlement_issue") or {})
            issues["observation"] = {"reason": str(exc)}
            repository.save("futures_settlement_issue", issues)
    event = (
        "ACCOUNT_INCONSISTENT" if inconsistent else
        "REST_DEFERRED" if isinstance(exc, RequestDeferred) else "API_ERROR"
    )
    repository.event(now, event, {
        "type": type(exc).__name__, "endpoint": getattr(exc, "endpoint", None),
        "http_status": getattr(exc, "http_status", None), "code": getattr(exc, "code", None),
        "retry_at": retry_at, "authenticated": authenticated,
        "request_weights": getattr(exc, "request_weights", None),
        "trace": tuple(f"{f.filename}:{f.lineno}:{f.name}" for f in traceback.extract_tb(exc.__traceback__)),
    })
    logging.getLogger("bnb_treasury").warning(
        "%s %s endpoint=%s status=%s; retry at %s (authenticated=%s)",
        "Account observation deferred" if inconsistent else "REST failure",
        type(exc).__name__, getattr(exc, "endpoint", None),
        getattr(exc, "http_status", None), retry_at, authenticated,
    )
    return retry_at


def clear_read_backoff(repository):
    if repository.load("transport_backoff"):
        repository.save("transport_backoff", {})
    runtime = repository.runtime()
    if runtime.read_failures or runtime.read_retry_at:
        repository.save("runtime", replace(runtime, read_failures=0, read_retry_at=None))
