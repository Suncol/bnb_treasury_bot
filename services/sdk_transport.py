"""Shared official-SDK transport, errors, cache and request budgets."""

import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from threading import RLock

from binance_common.errors import (
    BadRequestError, ForbiddenError, NotFoundError, RateLimitBanError,
    ServerError, TooManyRequestsError, UnauthorizedError, Error as SDKError,
)

from core.time_utils import utc
from .exchange_adapter import ExchangeError, RequestDeferred

D = Decimal


def millis(value):
    return int(utc(value).timestamp() * 1000)


def timestamp(value):
    return datetime.fromtimestamp(int(value) / 1000, timezone.utc)


class BinanceAPIError(ExchangeError):
    def __init__(self, code, *, message="", definitive=False, **metadata):
        self.code, self.definitive = code, definitive
        self.message = message
        super().__init__(
            f"Binance SDK error code {code}: {message}", code=code, **metadata
        )


def sdk_data(value):
    """Normalize SDK models (including oneOf/list responses) at this boundary."""
    if isinstance(value, (tuple, list)):
        return [sdk_data(item) for item in value]
    if hasattr(value, "to_dict"):
        return sdk_data(value.to_dict())
    if isinstance(value, dict):
        return {key: sdk_data(item) for key, item in value.items()}
    return value


class SDKTransport:
    def __init__(self, *, clock=None):
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.cache = {}
        self.request_not_before = None
        self.request_weights = {}
        self.weight_limits = {}
        self.weight_usage = {}
        self.read_checkpoint = None
        self._budget_lock = RLock()

    def ensure_request_allowed(self, *, protective=False, scope=None):
        with self._budget_lock:
            return self._ensure_request_allowed(protective=protective, scope=scope)

    def _ensure_request_allowed(self, *, protective=False, scope=None):
        now = self.clock()
        if self.request_not_before is not None and now < self.request_not_before:
            raise RequestDeferred(
                "Exchange request cooldown",
                retry_after=(self.request_not_before - now).total_seconds(),
            )
        if not protective:
            for (service, header), (used, expires) in self.weight_usage.items():
                limit = self.weight_limits.get((service, header))
                if (
                    (scope is None or service == scope)
                    and limit
                    and now < expires
                    and used >= limit * D("0.9")
                ):
                    raise RequestDeferred(
                        "Request budget reserved for protection",
                        endpoint=service,
                        retry_after=(expires - now).total_seconds(),
                        request_weights=self.request_weights,
                    )

    def _observe_request_budget(self, scope, headers, data):
        with self._budget_lock:
            self._observe_budget(scope, headers, data)

    def _observe_budget(self, scope, headers, data):
        periods = {
            "SECOND": (1, "s"),
            "MINUTE": (60, "m"),
            "HOUR": (3600, "h"),
            "DAY": (86400, "d"),
        }
        if isinstance(data, dict):
            for rule in data.get("rateLimits", []) or []:
                kind, interval = rule.get("rateLimitType"), rule.get("interval")
                if kind not in {"REQUEST_WEIGHT", "ORDERS"} or interval not in periods:
                    continue
                prefix = (
                    "x-mbx-used-weight-"
                    if kind == "REQUEST_WEIGHT"
                    else "x-mbx-order-count-"
                )
                self.weight_limits[
                    scope, prefix + str(rule["intervalNum"]) + periods[interval][1]
                ] = int(rule["limit"])
        now = self.clock()
        for header, value in headers.items():
            if not header.startswith(("x-mbx-used-weight-", "x-mbx-order-count-")):
                continue
            suffix = header.rsplit("-", 1)[-1]
            try:
                seconds = (
                    int(suffix[:-1])
                    * {"s": 1, "m": 60, "h": 3600, "d": 86400}[suffix[-1]]
                )
                used = int(value)
                if seconds <= 0 or used < 0:
                    continue
            except (ValueError, KeyError):
                continue
            end = (int(now.timestamp()) // seconds + 1) * seconds + 1
            expires = datetime.fromtimestamp(end, timezone.utc)
            previous = self.weight_usage.get((scope, header))
            if previous is not None and previous[1] == expires:
                used = max(used, previous[0])
            self.weight_usage[scope, header] = (
                used,
                expires,
            )

    def _call(self, method, *, checkpoint=True, **params):
        endpoint = getattr(method, "__name__", type(method).__name__)
        mutation = endpoint in {"new_order", "delete_order", "user_universal_transfer"}
        if checkpoint and not mutation and self.read_checkpoint is not None:
            self.read_checkpoint()
        owner = getattr(method, "__self__", None)
        scope = (
            "futures"
            if owner is not None and owner is getattr(self, "futures", None)
            else "wallet"
            if owner is not None and owner is getattr(self, "wallet", None)
            else "spot"
        )
        self.ensure_request_allowed(protective=endpoint == "delete_order", scope=scope)
        try:
            response = method(**params)
            weights = {
                k.lower(): v
                for k, v in getattr(response, "headers", {}).items()
                if k.lower().startswith(
                    ("x-mbx-used-weight", "x-mbx-order-count", "x-sapi-used")
                )
            }
            data = sdk_data(response.data())
            with self._budget_lock:
                self.request_weights = weights
                self._observe_request_budget(scope, weights, data)
            return data
        except SDKError as exc:
            status = next(
                (
                    http
                    for cls, http in (
                        (TooManyRequestsError, 429),
                        (RateLimitBanError, 418),
                        (BadRequestError, 400),
                        (UnauthorizedError, 401),
                        (ForbiddenError, 403),
                        (NotFoundError, 404),
                    )
                    if isinstance(exc, cls)
                ),
                None,
            )
            if isinstance(exc, ServerError):
                status = getattr(exc, "status_code", None)
            retry_after = getattr(exc, "retry_after", None)
            if status in {429, 418}:
                retry_after = (
                    max(float(retry_after or 0), 1)
                    if retry_after is not None
                    else (120 if status == 418 else 60)
                )
                with self._budget_lock:
                    deadline = self.clock() + timedelta(seconds=retry_after)
                    self.request_not_before = max(deadline, self.request_not_before or deadline)
            raise BinanceAPIError(
                getattr(exc, "status_code", None),
                message=getattr(exc, "error_message", ""),
                definitive=isinstance(exc, (BadRequestError, UnauthorizedError)),
                endpoint=endpoint,
                http_status=status,
                retry_after=retry_after,
                request_weights=self.request_weights,
            ) from exc
        except Exception as exc:
            raise ExchangeError(
                f"SDK transport or response failure: {type(exc).__name__}",
                endpoint=endpoint,
            ) from exc

    def _cached(self, key, seconds, fetch):
        cached = self.cache.get(key)
        if cached is None or time.monotonic() - cached[0] >= seconds:
            self.cache[key] = (time.monotonic(), fetch())
        return self.cache[key][1]
