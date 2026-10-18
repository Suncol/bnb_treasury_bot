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
from .exchange_adapter import ExchangeError, RequestDeferred, RequestNotSent

D = Decimal
ORDER_REFRESH_SECONDS = 30
BUDGET_PERIODS = {"SECOND": (1, "s"), "MINUTE": (60, "m"),
                  "HOUR": (3600, "h"), "DAY": (86400, "d")}
# Bootstrap bounds; exchangeInfo replaces Spot/Futures limits. SAPI endpoints
# have independent IP/UID limits (Wallet General Info). Costs follow the pinned
# official SDK and Binance REST endpoint docs; unknown endpoints fail closed.
DEFAULT_WEIGHT_LIMITS = {
    ("spot", "x-mbx-used-weight-1m"): 6000,
    ("futures", "x-mbx-used-weight-1m"): 2400,
    ("wallet:query_user_universal_transfer_history", "x-sapi-used-ip-weight-1m"): 12000,
    ("wallet:user_universal_transfer", "x-sapi-used-uid-weight-1m"): 180000,
}
REQUEST_COSTS = {
    "spot": {"time": 1, "exchange_info": 20, "get_account": 20,
             "get_order": 4, "my_filters": 40, "account_commission": 20,
             "new_order": 1, "delete_order": 1, "reference_price": 2,
             "avg_price": 2, "rate_limit_order": 40},
    "futures": {"check_server_time": 1, "exchange_information": 1,
                "futures_account_balance_v3": 5, "account_information_v3": 5,
                "get_current_multi_assets_mode": 30, "get_income_history": 30},
    "wallet": {"query_user_universal_transfer_history": 1, "user_universal_transfer": 300},
}


def request_cost(scope, endpoint, params):
    if scope == "spot":
        if endpoint == "depth":
            limit = params.get("limit", 100)
            return next(cost for maximum, cost in ((100, 5), (500, 25), (1000, 50), (5000, 250))
                        if min(limit, 5000) <= maximum)
        if endpoint == "get_open_orders":
            return 6 if params.get("symbol") else 80
        if endpoint == "my_trades":
            return 5 if params.get("order_id") is not None else 20
        if endpoint in {"ticker_price", "ticker_book_ticker"}:
            return 2 if params.get("symbol") else 4
        if endpoint in {"ticker", "ticker24hr"}:
            count = 1 if params.get("symbol") else len(params.get("symbols") or ())
            if endpoint == "ticker":
                return min(count * 4, 200) if count else 200
            return 2 if 0 < count <= 20 else 40 if count <= 100 and count else 80
    try:
        return REQUEST_COSTS[scope.split(":")[0]][endpoint]
    except KeyError:
        raise ExchangeError("Unknown SDK request weight", endpoint=endpoint) from None


def budget_expiry(header, now):
    suffix = header.rsplit("-", 1)[-1]
    seconds = int(suffix[:-1]) * {"s": 1, "m": 60, "h": 3600, "d": 86400}[suffix[-1]]
    if seconds <= 0:
        raise ValueError("Invalid budget interval")
    return datetime.fromtimestamp((int(now.timestamp()) // seconds + 1) * seconds + 1, timezone.utc)


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
        self.weight_limits = dict(DEFAULT_WEIGHT_LIMITS)
        self.weight_usage = {}
        self._weight_previous = {}
        self._weight_inflight = {}
        self.read_checkpoint = None
        self._budget_lock = RLock()
        self._budget_request_id = 0
        self._order_observation_ids = {}
        self._order_refresh_busy = False
        self._order_refresh_after = 0.0

    def ensure_request_allowed(self, *, protective=False, scope=None,
                               new_order=False, request_weight=0):
        with self._budget_lock:
            return self._ensure_request_allowed(
                protective=protective, scope=scope, new_order=new_order,
                request_weight=request_weight,
            )

    def _ensure_request_allowed(self, *, protective=False, scope=None,
                                new_order=False, request_weight=0):
        now = self.clock()
        if self.request_not_before is not None and now < self.request_not_before:
            raise RequestDeferred(
                "Exchange request cooldown",
                retry_after=(self.request_not_before - now).total_seconds(),
            )
        # Check general capacity first: an ORDERS-only veto must never hide a
        # weight cooldown. No new order may borrow the protection reserve.
        protective = protective and not new_order
        budgets = sorted(self.weight_usage.keys() | self.weight_limits.keys(),
                         key=lambda key: key[1].startswith("x-mbx-order-count-"))
        for service, header in budgets:
            orders_only = header.startswith("x-mbx-order-count-")
            limit = self.weight_limits.get((service, header))
            if scope is not None and service != scope or orders_only and not new_order:
                continue
            used, expires = self.weight_usage.get((service, header), (0, budget_expiry(header, now)))
            if not limit and now >= expires:
                continue  # Expired unknown observations cannot veto metadata forever.
            if not limit:
                raise RequestDeferred("Request budget limit unavailable", endpoint=service,
                                      retry_after=(expires - now).total_seconds())
            if orders_only:
                if now >= expires:
                    used, expires = 0, budget_expiry(header, now)
            else:
                used, expires = self._weight_window((service, header), now)
                previous = self._weight_previous.get((service, header))
                if previous is not None:
                    used += previous[0]
                    expires = min(expires, previous[1])
                used += sum(cost for active_scope, cost in self._weight_inflight.values()
                            if active_scope == service)
            ceiling = limit if protective else limit * D("0.9")
            exhausted = used >= ceiling or not orders_only and used + request_weight > ceiling
            if exhausted:
                raise RequestDeferred(
                    "New order quota reserved" if orders_only else
                    "Request budget reserved for protection",
                    endpoint=service,
                    retry_after=(expires - now).total_seconds(),
                    request_weights=self.request_weights,
                    orders_only=orders_only,
                )

    def _refresh_order_budget(self, method):
        """Bounded read before financial preparation; never called by submit."""
        with self._budget_lock:
            try:
                self._ensure_request_allowed(new_order=True)
            except RequestDeferred as exc:
                if not exc.orders_only:
                    raise
                if exc.endpoint != "spot":
                    return
            else:
                return
            if self._order_refresh_busy or time.monotonic() < self._order_refresh_after:
                return
            # The count API costs 40 and cannot use the protection reserve.
            self._ensure_request_allowed(scope="spot", request_weight=40)
            self._order_refresh_busy = True
            self._order_refresh_after = time.monotonic() + ORDER_REFRESH_SECONDS
        try:
            self._call(method, recv_window=5000)
        finally:
            with self._budget_lock:
                # A slow priority checkpoint may precede the HTTP call. Space
                # subsequent attempts from completion as well as entry.
                self._order_refresh_after = time.monotonic() + ORDER_REFRESH_SECONDS
                self._order_refresh_busy = False

    def _budget_observation(self):
        # Called under _budget_lock at dispatch, before entering the SDK.
        self._budget_request_id += 1
        return self._budget_request_id, utc(self.clock())

    def _weight_window(self, key, now):
        """Keep the previous window's fixed grace period separate from new cost."""
        current_expiry = budget_expiry(key[1], now)
        used, expires = self.weight_usage.get(key, (0, current_expiry))
        if expires < current_expiry:
            if now < expires:
                self._weight_previous[key] = used, expires
            used = 0
        # A clock rollback cannot shorten a known window or lower its usage.
        result = used, max(expires, current_expiry)
        self.weight_usage[key] = result
        previous = self._weight_previous.get(key)
        if previous is not None and now >= previous[1]:
            del self._weight_previous[key]
        return result

    def _finish_weight_reservation(self, observation):
        """Charge even failed/unknown calls; missing headers cannot refund cost."""
        reserved = self._weight_inflight.pop(observation[0], None)
        if reserved is None:
            return
        scope, cost = reserved
        now = utc(self.clock())
        for key in self.weight_limits:
            if key[0] != scope or key[1].startswith("x-mbx-order-count-"):
                continue
            used, expires = self._weight_window(key, now)
            self.weight_usage[key] = used + cost, expires

    def _futures_request_limits(self):
        data = self._call(self.futures.exchange_information)
        if not any(rule.get("rateLimitType") == "REQUEST_WEIGHT"
                   for rule in data.get("rateLimits") or ()):
            raise ExchangeError("Futures request weight limits are missing")
        return True

    def _observe_request_budget(self, scope, headers, data, *, observation=None,
                                order_limits=None):
        with self._budget_lock:
            self._observe_budget(scope, headers, data,
                                 observation or self._budget_observation(), order_limits)

    def _observe_budget(self, scope, headers, data, observation, order_limits=None):
        periods = BUDGET_PERIODS
        limits = {}
        if isinstance(data, dict):
            for rule in data.get("rateLimits", []) or []:
                kind, interval = rule.get("rateLimitType"), rule.get("interval")
                if kind not in {"REQUEST_WEIGHT", "ORDERS"}:
                    continue
                if interval not in periods:
                    raise ExchangeError("Unsupported request weight or order interval")
                prefix = (
                    "x-mbx-used-weight-"
                    if kind == "REQUEST_WEIGHT"
                    else "x-mbx-order-count-"
                )
                if (type(rule.get("intervalNum")) is not int or rule["intervalNum"] <= 0
                        or type(rule.get("limit")) is not int or rule["limit"] <= 0):
                    raise ExchangeError("Invalid request weight or order limit metadata")
                key = scope, prefix + str(rule["intervalNum"]) + periods[interval][1]
                if key in limits:
                    raise ExchangeError("Duplicate request weight or order interval")
                limits[key] = rule["limit"]
        self.weight_limits.update(limits)
        now = utc(self.clock())
        request_id, requested_at = observation
        for header, value in headers.items():
            if not header.startswith(("x-mbx-used-weight-", "x-mbx-order-count-", "x-sapi-used-")):
                continue
            try:
                expires = budget_expiry(header, now)
                used = int(value)
                if used < 0:
                    continue
            except (ValueError, KeyError):
                continue
            key = scope, header
            if header.startswith("x-mbx-order-count-"):
                # A delayed ORDERS reading must not be assigned a new window,
                # or used after clock rollback. Weight observations retain the
                # existing conservative maximum even when their age is unclear.
                if requested_at > now or budget_expiry(header, requested_at) != expires:
                    continue
                # ORDERS can fall after fills. Dispatch order prevents a late
                # response from an older request overwriting a newer reading.
                if request_id < self._order_observation_ids.get(key, 0):
                    continue
                self._order_observation_ids[key] = request_id
                if order_limits is not None and header in order_limits:
                    self.weight_limits[key] = order_limits[header]
            else:
                previous_used, expires = self._weight_window(key, now)
                used = max(used, previous_used)
            self.weight_usage[key] = (
                used,
                expires,
            )

    def _observe_order_counts(self, scope, headers, data, observation):
        # Only the documented count endpoint may supply body-based observations.
        # Validate the entire response before publishing any decreased counter.
        if not isinstance(data, list) or not data:
            raise ExchangeError("Missing unfilled order counts")
        counts, limits = {}, {}
        for row in data:
            if not isinstance(row, dict):
                raise ExchangeError("Invalid unfilled order count row")
            interval = row.get("interval")
            number, limit, count = (row.get(field) for field in ("intervalNum", "limit", "count"))
            if (row.get("rateLimitType") != "ORDERS" or interval not in BUDGET_PERIODS
                    or any(type(value) is not int for value in (number, limit, count))
                    or number <= 0 or limit <= 0 or count < 0):
                raise ExchangeError("Invalid unfilled order count")
            header = f"x-mbx-order-count-{number}{BUDGET_PERIODS[interval][1]}"
            if header in counts:
                raise ExchangeError("Duplicate unfilled order count interval")
            counts[header], limits[header] = count, limit
        # Conflicting observations in one response cannot lower each other.
        for header, value in headers.items():
            if header.startswith("x-mbx-order-count-"):
                used = int(value)
                if used < 0:
                    raise ExchangeError("Invalid unfilled order count header")
                counts[header] = max(counts.get(header, 0), used)
        self._observe_request_budget(scope, counts, {}, observation=observation,
                                     order_limits=limits)

    def _call(self, method, *, checkpoint=True, protective=False, **params):
        endpoint = getattr(method, "__name__", type(method).__name__)
        mutation = endpoint in {"new_order", "delete_order", "user_universal_transfer"}
        if checkpoint and not mutation and self.read_checkpoint is not None:
            self.read_checkpoint()
        owner = getattr(method, "__self__", None)
        scope = (
            "futures"
            if owner is not None and owner is getattr(self, "futures", None)
            else "wallet:" + endpoint
            if owner is not None and owner is getattr(self, "wallet", None)
            else "spot"
        )
        # Protection is explicit and restricted to one addressed order read,
        # plus cancellation itself. It cannot leak into unrelated SDK calls.
        protective = endpoint == "delete_order" or (
            protective and endpoint == "get_order" and bool(params.get("symbol"))
            and bool(params.get("order_id") or params.get("orig_client_order_id"))
        )
        cost = request_cost(scope, endpoint, params)
        try:
            if scope == "futures" and endpoint != "exchange_information":
                self.ensure_request_allowed(scope=scope, request_weight=cost)
                self._cached("futures_request_limits", 300, self._futures_request_limits)
            with self._budget_lock:
                self.ensure_request_allowed(
                    protective=protective, scope=scope, new_order=endpoint == "new_order",
                    request_weight=cost,
                )
                observation = self._budget_observation()
                self._weight_inflight[observation[0]] = scope, cost
        except RequestDeferred as exc:
            if not mutation:
                raise
            # This is the last boundary before invoking the SDK. Exceptions
            # raised by the SDK itself are handled below and cannot prove that
            # nothing reached the exchange, even if they have the same name.
            raise RequestNotSent(
                str(exc), endpoint=endpoint, retry_after=exc.retry_after,
                request_weights=exc.request_weights,
                orders_only=exc.orders_only,
            ) from exc
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
                self._finish_weight_reservation(observation)
                self.request_weights = weights
                if scope == "spot" and endpoint == "rate_limit_order":
                    # Keep observed request weight even when the count body is invalid.
                    weight_headers = {k: v for k, v in weights.items()
                                      if not k.startswith("x-mbx-order-count-")}
                    self._observe_request_budget(scope, weight_headers, {}, observation=observation)
                    self._observe_order_counts(scope, weights, data, observation)
                else:
                    self._observe_request_budget(scope, weights, data, observation=observation)
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
        finally:
            with self._budget_lock:
                self._finish_weight_reservation(observation)

    def _cached(self, key, seconds, fetch):
        cached = self.cache.get(key)
        if cached is None or time.monotonic() - cached[0] >= seconds:
            self.cache[key] = (time.monotonic(), fetch())
        return self.cache[key][1]
