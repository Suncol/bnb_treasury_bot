"""Shared validation for balance-dependent decisions and settlement."""

from datetime import timedelta

from .time_utils import fresh, utc


def valid_account_snapshot(account, now, cfg, *, previous=None):
    if account.ts is None:
        return False
    futures_end = utc(account.futures_observed_until or account.ts)
    spot_end = utc(account.spot_observed_until or account.ts)
    amounts = (
        account.contract_bnb,
        account.contract_max_withdraw_amount,
        account.spot_bnb,
        account.spot_usd,
        account.reserved_spot_usd,
        account.reserved_spot_bnb,
    )
    observations = (
        account.contract_available_balance,
        account.contract_quote_balance,
        account.contract_total_margin_balance,
    )
    return (
        fresh(account.ts, now, cfg.risk.max_account_age_seconds)
        and utc(account.ts) <= futures_end <= spot_end <= utc(now)
        and all(
            version is None or utc(version) <= futures_end + timedelta(seconds=1)
            for version in (account.contract_bnb_updated_at, account.contract_quote_updated_at)
        )
        and all(x.is_finite() and x >= 0 for x in amounts)
        and all(x is None or x.is_finite() for x in observations)
        and account.reserved_spot_usd <= account.spot_usd
        and account.reserved_spot_bnb <= account.spot_bnb
        and (previous is None or utc(account.ts) >= utc(previous.ts))
        and (previous is None or (
            futures_end >= utc(previous.futures_observed_until or previous.ts)
            and spot_end >= utc(previous.spot_observed_until or previous.ts)
        ))
        # Old unchanged versions are valid; only a regression/lost version is
        # rejected. Timestamp ties are not unique transaction sequence numbers.
        and (previous is None or all(
            getattr(previous, field) is None or (
                getattr(account, field) is not None
                and utc(getattr(account, field)) >= utc(getattr(previous, field))
            )
            for field in ("contract_bnb_updated_at", "contract_quote_updated_at")
        ))
    )
