"""Shared validation for balance-dependent decisions and settlement."""

from .time_utils import fresh, utc


def valid_account_snapshot(account, now, cfg, *, previous=None):
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
        and all(x.is_finite() and x >= 0 for x in amounts)
        and all(x is None or x.is_finite() for x in observations)
        and account.reserved_spot_usd <= account.spot_usd
        and account.reserved_spot_bnb <= account.spot_bnb
        and (previous is None or utc(account.ts) >= utc(previous.ts))
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
