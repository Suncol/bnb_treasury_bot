"""Shared validation for balance-dependent decisions and settlement."""

from .time_utils import fresh


def valid_account_snapshot(account, now, cfg):
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
    )
