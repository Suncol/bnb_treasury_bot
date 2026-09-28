from datetime import timedelta
from decimal import Decimal
from statistics import median

from core.models import OperationStatus
from core.time_utils import age_seconds, utc


def historical_metrics(repository, account, now):
    """Transfer-adjusted balance depletion; insufficient windows contribute nothing."""
    samples = repository.snapshots_since(utc(now) - timedelta(hours=49))
    transfers = repository.transfers_since(utc(now) - timedelta(hours=49))
    series = [s for s in samples if utc(s.ts) < utc(account.ts)] + [account]
    rates = []
    for hours in (6, 24):
        boundary = utc(now) - timedelta(hours=hours)
        before = [s for s in series if utc(s.ts) <= boundary]
        if not before:
            continue
        previous = before[-1]
        elapsed = Decimal(str(age_seconds(account.ts, previous.ts))) / 3600
        if elapsed <= 0:
            continue
        credits = sum(
            (
                t.amount if t.to_account == "USDⓈ-M Futures" else -t.amount
                for t in transfers
                if t.asset == "BNB"
                and t.status == OperationStatus.CONFIRMED
                and utc(previous.ts) < utc(t.ts) <= utc(account.ts)
            ),
            Decimal("0"),
        )
        # Average the whole window: sparse fee bursts must not disappear among
        # the zero-consumption intervals, regardless of snapshot spacing.
        rates.append(
            max(
                Decimal("0"),
                (previous.contract_bnb + credits - account.contract_bnb) / elapsed,
            )
        )
    boundary = utc(now) - timedelta(hours=24)
    previous_margin = [
        s
        for s in samples
        if utc(s.ts) <= boundary and s.contract_total_margin_balance is not None
    ]
    change = None
    if previous_margin and account.contract_total_margin_balance is not None:
        change = (
            account.contract_total_margin_balance
            - previous_margin[-1].contract_total_margin_balance
        )
    return (median(rates) if rates else Decimal("0")), change
