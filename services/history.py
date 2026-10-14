from datetime import timedelta
from decimal import Decimal
from statistics import median

from core.models import OperationStatus
from core.time_utils import age_seconds, utc

ZERO = Decimal("0")
INCOME_RECHECK_INTERVAL = timedelta(minutes=5)
# New or capital-related income types require an explicit accounting decision.
OPERATING_INCOME_TYPES = frozenset({
    "REALIZED_PNL", "FUNDING_FEE", "SPECIAL_FUNDING_FEE", "COMMISSION",
    "INSURANCE_CLEAR", "REFERRAL_KICKBACK", "COMMISSION_REBATE", "API_REBATE",
    "WELCOME_BONUS", "CONTEST_REWARD", "OPTIONS_PREMIUM_FEE",
    "OPTIONS_SETTLE_PROFIT", "DELIVERED_SETTELMENT",
    "POSITION_LIMIT_INCREASE_FEE", "FEE_RETURN", "BFUSD_REWARD",
})


def margin_baseline(samples, now, *, warmup=False):
    boundary = utc(now) - timedelta(hours=24)
    eligible = [s for s in samples if (
        s.contract_total_margin_balance is not None
        and s.contract_margin_asset is not None
        and s.wallet_event_cursor is not None
    )]
    return next(
        (
            s
            for s in reversed(eligible)
            if utc(s.ts) <= boundary
        ),
        eligible[0] if warmup and eligible else None,
    )


def transfer_flows(values):
    """Keep credits and debits separate: a zero net flow proves nothing."""
    values = tuple(values)
    return (
        sum((x for x in values if x > 0), ZERO),
        sum((x for x in values if x < 0), ZERO),
    )


def covered_transfers(repository, before, account, asset):
    if before.wallet_event_cursor is not None:
        through = account.wallet_event_cursor
        if through is None:
            through = repository.wallet_cursor()
        return tuple(
            repository.transfer_record(reference.removeprefix("transfer:"))
            for _, reference, _ in repository.wallet_changes(
                before.wallet_event_cursor, through=through
            )
            if reference.startswith("transfer:")
        )
    # Legacy in-flight operations retain their original timestamp boundary.
    version = "contract_bnb_updated_at" if asset == "BNB" else "contract_quote_updated_at"
    lower = utc(getattr(before, version) or before.ts)
    upper = utc(getattr(account, version) or account.ts)
    return tuple(
        t for t in repository.transfers_since(utc(before.ts) - timedelta(seconds=1))
        if t.status == OperationStatus.CONFIRMED
        and lower < utc(t.ts) <= upper
    )


def settlement_baselines(repository, samples, before, account, asset):
    """Try the nearest retained checkpoint only after the original proof fails."""
    version = "contract_bnb_updated_at" if asset == "BNB" else "contract_quote_updated_at"
    lower = utc(getattr(before, version) or before.ts)
    if before.wallet_event_cursor is not None and any(
        (t.asset == asset or asset != "BNB" and t.asset != "BNB")
        and utc(t.ts) <= lower
        for t in covered_transfers(repository, before, account, asset)
    ):
        return tuple(
            s for s in reversed(samples)
            if s.wallet_event_cursor is not None
            and 0 <= s.wallet_event_cursor <= before.wallet_event_cursor
            and utc(s.ts) <= utc(before.ts)
            and utc(getattr(s, version) or s.ts) < lower
            and s.contract_quote_balance is not None
        )
    return ()


def settlement_policy_issue(evidence):
    if evidence["unsupported_income_types"]:
        return {"reason": "Capital or unknown income requires unsupported settlement evidence",
                "income_types": evidence["unsupported_income_types"]}
    if evidence["foreign_transfer_assets"]:
        return {"reason": "Historical capital flows require unsupported currency conversion"}
    return None


def futures_settlement_evidence(repository, before, account, asset, *, samples=()):
    """The same signed-flow and wallet proof serves transfers and margin history."""
    evidence = _settlement_evidence(repository, before, account, asset, before)
    if evidence[0]:
        return evidence
    for anchor in settlement_baselines(repository, samples, before, account, asset):
        evidence = _settlement_evidence(repository, before, account, asset, anchor)
        if evidence[0]:
            return evidence
    return evidence


def _settlement_evidence(repository, before, account, asset, anchor):
    field = "contract_bnb" if asset == "BNB" else "contract_quote_balance"
    version = "contract_bnb_updated_at" if asset == "BNB" else "contract_quote_updated_at"
    lower = utc(getattr(before, version) or before.ts)
    upper = utc(getattr(account, version) or account.ts)
    previous, actual = getattr(before, field), getattr(account, field)
    anchor_lower = utc(getattr(anchor, version) or anchor.ts)
    anchor_wallet = getattr(anchor, field)
    since = utc(anchor.ts) - timedelta(seconds=1)
    records = repository.futures_income_since(since)
    covered_income = tuple(
        r for r in records
        if r.asset == asset and anchor_lower < utc(r.ts) <= upper
    )
    income = tuple(
        r for r in records
        if r.asset == asset and lower < utc(r.ts) <= upper
    )
    all_transfers = covered_transfers(repository, anchor, account, asset)
    transfers = tuple(t for t in all_transfers if t.asset == asset)
    foreign_transfers = tuple(
        t for t in covered_transfers(repository, before, account, asset)
        if t.asset not in {asset, "BNB"}
    )
    foreign_assets = {t.asset for t in foreign_transfers}
    foreign_proven = True
    for foreign_asset in {
        t.asset for t in foreign_transfers if asset != "BNB" and utc(t.ts) <= lower
    }:
        # Old request times only trigger replay. Signed receipts establish
        # whether these newly discovered flows were already in the baseline.
        matched = transfer_flows(
            t.amount if t.to_account == "USDⓈ-M Futures" else -t.amount
            for t in all_transfers if t.asset == foreign_asset
        ) == transfer_flows(
            r.amount for r in records
            if r.asset == foreign_asset and r.income_type == "TRANSFER"
            and anchor_lower < utc(r.ts) <= upper
        )
        foreign_proven = foreign_proven and matched
        if matched:
            foreign_assets.discard(foreign_asset)
    foreign_assets.update(
        r.asset for r in records
        if r.income_type == "TRANSFER" and r.asset not in {asset, "BNB"}
        and utc(before.ts) < utc(r.ts) <= utc(account.ts)
    )
    wallet_flows = transfer_flows(
        t.amount if t.to_account == "USDⓈ-M Futures" else -t.amount
        for t in transfers
    )
    income_flows = transfer_flows(r.amount for r in covered_income if r.income_type == "TRANSFER")
    expected = (
        previous + sum((r.amount for r in income), ZERO)
        if previous is not None else None
    )
    through = account.wallet_event_cursor
    if through is None:
        through = repository.wallet_cursor()
    cursors_valid = (
        before.wallet_event_cursor is None
        or 0 <= before.wallet_event_cursor <= through <= repository.wallet_cursor()
    )
    valid = (
        previous is not None and actual is not None
        and previous.is_finite() and actual.is_finite()
        and lower <= utc(before.ts) + timedelta(seconds=1)
        and lower <= upper <= utc(account.ts) + timedelta(seconds=1)
        and (getattr(account, version) is None or lower == upper or bool(income))
        and anchor_lower <= lower
        and anchor_lower <= utc(anchor.ts) + timedelta(seconds=1)
        and anchor_wallet is not None and anchor_wallet.is_finite()
        and anchor_wallet + sum(
            (r.amount for r in covered_income if utc(r.ts) <= lower), ZERO
        ) == previous
        and cursors_valid
        and foreign_proven
        and expected == actual and wallet_flows == income_flows
    )
    return valid, anchor, {
        "asset": asset, "before": previous, "expected": expected, "actual": actual,
        "from": lower, "through": upper,
        "since": since,
        "wallet_transfer_flows": wallet_flows, "income_transfer_flows": income_flows,
        "transfer_ids": tuple(t.transfer_id for t in transfers),
        "net_inflow": sum((r.amount for r in income if r.income_type == "TRANSFER"), ZERO),
        "income_receipts": tuple((r.income_type, r.transaction_id) for r in income),
        "foreign_transfer_assets": tuple(sorted(foreign_assets)),
        "unsupported_income_types": tuple(sorted({
            r.income_type for r in records
            if lower < utc(r.ts) <= upper
            and r.income_type != "TRANSFER"
            and r.income_type not in OPERATING_INCOME_TYPES
        })),
        "reason": "Futures balance or transfer flow lacks complete matching evidence",
    }


def historical_metrics(repository, account, now, quote_asset, *, samples=None):
    """Return depletion, adjusted margin and an explicit incomplete-evidence issue."""
    if samples is None:
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
    previous = margin_baseline(samples, now)
    warmed_up = previous is not None
    if previous is None:
        previous = margin_baseline(samples, now, warmup=True)
    change, issue = None, None
    if previous is not None and account.contract_total_margin_balance is None:
        issue = {"reason": "Current margin observation is missing"}
    elif account.contract_total_margin_balance is not None:
        if account.contract_margin_asset != quote_asset:
            issue = {"reason": "Aggregate margin requires unsupported currency conversion"}
        elif previous is not None:
            if previous.contract_margin_asset != quote_asset:
                issue = {"reason": "Historical margin baseline lacks currency or event coverage"}
            else:
                valid, _, evidence = futures_settlement_evidence(
                    repository, previous, account, quote_asset, samples=samples
                )
                if not valid:
                    issue = evidence
                elif (issue := settlement_policy_issue(evidence)) is None and warmed_up:
                    change = (
                        account.contract_total_margin_balance
                        - previous.contract_total_margin_balance
                        - evidence["net_inflow"]
                    )
    return (median(rates) if rates else ZERO), change, issue
