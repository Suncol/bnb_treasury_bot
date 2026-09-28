from __future__ import annotations

from decimal import Decimal

from .math_utils import ceil_to_multiple, floor_to_multiple, non_negative
from .models import GateStatus, RiskConfig, TransferDecision


def compute_usd_transfer_needed(
    delta_bnb: Decimal,
    ref_price: Decimal,
    spot_free_usd: Decimal,
    cfg: RiskConfig,
) -> Decimal:
    if delta_bnb <= Decimal("0") or ref_price <= Decimal("0"):
        return Decimal("0")

    usd_needed_gross = (
        delta_bnb
        * ref_price
        * (Decimal("1") + cfg.slippage)
        * (Decimal("1") + cfg.fee_reserve_rate)
    )
    usd_needed_net = non_negative(usd_needed_gross - spot_free_usd)
    if usd_needed_net == Decimal("0"):
        return Decimal("0")
    return ceil_to_multiple(usd_needed_net, cfg.u_min)


def evaluate_transfer_gate(
    usd_transfer_raw: Decimal,
    max_withdraw_amount: Decimal,
    budget_used_24h: Decimal,
    cfg: RiskConfig,
    min_transfer_usd: Decimal = Decimal("0"),
) -> TransferDecision:
    if usd_transfer_raw <= Decimal("0"):
        return TransferDecision(
            allow=False,
            gate_status=GateStatus.OPEN,
            level="NO_ACTION",
            adjusted_amount=Decimal("0"),
            message="No USD transfer needed",
        )

    if max_withdraw_amount <= Decimal("0"):
        return TransferDecision(
            allow=False,
            gate_status=GateStatus.BLOCKED,
            level="CRITICAL",
            adjusted_amount=Decimal("0"),
            message="maxWithdrawAmount is not positive",
        )

    minimum = ceil_to_multiple(max(cfg.u_min, min_transfer_usd), cfg.u_min)
    usd_transfer_raw = max(minimum, ceil_to_multiple(usd_transfer_raw, cfg.u_min))
    granularity_ratio = minimum / max_withdraw_amount
    if granularity_ratio > cfg.alpha_crit:
        return TransferDecision(
            allow=False,
            gate_status=GateStatus.HARD_VETO,
            level="CRITICAL",
            adjusted_amount=Decimal("0"),
            message="Minimum transfer unit exceeds critical maxWithdrawAmount ratio",
        )
    if granularity_ratio > cfg.alpha_warn:
        return TransferDecision(
            allow=False,
            gate_status=GateStatus.HARD_VETO,
            level="WARNING",
            adjusted_amount=Decimal("0"),
            message="Minimum transfer unit exceeds warning maxWithdrawAmount ratio",
        )

    if max_withdraw_amount < cfg.m_abs_min:
        return TransferDecision(
            allow=False,
            gate_status=GateStatus.BLOCKED,
            level="CRITICAL",
            adjusted_amount=Decimal("0"),
            message="maxWithdrawAmount is below the absolute minimum",
        )

    if max_withdraw_amount - usd_transfer_raw < cfg.m_abs_min:
        return TransferDecision(
            allow=False,
            gate_status=GateStatus.BLOCKED,
            level="CRITICAL",
            adjusted_amount=Decimal("0"),
            message="Transfer would reduce maxWithdrawAmount below the absolute minimum",
        )

    ratio = usd_transfer_raw / max_withdraw_amount
    if ratio > cfg.alpha_crit:
        return TransferDecision(
            allow=False,
            gate_status=GateStatus.BLOCKED,
            level="DANGER",
            adjusted_amount=Decimal("0"),
            message="Transfer amount exceeds critical ratio against maxWithdrawAmount",
        )

    if ratio > cfg.alpha_warn:
        return TransferDecision(
            allow=False,
            gate_status=GateStatus.BLOCKED,
            level="WARNING",
            adjusted_amount=Decimal("0"),
            message="Transfer amount exceeds warning ratio against maxWithdrawAmount",
        )

    budget_left = non_negative(cfg.u_budget_24h - budget_used_24h)
    if budget_left < minimum:
        return TransferDecision(
            allow=False,
            gate_status=GateStatus.BLOCKED,
            level="BUDGET_EXHAUSTED",
            adjusted_amount=Decimal("0"),
            message="Rolling 24h budget is exhausted",
        )

    adjusted_amount = usd_transfer_raw
    if usd_transfer_raw > budget_left:
        adjusted_amount = floor_to_multiple(budget_left, cfg.u_min)
        if adjusted_amount < minimum:
            return TransferDecision(
                allow=False,
                gate_status=GateStatus.BLOCKED,
                level="BUDGET_EXHAUSTED",
                adjusted_amount=Decimal("0"),
                message="Remaining 24h budget is below minimum transfer size",
            )
        return TransferDecision(
            allow=True,
            gate_status=GateStatus.OPEN,
            level="BUDGET_WARNING",
            adjusted_amount=adjusted_amount,
            message="Transfer amount clipped to remaining 24h budget",
        )

    return TransferDecision(
        allow=True,
        gate_status=GateStatus.OPEN,
        level="OK",
        adjusted_amount=adjusted_amount,
        message="Transfer is allowed",
    )
