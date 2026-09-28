from __future__ import annotations

from decimal import Decimal

from .math_utils import non_negative
from .models import (
    HysteresisDecision,
    ReplenishmentState,
    SchedulerConfig,
    StateDecision,
    StateInputs,
    StrategyConfig,
    ThresholdConfig,
    TransitionReason,
)


def compute_t_depletion(
    contract_bnb: Decimal, b_low: Decimal, rate: Decimal
) -> Decimal | None:
    if rate <= Decimal("0"):
        return None
    return non_negative((contract_bnb - b_low) / rate)


def compute_effective_supply(inputs: StateInputs, b_spot_reserve: Decimal) -> Decimal:
    # Account balances already include settled fills and settled transfers.
    # The two adjustment fields contain ONLY assets absent from both balances.
    spot_transferable_bnb = non_negative(inputs.spot_bnb - b_spot_reserve)
    return (
        inputs.contract_bnb
        + inputs.bnb_in_transit
        + inputs.filled_untransferred_bnb
        + spot_transferable_bnb
        + inputs.open_buy_remaining_qty
    )


def compute_delta_bnb(effective_supply: Decimal, b_target: Decimal) -> Decimal:
    return non_negative(b_target - effective_supply)


def classify_candidate_state(
    contract_bnb: Decimal, t_depletion: Decimal | None, thresholds: ThresholdConfig
) -> ReplenishmentState:
    if contract_bnb >= thresholds.b_high:
        return ReplenishmentState.IDLE
    if contract_bnb <= thresholds.b_low:
        return ReplenishmentState.URGENT
    if t_depletion is not None and t_depletion <= Decimal("6"):
        return ReplenishmentState.URGENT
    if contract_bnb < thresholds.b_alert:
        return ReplenishmentState.ACCUMULATE
    if t_depletion is not None and t_depletion <= Decimal("24"):
        return ReplenishmentState.ACCUMULATE
    if (
        contract_bnb < thresholds.b_high
        and t_depletion is not None
        and t_depletion <= Decimal("72")
    ):
        return ReplenishmentState.WATCH
    return ReplenishmentState.IDLE


def _classify_reason(
    contract_bnb: Decimal, t_depletion: Decimal | None, thresholds: ThresholdConfig
) -> TransitionReason:
    if contract_bnb >= thresholds.b_high:
        return TransitionReason.HIGH_BALANCE
    if contract_bnb <= thresholds.b_low:
        return TransitionReason.LOW_BALANCE
    if t_depletion is not None and t_depletion <= Decimal("6"):
        return TransitionReason.DEPLETION_6H
    if contract_bnb < thresholds.b_alert:
        return TransitionReason.LOW_BALANCE
    if t_depletion is not None and t_depletion <= Decimal("24"):
        return TransitionReason.DEPLETION_24H
    if t_depletion is not None and t_depletion <= Decimal("72"):
        return TransitionReason.DEPLETION_72H
    return TransitionReason.HIGH_BALANCE


def apply_hysteresis(
    previous_state: ReplenishmentState,
    previous_candidate_state: ReplenishmentState | None,
    candidate_state: ReplenishmentState,
    streak: int,
    cfg: SchedulerConfig,
) -> HysteresisDecision:
    if candidate_state == ReplenishmentState.URGENT:
        return HysteresisDecision(
            confirmed_state=ReplenishmentState.URGENT,
            candidate_streak=0,
            transitioned=previous_state != ReplenishmentState.URGENT,
        )

    if candidate_state == previous_state:
        return HysteresisDecision(
            confirmed_state=previous_state,
            candidate_streak=0,
            transitioned=False,
        )

    next_streak = streak + 1 if candidate_state == previous_candidate_state else 1
    if next_streak >= cfg.state_confirm_cycles:
        return HysteresisDecision(
            confirmed_state=candidate_state,
            candidate_streak=0,
            transitioned=True,
        )

    return HysteresisDecision(
        confirmed_state=previous_state,
        candidate_streak=next_streak,
        transitioned=False,
    )


def evaluate_state(inputs: StateInputs, cfg: StrategyConfig) -> StateDecision:
    t_depletion = compute_t_depletion(
        inputs.contract_bnb, cfg.thresholds.b_low, inputs.consumption_rate
    )
    effective_supply = compute_effective_supply(inputs, cfg.thresholds.b_spot_reserve)
    delta_bnb = compute_delta_bnb(effective_supply, cfg.thresholds.b_target)

    raw_candidate_state = classify_candidate_state(
        inputs.contract_bnb, t_depletion, cfg.thresholds
    )
    candidate_reason = _classify_reason(
        inputs.contract_bnb, t_depletion, cfg.thresholds
    )

    candidate_state = raw_candidate_state
    reason = candidate_reason

    if (
        inputs.previous_state == ReplenishmentState.URGENT
        and raw_candidate_state != ReplenishmentState.URGENT
        and inputs.contract_bnb < (cfg.thresholds.b_low + Decimal("1"))
    ):
        candidate_state = ReplenishmentState.URGENT
        reason = TransitionReason.EXIT_URGENT_BUFFER

    hysteresis = apply_hysteresis(
        inputs.previous_state,
        inputs.previous_candidate_state,
        candidate_state,
        inputs.candidate_streak,
        cfg.scheduler,
    )

    if (
        hysteresis.transitioned
        and hysteresis.confirmed_state != ReplenishmentState.URGENT
    ):
        reason = TransitionReason.HYSTERESIS_CONFIRMED

    return StateDecision(
        current_state=inputs.previous_state,
        raw_candidate_state=raw_candidate_state,
        candidate_state=candidate_state,
        confirmed_state=hysteresis.confirmed_state,
        transitioned=hysteresis.transitioned,
        reason=reason,
        candidate_reason=candidate_reason,
        t_depletion=t_depletion,
        effective_supply=effective_supply,
        delta_bnb=delta_bnb,
        candidate_streak=hysteresis.candidate_streak,
    )
