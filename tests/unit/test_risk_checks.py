from __future__ import annotations

from core.models import GateStatus
from core.risk_checks import compute_usd_transfer_needed, evaluate_transfer_gate
from tests.helpers import D, make_strategy_config


def test_usd_transfer_needed_zero_when_spot_already_funded() -> None:
    cfg = make_strategy_config()
    amount = compute_usd_transfer_needed(D("2"), D("600"), D("2000"), cfg.risk)
    assert amount == D("0")


def test_hard_veto_triggers_when_minimum_unit_exceeds_warning_ratio() -> None:
    cfg = make_strategy_config()
    decision = evaluate_transfer_gate(D("500"), D("4000"), D("0"), cfg.risk)
    assert decision.gate_status == GateStatus.HARD_VETO
    assert decision.allow is False
    assert decision.level == "WARNING"


def test_hard_veto_is_critical_above_critical_ratio() -> None:
    cfg = make_strategy_config()
    decision = evaluate_transfer_gate(D("500"), D("1500"), D("0"), cfg.risk)
    assert decision.gate_status == GateStatus.HARD_VETO
    assert decision.level == "CRITICAL"


def test_block_when_max_withdraw_below_absolute_minimum() -> None:
    cfg = make_strategy_config()
    decision = evaluate_transfer_gate(D("500"), D("1800"), D("0"), cfg.risk)
    assert decision.allow is False
    assert decision.level == "CRITICAL"


def test_block_when_transfer_breaks_absolute_minimum() -> None:
    cfg = make_strategy_config()
    custom_risk = cfg.risk.__class__(
        u_min=cfg.risk.u_min,
        alpha_warn=D("1"),
        alpha_crit=D("1"),
        m_abs_min=cfg.risk.m_abs_min,
        u_budget_24h=cfg.risk.u_budget_24h,
        slippage=cfg.risk.slippage,
        u_spot_idle_max=cfg.risk.u_spot_idle_max,
    )
    decision = evaluate_transfer_gate(D("5000"), D("6500"), D("0"), custom_risk)
    assert decision.allow is False
    assert decision.level == "CRITICAL"


def test_block_when_single_transfer_exceeds_critical_ratio() -> None:
    cfg = make_strategy_config()
    decision = evaluate_transfer_gate(D("2500"), D("6000"), D("0"), cfg.risk)
    assert decision.allow is False
    assert decision.level == "DANGER"


def test_block_when_budget_left_is_below_minimum_unit() -> None:
    cfg = make_strategy_config()
    decision = evaluate_transfer_gate(D("500"), D("6000"), D("2900"), cfg.risk)
    assert decision.allow is False
    assert decision.level == "BUDGET_EXHAUSTED"


def test_allow_budget_adjustment_when_budget_has_room_for_one_unit() -> None:
    cfg = make_strategy_config()
    decision = evaluate_transfer_gate(D("1000"), D("20000"), D("2250"), cfg.risk)
    assert decision.allow is True
    assert decision.adjusted_amount == D("500")
    assert decision.level == "BUDGET_WARNING"
