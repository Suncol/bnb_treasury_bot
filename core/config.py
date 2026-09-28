from dataclasses import fields, is_dataclass
from decimal import Decimal
from pathlib import Path
import tomllib
from typing import get_type_hints

from .models import (
    CrashGuardConfig,
    ExecutionConfig,
    RiskConfig,
    SchedulerConfig,
    StrategyConfig,
    ThresholdConfig,
)


def _section(cls, data):
    allowed = {f.name for f in fields(cls)}
    if set(data) - allowed:
        raise ValueError(f"Unknown {cls.__name__} keys: {sorted(set(data) - allowed)}")
    hints = get_type_hints(cls)
    values = {}
    for key, value in data.items():
        kind = hints[key]
        if kind is Decimal:
            if isinstance(value, bool):
                raise ValueError(f"{key} must be a decimal")
            value = Decimal(str(value))
        elif kind is int and type(value) is not int:
            raise ValueError(f"{key} must be an integer")
        elif kind is bool and type(value) is not bool:
            raise ValueError(f"{key} must be a boolean")
        elif key.endswith("discounts") or key == "layer_weights":
            value = tuple(Decimal(str(x)) for x in value)
        values[key] = value
    return cls(**values)


def validate_config(cfg: StrategyConfig) -> None:
    def validate_values(obj):
        for f in fields(obj):
            value = getattr(obj, f.name)
            if is_dataclass(value):
                validate_values(value)
            elif isinstance(value, Decimal) and (not value.is_finite() or value < 0):
                raise ValueError(f"{f.name} must be finite and nonnegative")
            elif type(value) is int and value <= 0:
                raise ValueError(f"{f.name} must be positive")

    validate_values(cfg)
    t, r, e, g = cfg.thresholds, cfg.risk, cfg.execution, cfg.crash_guard
    if not 0 < t.b_low < t.b_alert <= t.b_target < t.b_high:
        raise ValueError("Require 0 < b_low < b_alert <= b_target < b_high")
    if not 0 < r.alpha_warn <= r.alpha_crit <= 1 or min(r.u_min, r.u_budget_24h) <= 0:
        raise ValueError("Invalid transfer units, budget or risk ratios")
    if not 0 <= r.fee_reserve_rate < 1 or not 0 <= r.slippage < 1:
        raise ValueError("Fee and slippage reserves must be below 1")
    if (
        len(e.layer_weights) != 3
        or sum(e.layer_weights) != 1
        or any(x <= 0 for x in e.layer_weights)
    ):
        raise ValueError("Three positive layer weights must sum to 1")
    for discounts in (e.watch_discounts, e.accumulate_discounts):
        if len(discounts) != 3 or any(
            not x.is_finite() or not 0 <= x < 1 for x in discounts
        ):
            raise ValueError("Three discounts in [0, 1) are required")
    if not 0 < e.slice_qty_bnb <= e.slice_threshold_bnb:
        raise ValueError("Invalid slice quantity or threshold")
    if not 0 <= g.drawdown_1m_exit < g.drawdown_1m_enter < 1:
        raise ValueError("Crash recovery threshold must be below entry threshold")
    if not all(
        0 < x < 1
        for x in (g.drawdown_5m_enter, g.drawdown_15m_enter, g.keep_price_discount)
    ):
        raise ValueError("Invalid crash guard thresholds")
    if g.max_acquisition_bnb <= 0 or g.max_sample_gap_seconds < g.check_seconds:
        raise ValueError(
            "Invalid crash guard acquisition allowance or sampling interval"
        )
    if cfg.symbol != "BNB" + cfg.quote_asset or not cfg.strategy_id:
        raise ValueError(
            "The configured symbol must be BNB + quote_asset; strategy_id is required"
        )


def load_config(path: str | Path) -> StrategyConfig:
    with open(path, "rb") as source:
        data = tomllib.load(source, parse_float=Decimal)
    allowed = {
        "exchange",
        "thresholds",
        "risk",
        "execution",
        "scheduler",
        "crash_guard",
    }
    if set(data) - allowed:
        raise ValueError(f"Unknown config sections: {sorted(set(data) - allowed)}")
    exchange = data.get("exchange", {})
    if set(exchange) - {"symbol", "quote_asset", "strategy_id"}:
        raise ValueError("Unknown exchange configuration key")
    cfg = StrategyConfig(
        thresholds=_section(ThresholdConfig, data["thresholds"]),
        risk=_section(RiskConfig, data["risk"]),
        execution=_section(ExecutionConfig, data["execution"]),
        scheduler=_section(SchedulerConfig, data["scheduler"]),
        crash_guard=_section(CrashGuardConfig, data.get("crash_guard", {})),
        **exchange,
    )
    validate_config(cfg)
    return cfg
