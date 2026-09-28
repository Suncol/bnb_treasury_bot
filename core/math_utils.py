from __future__ import annotations

from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR


ZERO = Decimal("0")
ONE = Decimal("1")


def non_negative(value: Decimal) -> Decimal:
    return value if value > ZERO else ZERO


def floor_to_multiple(value: Decimal, multiple: Decimal) -> Decimal:
    if multiple <= ZERO:
        raise ValueError("multiple must be positive")
    if value <= ZERO:
        return ZERO
    units = (value / multiple).to_integral_value(rounding=ROUND_FLOOR)
    return units * multiple


def ceil_to_multiple(value: Decimal, multiple: Decimal) -> Decimal:
    if multiple <= ZERO:
        raise ValueError("multiple must be positive")
    if value <= ZERO:
        return ZERO
    units = (value / multiple).to_integral_value(rounding=ROUND_CEILING)
    return units * multiple
