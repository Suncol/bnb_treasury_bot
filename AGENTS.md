# Agent Instructions

## Environment

1. Python virtual environment activation path: `.venv/bin/activate`
2. Python interpreter path: `.venv/bin/python`

## Design Source of Truth

1. Repository design specification: [DESIGN.md](DESIGN.md)
2. Before changing strategy behavior, risk checks, transfer logic, order lifecycle, or data models, read [DESIGN.md](DESIGN.md) first.
3. If implementation behavior changes materially, update [DESIGN.md](DESIGN.md) in the same task so the document remains the source of truth.

## Strategy Constraints

1. The contract account in this repository is `USDⓈ-M Futures`.
2. Automatic USD transfer risk checks must use `maxWithdrawAmount` as the authoritative base.
3. The minimum USD transfer unit hard veto must be preserved: if `U_min / maxWithdrawAmount > alpha_warn`, automatic transfer must stop.
4. In-flight state and idempotency are first-class requirements. Do not add logic that can duplicate transfers or orders after timeouts or unknown execution states.

## Implementation Notes

1. Prefer `Decimal` for money, quantity, and price.
2. Keep exchange-facing code behind adapter/service boundaries.
3. Reconcile pending or unknown operations before placing new orders or transfers.
