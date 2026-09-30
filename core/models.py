from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum


class ReplenishmentState(str, Enum):
    IDLE = "IDLE"
    WATCH = "WATCH"
    ACCUMULATE = "ACCUMULATE"
    URGENT = "URGENT"


class RunMode(str, Enum):
    AUTO = "AUTO"
    URGENT_ONLY = "URGENT_ONLY"
    PAUSED = "PAUSED"


class GateStatus(str, Enum):
    OPEN = "OPEN"
    BLOCKED = "BLOCKED"
    HARD_VETO = "HARD_VETO"
    WAIT_RECONCILE = "WAIT_RECONCILE"


class TransitionReason(str, Enum):
    LOW_BALANCE = "LOW_BALANCE"
    DEPLETION_6H = "DEPLETION_6H"
    DEPLETION_24H = "DEPLETION_24H"
    DEPLETION_72H = "DEPLETION_72H"
    HIGH_BALANCE = "HIGH_BALANCE"
    HYSTERESIS_CONFIRMED = "HYSTERESIS_CONFIRMED"
    EXIT_URGENT_BUFFER = "EXIT_URGENT_BUFFER"
    DATA_INVALID = "DATA_INVALID"
    RUN_MODE_BLOCK = "RUN_MODE_BLOCK"
    WAIT_RECONCILE = "WAIT_RECONCILE"


@dataclass(frozen=True)
class ThresholdConfig:
    b_low: Decimal
    b_alert: Decimal
    b_target: Decimal
    b_high: Decimal
    b_spot_reserve: Decimal


@dataclass(frozen=True)
class RiskConfig:
    u_min: Decimal
    alpha_warn: Decimal
    alpha_crit: Decimal
    m_abs_min: Decimal
    u_budget_24h: Decimal
    slippage: Decimal
    u_spot_idle_max: Decimal
    fee_reserve_rate: Decimal = Decimal("0.001")
    sweep_enabled: bool = True
    max_account_age_seconds: int = 10
    margin_drop_limit: Decimal = Decimal("500")
    margin_target_reduction: Decimal = Decimal("3")


@dataclass(frozen=True)
class ExecutionConfig:
    watch_discounts: tuple[Decimal, Decimal, Decimal]
    accumulate_discounts: tuple[Decimal, Decimal, Decimal]
    layer_weights: tuple[Decimal, Decimal, Decimal]
    watch_reprice_hours: int
    accumulate_reprice_hours: int
    urgent_ioc_buffer: Decimal
    watch_batch_transfer_bnb: Decimal = Decimal("2")
    watch_ttl_hours: int = 24
    accumulate_ttl_hours: int = 8
    chase_return_limit: Decimal = Decimal("0.03")
    reprice_deviation: Decimal = Decimal("0.05")
    slice_threshold_bnb: Decimal = Decimal("10")
    slice_qty_bnb: Decimal = Decimal("3")
    slice_interval_seconds: int = 1800


@dataclass(frozen=True)
class SchedulerConfig:
    t_check_minutes: int
    state_confirm_cycles: int
    reconciliation_seconds: int = 30


@dataclass(frozen=True)
class CrashGuardConfig:
    drawdown_1m_enter: Decimal = Decimal("0.008")
    drawdown_5m_enter: Decimal = Decimal("0.015")
    drawdown_15m_enter: Decimal = Decimal("0.03")
    keep_price_discount: Decimal = Decimal("0.01")
    max_acquisition_bnb: Decimal = Decimal("3")
    max_keep_orders: int = 3
    cooldown_seconds: int = 300
    stable_seconds: int = 180
    drawdown_1m_exit: Decimal = Decimal("0.003")
    check_seconds: int = 1
    smooth_seconds: int = 3
    max_market_age_seconds: int = 5
    max_sample_gap_seconds: int = 3


@dataclass(frozen=True)
class StrategyConfig:
    thresholds: ThresholdConfig
    risk: RiskConfig
    execution: ExecutionConfig
    scheduler: SchedulerConfig
    symbol: str = "BNBUSDT"
    quote_asset: str = "USDT"
    strategy_id: str = "bnb-treasury"
    spot_activity_symbols: tuple[str, ...] = ()
    crash_guard: CrashGuardConfig = field(default_factory=CrashGuardConfig)


@dataclass(frozen=True)
class SymbolFilters:
    qty_step: Decimal
    price_tick: Decimal
    min_qty: Decimal
    min_notional: Decimal
    min_transfer_bnb: Decimal = Decimal("0.1")
    min_transfer_usd: Decimal = Decimal("0")
    max_qty: Decimal | None = None
    max_notional: Decimal | None = None
    min_price: Decimal = Decimal("0")
    max_price: Decimal | None = None
    max_position: Decimal | None = None
    max_open_orders: int | None = None
    exchange_order_slots: int | None = None
    observed_at: datetime | None = None


@dataclass(frozen=True)
class AccountSnapshot:
    ts: datetime | None
    contract_bnb: Decimal
    contract_max_withdraw_amount: Decimal
    contract_available_balance: Decimal
    spot_bnb: Decimal
    spot_usd: Decimal
    reserved_spot_usd: Decimal
    contract_total_margin_balance: Decimal | None = None
    # spot_bnb is the total balance; only the unlocked part may be transferred.
    reserved_spot_bnb: Decimal = Decimal("0")
    # Wallet balances/versions reconcile settlement; risk still uses maxWithdrawAmount.
    contract_quote_balance: Decimal | None = None
    contract_bnb_updated_at: datetime | None = None
    contract_quote_updated_at: datetime | None = None


@dataclass(frozen=True)
class PendingState:
    pending_bnb_to_contract: Decimal
    pending_usd_to_spot: Decimal
    open_buy_remaining_qty: Decimal
    # Only fills NOT reflected in spot_bnb; the live reconciler always sets zero.
    filled_untransferred_bnb: Decimal
    has_unknown_orders: bool = False
    has_unknown_transfers: bool = False
    pending_usd_to_contract: Decimal = Decimal("0")
    has_pending_orders: bool = False
    has_pending_cancels: bool = False
    # Only BNB debited from spot and not yet credited to futures belongs here.
    bnb_in_transit: Decimal = Decimal("0")

    @property
    def unresolved(self) -> bool:
        return bool(
            self.has_unknown_orders
            or self.has_unknown_transfers
            or self.has_pending_orders
            or self.has_pending_cancels
            or self.pending_bnb_to_contract
            or self.pending_usd_to_spot
            or self.pending_usd_to_contract
        )


@dataclass(frozen=True)
class MarketSnapshot:
    ts: datetime | None
    symbol: str
    best_bid: Decimal
    best_ask: Decimal
    mid_price: Decimal
    vwap_5m: Decimal
    return_1h: Decimal
    return_24h: Decimal
    smooth_price: Decimal | None = None
    drawdown_1m: Decimal | None = None
    drawdown_5m: Decimal | None = None
    drawdown_15m: Decimal | None = None
    sample_continuity: str | None = None
    sample_stable_since: datetime | None = None
    sampled_at: datetime | None = None

    @property
    def windows_ready(self) -> bool:
        return all(
            x is not None
            for x in (
                self.smooth_price,
                self.drawdown_1m,
                self.drawdown_5m,
                self.drawdown_15m,
            )
        )


@dataclass(frozen=True)
class StateInputs:
    contract_bnb: Decimal
    contract_max_withdraw_amount: Decimal
    contract_available_balance: Decimal
    spot_bnb: Decimal
    spot_usd: Decimal
    reserved_spot_usd: Decimal
    pending_bnb_to_contract: Decimal
    pending_usd_to_spot: Decimal
    open_buy_remaining_qty: Decimal
    filled_untransferred_bnb: Decimal
    consumption_rate: Decimal
    previous_state: ReplenishmentState
    previous_candidate_state: ReplenishmentState | None
    candidate_streak: int
    run_mode: RunMode
    has_unknown_orders: bool
    has_unknown_transfers: bool
    bnb_in_transit: Decimal = Decimal("0")


@dataclass(frozen=True)
class HysteresisDecision:
    confirmed_state: ReplenishmentState
    candidate_streak: int
    transitioned: bool


@dataclass(frozen=True)
class StateDecision:
    current_state: ReplenishmentState
    raw_candidate_state: ReplenishmentState
    candidate_state: ReplenishmentState
    confirmed_state: ReplenishmentState
    transitioned: bool
    reason: TransitionReason
    candidate_reason: TransitionReason
    t_depletion: Decimal | None
    effective_supply: Decimal
    delta_bnb: Decimal
    candidate_streak: int


@dataclass(frozen=True)
class TransferDecision:
    allow: bool
    gate_status: GateStatus
    level: str
    adjusted_amount: Decimal
    message: str


@dataclass(frozen=True)
class OrderPlan:
    symbol: str
    side: str
    order_type: str
    qty: Decimal
    price: Decimal | None
    time_in_force: str | None
    post_only: bool


@dataclass(frozen=True)
class BuyPlan:
    state: ReplenishmentState
    orders: tuple[OrderPlan, ...] = ()
    reason: str = ""


@dataclass(frozen=True)
class AssetTransferPlan:
    asset: str
    amount: Decimal
    from_account: str
    to_account: str
    reason: str


@dataclass(frozen=True)
class ExecutionGates:
    run_mode_gate: GateStatus
    transfer_gate: GateStatus
    reconciliation_gate: GateStatus
    data_gate: GateStatus = GateStatus.OPEN


@dataclass(frozen=True)
class Alert:
    level: str
    code: str
    message: str


@dataclass(frozen=True)
class EngineInputs:
    account: AccountSnapshot
    pending: PendingState
    market: MarketSnapshot
    filters: SymbolFilters
    run_mode: RunMode
    consumption_rate: Decimal
    previous_state: ReplenishmentState
    previous_candidate_state: ReplenishmentState | None
    candidate_streak: int
    budget_used_24h: Decimal
    now: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    orders: tuple[OrderView, ...] = ()
    crash_guard: CrashGuardState = field(default_factory=lambda: CrashGuardState())
    margin_change_24h: Decimal | None = None
    slice_state: SliceState = field(default_factory=lambda: SliceState())
    advance_state: bool = True
    allow_new_cycle: bool = True
    snapshot_consistent: bool = True
    allow_guard_recovery: bool = True
    reserve_replenishment_funds: bool = False
    allow_usd_funding: bool = True


@dataclass(frozen=True)
class CyclePlan:
    state_decision: StateDecision
    transfer_decision: TransferDecision
    buy_plan: BuyPlan
    bnb_transfer_plan: AssetTransferPlan | None
    alerts: tuple[Alert, ...]
    gates: ExecutionGates
    spot_usd_sweep_plan: AssetTransferPlan | None = None
    cancellations: tuple[CancelPlan, ...] = ()
    crash_guard: CrashGuardState = field(default_factory=lambda: CrashGuardState())
    slice_state: SliceState = field(default_factory=lambda: SliceState())
    active_buy_target: Decimal = Decimal("0")
    inventory_valid: bool = True


class OperationStatus(str, Enum):
    PENDING = "PENDING"
    UNKNOWN = "UNKNOWN"
    CONFIRMED = "CONFIRMED"
    FAILED = "FAILED"


class OperationKind(str, Enum):
    ORDER = "ORDER"
    CANCEL = "CANCEL"
    TRANSFER = "TRANSFER"


@dataclass(frozen=True)
class OrderView:
    symbol: str
    order_id: str
    client_id: str
    strategy_id: str | None
    price: Decimal
    qty: Decimal
    filled_qty: Decimal
    created_at: datetime
    state: ReplenishmentState = ReplenishmentState.WATCH
    side: str = "BUY"
    status: str = "NEW"
    time_in_force: str = "GTC"
    cancel_pending: bool = False

    @property
    def remaining_qty(self) -> Decimal:
        return (
            max(self.qty - self.filled_qty, Decimal("0"))
            if self.is_open
            else Decimal("0")
        )

    @property
    def is_open(self) -> bool:
        return self.status not in {
            "FILLED",
            "CANCELED",
            "EXPIRED",
            "EXPIRED_IN_MATCH",
            "REJECTED",
        }


@dataclass(frozen=True)
class CancelPlan:
    symbol: str
    order_id: str
    reason: str
    replenish_after_cancel: bool = False


@dataclass(frozen=True)
class CrashGuardState:
    active: bool = False
    episode_id: str | None = None
    started_at: datetime | None = None
    last_trigger_at: datetime | None = None
    stable_since: datetime | None = None
    last_checked_at: datetime | None = None
    keep_price_ceiling: Decimal | None = None
    filled_bnb: Decimal = Decimal("0")
    reasons: tuple[str, ...] = ()
    ended_at: datetime | None = None


@dataclass(frozen=True)
class SliceState:
    active: bool = False
    next_at: datetime | None = None


@dataclass(frozen=True)
class Fill:
    symbol: str
    trade_id: str
    order_id: str
    ts: datetime
    qty: Decimal
    commission: Decimal = Decimal("0")
    commission_asset: str = ""
    strategy_id: str | None = None
    quote_qty: Decimal | None = None
    side: str = "BUY"
    base_asset: str = "BNB"
    quote_asset: str = ""


@dataclass(frozen=True)
class TransferRecord:
    transfer_id: str
    asset: str
    amount: Decimal
    from_account: str
    to_account: str
    ts: datetime
    status: OperationStatus


@dataclass(frozen=True)
class Operation:
    client_id: str
    kind: OperationKind
    scope: str
    payload: OrderPlan | CancelPlan | AssetTransferPlan
    created_at: datetime
    status: OperationStatus = OperationStatus.PENDING
    exchange_id: str | None = None
    checked_at: datetime | None = None
    error: str = ""
    state: ReplenishmentState = ReplenishmentState.IDLE
    balance_before: AccountSnapshot | None = None
    balance_fill_cursor: int | None = None
    balance_pending: bool = False
    failure_recorded: bool = False

    @property
    def unresolved(self) -> bool:
        return self.status in {OperationStatus.PENDING, OperationStatus.UNKNOWN} or (
            self.status == OperationStatus.CONFIRMED and self.balance_pending
        )


@dataclass(frozen=True)
class OperationResult:
    status: OperationStatus
    exchange_id: str | None = None


@dataclass(frozen=True)
class RuntimeState:
    state: ReplenishmentState = ReplenishmentState.IDLE
    candidate: ReplenishmentState | None = None
    candidate_streak: int = 0
    run_mode: RunMode = RunMode.AUTO
    guard: CrashGuardState = field(default_factory=CrashGuardState)
    slice_state: SliceState = field(default_factory=SliceState)
    last_inventory_at: datetime | None = None
    last_inventory_attempt_at: datetime | None = None
    read_retry_at: datetime | None = None
    read_failures: int = 0
    api_errors: int = 0
    transfer_failures: int = 0
    pause_reason: str = ""
    resume_replenishment: bool = False
    resume_repricing: bool = False
