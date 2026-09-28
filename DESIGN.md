# BNB 现货买入与合约账户补仓设计方案

## 1. 目标与背景

本系统用于维护合约账户中的 BNB 余额，核心目标是：

1. 尽量保证合约账户 `contract_bnb >= 25`。
2. 大多数时间不追价，通过现货分层限价单以更优价格买入 BNB。
3. 在需要买入 BNB 时，先从合约账户安全地划转稳定币到现货，再买入并划转 BNB 回合约。
4. 严格限制自动把大量稳定币转换成 BNB，避免侵蚀保证金与过度持仓。

本文中的 `USD` 统一表示可在合约与现货之间划转、并用于买入 BNB 的稳定币资产。实盘中通常对应 `USDT`、`FDUSD` 或 `USDC`，具体币种由配置决定。

本设计默认交易所为 Binance，合约账户类型明确为 `USDⓈ-M Futures`。涉及“从合约账户划出多少稳定币”时，统一以 USDⓈ-M Futures 的 `maxWithdrawAmount` 作为自动划转安全口径；`availableBalance` 只作为运行态观察指标，不直接替代 `maxWithdrawAmount` 做自动划转决策。

## 2. 资金流与设计原则

```text
合约账户 (USD)  --划转 USD-->  现货账户 (USD)
                                 |
                                 v
                         限价/分层买入 BNB
                                 |
                                 v
                          现货账户 (BNB)
                                 |
                         划转 BNB 至合约账户
                                 |
                                 v
                        合约账户 (BNB >= 25)
```

设计原则如下：

1. 先保证合约可用性，再优化买入价格。
2. 先判断紧迫度，再决定订单 aggressiveness。
3. 先看全局敞口，再决定划转和下单数量。
4. 任何自动行为都必须通过 `maxWithdrawAmount`、预算、最小粒度、熔断规则约束。
5. 任何数量与价格都必须经过交易所过滤器归一化，不能直接用理论值下单。

## 3. 范围与非目标

本设计覆盖：

1. 合约账户 BNB 底仓监控。
2. USDⓈ-M Futures 到现货账户的稳定币划转决策。
3. 现货买入 BNB 的分层下单与订单管理。
4. 现货到 USDⓈ-M Futures 的 BNB 回补。
5. 风控、熔断、告警与幂等。

本设计不覆盖：

1. 方向性投机，不基于技术指标主动做趋势交易。
2. 合约仓位风控本身，只消费 USDⓈ-M Futures 账户暴露出的 `maxWithdrawAmount`、`availableBalance` 与余额信息。
3. 完整量化回测框架，但会给出后续实现接口。

## 4. 核心参数

### 4.1 业务参数

| 参数 | 符号 | 默认值 | 说明 |
|---|---:|---:|---|
| 合约 BNB 下限阈值 | `B_low` | 25 BNB | 低于此值进入紧急补仓 |
| 合约 BNB 预警阈值 | `B_alert` | 28 BNB | 低于此值开始主动挂单 |
| 合约 BNB 目标值 | `B_target` | 32 BNB | 单轮补仓目标 |
| 合约 BNB 上限阈值 | `B_high` | 40 BNB | 高于此值停止买入 |
| 现货 BNB 保留量 | `B_spot_reserve` | 0.5 BNB | 现货保留量，不参与回划 |
| 稳定币最小划转单位 | `U_min` | 500 USD | 合约到账户的最小自动划转粒度 |
| 划转预警比例 | `alpha_warn` | 10% | `U_min` 或单次划转额 / `maxWithdrawAmount` 超过后触发硬 veto |
| 划转危险比例 | `alpha_crit` | 25% | `U_min` 或单次划转额 / `maxWithdrawAmount` 超过后触发 CRITICAL veto |
| 可划转绝对下限 | `M_abs_min` | 2000 USD | 自动划转前后，`maxWithdrawAmount` 都必须高于此值 |
| 24h 稳定币预算上限 | `U_budget_24h` | 3000 USD | 滚动 24h 最大划转额度 |
| 价格滑点缓冲 | `slippage` | 1.5% | 估算所需稳定币时使用 |
| 默认检查周期 | `T_check` | 30 分钟 | 定时轮询周期 |
| 现货闲置稳定币上限 | `U_spot_idle_max` | 1000 USD | 无活动订单时可回划合约 |

### 4.2 交易所约束参数

订单过滤器从交易所元数据动态获取并定期刷新。通用内部划转接口未提供最小划转量元数据，`transfer_min_bnb/usd` 通过适配器配置覆盖；不能使用链上提币最小金额代替内部划转约束：

| 参数 | 说明 |
|---|---|
| `qty_step` | BNB 下单数量步长 |
| `price_tick` | BNB 下单价格最小跳动 |
| `min_qty` | 最小下单数量 |
| `min_notional` | 最小成交额 |
| `transfer_min_bnb` | 最小 BNB 划转量 |
| `transfer_min_usd` | 最小稳定币划转量，如果交易所有额外限制需覆盖 `U_min` |

所有计算结果都必须通过：

```python
qty = floor_to_step(qty, qty_step)
price = floor_to_step(price, price_tick)
notional = qty * price
assert qty >= min_qty and notional >= min_notional
```

### 4.3 急跌选择性撤单参数（设计候选值）

本节及第 11.1 节已接入纯决策函数、秒级行情采样、持久化和执行编排。以下默认值仍属于候选参数，功能测试通过不代表参数已通过实盘或历史收益验证；配置读取后统一转换为 `Decimal` 或整数。

| 参数 | 配置名 | 候选值 | 说明 |
|---|---|---:|---|
| 1 分钟回撤触发线 | `drawdown_1m_enter` | 0.8% | 当前价格相对窗口高点的回撤 |
| 5 分钟回撤触发线 | `drawdown_5m_enter` | 1.5% | 任一窗口达到触发线即进入保护 |
| 15 分钟回撤触发线 | `drawdown_15m_enter` | 3% | 捕捉持续下跌 |
| 留单价格折扣 | `keep_price_discount` | 1% | 基于最新参考价计算普通买单保留上限 |
| 单次保护期间累计买入上限 | `max_acquisition_bnb` | 3 BNB | 已成交量与尚可能成交量共用额度，包含紧急买入 |
| 最多保留普通买单笔数 | `max_keep_orders` | 3 笔 | 只处理本策略订单 |
| 最短无触发时间 | `cooldown_seconds` | 300 秒 | 距最近一次触发条件成立的时间 |
| 恢复稳定观察时间 | `stable_seconds` | 180 秒 | 条件必须连续成立 |
| 恢复时 1 分钟回撤上限 | `drawdown_1m_exit` | 0.3% | 小于触发线，避免反复切换 |
| 行情采样与检查间隔 | `check_seconds` | 1 秒 | 独立于 30 分钟库存检查周期 |
| 价格中位数窗口 | `smooth_seconds` | 3 秒 | 降低单次报价尖峰的影响 |
| 行情失效时间 | `max_market_age_seconds` | 5 秒 | 缺失、过期或异常行情不得用于放宽限制 |

## 5. 状态模型

系统采用 4 个运行状态：

| 状态 | 条件 | 行为 |
|---|---|---|
| `IDLE` | `contract_bnb >= B_high` 或 `contract_bnb >= B_alert` 且 `T_depletion > 72h` | 取消多余买单，不新开仓 |
| `WATCH` | `B_alert <= contract_bnb < B_high` 且 `T_depletion <= 72h` | 分层挂低吸单，偏好 Maker |
| `ACCUMULATE` | `B_low < contract_bnb < B_alert` 或 `T_depletion <= 24h` | 收紧折扣，加速成交 |
| `URGENT` | `contract_bnb <= B_low` 或 `T_depletion <= 6h` | 立即补仓，允许吃单或市价 |

### 5.1 消耗速率与耗尽时间

```python
r = max(0, (bnb_at_t_minus_dt + confirmed_bnb_in - confirmed_bnb_out - bnb_now) / dt_hours)

if r == 0:
    T_depletion = inf
else:
    T_depletion = max(0, (contract_bnb - B_low) / r)
```

分别计算最近 6h 与 24h 的整窗平均消耗率，再取双窗口中位数，降低偶发波动影响：

```python
r_use = median(r_6h, r_24h)
```

每个窗口选取边界时刻或此前最近的余额快照，以该快照至当前账户快照的实际小时数为分母；只调整此期间已确认的 BNB 划入与划出，起点不含、终点包含。不能对相邻快照的消耗率先取中位数，否则间歇性集中扣费会被多数零消耗区间抹掉。不足一个完整窗口时不使用该窗口；两个窗口均不足时，只使用余额阈值判定，不依赖 `T_depletion`。

### 5.2 状态抖动控制

为避免状态来回跳变：

1. `URGENT` 立即生效，不延迟。
2. 其他状态切换需连续 2 个周期满足条件后确认。
3. 从 `URGENT` 退出到 `ACCUMULATE/WATCH` 时，要求 `contract_bnb >= B_low + 1`。

## 6. 账户视图与在途资产

为了避免重复划转、重复下单，系统决策不能只看静态余额，必须看“有效可用量”。

### 6.1 统一库存口径

余额是实际净库存的唯一主来源。`spot_bnb` 和 `spot_usd` 都表示现货总余额，包含冻结量；只有总余额减冻结量后才能用于执行。`contract_max_withdraw_amount` 取配置稳定币的 USDⓈ-M Futures 资产级 `maxWithdrawAmount`，不得用账户总 USD 估值或 `availableBalance` 替代。

| 字段 | 含义 |
|---|---|
| `reserved_spot_usd` / `reserved_spot_bnb` | 冻结的现货资产，不能支出或回划 |
| `pending_bnb_to_contract` | BNB 回划未决操作的总量，用于门禁，不直接加到库存 |
| `pending_usd_to_spot` / `pending_usd_to_contract` | 两个方向的 USD 未决划转，用于门禁 |
| `bnb_in_transit` | 能确认已从现货扣除、又未计入合约余额的 BNB；只补这段余额缺口 |
| `filled_untransferred_bnb` | 兼容纯决策调用的修正量：仅表示尚未反映在所传现货余额中的净成交 BNB，不能填写所有未回划成交 |
| `open_buy_remaining_qty` | 本交易对所有买单的未成交量，包括人工及其他策略订单 |
| `has_pending_orders` / `has_pending_cancels` | 提交或撤单结果未决，未终结的 IOC 也在此门禁内 |
| `has_unknown_orders` / `has_unknown_transfers` | 结果未知，禁止新增操作直至对账 |

实盘 `Reconciler` 先查询操作、订单和成交，再读取余额并再次核对活动订单。成交已计入净余额，所以 `filled_untransferred_bnb` 始终置零；未决划转直接阻断新增操作，不能推测扣款/到账来解除门禁。两次订单视图不一致，或订单累计成交量尚未被成交明细覆盖时，禁止新增动作并继续对账。成交按 `(symbol, trade_id)` 去重，矛盾的同 ID 成交记录报错，不覆盖原账。

### 6.2 有效缺口计算

```python
owned_supply = (
    contract_bnb
    + max(spot_bnb - B_spot_reserve, 0)
    + bnb_in_transit
    + filled_untransferred_bnb  # 仅未反映在余额中的修正量；实盘为零
)
effective_supply = owned_supply + open_buy_remaining_qty
delta_bnb = max(active_buy_target - effective_supply, 0)
```

划转意图不等于新增资产。源账户尚未扣款时只计算现货余额；已扣未到时仅计算确定的 `bnb_in_transit`；到账后只计算合约余额。成交日志用于审计和保护事件的**毛买入额度**，余额用于扣费后的**净库存**，两者不叠加。交易所确认划转成功但两侧余额尚未对齐时，仍视为未完成对账，禁止新增订单与划转；不能把暂时少显示的 BNB 当成新的库存缺口。此类未决、过期或不一致快照不写入库存历史。

## 7. 合约到账户划转稳定币策略

### 7.1 所需金额计算

仅在状态机确认的状态为 `WATCH`、`ACCUMULATE` 或 `URGENT`，且存在净补货缺口时，才计算买入所需的 USD 划转。确认状态为 `IDLE` 时，即使有效库存低于 `B_target`，也不生成新买单或合约到现货的 USD 划转；候选状态尚未完成连续周期确认时同样适用。已有可用 BNB 回划和闲置现货 USD 回划仍按各自规则及运行模式、对账门禁处理。

划转前复用订单规划器，将本轮允许的补货量按最终订单价格、数量步长、最小数量、最小成交额及最大数量/金额约束归一化；此阶段暂不施加现货资金预算，仅用于估算需求。若无法生成任何合规订单，则不调入 USD，包括缺口小于数量步长、折扣后金额不足及追价过滤等情况。普通订单仍以参考价保守估算资金，紧急 IOC 使用向上归整后的最终限价。

```python
executable_qty = sum(order.qty for order in normalized_demand.orders)
usd_needed_gross = executable_qty * funding_price * (1 + slippage) * (1 + fee_reserve_rate)
spot_free_usd = max(spot_usd - reserved_spot_usd, 0)
usd_needed_net = max(usd_needed_gross - spot_free_usd, 0)

usd_transfer_raw = ceil(usd_needed_net / U_min) * U_min
```

如果 `usd_needed_net == 0`，则无需从合约划转稳定币。

### 7.2 最小粒度单独检查

这是本设计最关键的补强点之一。即使本轮实际只需要很小金额，只要最小划转单位 `U_min` 相对 `maxWithdrawAmount` 过大，也不能让系统持续自动划转。

```python
granularity_ratio = U_min / contract_max_withdraw_amount
```

规则如下：

1. 若 `granularity_ratio > alpha_crit`，直接 `CRITICAL + HARD_VETO`。
2. 若 `alpha_warn < granularity_ratio <= alpha_crit`，直接 `WARNING + HARD_VETO`。
3. 只有 `granularity_ratio <= alpha_warn` 时，才允许进入后续自动划转检查。

这条规则优先级高于普通的 `usd_transfer_raw / maxWithdrawAmount` 比例判断，因为它专门解决“最小划转单位 500 USD 已经太大”的问题，并且它是硬 veto，不因状态切换为 `URGENT` 而失效。

### 7.3 划转约束的执行顺序

`core/risk_checks.py:evaluate_transfer_gate()` 为唯一划转风控实现。需要划转时先确定：

```python
minimum = ceil_to_multiple(max(U_min, transfer_min_usd), U_min)
requested = max(minimum, ceil_to_multiple(usd_transfer_raw, U_min))
```

依次检查：

1. `maxWithdrawAmount` 必须为正；`minimum / maxWithdrawAmount` 超过 `alpha_crit` 或 `alpha_warn` 时硬 veto。这包含并加强了原有 `U_min / maxWithdrawAmount > alpha_warn` 硬 veto。
2. 划转前后 `maxWithdrawAmount` 均不得低于 `M_abs_min`。
3. 单次申请金额比例超过 `alpha_crit` 或 `alpha_warn` 时停止，不自动降低申请金额来重试。
4. 滚动 24h 预算不足时，可以按 `U_min` 向下取整裁剪，但裁剪后仍必须达到 `minimum`，否则停止。

24h 预算来自交易所已确认的合约转现货记录，包含同账户的人工划转；未决记录保守预占预算，已知流水号不重复累计。无法归属的 UNKNOWN 划转不会因为余额增加而被推定成功。

### 7.4 执行前置条件

获准划转的资金不计入当前买单预算。有获准的 USD 划转时，`CyclePlan` 的买单组为空；执行器先持久化意图并发起划转，确认交易所结果、刷新现货余额后，重新调用 `build_cycle_plan()`。仅重新规划产生的买单可以执行。

划转 veto 只禁止合约转现货 USD，不跳过对账或保护性撤单；现货已有资金能否使用，由运行模式、数据、在途和急跌额度共同决定。所有订单的名义金额加费用预留不得超过已确认的现货可用资金。

### 7.5 紧急状态特例

在 `URGENT` 状态下：

1. `URGENT` 只提升下单 aggressiveness，不绕过任何 USD 划转风控。
2. 只要触发“最小粒度硬 veto”，即使状态是 `URGENT`，也禁止自动从 USDⓈ-M Futures 划转稳定币。
3. 触发上述划转 veto 后，`URGENT` 只能消费现货已有的 USD/BNB，或等待人工处理。
4. 急跌保护期间还须遵守第 11.1 节的应急目标与累计买入额度。

### 7.6 现货闲置稳定币回划

为避免现货账户残留过多稳定币：

1. 仅当没有活动买单、状态不是 `URGENT`、且 `spot_free_usd > U_spot_idle_max` 时，才考虑回划合约。
2. 回划后保留 `max(U_min, 0.5 * usd_needed_gross)`；`usd_needed_gross` 按当前完整净缺口估算，不能仅用本切片需求。
3. `risk.sweep_enabled` 独立控制回扫；按 USD 划转粒度归整并检查交易所最小划转量。WATCH/ACCUMULATE 没有新买单或资金调入需求时也可回扫。
4. 有任意未决操作、冻结 USD、现存买单或本轮计划撤单时不回扫；本轮执行过资金调入后也不反向回扫。
5. 普通订单重定价或 USD 调入已进入续作流程时，保留补货资金；即使首次下单明确拒绝后等待重新规划，或正在等待切片间隔、有效数据、追价限制、运行模式等门禁，也不把已调入或撤单释放的 USD 当成闲置资金回扫。续作完成、目标已覆盖、进入 IDLE 或急跌保护终止普通续作后，才重新按闲置资金规则判断。

## 8. 现货买入策略

### 8.1 参考价格

建议使用短周期稳健锚点，而不是单个最新成交价：

```python
ref_price = min(mid_price, vwap_5m, best_ask * 0.999)
```

这样可以减少用瞬时尖峰价格做锚导致的追价问题。急跌期间，用新鲜行情重新计算此参考价，并按第 11.1 节计算普通买单的保留价格上限；不得复用下单时的旧参考价。

### 8.2 状态对应买入策略

| 状态 | 下单方式 | 折扣 | 订单类型 | 单笔生命周期 |
|---|---|---|---|---|
| `WATCH` | 3 层分布 | `-0.5% / -1.2% / -2.5%` | `LIMIT_MAKER` 优先 | 24h，超时重估 |
| `ACCUMULATE` | 3 层分布 | `-0.3% / -0.7% / -1.5%` | `LIMIT` 或 `LIMIT_MAKER` | 8h，超时收紧 |
| `URGENT` | 1 次或多次快补 | `0%` 或 `<=0.2%` | `MARKET` / 对手价 IOC | 立即成交 |

说明：

1. 原始方案中的 `WATCH 48h` 更适合作为策略窗口，不建议单张订单存活 48h。
2. `WATCH` 应每 6h 重定价一次，`ACCUMULATE` 应每 2h 重定价一次。
3. 若交易所支持 `postOnly`，`WATCH` 优先使用，减少 taker 成本。

紧急 IOC 必须先确定按 `price_tick` 向上归整后的最终限价，再用该价格计算可买数量和检查最小成交额：

```python
ioc_price = ceil_to_tick(max(best_ask, ref_price) * (1 + urgent_ioc_buffer))
qty = floor_to_step(min(delta_bnb, quote_budget / ioc_price), qty_step)
# 不满足 min_qty 或 min_notional 时不生成订单。
assert qty >= min_qty and qty * ioc_price >= min_notional
assert qty * ioc_price <= quote_budget
```

补货缺口也须按数量步长向下归整，不能因缺口小于预算允许数量而生成不符合步长的 IOC。以预算 3000 USD、最终限价 601.71、数量步长 0.01 为例，最多买入 4.98 BNB，订单名义金额为 2996.5158 USD。此检查约束订单名义金额；`fee_reserve_rate` 已在引擎中预留，传给订单规划器的是 `spot_free_usd / (1 + fee_reserve_rate)`。上述 4.98 BNB 示例指不含费用的订单预算；SDK 适配器还读取账户费率，若配置预留低于 Maker/Taker 费率则停止执行。

### 8.3 分层数量分配

默认分配：

```python
layers = [
    (0.30, 0.005),
    (0.40, 0.012),
    (0.30, 0.025),
]
```

或在 `ACCUMULATE` 使用：

```python
layers = [
    (0.30, 0.003),
    (0.40, 0.007),
    (0.30, 0.015),
]
```

每层下单前：

```python
qty_i = floor_to_step(total_qty * weight_i, qty_step)
price_i = floor_to_step(ref_price * (1 - discount_i), price_tick)
```

若某层因精度或最小成交额无法满足 `min_notional`，则并入相邻层。

### 8.4 大额补仓切片

当 `delta_bnb > 10` 时，建议在状态窗口内进一步切片：

```python
slice_qty = min(3, delta_bnb)
num_slices = ceil(delta_bnb / slice_qty)
```

达到切片触发条件后持久化 `SliceState.active`，即使剩余缺口降到 10 BNB 以下仍保持每组最多 3 BNB，直到目标覆盖或状态回到 IDLE。默认每组间隔 1800 秒（`slice_interval_seconds`）。首次提交订单的意图和下一次允许时间在同一个 SQLite 事务中写入，重启、UNKNOWN 或后续重规划都不能跳过等待。每个切片可以包含三层价格订单，切片总量共用上限；分层与分时是两个独立约束。

秒级缓存检查必须读取最新持久化切片状态，且只持久化风险与状态判断，不回写缓存计算的切片状态。订单提交后即使响应超时，也不能用提交前的缓存清除或提前退休切片等待时间；只有完整对账后的规划可以更新切片状态。

### 8.5 追涨过滤器

当 BNB 出现短线快速上涨时，不急于追价：

```python
if state != "URGENT" and return_1h > 0.03:
    keep_existing_lower_bids_only()
    do_not_raise_above(ref_price)
```

实现采取保守口径：非 URGENT 且 1h 涨幅严格大于 3% 时不新增买单或调入 USD，只保留符合库存、价格及 TTL 的旧单，并暂停会导致抬价的定期重定价。

这条规则与第 11.1 节的急跌选择性撤单配套。急跌保护未解除时，普通买单不得新增或向上重定价；价格反弹也不抬高本次保护的留单价格上限。退出保护后，仍须独立通过本节追涨检查。

## 9. 订单生命周期管理

### 9.1 基本规则

每个周期执行以下动作：

1. 拉取并对账订单、成交与划转状态，更新库存及冻结资金。
2. 判断业务状态与急跌保护状态；即使补货缺口为零，也必须执行订单管理。
3. 只针对本策略订单，生成超时、价格过高、数量超限等撤单计划，先持久化意图再提交。
4. 撤单后确认最终累计成交量与剩余量，再刷新余额。撤单 `PENDING/UNKNOWN` 时仍占用敞口，不得提前释放资金或生成替代单。
5. 用对账后的数据重新计算缺口；仅在所有门禁允许时补挂净新增需求。急跌期间不补挂普通买单。

### 9.2 超时与重定价

```python
if order_age > ttl_by_state[state]:
    cancel(order)
    # 撤单确认并刷新余额后，才重新判断是否允许重挂。
    reprice_if_needed_after_reconciliation()
```

第 11.1 节优先于普通重定价规则。保护期间低价单也受原 TTL 约束，到期撤销，不延长寿命、不自动补挂。

WATCH/ACCUMULATE 的普通 TTL 到期、定期或偏离重定价，可以在非库存周期启动受控续作：撤单意图与 `resume_repricing` 标志原子持久化，撤单确认并完成订单、成交、余额对账后，按最新净缺口补挂一组订单，无须等待下个库存周期。未决撤单仍阻断补挂；重启保留续作标志，未知撤单不重发。续作重新检查急跌、追涨、运行模式、行情与余额有效期、资金及 USD 划转风控、切片等待。首次新订单意图先消耗续作资格；若提交结果明确为 FAILED，则与失败状态原子恢复提交前的续作标志，留待后续对账周期重新规划，不在同轮立即重试，也不回退已持久化的切片截止时间。资金调入后的 `resume_replenishment` 同样适用。已接受或结果未决的订单仍消耗资格，同一进程只可继续该组已经限量的剩余分层；普通行情周期不会因此反复新开补货组。保护性撤单不授予普通补挂资格，连续拒单仍触发失败暂停。

### 9.3 偏离过大处理

```python
deviation = (current_price - order.price) / order.price

if crash_guard.active:
    apply_selective_cancel_policy(order, crash_guard)
elif deviation > 0.05 and state in {"ACCUMULATE", "URGENT"}:
    request_cancel_then_replan_after_reconciliation(order)
```

保护期间是否留单，以第 11.1 节的价格、剩余数量、笔数和 TTL 条件共同决定，不再使用“价格下跌超过 3% 就保留 WATCH 买单”的规则。若行情已经穿过买单价格，应先查询实际成交状态；不能假设该单仍可撤销。

### 9.4 部分成交处理

若订单部分成交：

1. 成交按唯一 ID 记入日志，刷新余额后只通过 `spot_bnb` 计入净库存，不再叠加未回划成交。
2. `ACCUMULATE` 和 `URGENT` 下，只要可回划数量达到最小阈值，就优先回划合约。
3. `WATCH` 下可累计到 `>= 2 BNB` 再批量回划，减少频繁操作。

## 10. BNB 从现货回划合约

### 10.1 划转规则

```python
transferable_bnb = max(spot_bnb - B_spot_reserve, 0)
needed_bnb = max(B_target - contract_bnb - pending_bnb_to_contract, 0)
bnb_transfer = min(transferable_bnb, needed_bnb)
```

执行条件：

1. `bnb_transfer >= transfer_min_bnb`
2. `contract_bnb < B_high`
3. 不存在同一资产、同一方向的重复在途划转

### 10.2 划转时机

| 状态 | 回划时机 |
|---|---|
| `WATCH` | 已成交 BNB 累积到 2 BNB 再划转 |
| `ACCUMULATE` | 有成交就尽快回划 |
| `URGENT` | 成交后立即回划 |

## 11. 熔断与异常场景

### 11.1 BNB 价格急跌：选择性撤单

目标是在近期价格快速下降时，撤销基于旧价格或规模过大的买单，同时保留符合最新价格限制的低价订单。该机制限制新增库存，不能撤销已经发生的成交，也不自动卖出现有 BNB。

#### 11.1.1 触发与数据口径

官方现货 SDK 的单交易对 ticker 推送提供每秒买卖价及交易所事件时间；独立行情线程持续采样，不受 REST 执行阻塞。取最近 3 秒有效中间价的中位数为 `p(t)`。当前价与窗口高点使用同一平滑价格序列：

```text
high_W(t) = max(p(s), s 位于最近 W 时间窗口)
drawdown_W(t) = 1 - p(t) / high_W(t)

trigger = drawdown_1m >= 0.008
       or drawdown_5m >= 0.015
       or drawdown_15m >= 0.03
```

以上是第 4.3 节的候选配置。任一条件成立即进入 `GUARDED`，不等待业务状态的两周期确认。`NORMAL/GUARDED` 是独立风险状态，不替代 `IDLE/WATCH/ACCUMULATE/URGENT`。24h 跌幅仅保留为观察和告警指标，不触发本项急跌保护。

行情服务应检查时间戳、正价格、买卖价关系和数据连续性。历史不足的窗口标记为不可用，不能当成回撤为零；已有完整短窗口可以触发保护。当前实现保守要求所有新增买单（含 IOC）和保护解除均具备完整 15 分钟连续窗口；启动/断流后需要重新预热。行情缺失、断流、过期或异常时禁止新增买单、清空恢复观察计时，并保留原保护状态，不得自动恢复。使用行情推送与秒级检查，不等待 30 分钟库存轮询或 K 线收盘。行情线程复用相同触发逻辑，保留未处理触发的首次时间、最近触发时间和最低留单上限；REST 等待期间即使价格反弹也不能覆盖它。主循环在对账前后及提交动作前消费触发，按首次触发时间归集成交，先原子持久化运行态、保护事件及审计记录，再确认消费。缓冲最多保存一个待确认汇总和一个新增汇总，确认旧事件不会清除期间的新触发；断流只清空行情和窗口，保留未处理风险。

#### 11.1.2 按最新价格筛选旧单

在有效行情下，按第 8.1 节重算参考价：

```text
new_ceiling = floor_to_tick(
    min(ref_price_now, p(t)) * (1 - keep_price_discount)
)

首次触发：keep_price_ceiling = new_ceiling
保护期间：keep_price_ceiling = min(上次上限, new_ceiling)
```

仅检查本策略创建的普通买单：

1. `order.price > keep_price_ceiling`：撤销剩余未成交部分。
2. `order.price <= keep_price_ceiling`：进入留单候选集，还须通过数量、笔数、TTL、业务状态和其他风控限制。
3. 保护期间上限可以随价格下降而降低，不随反弹升高；被撤订单不重新挂回。
4. 不新增普通买单，不把高价单撤掉后立即换成低价单，不因已有低价单成交而补挂。
5. 行情失效时，不提高上次有效上限，继续对账并执行可确定的保护性撤单；如果连上次有效上限或敞口都无法确定，则撤销已知的本策略普通买单并告警，待数据恢复后重新评估。

#### 11.1.3 限制笔数与累计买入量

每次从 `NORMAL` 进入 `GUARDED` 创建一个持久化的保护事件。该事件中，所有本策略买单的成交量，包括撤单途中成交、紧急买入及延迟到达的成交回报，均按真实成交时间和唯一成交标识去重计入 `guard_filled_bnb`。使用成交数量计额度，库存则使用扣费后的实际到账量。

```text
episode_remaining = max(max_acquisition_bnb - guard_filled_bnb, 0)
inventory_remaining = max(active_buy_target - owned_supply - external_buy_remaining_qty, 0)
keep_qty_cap = min(episode_remaining, inventory_remaining)
```

`owned_supply` 表示已持有的合约 BNB、可用现货 BNB 及不与两者重复的 BNB 在途库存，排除现货保留量和未成交买单。成交已反映在余额中时不得再叠加 `filled_untransferred_bnb`；划转已从源账户扣除或已到账时也不得重复计算。`external_buy_remaining_qty` 是已知人工或其他策略买单的剩余 BNB，计入总库存敞口，但不由本策略撤销。普通状态的 `active_buy_target` 为当前目标库存（含其他风控下调），紧急状态按第 11.1.4 节取更低目标。

留单候选按价格从低到高、创建时间从早到晚、订单 ID 排序。逐单保留，要求剩余未成交数量之和不超过 `keep_qty_cap`，笔数不超过 `max_keep_orders`。某单剩余量放不进额度时整单撤销，继续检查后续候选；第一版不自动拆单或重挂。默认最多保留 3 笔，且一次保护事件中的累计成交量加尚可能成交量不超过候选额度 3 BNB。

该额度不能在每次轮询、每次触发、每次成交或重启时重置。撤单尚未确认的订单、提交结果未知的订单仍按最大可能剩余量占用敞口，阻止相关新增买单；即使暂时超限，也须继续撤销超额部分并告警，不能视作已经释放额度。若撮合先于撤单导致实际累计成交超限，记录全部成交、撤销其余买单，并停止本次保护事件中的新增买入。

例如，最新参考价和 `p(t)` 均为 600，折扣为 1%，留单上限为 594；剩余买入额度为 3 BNB：

| 已有买单价格 | 未成交量 | 决策 |
|---:|---:|---|
| 598 | 1 BNB | 高于 594，撤销 |
| 590 | 2 BNB | 低价候选，但保留 580 的订单后额度不足，整单撤销 |
| 580 | 2 BNB | 优先保留，仍占用 2 BNB 买入额度 |

580 的订单若成交 1 BNB，累计已买入 1 BNB、剩余挂单 1 BNB；剩余额度不会恢复为初始的 3 BNB。撤单期间发生的额外成交也必须扣减额度。

#### 11.1.4 资金与紧急例外

1. 普通补货不新增合约到现货的 USD 划转；保留买单使用其已冻结的资金。撤单计划尚未确认前不能把冻结资金视为可用或闲置资金。
2. 实际库存不足时优先回划已有可用 BNB，且不存在同方向 `PENDING/UNKNOWN` 划转。
3. `URGENT` 的买入目标为 `min(当前目标库存, B_low + 1)`，默认最多补到 26 BNB。仅因消耗速度进入紧急状态、但已有库存达到该目标时，不新增应急买单。
4. 低价挂单属于潜在敞口，不代表已到手库存。若需用 IOC 尽快补货，应先撤销拟替换的低价单、确认最终成交与余额，再计算净需求；不能忽略旧单后额外买入。
5. 应急 IOC 可以使用独立的对手价与限价缓冲，不受普通留单价格折扣约束；但仍与保留普通买单共用本次保护的累计买入额度，且全部潜在成交不得超过应急目标缺口。数量按 IOC 实际限价及费用预留计算，不能按较低参考价计算可购买量。
6. 优先使用现货资金。确需补充 USD 时仍须通过 `maxWithdrawAmount`、最小粒度硬 veto、比例、绝对下限和 24h 预算检查，并确认到账后再买入；保护期间不得自动上调额度或重开保护事件来绕过限制。额度或资金不足以补到应急目标时告警，等待恢复或人工处理。
7. 本节不绕过运行模式和对账门禁。`PAUSED` 禁止新增买入与划转；对账及已知订单的保护性撤单仍需处理。只有能确认属于本策略的订单可被撤销，不操作人工或其他策略的订单。

余额、成交、划转历史、活动订单或过滤器的读取失败时，进入仅保护性撤单的降级路径。该路径不构造可交易的账户快照，不新增买入、划转或回扫，也不解除急跌保护。它独立更新可确认的急跌触发状态，并以操作日志和已跟踪订单确定归属；由于无法验证完整敞口，保守撤销已知的本策略普通买单。单笔订单查询失败仍可使用已持久化的交易所订单 ID 撤单，未取得可确认身份的订单继续等待对账；人工及其他策略订单不处理。已有 PENDING/UNKNOWN 或已确认终态的撤单不重复提交。只观察模式同样禁止降级路径的实际撤单。

#### 11.1.5 恢复与重启

触发条件成立时更新 `last_trigger_at`；仅首次进入保护创建事件，后续触发不重置额度。解除保护须同时满足：

1. 距最近一次触发条件成立至少 `cooldown_seconds`，且全部窗口均有效、当前均未触发。
2. `drawdown_1m < drawdown_1m_exit` 连续满足 `stable_seconds`；数据失效或条件不满足即清空稳定计时。
3. 相关撤单、订单及划转已经完成对账，不存在结果不明的操作，且余额新鲜、视图一致。缓存行情检查可以推进观察计时，但解除保护必须等待一次实际对账。

恢复时不要求价格回到下跌前水平。重新读取行情、余额和在途状态，保留仍有效的订单并计入敞口，再按最新参考价及净缺口规划普通买单，同时继续遵守追涨、资金和运行模式限制。

保护事件 ID、开始时间、最近触发时间、稳定观察起点、留单价格上限、累计成交量及相关操作均需持久化。重启后先恢复和对账，补齐行情窗口，重新进行连续稳定观察；停机时间不得算作有效稳定观察，也不能重置本次事件的已用额度。

#### 11.1.6 验收场景

1. 1m/5m/15m 任一窗口恰好达到阈值即触发；24h 跌幅本身不触发。
2. 高于留单价格上限的订单撤销，等于或低于上限的订单仍须通过数量、笔数和 TTL 检查。
3. 低价候选过多时按确定顺序保留，剩余数量过大的订单整单撤销，不能自动拆单补挂。
4. 部分成交、重复或延迟成交回报、撤单竞态和重启都不会重置累计额度或重复记账。
5. 撤单 `UNKNOWN` 时旧单继续占用敞口，禁止替代买单；急跌保护也不能提前回扫其冻结资金。
6. 价格反弹期间不抬高留单上限；行情失效或恢复条件未连续满足时不解除保护。
7. 紧急买入不超过应急目标、事件剩余额度与实际资金，任何情况下均不绕过 USD 划转硬 veto。
8. 即使净补货缺口为零，也会处理高价、超量和过期订单；人工及其他策略订单不被撤销。

### 11.2 保证金持续恶化

```python
if margin_change_24h < -500:
    dynamic_target = max(B_low + 2, B_target - 3)
```

这时系统会降低目标库存，优先保住保证金安全。

### 11.3 连续 API/划转失败

```python
if transfer_failed_twice or api_errors_consecutive >= 3:
    pause_trading()
    alert("CRITICAL", "划转或 API 连续失败，系统暂停")
```

### 11.4 最小粒度无法满足风险限制

当：

```python
U_min / contract_max_withdraw_amount > alpha_warn
```

或等价地，最小划转单位本身已经不安全时：

1. 立即触发 `HARD_VETO`，禁止自动划转。
2. 发出 `WARNING` 或 `CRITICAL`。
3. 依赖人工处理合约风险，而不是让系统继续把稳定币自动换成 BNB。

## 12. 告警体系

### 12.1 级别定义

| 级别 | 场景 | 动作 |
|---|---|---|
| `INFO` | 状态变更、订单成交、正常回划 | 记录日志 |
| `WARNING` | 预警比例触发、预算接近上限、价格急跌 | 日志 + 消息通知 |
| `DANGER` | 进入 `URGENT`、单次划转触及危险边界 | 即时消息 |
| `CRITICAL` | `maxWithdrawAmount` 低于绝对下限、最小粒度也不安全、连续失败 | 多渠道报警并暂停自动交易 |

### 12.2 关键规则

```python
alerts = {
    "bnb_urgent": contract_bnb <= B_low,
    "bnb_near_zero": contract_bnb <= B_low * 0.5,
    "withdraw_low": contract_max_withdraw_amount < M_abs_min,
    "granularity_hard_veto": U_min / contract_max_withdraw_amount > alpha_warn,
    "budget_80pct": usd_transferred_24h >= U_budget_24h * 0.8,
    "budget_exhausted": usd_transferred_24h >= U_budget_24h,
    "bnb_crash_guard": crash_guard.active,
    "bnb_24h_drop_observation": return_24h < -0.08,  # 仅观察告警
    "transfer_failed": transfer_failed,
    "api_error": api_errors_consecutive >= 3,
}
```

急跌保护的进入、原因变化、累计额度超限和退出各记录一次状态事件；相同状态的每秒检查不重复发送告警。

## 13. 主控制循环

以下为编排示意，急跌检查由独立的秒级行情任务唤醒，同账户同交易对的决策与执行串行化，避免与库存定时任务并发提交。秒级行情检查只更新风险并触发必要的订单管理，不增加业务状态机的确认周期计数，也不每秒新开普通补货周期；第 5.2 节的确认次数仍按库存调度周期计算。撤单和划转会改变余额与敞口，必须在确认后刷新并重新规划；不得把一个旧快照生成的所有动作无条件执行到底。所有新增动作受运行模式、数据有效性和对账门禁约束。

`services/runner.py:Runner` 实现以下顺序：

1. 恢复持久化运行态，对账所有未决请求；未知结果不补发。
2. 拉取活动订单、已知订单终态、去重成交和划转历史，再读取余额并复核订单视图。
3. 读取最新行情与过滤器，更新保护事件、动态目标、库存状态和资金门禁。
4. 调用 `build_cycle_plan()`，先持久化状态及告警；只观察模式在这里结束。
5. 优先执行一个保护性撤单。未决操作不允许新增买单/划转，但不阻止其他已知订单的保护性撤单。
6. 没有撤单需要处理且门禁允许时，依次考虑已有 BNB 回划、获准 USD 调入、已用实际余额重新预算的买单、闲置 USD 回扫。
7. 每次写请求先提交操作意图，只发一次；回到第 1 步刷新并重新规划，不继续执行旧快照。
8. 本轮动作组耗尽、没有可执行动作或出现未决新增请求时结束；后续调度负责继续对账。

主循环的告警处理仅写 outbox 并唤醒独立投递线程，Webhook 网络请求不占用交易循环或账户锁。新增操作进入执行前，按实际当前时间再次检查其依赖的余额有效期及行情有效期；BNB 回划仅依赖有效余额，其余新增动作仍要求有效行情。计划过期时丢弃旧候选，重新读取、对账和规划；同一组已提交的数量和切片等待仍然有效。完整读取失败则转入第 11.1.4 节的仅撤单路径。

划转 veto 不等于整个周期提前返回：保护性撤单、现货资金可承受的紧急买入和 BNB 回划仍按各自门禁判断。USD 调入计划与依赖它的买单不会同时出现在一份可执行计划中。

## 14. 幂等、持久化与实现边界

### 14.1 模块职责

| 模块 | 职责 |
|---|---|
| `core/replenishment_engine.py` | 纯计划：状态、动态目标、门禁、撤单、补货、回划、回扫、告警 |
| `core/order_manager.py` / `core/crash_guard.py` | 逐单筛选及独立急跌状态，不调用交易所 |
| `services/runner.py` | 串行对账、规划、执行后刷新；库存周期推进防抖，行情周期只更新风险与订单管理 |
| `services/reconciliation.py` | 查询未决结果，逐单恢复，归集并校验成交、余额和活动订单 |
| `services/executor.py` | 先提交持久化意图，再执行一次请求；异常结果归入 UNKNOWN |
| `services/binance_adapter.py` | 官方 Binance SDK 与规范化模型之间的唯一 REST 边界 |
| `services/market_stream.py` / `services/market_data.py` | 官方 SDK 秒级行情推送、连续窗口、平滑价格与回撤 |
| `services/history.py` | 6h/24h 消耗率及 24h 保证金变化；BNB 划转调整余额序列，历史不足不猜速率 |
| `storage/repository.py` | SQLite 操作日志、成交去重、预算、运行状态、事件、通知 outbox |
| `services/alert_service.py` | 独立线程消费 outbox，日志/Webhook 投递；状态变化去重、失败重试、逐渠道确认 |
| `core/config.py` / `config/strategy.toml` | Decimal 配置读取、未知字段及范围校验 |

### 14.2 持久化与操作状态

SQLite 使用 WAL 和 FULL 同步。`operations` 保存唯一客户端 ID、操作类型/方向、完整载荷、创建时间、交易所 ID 及 PENDING/UNKNOWN/CONFIRMED/FAILED 状态；数据库唯一索引限制同语义范围的 PENDING/UNKNOWN 意图，执行器另以全局未决门禁拦截尚待余额确认的成功划转。所有意图先提交，网络请求后才更新结果。进程在意图写入后崩溃时，只查询结果，不补发请求。

订单的 CONFIRMED 表示提交已被接受，**不代表成交或订单结束**；活动订单的原量、累计成交、剩余量、归属、时间及撤单状态单独维护。取消和划转必须确认终态。提交回执丢失后，通过原客户端 ID 查到匹配订单时，先核验交易对、方向、原数量和价格，再将交易所订单 ID 写入操作日志，之后才能保护性撤单；完整账户读取失败时的降级撤单也遵守该顺序。旧日志可从已持久化的订单跟踪记录恢复已知 ID。有交易所 ID 后，查询和归属以该 ID 为准，继续核验交易对、方向、价格及合法成交量；根据 [Binance 保留优先级改单规则](https://github.com/binance/binance-spot-api-docs/blob/master/faqs/order_amend_keep_priority.md)，允许同一已绑定订单 ID 的数量减少，要求 `0 < 当前数量 <= 原请求数量` 且 `0 <= 累计成交 <= 当前数量`，按当前数量管理剩余敞口。不允许增加数量或修改价格、方向；未绑定交易所 ID 时仍严格匹配原数量。撤单改变客户端 ID 或外部订单复用旧客户端 ID，不得使原订单永久 UNKNOWN 或误认外部订单。发现订单不直接跳过操作结果对账。

划转意图同时保存提交前余额和成交账本游标。CONFIRMED 仅确认交易所终态，`balance_pending` 未清除前仍属于未决操作：现货余额必须匹配原余额、划转额及其间去重成交/手续费的净变化；合约余额须反映对应借贷变化，或其资产级 `updateTime` 已晚于成功确认时间加 1 秒时钟容差，以容纳到账后的手续费/盈亏。使用实际成交 `quoteQty` 核对稳定币支出，不能用挂单限价代替实际成本；合约 quote 钱包余额仅用于到账核对，资金风控仍以 `maxWithdrawAmount` 为唯一基准。不把现货账户元数据的 `updateTime` 当成资产余额水位，不因多次读到相同余额或等待超时而放行。无法解释的余额变化继续阻断；旧未决日志若缺少划转前基线，也不能自动推断到账，升级前应完成旧在途操作的对账。

正常响应、自动对账和人工绑定共用操作结果提交路径。终态、续作标志与失败计数同事务落库；确认 USD 调入失败时清除续作标志，后续周期可以重新规划。同一操作从 UNKNOWN 转为 FAILED 不重复累计操作失败，独立的查询/API 故障仍按原熔断规则计数。

同账户的控制器必须共用同一个数据库。线程锁与文件锁覆盖完整的对账/决策/执行序列；不会在网络等待时持有 SQL 事务。不同数据库之间没有分布式锁，不能为同一账户启动互不知情的多个执行器。

保护事件、稳定观察起点、累计毛成交、留单价格上限、切片状态、防抖上下文、运行模式、连续失败次数均持久化。重启清空稳定观察计时及行情窗口，保留事件已用额度。历史迟到成交按真实成交时间修复对应事件，不按接收时间重新归属。

### 14.3 Binance 官方 SDK 接入

依赖使用 [binance/binance-connector-python](https://github.com/binance/binance-connector-python) 当前模块化发行包，版本固定在 `requirements.txt`：`binance-sdk-spot`、`binance-sdk-derivatives-trading-usds-futures`、`binance-sdk-wallet` 及其共享 `binance-common`。签名、HTTP 和 WebSocket 由官方 SDK 负责；适配器不自行实现签名或 HTTP 交易请求。

三个 REST SDK 均设置 `retries=0`，删除订单也不重试。钱款参数以十进制字符串交给 SDK，避免转为 float；测试通过 SDK 的实际序列化路径核对最终请求。超时、5xx、未知错误以及成功响应解析失败都不等价于失败，须先对账。只有明确的拒绝码才标为 FAILED。适配器检查系统时钟与两个服务的时间偏差，超过 1 秒停止；由系统时间同步服务校准，不篡改 SDK 的全局时钟。

适配器保留 SDK 的 Binance 错误码和错误原因。根据 [Binance 官方错误说明](https://github.com/binance/binance-spot-api-docs/blob/master/errors.md)，新订单提交收到明确的 `-2010` 且原因为 `Account has insufficient balance for requested action.` 时记为 FAILED；`LIMIT_MAKER` 的 `Order would immediately match and take.` 拒单同样处理。后续对账不会把明确拒单重新置为 UNKNOWN，后续周期仍需刷新余额、行情并重新规划。该分类只适用于明确的新订单拒绝响应；5xx、`-2010` 的重复客户端 ID、未识别原因及超时继续按 UNKNOWN 处理，不能仅凭相同错误码判定无订单，也不能用查询“订单不存在”解除未知提交的门禁。

划转请求收到明确的非 5xx 拒绝响应 `-3020 EXCEED_MAX_ROLLOUT`（划出金额超过上限）时，根据 [Wallet 错误码说明](https://developers.binance.com/en/docs/products/wallet/error-code#-3020-exceed_max_rollout)记为 FAILED，并计入连续划转失败熔断，不要求绑定不存在的流水号。5xx 携带相同错误码、`-5012` 等待执行及 `-3029` 泛化划转失败仍按 UNKNOWN 处理，不据此重发。

订单使用持久化 `newClientOrderId`；查询没有找到订单时仍保持 UNKNOWN，不自动重新使用该 ID。[官方订单接口](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/trade)允许已成交后的客户端 ID 被再次使用，因此客户端 ID 本身不是无限期幂等保障。

[通用划转接口](https://developers.binance.com/en/docs/catalog/core-trading-wallet/api/rest-api/asset)不支持客户端幂等 ID。本地操作 ID 用于日志，交易所 `tranId` 用于精确核对。未取得 `tranId` 的超时不能仅凭相同金额或余额变化自动归属，持续阻断新增操作。操作员可通过 `--bind-transfer-id CLIENT_ID TRANSFER_ID` 提供明确流水号；工具核验资产、金额、方向和交易所终态，防止同一流水被重复归属，通过统一结果提交路径更新续作与失败状态，记录人工绑定事件后退出；确认成功的划转仍需在后续对账中通过余额门禁。不能核验时继续阻断。

现货过滤器和手续费定期刷新；内部划转最小量通过 `--min-transfer-bnb/usd` 覆盖，不把链上提币规则当内部划转规则。历史接口分页读取，数据不完整或分页停滞时停止新增操作。资金只往 SPOT 与 USDⓈ-M Futures 两类账户之间划转。

### 14.4 执行顺序与限制

每次执行前用最新的对账结果和行情生成计划；撤单优先，其次回划可用 BNB、调入 USD、买单、闲置 USD 回扫。每次操作后废弃旧快照，重新对账。三层订单可以保留尚未提交的候选，但每层必须重新通过最新的数量、价格、资金、精度和风控条件；风险发生变化时丢弃旧候选。

每次控制循环最多一笔 USD 调入、一组买单、一笔 BNB 回划和一笔回扫，调入和回扫不能在同一轮反向执行。待到账的调入保存续作标志，普通对账周期也能在确认后继续补货，无须等待下次 30 分钟库存周期。临时门禁阻止买入时，标志与资金保留跨周期、跨重启持续有效，终止条件见第 7.6 节；首单意图与标志消费原子持久化，明确拒单时恢复续作，UNKNOWN 时仅对账。已调入资金的续作和已经开始的分层订单组关闭再次调入权限，价格上涨时按已确认现货余额重新预算可买数量，不能因完整缺口需要更多资金而空等或追加划转。所有 PENDING/UNKNOWN 操作以及 CONFIRMED 但余额仍待确认的划转使用全局保守门禁；保护性撤单仍可针对已知归属的订单执行，同一未决撤单不会重复提交。

账户 REST 接口没有跨端点原子快照；当前通过订单前后核对、累计成交覆盖、划转两侧余额核对、快照过期检查及操作后重新读取降低竞态风险。外部人工交易仍可能在最终检查后改变账户，交易所拒绝或未知结果继续走同一对账流程，不能宣称网络调用与撮合之间是原子的。

### 14.5 调度、配置与投递

`python -m services.cli` 启动控制器，默认只观察；`--live` 才启用实际订单、撤单及划转。`--mode` 明确修改持久化运行模式，恢复 AUTO 时也不能绕过未决门禁。环境变量只向 SDK 提供凭据，不写入数据库或日志。

秒级行情线程独立运行，主线程串行评估。完整账户对账间隔取 `reconciliation_seconds` 与余额有效期的较小值；库存周期按 `t_check_minutes` 计算；执行后重规划及行情检查均不累计额外防抖次数。默认最大有效行情年龄 5 秒，余额 10 秒，行情采样间隔断开超过 3 秒重新预热。

行情任务同时监控 SDK 连接状态和最近一条 ticker 的到达时间；意外 EOF、连接错误或超过行情有效期没有新消息时，通过 SDK 重连恢复订阅及回调。SDK 自行进行定时或服务端通知触发的连接替换时，行情任务等待该重连完成，避免并发重连；SDK 重试耗尽后重新建立连接和订阅。重新订阅发送失败时，复用固定版本 `binance-common` 的订阅映射清理函数移除旧连接记录，避免后续订阅被误判为已存在。连接关闭、报错、重连及新连接建立时清除旧快照和行情窗口，恢复后的窗口必须重新预热，不沿用断线前的连续性。

告警按 code、级别及原因变化进入 outbox，相同状态不重复入队，恢复后再次触发可重新告警。独立投递线程在入队时唤醒，并每 5 秒重试尚未送达的记录；每个渠道独立记录投递完成，失败仅重试该渠道。投递线程仅在读取 outbox 和保存确认时短暂取得账户锁，网络发送期间释放锁。关闭时停止线程后再关闭数据库，未完成记录保留供下次运行继续处理。Webhook 传递稳定的 `Idempotency-Key`；进程在远端接受、写本地投递确认之前崩溃时，接收方仍须按该 key 去重。未配置 Webhook 时只写日志。

## 15. 参数调优建议

### 15.1 按资金规模

| 合约规模 | `B_target` | `B_high` | `M_abs_min` | `U_budget_24h` |
|---|---:|---:|---:|---:|
| `< 10,000 USD` | 28 | 32 | 2,000 | 1,500 |
| `10,000 - 50,000 USD` | 32 | 40 | 5,000 | 3,000 |
| `> 50,000 USD` | 35 | 50 | 10,000 | 5,000 |

### 15.2 按消耗速率

| 消耗速率 | `B_alert` | `T_check` | 折扣风格 |
|---|---:|---:|---|
| `< 0.2 BNB/h` | `B_low + 2` | 60 分钟 | 宽松 |
| `0.2 - 1.0 BNB/h` | `B_low + 3` | 30 分钟 | 中等 |
| `> 1.0 BNB/h` | `B_low + 5` | 10 分钟 | 紧凑 |

## 16. 实现与验证范围

以上控制流程已实现。测试覆盖纯决策边界、各类在途门禁、选择性撤单、行情中断与恢复、资金到账依赖、订单/撤单/划转超时、部分成交竞态、持久化重启、切片等待、历史预算、消息去重和官方 SDK 请求序列化/零重试。

组合回归还覆盖：订单超时后的连续秒级检查与切片截止时间；告警渠道阻塞时仍可执行保护性检查；生成计划后行情或余额过期时禁止提交旧动作；多种读取失败、PAUSED 和只观察模式下的已知订单保护；官方 SDK 的 Maker/余额不足明确拒单与重复 ID/未知结果区分；非库存周期的 TTL/重定价补挂、部分成交、撤单 UNKNOWN 后重启、切片等待资金保留以及续作时重新检查所有门禁；首次补挂或调入后首单明确拒绝的续作恢复、重启与事务回滚；集中手续费消耗、不规则采样、划转调整与 6h/24h 整窗统计。

专项回归还覆盖：真实 SDK 接收循环在重复 EOF、错误帧、静默中断、定时连接替换、初次连接失败、重试耗尽和重新订阅发送失败后的行情恢复及窗口重置；订单回执丢失后保护性撤单改变客户端 ID、降级读取、部分成交、旧日志及重启恢复、外部订单复用客户端 ID；`-3020` 明确拒绝与不确定响应的分类及失败熔断；到账后跨临时门禁和重启续作、涨价时按现货余额买入、续作结束后释放回扫保留；不可交易小额缺口、折扣后最小成交额及最大数量对资金需求的限制。

本次修复回归覆盖：划转确认后任一侧余额延迟及重启、USD 调入与回扫的余额核对、划转期间成交和手续费、到账后合约手续费版本更新、人工绑定 FAILED 后重新补货及事务回滚、REST/计划发布等待期间急跌后反弹、急跌期间成交归属、风险确认消费竞态与断流、同 ID 减量改单后的正常/降级保护性撤单及重启。

离线模拟测试不会向 Binance 发起真实订单、划转或外部通知，也不能替代目标账户的权限、资产支持、账户模式及实际网络验证。急跌参数仍需使用实际历史行情回放验证；程序没有模拟保证金账户的全部行为，也不提供策略收益或绝对不超额的保证。

使用方法及 UNKNOWN 恢复命令见 `README.md`。

## 17. 一句话执行摘要

定时监控合约 BNB、USDⓈ-M Futures 的 `maxWithdrawAmount` 与运行态余额，按紧迫度决定补仓 aggressiveness；只有在稳定币划转通过最小粒度硬 veto、比例、绝对可划转下限和预算检查后，才从合约划转到现货并分层买入 BNB，成交后尽快回划合约，同时通过状态机、预算、在途状态与幂等控制防止把大量稳定币持续换成 BNB。
