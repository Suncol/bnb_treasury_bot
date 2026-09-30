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

这些规则只消费已完成对账的有效库存。账户过期、时间超前、金额非有限/不合法、订单前后视图不一致或成交/划转尚未结算时，先关闭库存决策门禁，冻结业务状态、候选计数、切片状态和 `last_inventory_at`。`last_inventory_attempt_at` 单独记录尝试，失败尝试不算成功的 30 分钟库存周期，也不压缩连续确认间隔。有效行情仍可独立触发急跌保护及已知订单撤单。

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
| `has_pending_orders` / `has_pending_cancels` | 提交、撤单或现货结算尚未完成；已终结但余额未覆盖的成交、未终结 IOC 也在此门禁内 |
| `has_unknown_orders` / `has_unknown_transfers` | 结果未知，禁止新增操作直至对账 |

实盘 `Reconciler` 先查询操作、订单、成交和划转，再读取余额并再次核对活动订单。`filled_untransferred_bnb` 始终置零，不能将成交简单加到余额上。两次订单视图不一致，或订单累计成交量尚未被成交明细覆盖时，禁止新增动作并继续对账。成交按 `(symbol, trade_id)` 去重，保存 BUY/SELL、base/quote 资产、实际 `quoteQty` 和收费资产；矛盾的同 ID 记录报错，不覆盖原账。

持久化的 `spot_checkpoint` 保存最近一次已覆盖的 BNB、quote 总余额及资产事件游标。下一份余额必须同时等于该基线加游标之后的全部已知净变动，才推进基线；买单终态本身不能证明余额已经更新。无论 IOC 全成、GTC 部分成交还是撤单途中成交，只要任一现货资产未覆盖成交，都阻止新买单及划转，重启后继续使用原基线。已确认划转还须通过合约侧核对。缺少实际成本、存在无法解释的差额或 UNKNOWN 请求时，不使用请求时间、重复读数或超时等待代替证据。

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

划转意图不等于新增资产。源账户尚未扣款时只计算现货余额；已扣未到时仅计算确定的 `bnb_in_transit`；到账后只计算合约余额。成交日志用于审计和保护事件的**毛买入额度**，余额用于扣费后的**净库存**，两者不叠加。成交或划转已确认但余额尚未覆盖时，禁止新增订单与划转；不能把暂时少显示的 BNB 当成新缺口。此类未决、过期或不一致快照不写入库存历史，也不推进库存业务状态。

上述 `effective_supply` 是经济总敞口，包含冻结 BNB 和所有买单的潜在成交。可立即回划的量则为 `max(spot_bnb - reserved_spot_bnb - B_spot_reserve, 0)`。当合约紧急、总敞口已覆盖目标但没有可回划 BNB 时，发出 `INVENTORY_UNAVAILABLE`，由操作员协调冻结资产或外部订单；不会忽略这些敞口继续加买，也不能保证任何情况下立即恢复合约下限。

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

`U_budget_24h` 仅限制调入 USD，并不是 24h 买入成交额上限；现货原有资金仍可在其他门禁允许时使用。当前保留完整申请金额先通过比例和绝对下限检查的政策，不自动寻找更小的安全调入量来规避本次 veto。独立买入支出预算或部分资金补货属于另行设计的策略变更。

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

补货缺口也须按数量步长向下归整，不能因缺口小于预算允许数量而生成不符合步长的 IOC。以预算 3000 USD、最终限价 601.71、数量步长 0.01 为例，最多买入 4.98 BNB，订单名义金额为 2996.5158 USD。此检查约束订单名义金额；`fee_reserve_rate` 已在引擎中预留，传给订单规划器的是 `spot_free_usd / (1 + fee_reserve_rate)`。上述 4.98 BNB 示例指不含费用的订单预算。SDK 适配器从账户/交易对 commission 读取 standard、tax、special 三类费用，分别合计 BUY 的 maker + buyer、taker + buyer；不依赖 BNB 折扣降低预留。配置低于完整费率、费用非有限或不支持的负费率时停止新增执行，实际净结算使用成交返回的收费资产及金额。

资金需求规划和最终 IOC/分层订单共用同一过滤器检查：价格上下限及 tick、数量上下限及 step、名义金额上下限、动态百分比区间；引擎再约束 MAX_POSITION 和交易对/交易所剩余订单数。不能合规下单的量不触发资金调入。动态价格及订单容量每轮重读，时效从动态读取开始计，排队候选在提交前重新校验。`urgent_ioc_buffer` 必须在 0–1% 内。

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

`owned_supply` 表示已持有的合约 BNB、现货总 BNB（含冻结量）及不与两者重复的 BNB 在途库存，排除现货保留量和未成交买单。它约束经济敞口；能否立即回划还须另扣冻结量。成交已反映在余额中时不得再叠加 `filled_untransferred_bnb`；划转已从源账户扣除或已到账时也不得重复计算。`external_buy_remaining_qty` 是已知人工或其他策略买单的剩余 BNB，计入总库存敞口，但不由本策略撤销。普通状态的 `active_buy_target` 为当前目标库存（含其他风控下调），紧急状态按第 11.1.4 节取更低目标。

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
3. 相关撤单、订单及划转已经完成对账，不存在结果不明或余额未覆盖的操作，且余额新鲜、视图一致。缓存行情检查可以推进观察计时，但解除保护必须等待一次实际对账。

连续采样端维护窗口版本 `sample_continuity`、`sample_stable_since` 和 `sampled_at`。主循环检查采样证据有效期，并将稳定起点限制在最近触发之后；REST 使控制线程检查间隔超过 3 秒，不再等价于行情断流。真实断流、重复/倒序行情、异常数据或重启会换版本并清空窗口与稳定区间。没有采样端证据的纯决策调用仍使用保守的相邻检查间隔规则。

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

读取失败采用 2–60 秒封顶指数退避加抖动，交易所 `Retry-After` 为更高的最小等待要求。`read_failures` 和 `read_retry_at` 持久化，PAUSED、切换模式和重启均不能跳过。退避期间只检查本地行情、已知风险和告警；429/418 的禁用窗口同样限制撤单请求。正常读取成功后清除读取退避，自动交易暂停仍须显式恢复。客户端主动预留请求容量造成的延后不算一次 API 失败。

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

1. 恢复持久化运行态和请求退避；先消费独立行情线程锁存的风险。保护已触发时，利用日志中的订单 ID 优先撤销不符合价格、额度、笔数及 TTL 的本策略普通买单。
2. 查询未决请求、活动订单、已知订单终态、成交和划转，再读取余额、复核订单视图及结算覆盖。每个 Reconciler 读取边界和 SDK 下一次读取前都检查风险，分页也不能连续跳过保护检查；UNKNOWN 不补发。
3. 若读取过程中执行了撤单，废弃该轮不完整视图和排队候选，重新对账；已经提交的数量和切片截止时间仍有效。此路径不提前消耗库存状态确认次数。
4. 读取最新行情与过滤器，生成计划，持久化可信的库存状态、风险状态及告警；只观察模式结束。
5. 优先执行剩余撤单；门禁允许时依次考虑已有 BNB 回划、获准 USD 调入、实际余额支持的买单、闲置 USD 回扫。
6. 每次写请求先提交唯一意图，只发送一次，然后废弃旧快照并返回对账。没有可执行动作、动作组耗尽或新增操作未决时结束，后续调度继续恢复。

优先撤单不读取无关划转历史或过滤器；尚未绑定 ID 的订单仍须先核验身份，不能猜归属。缺少完整账户时仅按保守的已知敞口筛选，完整对账随后继续收紧库存约束。在途的一次同步 SDK 调用仍无法被中断，因此响应上界受该调用及撤单网络耗时影响；这不是独立执行线程，也不承诺生产环境一秒内撤单完成。`CRASH_GUARD`、`CANCEL_INTENT`、`CANCEL_SENT`、`CANCEL_TERMINAL` 事件可用于测量各段延迟。

主循环告警仅写 outbox 并唤醒独立连接的投递线程，投递不等待账户网络锁。新增操作在实际提交前再次检查账户、行情和动态过滤器时效；BNB 回划的计划不要求完整行情窗口。计划过期则丢弃候选并重新读取，完整读取失败转入仅撤单路径。

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

SQLite 使用 WAL 和 FULL 同步。`operations` 保存唯一客户端 ID、类型/方向、载荷、时间、交易所 ID 及 PENDING/UNKNOWN/CONFIRMED/FAILED 状态。新订单和划转意图同时保存提交前账户、结算基线及运行态；全部意图提交成功后才发网络请求。数据库限制同范围 PENDING/UNKNOWN，执行器另以全局未决及余额差额门禁拦截新操作。即使交易所已经成交但本地结果提交失败，重启也只查询原意图，不补发。

订单 CONFIRMED 仅表示提交接受。活动订单的原量、累计成交、剩余量、归属及撤单状态单独维护；部分成交可以重新令已确认订单 `balance_pending=True`，订单终结并且现货余额覆盖后才退出活动集合。已记录的非 FAILED 撤单（包括确认终态）不重复提交。身份恢复先验证交易对、方向、数量和价格再绑定订单 ID，之后以交易所 ID 查询；慢查询不能覆盖保护路径已绑定的新身份。根据 [Binance 保留优先级改单规则](https://github.com/binance/binance-spot-api-docs/blob/master/faqs/order_amend_keep_priority.md)，已绑定 ID 允许原数量减少，但不得增加或修改价格、方向；未绑定时仍严格匹配原数量。外部订单复用客户端 ID 不构成策略归属证明。

资产事件账本 `wallet_events` 使用唯一来源标识（成交/划转 ID）和单调游标，保存每个资产的净变化。第 6.1 节的一个持久化现货检查点统一覆盖订单和划转，替代逐划转重放本交易对买入的核对方式。历史成交保留 BUY/SELL、实际成本和任意收费资产；同 ID 的缺失成本可由完整回执补齐，冲突证据拒绝导入。合约侧还须反映对应借贷变化，或资产级 `updateTime` 晚于成功确认时间加 1 秒容差，以容纳之后的费用/盈亏。合约 quote 余额只用于结算，划转风险仍以 `maxWithdrawAmount` 为唯一基准。

支持的共享账户活动包括本交易对买卖、`exchange.spot_activity_symbols` 声明的其他现货交易对（最多 16 个）及 SPOT ↔ USDⓈ-M Futures 划转。其他交易对使用 BNB 支付的手续费同样入账。遇到差额时保存期望值、实测值、逐资产差额、缺失成本回执、基线时间和相关操作 ID，发出 `SPOT_SETTLEMENT`；不以多次同余额或等待时间放行。操作员可停止控制器后执行 `--import-spot-trades SYMBOL... --operator ... --reason ...`，从认证 API 补取冻结基线以来的真实成交，再按原结算规则重算；保存操作员、原因、回执引用及前后状态，重复导入不重复入账。该命令不提交交易、不任意修改余额，也不解除身份未知的订单/划转。

充值、提币、理财、兑换、奖励及其他未接入资金来源不属于自动解释范围；差额持续阻断，部署账户必须将这些活动与自动控制协调隔离。恢复须先取得可核验的净资产事件并为该来源实现适配及回归验证，不能用任意差额补丁或直接删除基线恢复运行。本实现不提供强制清除 UNKNOWN、任意余额重置或“确认没发生”按钮。

schema `user_version=1` 在事务内迁移原库，保留成交 rowid、操作身份与历史；活动订单和保护事件由大 KV 集合改为独立表。已证明终结且不含未决余额的旧订单归档，其余保留在活动集合。热路径仅查询活动/未决投影，时间、订单 ID、交易所 ID 及未投递消息有索引；空成交批次不扫描历史，迟到成交仅增量修正其所属的一个事件。缺少可信基线的旧活动意图保持阻断，因此升级前必须在旧版本完成在途操作及存量订单的核对，并核实已结束订单的成交已经反映在 BNB 和 quote 两侧余额中；停止并发资产活动后通过 SQLite backup 保存一致备份。新库或完成升级的空活动集只在有效一致的账户快照上建立初始基线，不能在旧成交仍可能结算时将该快照当作初始化依据。不能复制尚在写入的单个数据库文件而忽略 WAL，也不能删除 journal 解决迁移门禁。

运行模式、续作资格、切片、防抖、保护额度及失败计数持久化。正常响应、自动对账和人工绑定共用结果提交事务；确认资金调入失败会清除续作标志，同一意图的失败只计一次。迟到成交按真实时间归属，重启保留已用额度并清空稳定观察和行情窗口。

数据库绑定认证账户 UID 的哈希、生产环境、USDⓈ-M 类型、交易对、quote 和策略身份，不兼容复用立即停止；配置摘要变化记录事件。所有控制器（含只观察）和维护命令持有同一数据库的进程生命周期租约；`--status` 可并行查看。周期文件锁争用只推迟本轮，不计 API 错误。网络等待不持有 SQLite 事务。仍要求一个账户只使用一个执行库，独立数据库或机器之间没有分布式互斥。

### 14.3 Binance 官方 SDK 接入

依赖使用 [binance/binance-connector-python](https://github.com/binance/binance-connector-python) 当前模块化发行包，版本固定在 `requirements.txt`：`binance-sdk-spot`、`binance-sdk-derivatives-trading-usds-futures`、`binance-sdk-wallet` 及其共享 `binance-common`。签名、HTTP 和 WebSocket 由官方 SDK 负责；适配器不自行实现签名或 HTTP 交易请求。

三个 REST SDK 均设置 `retries=0`，删除订单也不重试。钱款参数以十进制字符串交给 SDK，避免转为 float；测试通过 SDK 的实际序列化路径核对最终请求。超时、5xx、未知错误以及成功响应解析失败都不等价于失败，须先对账。只有明确的拒绝码才标为 FAILED。适配器检查系统时钟与两个服务的时间偏差，超过 1 秒停止；由系统时间同步服务校准，不篡改 SDK 的全局时钟。

适配器保留 SDK 的 Binance 错误码和错误原因。根据 [Binance 官方错误说明](https://github.com/binance/binance-spot-api-docs/blob/master/errors.md)，新订单提交收到明确的 `-2010` 且原因为 `Account has insufficient balance for requested action.` 时记为 FAILED；`LIMIT_MAKER` 的 `Order would immediately match and take.` 拒单同样处理。后续对账不会把明确拒单重新置为 UNKNOWN，后续周期仍需刷新余额、行情并重新规划。该分类只适用于明确的新订单拒绝响应；5xx、`-2010` 的重复客户端 ID、未识别原因及超时继续按 UNKNOWN 处理，不能仅凭相同错误码判定无订单，也不能用查询“订单不存在”解除未知提交的门禁。

划转请求收到明确的非 5xx 拒绝响应 `-3020 EXCEED_MAX_ROLLOUT`（划出金额超过上限）时，根据 [Wallet 错误码说明](https://developers.binance.com/en/docs/products/wallet/error-code#-3020-exceed_max_rollout)记为 FAILED，并计入连续划转失败熔断，不要求绑定不存在的流水号。5xx 携带相同错误码、`-5012` 等待执行及 `-3029` 泛化划转失败仍按 UNKNOWN 处理，不据此重发。

订单使用持久化 `newClientOrderId`；查询没有找到订单时仍保持 UNKNOWN，不自动重新使用该 ID。[官方订单接口](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/trade)允许已成交后的客户端 ID 被再次使用，因此客户端 ID 本身不是无限期幂等保障。

[通用划转接口](https://developers.binance.com/en/docs/catalog/core-trading-wallet/api/rest-api/asset)不支持客户端幂等 ID。本地操作 ID 用于日志，交易所 `tranId` 用于精确核对。未取得 `tranId` 的超时不能仅凭相同金额或余额变化自动归属，持续阻断新增操作。操作员可通过 `--bind-transfer-id CLIENT_ID TRANSFER_ID` 提供明确流水号；工具核验资产、金额、方向和交易所终态，防止同一流水被重复归属，通过统一结果提交路径更新续作与失败状态，记录人工绑定事件后退出；确认成功的划转仍需在后续对账中通过余额门禁。不能核验时继续阻断。

静态过滤器及完整 commission 元数据缓存 300 秒；过滤器合并 `exchangeInfo` 与 `myFilters`。支持所用限价订单的 PRICE_FILTER、LOT_SIZE、MIN_NOTIONAL/NOTIONAL、PERCENT_PRICE/PERCENT_PRICE_BY_SIDE、MAX_NUM_ORDERS、MAX_POSITION、EXCHANGE_MAX_NUM_ORDERS 和 MAX_ASSET；仅略过适用于未使用订单类型的冰山、算法单、列表等规则。未知适用规则直接阻断，不先调入资金再试单。动态百分比优先使用交易所 referencePrice；为空时按实际规则使用最新成交价或匹配 `avgPriceMins` 的新鲜均价，不能用不匹配的 5 分钟均价代替。相关拒单失效过滤器缓存，下轮重新读取规划，未知提交仍不重发。过滤器与费用依据 [官方过滤器说明](https://developers.binance.com/en/docs/products/spot/filters) 和 [commission FAQ](https://developers.binance.com/en/docs/products/spot/faqs/commission_faq)。

保留可获得的端点、HTTP 状态、业务码、Retry-After、权重及脱敏调用栈。429/418 按 [REST 限流规则](https://developers.binance.com/en/docs/products/spot/rest-api) 跨三个 SDK 共享禁用截止时间；无 Retry-After 时分别至少等待 60/120 秒。已观测到服务声明的 REQUEST_WEIGHT/ORDERS 上限及响应计数时，90% 起推迟普通请求，为保护性撤单留余量；服务器禁用窗口不可绕过。SDK 未暴露的响应信息不伪造，其他程序共享 IP 的流量仍可能触发服务端限流。写请求遇限流仍为 UNKNOWN 并持久化等待，提交前本地已知应延后时不创建未发送意图。

内部划转最小量通过 `--min-transfer-bnb/usd` 覆盖，须有限且非负，不把链上提币规则当内部划转规则。历史接口分页读取，数据不完整或分页停滞时停止新增操作。资金只往 SPOT 与 USDⓈ-M Futures 两类账户之间划转。

### 14.4 执行顺序与限制

每次执行前用最新的对账结果和行情生成计划；撤单优先，其次回划可用 BNB、调入 USD、买单、闲置 USD 回扫。每次操作后废弃旧快照，重新对账。三层订单可以保留尚未提交的候选，但每层必须重新通过最新的数量、价格、资金、精度和风控条件；风险发生变化时丢弃旧候选。

每次控制循环最多一笔 USD 调入、一组买单、一笔 BNB 回划和一笔回扫，调入和回扫不能在同一轮反向执行。待到账的调入保存续作标志，普通对账周期也能在确认后继续补货，无须等待下次 30 分钟库存周期。临时门禁阻止买入时，标志与资金保留跨周期、跨重启持续有效，终止条件见第 7.6 节；首单意图与标志消费原子持久化，明确拒单时恢复续作，UNKNOWN 时仅对账。已调入资金的续作和已经开始的分层订单组关闭再次调入权限，价格上涨时按已确认现货余额重新预算可买数量，不能因完整缺口需要更多资金而空等或追加划转。所有 PENDING/UNKNOWN 操作、CONFIRMED 但余额仍待确认的订单/划转及未解释的资产差额使用全局保守门禁；保护性撤单仍可针对已知归属的订单执行，同一未决撤单不会重复提交。

账户 REST 接口没有跨端点原子快照；当前通过订单前后核对、累计成交覆盖、持久化资产检查点、划转合约侧核对、快照过期检查及操作后重新读取降低竞态风险。外部人工交易仍可能在最终检查后改变账户，交易所拒绝或未知结果继续走同一对账流程，不能宣称网络调用与撮合之间是原子的。

### 14.5 调度、配置与投递

`python -m services.cli` 默认只观察；`--live` 才启用订单、撤单及划转。`--mode` 是启动时修改模式的参数，维护和改模式须先停止现有控制器，并非另开一个进程向运行中实例发命令。PAUSED 禁止新增买单/划转，live 下仍执行必要的保护性撤单；不是全部撤单或平仓。正常退出同样不会撤销所有 GTC，停机后原单仍可能成交。恢复 AUTO 不能绕过 UNKNOWN、结算、行情预热或请求退避。

完整对账间隔取 `reconciliation_seconds` 与余额有效期的较小值，库存周期按 `t_check_minutes`；无效尝试单独限频。默认行情有效期 5 秒、账户 10 秒、真实采样断开超过 3 秒重置，完整 15 分钟预热适用于紧急买入。只观察仍更新同一数据库的状态和证据，不能与实盘控制器同时持有租约。

行情线程持续检查连接及最新消息时间，处理 EOF、错误、静默、SDK 定时替换和订阅失败；外层监督异常退出并重建连接。订阅清理和连接关闭分别捕获异常，关闭有超时，真正的 asyncio 取消继续传播，单次清理失败不再杀死恢复循环。断流/重连在同一锁下清空旧快照和窗口，保留未消费风险。`health()` 暴露线程存活、最近消息年龄及清理/连接错误；失活线程的 quote 不可用。

告警按 code、级别和原因变化入 outbox，独立 SQLite 连接每批最多取 100 条，网络投递与短事务确认均不取得账户执行锁。每渠道单独确认；失败 5 秒后重试，关闭时保留未完成记录。Webhook 必须处理稳定 `Idempotency-Key`，以防远端接受后本地确认前崩溃造成重复；未配置 Webhook 时只写日志。

`--status` 离线显示运行态、未决、结算差额、账户绑定及数据库健康。每分钟检查磁盘余量、WAL、outbox 积压和最老未决年龄：少于 256 MiB、WAL 超过 64 MiB、积压至少 1000 条、未决超过 1 小时告警。SQLite 写失败或磁盘 I/O/空间错误直接写 stderr 并停止新增请求，不依赖同一故障库记录错误。已结束历史当前不自动删除；运维使用一致备份、安排 WAL checkpoint，不能清除幂等流水、成交唯一键或未决日志。外部进程监督及目标网络延迟指标仍属于部署责任。

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

结算与身份回归还覆盖：划转任一侧余额延迟及重启、USD 调入与回扫、划转期间成交/手续费、到账后合约资产版本更新、人工绑定 FAILED 及事务回滚、REST 等待期间急跌后反弹、风险消费竞态、同 ID 减量改单、未知订单恢复后撤单改变客户端 ID。

附件审计对应的修复验收：

| 审计项 | 可重复的离线验证 |
|---|---|
| R01 | tick/run_once 下已知 IOC 成交但 BNB/quote 任一或两者余额延迟；多次检查及重启仅提交一次；GTC 部分成交重新关闭门禁；成交后结果落库失败重启不补发 |
| R02 | 过期、未来时间、NaN/Infinity、订单不一致、结算未完冻结状态；过期 URGENT 不诱发随后有效输入的 IOC |
| R03 | 共享账户 quote 支出、其他交易对 BNB 手续费跨重启阻断；认证回执导入后解除，重复导入不重复记账；同交易对 SELL 及 quote 手续费直接覆盖 |
| R04 | 多个慢历史读取前先撤已知高价单；读取中触发在下个边界撤单；慢查询成功/失败都保留保护路径绑定的身份；outbox 不等账户网络锁 |
| R05 | SDK 429/418 的 Retry-After、跨重启及暂停持续故障；写限流保存 UNKNOWN 和冷却；90% 权重留保护请求容量且不生成未发送新意图 |
| R06 | 临时周期锁争用不退出、不算 API 失败；第二控制器被生命周期租约拒绝，状态查看可并行 |
| R07 | 3,000/36,500 条历史操作与 365 个事件下热查询使用索引；空成交不查历史；迁移保留 rowid、未知记录、已结束归档；迟到成交只更新所属事件 |
| R08 | 行情持续每秒采样、主循环每 4 秒检查能恢复；真实断流换版本并重新预热 |
| R09 | 实际固定版本 SDK 的订阅清理/连接关闭异常后能恢复订阅；真实取消继续传播 |
| R10 | 动态价格区间、MAX_ASSET、额外 BUY 费用、未知规则及不匹配均价、过滤器读取耗时；原有资金、数量、价格和最小粒度回归 |

附件原始历史基准（2,000 笔成交、0/20/80 个已结束事件）重新执行，空批次不再重算事件；时间查询的执行计划为 `SEARCH fills USING INDEX fills_time`。具体本机微秒耗时仅用于验证消除历史扫描，不推断生产延迟。

离线模拟测试不会向 Binance 发起真实订单、划转或外部通知，也不能替代目标账户的权限、资产支持、账户模式及实际网络验证。急跌参数仍需使用实际历史行情回放验证；程序没有模拟保证金账户的全部行为，也不提供策略收益或绝对不超额的保证。

使用方法及 UNKNOWN 恢复命令见 `README.md`。

## 17. 一句话执行摘要

定时监控合约 BNB、USDⓈ-M Futures 的 `maxWithdrawAmount` 与运行态余额，按紧迫度决定补仓 aggressiveness；只有在稳定币划转通过最小粒度硬 veto、比例、绝对可划转下限和预算检查后，才从合约划转到现货并分层买入 BNB，成交后尽快回划合约，同时通过状态机、预算、在途状态与幂等控制防止把大量稳定币持续换成 BNB。
