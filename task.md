# 分步实现任务清单

## 1. 文档目的

本文档用于把“将 `CyclePlan` 接到真实下单/划转链路”的实现过程拆成可逐步交付的任务。

目标：

1. 保持 [DESIGN.md](DESIGN.md) 里的状态机、资金流、风控和幂等约束不变。
2. 保持 `core/` 继续是纯函数决策层。
3. 把所有交易所副作用集中在 `services/`。
4. 把所有待恢复状态、幂等信息和执行审计集中在 `storage/`。
5. 每一阶段都可以单独测试、单独验收。

当前推荐实现顺序：

1. Binance 原子客户端
2. `exchange_adapter` 读侧与写侧映射
3. repository 与 operation journal
4. 对账恢复服务
5. 执行器
6. 应用编排层

---

## 2. 全局实现约束

在所有阶段里，必须始终满足以下约束：

1. 不修改 `core/state_machine.py`、`core/risk_checks.py`、`core/order_planner.py` 的纯函数边界，除非发现明确 bug。
2. 不让 `services/` 直接重算业务状态；状态与动作计划只来自 `build_cycle_plan()`。
3. 不让 `executor` 直接解析 Binance 原始 JSON；解析统一在 adapter 层。
4. `maxWithdrawAmount` 仍是 USD 划转唯一风险基数。
5. `UNKNOWN` / `PENDING` 是一等状态，必须可持久化、可恢复、可对账。
6. 始终保持资金流方向：
  `FUTURES_USD -> SPOT_USD -> SPOT_BNB -> FUTURES_BNB`

建议每一阶段都先写测试，再落实现。

---

## 3. 阶段一：Binance 原子客户端

### 3.1 目标

先实现最底层的 Binance HTTP client，只负责：

1. 请求签名
2. 时间戳与 `recvWindow`
3. 请求发送
4. 错误分类
5. 响应原样返回

这一步不返回领域模型，不关心 `AccountSnapshot`、`CyclePlan`、状态机。

### 3.2 建议文件

- `services/binance_raw_client.py`
- `tests/services/test_binance_raw_client.py`

### 3.3 需要实现的能力

建议至少支持以下方法：

1. `get(path, params, signed=False)`
2. `post(path, params, signed=False)`
3. `delete(path, params, signed=False)`

额外需要：

1. 签名函数
2. query string 规范化
3. 统一异常类型，例如：
  - `TransportError`
  - `RateLimitError`
  - `ExchangeRejectedError`
  - `UnknownExecutionError`

### 3.4 测试

应覆盖：

1. 签名结果是否正确
2. signed 请求是否自动追加 `timestamp`
3. HTTP 4xx / 5xx 是否映射成预期异常
4. 超时、连接失败是否落到 `TransportError`
5. 响应体可被原样返回给上层

### 3.5 验收标准

满足以下条件后可进入下一阶段：

1. 不依赖业务逻辑即可独立通过测试
2. 可以稳定处理 Binance 风格错误响应
3. 上层不需要知道 HTTP 细节

---

## 4. 阶段二：Exchange Adapter 映射层

### 4.1 目标

在原子客户端之上实现 `BinanceExchangeAdapter`，把 Binance 原始数据映射成当前 `core/models.py` 需要的标准对象，并把 `OrderPlan` / `AssetTransferPlan` 转成 Binance 请求。

这一阶段只解决“数据翻译”，不解决状态恢复和执行编排。

### 4.2 建议文件

- `services/exchange_adapter.py`
- `services/binance_exchange_adapter.py`
- `tests/services/test_exchange_adapter_reads.py`
- `tests/services/test_exchange_adapter_writes.py`

### 4.3 读侧任务

实现以下读取接口：

1. `fetch_account_snapshot() -> AccountSnapshot`
2. `fetch_market_snapshot(symbol) -> MarketSnapshot`
3. `fetch_symbol_filters(symbol) -> SymbolFilters`
4. `fetch_open_orders(symbol)`
5. `fetch_recent_fills(symbol, since)`
6. `fetch_recent_transfers(asset, since)`

注意事项：

1. `USDⓈ-M Futures` 的 `maxWithdrawAmount` 必须映射到 `contract_max_withdraw_amount`
2. `availableBalance` 只映射到 `contract_available_balance`
3. 现货可用余额与冻结余额要分清
4. 不要把 Binance 字段名泄漏到 `core/`

### 4.4 写侧任务

实现以下写入接口：

1. `place_order(OrderPlan)`
2. `transfer_asset(AssetTransferPlan)`

要求：

1. `LIMIT_MAKER`、`LIMIT + IOC`、普通 `LIMIT` 都能正确映射
2. `SPOT -> USDⓈ-M Futures` 与 `USDⓈ-M Futures -> SPOT` 的划转方向能正确映射
3. 每次请求都支持传入幂等标识

### 4.5 测试

应覆盖：

1. balance payload 到 `AccountSnapshot` 的映射
2. market payload 到 `MarketSnapshot` 的映射
3. symbol filters 到 `SymbolFilters` 的映射
4. `OrderPlan` 到 Binance 下单参数的映射
5. `AssetTransferPlan` 到 Binance 划转参数的映射

### 4.6 验收标准

1. Adapter 输出的对象可以直接喂给 `build_cycle_plan()`
2. 所有写请求都能从标准对象稳定生成 Binance 参数
3. 读写测试不依赖真实网络

---

## 5. 阶段三：Repository 与 Operation Journal

### 5.1 目标

实现本地持久化接口，支持记录：

1. 状态决策
2. 周期计划
3. 已提交操作
4. `PENDING` / `UNKNOWN` / `CONFIRMED` / `FAILED` 操作状态
5. 24h 预算统计所需的成功划转记录

建议先做内存版，再做磁盘版。

### 5.2 建议文件

- `storage/models.py`
- `storage/repository.py`
- `storage/in_memory_repository.py`
- `storage/budget.py`
- `tests/storage/test_in_memory_repository.py`

### 5.3 必须补充的 repository 接口

当前的 `save_state_decision()` / `save_cycle_plan()` 不够，建议加上：

1. `record_operation(...)`
2. `update_operation_status(...)`
3. `list_pending_operations()`
4. `list_unknown_operations()`
5. `get_budget_used_24h(asset, now)`
6. `save_execution_result(...)`
7. `load_last_state_context()`

### 5.4 操作模型建议

至少记录：

1. `operation_id`
2. `client_id`
3. `operation_type`：`ORDER` / `TRANSFER`
4. `direction`
5. `asset`
6. `symbol`
7. `amount`
8. `status`
9. `exchange_id`
10. `created_at`
11. `last_checked_at`
12. `error_message`

### 5.5 测试

应覆盖：

1. 新操作写入后可以被查询到
2. 状态从 `PENDING -> CONFIRMED/FAILED/UNKNOWN` 可正确更新
3. 24h 预算统计只计算成功的稳定币划转
4. 重启场景下可恢复最近状态上下文

### 5.6 验收标准

1. 上层可以只通过 repository 完成幂等与恢复判断
2. 所有执行相关状态都能落库并查询
3. In-memory 版本足够支撑后续集成测试

---

## 6. 阶段四：对账恢复服务

### 6.1 目标

实现 `reconciliation_service`，负责把 repository 中的 `PENDING/UNKNOWN` 操作与交易所真实状态对齐。

它不负责生成新计划，只负责恢复旧操作状态。

### 6.2 建议文件

- `services/reconciliation_service.py`
- `tests/services/test_reconciliation_service.py`

### 6.3 核心职责

1. 读取 repository 中未完成操作
2. 通过 adapter 查询交易所状态
3. 将操作更新为：
  - `CONFIRMED`
  - `FAILED`
  - `UNKNOWN`（继续保留）
4. 对部分成交订单，产出标准化的成交信息，供上层刷新 `filled_untransferred_bnb`

### 6.4 恢复规则

建议固定规则：

1. 请求超时后，本地状态先记为 `UNKNOWN`
2. 下一个周期开始时，优先执行 reconciliation
3. 若交易所查到成功结果，则转成 `CONFIRMED`
4. 若明确失败，则转成 `FAILED`
5. 若仍查不到，保留 `UNKNOWN` 并更新时间戳

### 6.5 测试

应覆盖：

1. 划转成功但响应丢失
2. 订单成功但 ACK 丢失
3. 订单部分成交
4. 查询仍无结果时继续保持 `UNKNOWN`
5. 存在 `UNKNOWN` 操作时，不允许生成新同语义动作

### 6.6 验收标准

1. `UNKNOWN` 不再依赖人工猜测
2. 系统能在下一个周期恢复绝大部分未完成操作
3. reconciliation 本身是可重复执行的

---

## 7. 阶段五：执行器

### 7.1 目标

实现 executor，把 `CyclePlan` 转成真实交易所动作。

执行器不能重算状态，也不能绕开 `CyclePlan` 自行做业务判断。

### 7.2 建议文件

- `services/executor.py`
- `tests/services/test_executor.py`

### 7.3 固定执行顺序

每个周期建议固定为：

1. 先运行 reconciliation
2. 读取最新 snapshot / pending 状态
3. 调用 `build_cycle_plan()`
4. 先把 `CyclePlan` 落库
5. 按顺序执行：
  - USD 划转
  - 买单
  - BNB 回划
  - 闲置 USD 回扫
6. 逐项更新操作状态

### 7.4 执行约束

1. 同一周期最多执行：
  - 1 次 USD 划转
  - 1 组新增买单
  - 1 次 BNB 回划
  - 1 次闲置 USD 回扫
2. 若 transfer gate 关闭，不得发起期货到现货 USD 划转
3. 若 reconciliation gate 是 `WAIT_RECONCILE`，本轮不得执行新同语义动作
4. 任何提交到交易所的动作都必须先写 operation journal

### 7.5 测试

应覆盖：

1. 执行顺序正确
2. 每类动作调用次数不超过限制
3. `CyclePlan` 中没有的动作不会被擅自执行
4. 调用 adapter 失败时，操作正确落到 `UNKNOWN` 或 `FAILED`
5. 执行后 repository 状态被正确更新

### 7.6 验收标准

1. 给定一个 `CyclePlan`，执行结果可预测
2. executor 不包含策略分支，只包含执行顺序和状态更新
3. 失败可恢复，成功可审计

---

## 8. 阶段六：应用编排层

### 8.1 目标

把前面各模块串成一个可运行的周期任务，包括：

1. 配置加载
2. 依赖注入
3. 周期调度
4. dry-run
5. 日志
6. 告警

### 8.2 建议文件

- `app/runtime.py`
- `app/main.py`
- `tests/integration/test_runtime_cycle.py`

### 8.3 需要实现的能力

1. 从配置文件创建 adapter / repository / reconciliation / executor
2. 支持：
  - `AUTO`
  - `URGENT_ONLY`
  - `PAUSED`
3. 支持 dry-run 模式
4. 每轮输出：
  - snapshot 摘要
  - state decision
  - gates
  - 执行动作
  - 告警

### 8.4 测试

应覆盖：

1. dry-run 模式不触发真实写操作
2. `PAUSED` 模式只运行读取和日志，不执行动作
3. 一个完整周期能正确串起：
  `reconcile -> fetch -> plan -> execute -> persist`

### 8.5 验收标准

1. 能跑一个完整周期
2. 能切换运行模式
3. 能在 fake adapter 上完成端到端测试

---

## 9. 推荐测试策略

按层测试，不要一上来就做全量端到端。

### 9.1 Unit tests

适用于：

1. 原子客户端签名和错误映射
2. adapter 映射
3. repository 状态更新
4. reconciliation 状态分类
5. executor 的调用顺序

### 9.2 Integration tests

适用于：

1. `InMemoryRepository + FakeExchangeAdapter + real ReconciliationService`
2. `build_cycle_plan() + executor`
3. 单轮周期行为

### 9.3 Fake 组件优先

建议优先实现：

1. `FakeExchangeAdapter`
2. `InMemoryRepository`

用它们先把所有流程测试跑通，再接真实 Binance。

---

## 10. 建议的里程碑

### Milestone 1

完成：

1. Binance 原子客户端
2. 读侧 adapter
3. 对应测试

完成标志：

1. 能稳定得到 `AccountSnapshot`、`MarketSnapshot`、`SymbolFilters`

### Milestone 2

完成：

1. InMemory repository
2. operation journal
3. budget 统计
4. 对应测试

完成标志：

1. 可以持久化 `PENDING/UNKNOWN/CONFIRMED/FAILED`

### Milestone 3

完成：

1. reconciliation service
2. 对应测试

完成标志：

1. 能恢复超时或未知状态操作

### Milestone 4

完成：

1. 写侧 adapter
2. executor
3. 对应测试

完成标志：

1. `CyclePlan` 能被安全执行

### Milestone 5

完成：

1. app runtime
2. dry-run
3. 端到端 fake 集成测试

完成标志：

1. 能跑完整周期且不破坏 `DESIGN.md` 的资金流与风控约束

---

## 11. 当前建议的下一步

为了方便测试，下一步优先级建议固定为：

1. 先做 `services/binance_raw_client.py`
2. 再做 `storage/in_memory_repository.py`
3. 然后做 `services/reconciliation_service.py`

原因：

1. 这三步不依赖真实执行链路，也不容易把副作用和业务逻辑绑死。
2. 做完这三步后，`executor` 的测试会容易很多。
3. 如果后面发现 Binance 接口细节变化，也只会影响 client/adapter，不会回溯污染 `core/`。
