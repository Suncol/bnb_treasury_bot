# 实现状态与维护索引

更新日期：2026-10-15。

原分步计划用于把纯决策 `CyclePlan` 接入交易所执行链路，该链路现已实现。本文记录原计划与当前代码的对应关系，不再将已完成能力列为待办。策略行为以 [DESIGN.md](DESIGN.md) 为准，安装、运行和恢复命令见 [README.md](README.md)。

## 当前实现

| 原计划阶段 | 当前入口 | 已实现范围 |
|---|---|---|
| Binance 原子客户端 | [sdk_transport.py](services/sdk_transport.py) | 基于官方模块化 SDK 处理请求、错误分类、超时和限流；SDK 版本固定，关闭内部自动重试 |
| Exchange Adapter | [exchange_adapter.py](services/exchange_adapter.py)、[binance_adapter.py](services/binance_adapter.py)、[spot_market.py](services/spot_market.py) | 交易所接口约定、账户及流水映射、订单与划转请求、公开行情与过滤器 |
| Repository 与操作流水 | [repository.py](storage/repository.py)、[codec.py](storage/codec.py) | SQLite 持久化、先记录意图、账户绑定、进程租约、成交去重、预算与审计 |
| 对账恢复 | [reconciliation.py](services/reconciliation.py)、[history.py](services/history.py) | 未决操作恢复、现货及合约结算证据、历史收入与消耗统计、人工证据维护 |
| 执行器 | [executor.py](services/executor.py) | 执行前检查、操作意图与结果记录、错误状态分类、受控撤单恢复 |
| 应用编排 | [cli.py](services/cli.py)、[runner.py](services/runner.py)、[alert_service.py](services/alert_service.py) | CLI、库存与行情周期、模式控制、动作续作、告警 outbox |
| 行情采样与刷新 | [market_stream.py](services/market_stream.py)、[market_depth.py](services/market_depth.py)、[market_data.py](services/market_data.py)、[price_refresh.py](services/price_refresh.py) | 深度快照与事件时间匹配、行情窗口、断流恢复、后台公共价格刷新 |
| Paper 模拟交易 | [paper_exchange.py](services/paper_exchange.py)、[paper_ledger.py](storage/paper_ledger.py) | 独立虚拟账户、报价驱动撮合、费用和划转结算，复用原策略、执行与对账链路 |

旧计划中的 `binance_raw_client.py`、`binance_exchange_adapter.py`、`in_memory_repository.py`、`reconciliation_service.py` 和 `app/main.py` 是早期建议路径，不是缺失实现。当前采用官方 SDK、SQLite 和 `services.cli`，无需按旧文件名另建一套实现。测试使用临时 SQLite 数据库验证真实存储和恢复边界。

## 原计划中已修正的假设

- 通用划转没有客户端幂等参数。没有取得 `tranId` 的 UNKNOWN 划转保留门禁，必须取得可核验流水后恢复，不能用余额或金额猜测归属。
- 成交回执不能直接叠加到账户余额。实盘对账的 `filled_untransferred_bnb` 为零，余额必须覆盖已知成交和划转后，才允许继续新增动作。
- 观察模式不提交交易所写请求。`PAUSED` 禁止新增买单和划转，但启用执行时仍允许必要的保护性撤单；Paper 中存量订单成交、已受理划转结算和费用消耗仍可继续。
- 执行不是固定的单次“划 USD → 买入 → 回划 BNB”列表。Runner 根据最新对账结果重新生成计划，处理撤单、已有 BNB 回划、资金到账和补单续作；操作结果未知或余额尚未结算时保持相应门禁。

后续维护继续遵守 [AGENTS.md](AGENTS.md) 和设计文档：USDⓈ-M Futures 的自动 USD 划转以 `maxWithdrawAmount` 为唯一风险基数，保留 `U_min / maxWithdrawAmount > alpha_warn` 硬 veto，并在新增请求前核对未决操作。

## 测试组织与验证

- [tests/unit/](tests/unit/)：状态机、库存口径、划转风控、订单规划、行情窗口、存储和恢复证据。
- [tests/integration/](tests/integration/)：执行与到账依赖、UNKNOWN 和重启恢复、SDK 模拟传输、行情中断、Paper CLI 与账务。
- [tests/helpers.py](tests/helpers.py)：公共配置、账户、状态、订单视图和引擎输入构造。
- [tests/execution_helpers.py](tests/execution_helpers.py)：共享的 FakeExchange 执行环境、单周期运行和预置订单。
- [tests/recovery_helpers.py](tests/recovery_helpers.py)：共享的恢复测试账户、检查点和划转设置。
- [tests/fake_exchange.py](tests/fake_exchange.py)、[tests/paper_helpers.py](tests/paper_helpers.py)：简化交易所、Paper 虚拟时钟与测试行情。
- [tests/month_metrics.py](tests/month_metrics.py)：两套月度模拟共同使用的库存恢复和补仓轮次统计。

全量离线回归：

```bash
.venv/bin/python -m pytest -q
```

两套 30 天测试分别位于 [test_virtual_month.py](tests/integration/test_virtual_month.py) 和 [test_paper_month.py](tests/integration/test_paper_month.py)，使用不同模拟器验证执行守恒与报价撮合、费用和重启恢复。它们使用虚拟时间与合成行情，不代表实际运行 30 天或历史收益回测。

## 后续工作边界

核心执行链路不再作为待实现任务。后续若安排策略变更或大模块拆分，应另行明确范围，并按 [DESIGN.md 第 16 节](DESIGN.md#16-实现与验证范围) 选择相关回归。

实盘账户权限、实际收费、生产网络延迟、外部进程监督，以及急跌候选参数的历史行情验证仍需按部署环境完成；离线测试不能替代这些验证。详细限制以设计文档和运行说明为准。
