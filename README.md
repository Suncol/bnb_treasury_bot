# BNB Treasury Bot

维护 USDⓈ-M Futures 的 BNB 库存。策略规则见 [DESIGN.md](DESIGN.md)，默认配置见 [config/strategy.toml](config/strategy.toml)。纯决策与交易所执行分离，所有写操作先落 SQLite 日志，UNKNOWN 不自动重发。

## 安装与检查

需要 Python 3.11+。当前项目使用 `.venv/bin/python`；Binance 接入使用 [官方 binance-connector-python](https://github.com/binance/binance-connector-python) 发布的三个模块化 SDK，固定版本见 `requirements.txt`。

```bash
uv pip install --python .venv/bin/python -r requirements.txt
.venv/bin/python -m pytest -q
.venv/bin/python -m services.cli --help
```

测试使用模拟交易所与官方 SDK 的模拟 HTTP/WebSocket 网络传输，不需要凭据或网络连接。测试环境需要 pytest。

## 运行

通过环境变量配置 `BINANCE_API_KEY` 和 `BINANCE_API_SECRET`，密钥不写进配置文件。现货交易和通用划转权限由目标账户提供，系统时钟须同步。

只读取账户、记录状态并生成计划：

```bash
.venv/bin/python -m services.cli --db var/observe.sqlite3 --once
```

持续观察时去掉 `--once`。新启动或行情断流后先积累完整 15 分钟行情窗口，这期间允许对账和已知订单的保护性管理，但不生成新买单。只观察模式不会提交撤单或其他交易请求。

明确启用实盘执行：

```bash
.venv/bin/python -m services.cli --db var/treasury.sqlite3 --live --mode AUTO
```

随后重启不必再传 `--mode`，保留数据库中的模式与失败暂停状态。`--mode PAUSED` 禁止新增买单和划转；启用 `--live` 时仍处理本策略已知订单的保护性撤单。每个账户只使用一个执行数据库，单机锁不能协调独立数据库或其他机器上的控制器。

`--min-transfer-bnb`、`--min-transfer-usd` 可覆盖内部划转最小量。订单过滤器、费率由官方 SDK 读取，配置的费用预留不能低于实际 Maker/Taker 费率。支持的稳定币还取决于目标 USDⓈ-M Futures 账户是否提供相应资产的 `maxWithdrawAmount`。

## UNKNOWN 与恢复

订单按客户端 ID 或已知订单 ID 对账。通用划转没有交易所客户端幂等参数；未拿到 `tranId` 的超时会持续阻断新增操作，余额增加不能作为成功证明。

若操作员已经核实对应交易所流水，可执行：

```bash
.venv/bin/python -m services.cli --db var/treasury.sqlite3 --status
.venv/bin/python -m services.cli --db var/treasury.sqlite3 \
  --bind-transfer-id LOCAL_CLIENT_ID EXCHANGE_TRANSFER_ID
```

此命令仅核验并绑定本地未决记录，不提交交易，完成后退出。确认调入失败会同时清除补货续作标志；确认成功后仍须等待两侧余额通过对账，才能新增交易。无法证明归属时保留 UNKNOWN；不要通过清空数据库或删除未决记录来重试。

划转日志会保留提交前余额和成交账本游标。升级前应完成旧版本在途划转的对账；缺少上述基线的旧未决记录不会被自动推定到账。

## 告警与数据

默认写日志。设置 `ALERT_WEBHOOK_URL` 后，可投递到 HTTPS 接收端；接收端应支持 `Idempotency-Key` 去重。投递失败保留在 outbox，成功渠道不会因其他渠道失败而重复发送。

SQLite 保存操作日志、成交去重记录、划转历史、保护事件、运行模式、防抖与切片状态。预算采用滚动 24h 的已确认/未决调入流水；所有实际下单只使用已确认的现货可用资金。成交日志使用毛数量限制急跌买入额度，余额使用扣费后的净资产计算库存。
