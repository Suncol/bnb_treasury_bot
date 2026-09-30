# BNB Treasury Bot

维护 USDⓈ-M Futures 的 BNB 库存。策略规则见 [DESIGN.md](DESIGN.md)，默认配置见 [config/strategy.toml](config/strategy.toml)。纯决策与交易所执行分离，写请求先落 SQLite 意图；UNKNOWN 不自动重发，已知成交也必须等余额覆盖后才能继续。

## 安装与检查

需要 Python 3.11+。项目使用 `.venv/bin/python`；Binance 接入采用[官方模块化 SDK](https://github.com/binance/binance-connector-python)，固定版本见 `requirements.txt`。

```bash
uv pip install --python .venv/bin/python -r requirements.txt
.venv/bin/python -m pytest -q
.venv/bin/python -m services.cli --help
```

测试通过模拟交易所和官方 SDK 的模拟 HTTP/WebSocket 传输运行，不需要凭据，不发真实订单、划转或通知。测试环境需要 pytest。

## 运行与暂停

通过 `BINANCE_API_KEY`、`BINANCE_API_SECRET` 提供凭据，密钥不写入配置。目标账户需要现货读取/交易及通用划转权限，系统时钟须同步。

只读取账户、保存状态并生成计划：

```bash
.venv/bin/python -m services.cli --db var/treasury.sqlite3 --once
```

去掉 `--once` 持续观察；观察模式不提交撤单。确认目标账户和权限后启用实盘：

```bash
.venv/bin/python -m services.cli --db var/treasury.sqlite3 --live --mode AUTO
```

一个账户只使用一个执行数据库。数据库绑定认证账户 UID 的哈希、环境、策略及交易对；更换身份不能复用旧库。所有控制器（包括观察模式）和维护命令持有生命周期租约，第二个写进程直接拒绝；不同数据库或机器无法用此锁互相协调。`--status` 可以与运行中的控制器并行使用。

`--mode` 是启动参数，修改模式、绑定流水或导入证据前应先停止已有控制器。后续正常重启不传 `--mode`，保留暂停状态。`--mode PAUSED` 禁止新增买单和划转；启用 `--live` 时仍执行必要的保护性撤单。PAUSED 和退出都不会保证撤销全部 GTC，停机后存量单仍可能成交；本程序没有全部撤单后停机命令。

冷启动或行情断流后重新积累完整 15 分钟窗口，这期间包括 URGENT 在内的新买单都被禁止，已有可确认 BNB 的回划和订单保护使用各自门禁。失败读取采用持久化退避；429/418 尊重 Retry-After，模式重置及重启不能跳过等待。

`--min-transfer-bnb/usd` 覆盖内部划转最小量，须有限且非负。配置费用预留必须覆盖完整 BUY commission，IOC 缓冲不得超过 1%。资产支持还取决于目标合约账户是否为该稳定币提供资产级 `maxWithdrawAmount`；`availableBalance` 不能替代它，最小划转粒度硬 veto 不因 URGENT 放松。

## UNKNOWN 与余额结算恢复

```bash
.venv/bin/python -m services.cli --db var/treasury.sqlite3 --status
```

状态包含未决请求、期望/实际余额差额、相关操作、账户绑定、数据库/WAL 大小、剩余磁盘、告警积压和最老未决时间。

订单用客户端 ID 或已经核验的交易所 ID 查询。通用划转没有客户端幂等参数；未取得 tranId 的超时始终阻断，余额增加、一次查询不到或等得足够久都不是归属/失败证明。取得明确交易所流水后，停止控制器并执行：

```bash
.venv/bin/python -m services.cli --db var/treasury.sqlite3 \
  --bind-transfer-id LOCAL_CLIENT_ID EXCHANGE_TRANSFER_ID
```

命令只核验未决划转的资产、金额、方向、终态及唯一归属，保存结果后退出，不提交交易。CONFIRMED 后还须等待现货和合约余额完成核对。没有可核验流水的 UNKNOWN 需继续保留并升级人工核查；不能删除日志、清空数据库或盲重发。

现货结算以一个持久化余额基线加去重资产事件核对 BNB 和 quote 两种资产，涵盖买入、卖出、真实 quoteQty、手续费和现货/合约划转。即使订单已全成或撤销，任何一侧余额滞后都不能制造新补货缺口。

共享账户的其他交易对须在 `exchange.spot_activity_symbols` 声明，例如 `["ETHUSDT", "ETHBTC"]`；在其他交易对支付 BNB 手续费也需要读取该交易对。若此前漏配，状态会保留差额。停止控制器后可以从认证接口补取缺失成交：

```bash
.venv/bin/python -m services.cli --db var/treasury.sqlite3 \
  --import-spot-trades ETHUSDT ETHBTC \
  --operator operator-name --reason '核对共享账户成交及 BNB 手续费'
```

工具保存操作员、原因、真实回执引用和基线前后状态，重新对账后输出剩余差额；重复导入不会重复入账。不接受任意金额覆盖，不解除 UNKNOWN 身份，不提交交易。修正配置后再启动控制器。

充值、提币、理财、兑换、奖励及其他未接入的资产事件仍会安全阻断；自动运行的账户须协调隔离这些活动。遇到这些差额，需要取得完整来源证据并实现经过测试的适配后恢复；不能用上述成交命令伪造现金流或直接重置余额。

## 升级与数据维护

升级前在旧版本完成在途请求和存量订单对账，核实已成交订单的 BNB 与 quote 余额均已反映到账，并停止控制器及其他并发资产活动；使用 SQLite backup 保存一致备份。新版本在事务内迁移至 schema 1，保留原成交 rowid、操作身份及历史，已结束订单归档。缺少可信提交前基线的旧活动意图保持阻断，不自动推定结算。不要仅复制正在写入的主数据库文件而漏掉 WAL。

活动/未决操作、订单、成交时间及 outbox 使用索引，空成交批次不扫描历史保护事件。历史流水不自动删除；安排备份与 WAL checkpoint，保留幂等身份和未决证据。数据库写失败会写 stderr 并停止新增执行，部署应配外部进程监督。

## 告警与策略边界

默认日志告警；设置 `ALERT_WEBHOOK_URL` 后投递至 HTTPS 接收端，接收端须支持 `Idempotency-Key`。outbox 使用独立数据库连接，慢账户读取不阻塞投递；失败记录及逐渠道送达状态保留。磁盘不足、WAL 增长、消息积压和长时间未决均有告警。

冻结 BNB 和外部买单仍计入经济总敞口，但不能当作立即可回划库存。合约紧急且库存受限时发出 `INVENTORY_UNAVAILABLE`，需协调冻结/外部订单，不忽略它们额外加买。

24h 预算限制合约向现货的稳定币调入，并不限制全部买入支出；现货原有资金仍可在其他门禁允许时使用。完整资金申请若违反比例或绝对下限，保持 veto，不自动改成更小申请。成交毛数量限制保护事件买入额度，实际净余额决定库存。

离线回归不能替代目标账户的权限、收费、资产支持、共享限流和生产网络验证。保护性撤单会在历史读取前及读取之间优先执行，但正在进行的一次同步 SDK 请求仍可能延迟响应；实际延迟可通过保护与撤单事件时间测量。
