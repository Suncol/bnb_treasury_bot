# 六票实现审查修复（2026-10-16，Asia/Taipei）

基准：`codex/p1-implementation` 的 `b2f3674`。用户已授权修复三项发现，先处理 P1，逐项增加复现回归并同步设计，最终运行完整测试。

| 修复 | 对应票据 | 状态 | 提交 |
|---|---|---|---|
| R1：跨交易对 orderId 碰撞不能绕过接受边界 | 03、04 | done | `a9f550d` |
| R2：退休订单迟到成交补齐后按原身份恢复 | 03、04、05、06 | done | `e4009dd` |
| R3：资金不足结束旧续作，后续库存周期重新评估 | 04、06 | done | `26e56e9` |
| R4：实际挂单价格仍能买入时保留缩量续作 | 04 | done | `0020559` |
| R5：旧网格订单不遮蔽可负担的未占低价层 | 04 | done | `07b98c1` |

用户对 R3 明确变更业务决定：本机调资已确认结算、相关操作无未决责任、其他交易条件满足时，当前可用资金不足最小合规买单即可结束旧续作。退出只记录当前资金不足，不推断资金去向；接受新 baseline 本身不触发追加调资。只有下一有效库存周期结合冷却、累计预算及全部划转风控重新评估。此决定取代原先依赖外部支出/冻结证据的退出条件，原六票草稿作为历史保留。

R1：先确认同 ID 的复现失败、不同 ID 对照通过；修复后库存接受与旧库恢复测试 44 项通过。交易对、方向及本机归属必须同时匹配，跨重启仍阻断旧外部回执覆盖当前扣款。DESIGN 已同步。

R2：先确认进程内及重启后的复现均失败；补充原 ID 终态重查后，复现、旧库、未知操作与执行边界共 144 项通过（24.77s）。累计量修正与双边余额完整后可关闭窗口，原意图及写请求数不变；DESIGN 已同步。

R3：先确认有/无外部成交回执两种原始复现均失败。共用真实买单规划器判断当前资金不足：非资金门禁通过、无资金约束可形成合规买单、实际资金却不能形成最小合规买单时，才允许已确认结算的调资续作退出。退出事务核验原调资身份和全部未决操作，原子保存 `FUNDING_INSUFFICIENT_FUNDS` 观察及下一有效库存周期屏障。删除仅为外部支出/冻结归因服务的空闲查询和诊断；严格窗口证据仍保留。DESIGN、README、GLOSSARY 与 ADR 0005 同步。

新增回归共 12 项参数化用例，覆盖 R1/R2 的进程内及重启场景，R3 的基线接受、有/无来源回执、禁止无关历史读取、重启/普通 tick/预算自然释放，以及下一库存周期的冷却、累计预算、最小单位 veto、比例和绝对下限。已有事务回滚、UNKNOWN、迟到收入及执行边界回归保留；旧的外部归因断言按用户的新业务规则调整。

最终完整回归：**1,069 passed in 365.61s (0:06:05)**；测试运行期间未修改被测代码或测试，结果对应上述三笔提交的最终内容。`git diff --check` 通过。没有连接实盘或发送真实订单/划转。

在 `codex/p1-implementation` 工作树执行：

```bash
PYTHONDONTWRITEBYTECODE=1 /home/suncong/bnb_treasury_bot/.venv/bin/python -m pytest -q -p no:cacheprovider
```

完整输出：`/tmp/bnb-review-fixes-full-regression.txt`。六张正式票据全部为 `done`，本文件为三项审查修复的汇总证据。

复审后的 R4（2026-10-16）：前述六票完成及全量结果为 R1–R3 时点记录。复审确认 5.99 USDT 足以买入当前层位的 0.01 BNB，但按未折价参考价折算导致空计划，进而误退旧续作。按用户授权完成修复并提交 `0020559`，04 再次完成，六票当前全部为 `done`。

R4 定向验证：5.99 USDT 续作回归先在旧实现失败（没有买单）；修复后与分层规划相关共 56 项通过（7.85s）。新增 25 项用例包括 5.99 USDT 的正常/响应丢失后重启、三层实际含费成本的等值及上下边界、仅覆盖名义成本时的退出、WATCH 最低层边界、最小量/名义金额向步长取整、价格上下限、已占层位和缺口限制。正常分层为空时才尝试实际可挂层位的最小候选，旧续作使用原执行器提交；DESIGN、README、GLOSSARY 与 ADR 0005 已同步。

R4 最终完整回归：**1,094 passed in 356.64s (0:05:56)**。在目标工作树使用上文同一完整测试命令，运行期间没有修改被测代码或测试；结果对应提交 `0020559`。`git diff --check` 通过，没有连接实盘或发送真实订单/划转。完整输出：`/tmp/bnb-affordability-full-regression.txt`。

R5 完成（2026-10-16，`07b98c1`）：基于 `0020559` 的二次复审，已有有效旧网格买单 `0.01 BNB @ 598` 时，正常分层按剩余两个槽位截取前两层，导致最小回退遗漏尚未占用的 590.90。可用资金 5.92 USDT 实际足以支付该层最小订单及费用预留（5.914909 USDT），却错误结束旧续作。按用户授权，正常分层仍使用原槽位截取与权重分配；单笔最小回退在有槽位时按原序检查全部未占目标价，最多返回一笔。

R5 新增 23 项回归：旧网格一个/两个/三个占位、WATCH/ACCUMULATE、正常分层原数量分配、已占最低价、tick 合并后满槽、缺口/最大数量/价格/名义金额限制，以及真实调资结算后 5.92 USDT、最低层含费临界值等于/略高/略低和仅够名义成本。端到端用例提供开放 GTC 严格窗口内的真实支出回执，覆盖提交前重启、订单响应丢失后恢复同一身份、旧单不撤销、不重复下单或调资，以及预算与退出屏障持久化。

新增用例在修复前 **8 failed, 53 passed（10.54s）**，失败均为可负担的后续空层被遗漏；修复后订单规划、层位规划与资金续作定向测试 **79 passed（11.46s）**。DESIGN、README、GLOSSARY 和 ADR 0005 已同步；`git diff --check` 通过。日志：`/tmp/bnb-offgrid-red.txt`、`/tmp/bnb-offgrid-focused.txt`。

R5 最终完整回归：**1,117 passed in 358.74s (0:05:58)**。在目标工作树执行上文同一完整测试命令，测试期间未修改被测代码或测试；结束后核验差异与启动时完全一致，结果对应提交 `07b98c1`。04 票及 R5 均为 `done`，六票全部完成。完整输出：`/tmp/bnb-offgrid-full-regression.txt`。未连接实盘或发送真实订单/划转。

## 本地 main 合并验收（2026-10-16）

按用户授权，main 从 `80dbda4` 快进合并到 `codex/p1-implementation` 的 `07b98c19fb7d4b1f72475b4b8f2bb09b81198c9e`，没有冲突。六票及 R1–R5 均随分支历史纳入本地 main。

在 `/home/suncong/bnb_treasury_bot` 的 main 工作区执行 `PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider`，结果 **1,121 passed in 333.56s (0:05:33)**。其中 `tests/` 为 1,117 项，本地已有 `var/churn-diagnosis/test_churn_probe.py` 另有 4 项，全部通过；没有修改该诊断文件。完整日志：[validation/main-07b98c1-pytest.txt](./validation/main-07b98c1-pytest.txt)。

本次结果独立于合并前的分支回归。测试期间未修改被测代码或测试；结束后核验 main 提交未变、与实现分支文件树相同、工作区和索引无已跟踪差异。六张正式票据均已追加合并验收并保持 `done`，本地票据继续保存在 `.scratch/balance-driven-inventory`；该合并验收时尚未推送，后续记录如下。

## 推送及本地清理（2026-10-16）

按用户后续授权，已将 main 推送到 `https://github.com/Suncol/bnb_treasury_bot.git` 的 `refs/heads/main`，结果为 `80dbda4..07b98c1 main -> main`。推送后和清理前直接执行 `git ls-remote --exit-code origin refs/heads/main`，均确认远端为 `07b98c19fb7d4b1f72475b4b8f2bb09b81198c9e`，与本地 main 一致。

清理前已验证实现分支是 main 的祖先，目标 worktree 没有未提交、未跟踪或忽略文件。随后用 `git worktree remove` 删除 `/home/suncong/.codex/worktrees/p1-implementation/bnb_treasury_bot`，并用 `git branch -d` 删除本地 `codex/p1-implementation`；均成功，无强制删除。main、本地六票及其草稿和验证日志完整保留；首次清理时未处理 `codex/p1-review-fixes`，其后续核验与清理见下方记录。

六张票状态保持 `done` 并追加推送/清理记录。代码及 main 提交未变化，沿用合并后 **1,121 passed in 333.56s** 的验收结果，本次不重复运行测试。

## 追加清理 p1-review-fixes（2026-10-16）

按用户后续授权核查 `codex/p1-review-fixes`：HEAD 为 `80dbda4`，没有 main 以外的提交，祖先检查通过；没有暂存或已跟踪文件差异，也没有忽略文件。

发现 5 个未跟踪文档，分别为 `GLOSSARY.md`、ADR 0001–0003 和原 P1 修复票据。它们全部逐字节匹配已合入 main 的 `b8b6b4ebdd0520cc1f583d9830e8c479ef2f21a0` 中同路径的 Git blob，确认没有独有未提交内容。先将全部文件原样归档并复核，再移除旧副本；[归档清单](./archives/p1-review-fixes-80dbda4/manifest.json)保存文件路径、SHA-256、大小及匹配提交。

worktree 清空未跟踪副本后状态干净，`git worktree remove /home/suncong/.codex/worktrees/p1-review-fixes/bnb_treasury_bot` 与 `git branch -d codex/p1-review-fixes` 均成功，未使用强制删除。main 仍为 `07b98c1`，当前仅登记 main 工作区。本地 tickets、历史草稿、上述归档及验收日志保留；六票维持 `done` 并同步追加清理结果。生产代码未变，不重复运行测试。

## 清理剩余 p1 分支（2026-10-16）

按用户授权逐一核对剩余 `codex/p1-*` 本地分支。以下 8 个分支的 tip 均通过 `git merge-base --is-ancestor <tip> main`，`git rev-list --count main..<branch>` 均为 0；全部提交已保留在 main 历史中，没有需要保留的未合并分支。清理前 `git worktree list --porcelain` 仅登记 main，8 个分支均无关联 worktree；main 的工作区和暂存区没有已跟踪改动，未跟踪的本地 tickets 原样保留。

| 本地分支 | 清理前 tip | 未合并提交数 | 关联 worktree | 结果 |
|---|---|---|---|---|
| `codex/p1-01-unsent` | `723e0e2` | 0 | 无 | 已删除 |
| `codex/p1-02-rejections` | `2c47af1` | 0 | 无 | 已删除 |
| `codex/p1-03-continuation` | `fefd89a` | 0 | 无 | 已删除 |
| `codex/p1-04-observation` | `b540e83` | 0 | 无 | 已删除 |
| `codex/p1-05-income-audit` | `d9500d8` | 0 | 无 | 已删除 |
| `codex/p1-06-depth-book` | `30de3ff` | 0 | 无 | 已删除 |
| `codex/p1-07-preflight` | `33dfedc` | 0 | 无 | 已删除 |
| `codex/p1-final-review-fixes` | `06a23a4` | 0 | 无 | 已删除 |

使用普通 `git branch -d -- <上述分支>` 删除，8 项均成功，未使用强制删除。本次没有 worktree 或工作文件需要删除。清理后本地分支及登记的 worktree 均仅剩 main；main 与本地 origin/main 引用仍为 `07b98c19fb7d4b1f72475b4b8f2bb09b81198c9e`，文件树保持 `151fe1e729cc3fb450e8e9530592dcfd3d2d4d55`。除这 8 个本地分支外，其余 Git 引用和 stash 列表均与清理前一致。

六张正式票据仍为 `done`，逐票追加本次清理记录；原有验收和两轮清理记录保留。六份草稿、旧 worktree 的五份文档归档及清单、完整验收日志均核对 SHA-256 与清理前一致。逐项完整提交与核验信息见 [清理核验清单](./validation/p1-local-branches-cleanup-2026-10-16.json)。本次仅清理本地分支并更新本地记录，生产代码及测试未变，沿用合并后 **1,121 passed in 333.56s** 的验收结果，未重新运行测试。
