# 库存观察升级夹具

`legacy_inventory.json` 由提交 `dc8f3f3438d47f0ea75748e799ea8e35ca7faaf6` 的真实 Repository、Executor、Reconciler 和 FakeExchange 生成，含 12 个 SQLite SQL dump 及对应交易所状态。所有数据均为离线合成，无凭证或生产记录。

复现：把该提交 `git archive` 解包到临时目录，以其为 `PYTHONPATH` 使用仓库 `.venv/bin/python` 执行本目录 `generate_legacy_inventory.py`。生成器拒绝已经含新代次字段的模型。将导出的 dump 加载为真实 SQLite 后，再以新版本打开，验证兼容读取和恢复；不得把新模型删字段当作旧库来源。
