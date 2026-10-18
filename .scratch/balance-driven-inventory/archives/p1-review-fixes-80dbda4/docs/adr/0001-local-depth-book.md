---
status: accepted
---

# 用连续本地订单簿提供可验证的盘口报价

现有 partial depth 与 diff depth 的末编号精确相等关联依赖协议未保证的同步关系，持续错位会让有效流无法形成报价。采用官方 REST 深度快照初始化并连续应用 diff，以实际更新后的盘口及其事件时间提供价格和卖一数量；接受初始化、缺口重建与有限缓存的维护成本，保留 5 秒有效期、15 分钟预热及 paper 流动性去重。

不采用仅放宽两个编号的匹配范围或用本地接收时间给旧盘口续期的方案，因为它们不能证明价格、数量与源时间属于同一已更新版本。快照深度有界、已知档位不足或序列缺失时停止发布并重建。

2026-10-15 用户确认。实施与验收见 [P1-06](../tickets/2026-10-15-p1-review-80dbda4.md#p1-06用连续本地订单簿替换双深度流末编号关联)；本轮仅记录决定，尚未替换当前行情实现。[官方协议依据](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#how-to-manage-a-local-order-book-correctly)。
