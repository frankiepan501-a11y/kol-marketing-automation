# KOL集中宣发任务日报卡｜生产交接（2026-08-21）

## 结论

- 日报卡已完成开发、测试、部署和Frankie-only真实试发。
- 真实样卡message_id：`om_x100b674afba06cb0c00611d1b8a2689`。
- 运营群定时发送尚未启用，等待Frankie确认真实客户端渲染。

## 本次事故与修复

- 问题：首张样卡被飞书在消息创建前拒绝，没有生成坏消息。
- 根因：payload声明Card JSON 2.0，但正文仍使用旧`note`组件；飞书返回HTTP 400、外层`230099`、卡片内部`200861`。
- 改动：`app/launch_daily_report.py`把说明组件改为`markdown`；只在明确的消息创建前400结构拒绝时把技术回执标为`rejected`。任何5xx、超时或空message_id仍停在`sending`，防止重复发卡。
- 回执修复：只把Dave活动记录`recvsFoRmeGj4Y`中已证实未创建消息的同一回执由`sending`改为`rejected`，非技术业务备注读写前后完全一致。

## 生产证据

- 实现commit：`f2571ee368575896080755967c9b46ca800c617b`
- 兼容修复commit：`92e2e0dde67101ffa10daf7ac9f3319692bdefe4`
- Zeabur deployment：`6a87ea1b29f0931a12bfb25b`，状态`RUNNING`
- 后台样卡任务：`launchreport-12df0a22e5cd`，状态`success`
- 业务写入：0；技术发送回执写入：2；最终回执：`sent`
- 飞书消息回读：`msg_type=interactive`、未删除，回退摘要标题为`KOL集中宣发任务日报 · 2026-08-21`。

## 验证

- 日报/路由聚焦测试：42 passed + 3 subtests。
- 活动相关测试：228 passed + 3 subtests。
- 全仓：494 passed；仅既有`test_zeabur_watchdog`旧日期fixture失败，与日报卡无关。
- 两位独立复审最终均无P0/P1。

## 剩余待办

- P1：Frankie确认真实客户端里的状态色、4条进度条和整体排版后，才启用每天17:15发送到当前KOL运营群。
- P0：按原计划完成食人花当日只读验证；该验证只由Codex回查，不创建飞书提醒卡。
