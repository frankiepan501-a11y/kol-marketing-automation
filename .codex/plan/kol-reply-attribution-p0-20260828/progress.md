# Progress

- 2026-08-28：确认现状根因入口在 `app/launch_daily_report.py`：无法唯一匹配的 live reply 只进入 `malformed_reply` 数据异常并从统计排除，没有运营归属动作。
- 2026-08-28：既有测试 `test_live_reply_with_unknown_mid_is_excluded_and_reported` 固化了旧行为，将先改为红测试再修。
- 2026-08-28：确认卡片必须复用聪哥分身3号 / n8n event-hub；真实业务卡前必须 Frankie-only 自测。
- 2026-08-28：新增回复归属模块、5 个草稿持久字段、扫描端点和幂等回调；归属确认后日报重新计入既有回复队列。
- 2026-08-28：飞书 Event Hub `YjTXaoWAcy89xZpT` 仅新增 `launch_reply_attribution_confirm` 精确分支，节点数 76→78，回读 `active=true`。
- 2026-08-28：专项回归 43 项通过；仓库全量回归 823 项通过，唯一失败为既有固定日期 Zeabur 看门狗用例，与本次改动无关。
- 2026-08-28：本地只读扫描因本机未加载生产 KOL 服务环境而未执行；不使用历史数据猜测，改为部署后调用生产 dry-run 端点盘点。
