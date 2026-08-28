# Progress

- 2026-08-28：确认现状根因入口在 `app/launch_daily_report.py`：无法唯一匹配的 live reply 只进入 `malformed_reply` 数据异常并从统计排除，没有运营归属动作。
- 2026-08-28：既有测试 `test_live_reply_with_unknown_mid_is_excluded_and_reported` 固化了旧行为，将先改为红测试再修。
- 2026-08-28：确认卡片必须复用聪哥分身3号 / n8n event-hub；真实业务卡前必须 Frankie-only 自测。
- 2026-08-28：新增回复归属模块、5 个草稿持久字段、扫描端点和幂等回调；归属确认后日报重新计入既有回复队列。
- 2026-08-28：飞书 Event Hub `YjTXaoWAcy89xZpT` 仅新增 `launch_reply_attribution_confirm` 精确分支，节点数 76→78，回读 `active=true`。
- 2026-08-28：专项回归 43 项通过；仓库全量回归 823 项通过，唯一失败为既有固定日期 Zeabur 看门狗用例，与本次改动无关。
- 2026-08-28：本地只读扫描因本机未加载生产 KOL 服务环境而未执行；不使用历史数据猜测，改为部署后调用生产 dry-run 端点盘点。
- 2026-08-28：生产定向扫描连续 500 的根因已定位：`_load_source()` 返回只读诊断字段 `load_warnings`，但 `scan_and_send()` 用 `**source` 把它传进不接收该参数的业务收集器；读取完成后触发 `TypeError`。提交 `2d262ba` 改为白名单显式传参，并补回归测试。
- 2026-08-28：专项测试 8/8；全量 865 通过、26 subtests 通过，仅 1 项既有固定日期 Zeabur 看门狗用例失败。
- 2026-08-28：Zeabur `kol-automation` 已运行提交 `2d262ba`，`/health` HTTP 200。
- 2026-08-28：生产 dry-run 只命中 1 条食人花未归属回复：`recvtk1FNL7zcN`（Just the Gems / YM24食人花-二代），唯一候选活动 `launch-20260915-powkong-piranha-v2`；3 条已删除旧原草稿仅记警告，不阻塞。
- 2026-08-28：Frankie-only 真实样卡已发送，message_id=`om_x100b662d972b70a4c4257eb092f24c1`；草稿回读为 `活动归属状态=待运营确认`，原回复草稿仍为 `待审`。等待 Frankie 在卡片选择活动，以验证 event-hub 回调、归属写回与原卡 PATCH。
- 2026-08-28：核对原回复审核卡并未缺失：2026-08-25 已发 3 个个人卡及 1 个群卡，草稿 `卡片发送状态=已发送`；归属卡是补齐活动归属，不重复生成邮件或重复派回复卡。
