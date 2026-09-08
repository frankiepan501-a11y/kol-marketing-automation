# 进度记录

## 2026-09-08

- 已读取飞书 App 中央台账：业务域为外贸 B2B，发送身份保持 `ACTIVE` 外贸助手 App。
- 已读取飞书卡片与通知规则；用户本次明确要求优先于默认群通知规则。
- 已定位生产接口 `/b2b-linkedin-pool-summary/run`、n8n `WYIlpphakiiVFzrE` 和发送实现 `app/b2b_linkedin_daily_card.py::run_pool_summary`。
- 已确认当前实现默认发送到 `B2B_GROUP_CHAT_ID`，需要移除该默认群路径。
- 仓库存在与本任务无关的用户改动；本次将使用独立计划目录并只提交目标文件。
- 历史项目资料确认：吴晓丹需要查看 B2B 全局跟进情况；该汇总日报应私发吴晓丹，三位业务员的每日开发卡仍按各自负责人私发。
- 决定复用现有 `B2B_LINKEDIN_OWNER_NOTIFY_JSON` 的外贸助手同命名空间映射，不新增或硬编码跨 App `open_id`。
- 负责人映射缺失或仍指向已知 B2B 群时必须安全失败，禁止回退群聊。
- 已新增定向测试 5 项并完成实现：负责人私聊、缺映射安全失败、任意 `chat_id` 拒绝、通配映射拒绝、Frankie-only 私聊均通过。
- 实现保留 `_notify_target()` 对每日个人开发卡的原有群兜底，只对汇总日报启用严格私聊目标，避免扩大影响。
- B2B 回归 38 项全部通过；`py_compile` 与目标差异检查通过。
- 全量测试 220 项：219 通过、1 项既有 `test_zeabur_watchdog.py::test_run_once_alerts_any_project_service_failed_deployment` 失败；外贸项目历史交接已记录同一既有失败，与本次改动无关。
- 双轴代码复核发现并修复：汇总日报不再接受任何 `chat_id`，且只认负责人的精确映射，不使用 `*` 通配；接口说明同步改为负责人私聊。
- 复审确认原问题全部关闭，无规范或需求阻塞项。
- 最新远端 `origin/master` 的部署候选提交已在独立 worktree 准备并通过 39 项 B2B 回归；尚未 push，等待生产写入确认。
- Zeabur 只读状态 helper 因本机 token 失效无法读取部署列表；公开 `/health` 当前返回 `status=ok`。该问题不影响代码准备，但 push 后需通过公开健康接口和版本/部署证据确认上线。
