# KOL 活动竞品证据与参与记录交接

状态：本地代码与测试完成，尚未执行生产表结构迁移、部署或真实外联。

## 已实现

- 活动创建支持三种竞品证据模式：发起新分析、引用历史证据、不使用竞品证据。
- 新分析支持 `start → submit → confirm`，失败后可 `retry`；每次写操作校验证据配置版本。
- 只读候选预览与单条回放支持 `campaign_id`；证据失效时关闭竞品加分，但保留基础预览。
- NYXI 证据只在指定的 Dave 活动、指定产品主记录、对象类型为 KOL 时参与排序。
- 活动参与记录按 KOL/媒体人分别锁定完整名单；失败时区分可自动重试和需人工修复。
- 已锁定记录保存最多 10 版排序快照，单条回放优先显示当前名单版本对应的历史快照。
- 表结构脚本默认 dry-run；只新增，不删除、不重命名、不覆盖同名异类型字段。

## 安全边界

- `LAUNCH_EVIDENCE_ENABLED` 默认 `0`。
- `LAUNCH_PARTICIPATION_WRITE_ENABLED` 默认 `0`。
- 本期没有创建 KOL/媒体人任务、邮件草稿、卡片、寄样单或真实邮件的代码路径。
- 未修改 `auto_send`、`reply_monitor`、`followup`、`ship_recon` 或 `sla_check`。
- 本次没有运行 `scripts/apply_launch_evidence_schema.py` 连接生产飞书。

## 本地验证

- 活动专项：`test_launch_*.py` 全部通过。
- 全仓：264 项中 263 项通过；唯一失败为本次未改动的 `test_zeabur_watchdog.ZeaburWatchdogTests.test_run_once_alerts_any_project_service_failed_deployment`。单独重跑仍失败，相关文件相对实施前固定点无 diff。
- `py_compile` 通过。
- `git diff --check` 通过（仅有仓库现有 LF/CRLF 提示）。

## 生产执行闸门

1. G1：先运行 schema dry-run，把新增表/字段和类型差异交 Frankie 确认。
2. G2：获准后写表结构，只开启证据开关；确认 Dave 本次引用的 NYXI 帖子。
3. G3：再单独确认首个影子 KOL 名单，临时开启参与记录写入并回读后关闭。
4. G4：真实开发信、付费、寄样和运营群卡片仍需新的明确授权。

## 回滚

- 代码层：两个功能开关设为 `0`；旧产品级只读预览继续可用。
- 表结构：本期只新增。停止使用新增表和字段即可，不做破坏性删除。
- 参与名单：KOL 与媒体人分别按失败批次恢复；人工阻塞状态禁止自动重试。

## 待填写的生产事实

- 部署 commit / deployment ID：未部署。
- 参与表 ID：未创建。
- 新增字段 ID：未创建。
- Dave 证据配置版本、排序版本：未写生产。
- Dave 首个名单版本：未写生产。
