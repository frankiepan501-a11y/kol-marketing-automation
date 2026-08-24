# KOL Launch 零模型 Dry-run 规格

## 目标

为 `POST /launch/runtime/autonomous-refill` 增加安全的零模型演练模式，验证活动补池在不调用 DeepSeek、不写飞书、不创建草稿、不发送邮件的前提下，仍能输出候选、资料刷新、固定模板和确定性校验计划。

## 已确认范围

- 生产 n8n 工作流 `uvBfJBtGH93FPa6w` 在开发和验证期间保持停用。
- 本阶段只交付 `dry_run=true`；不得开放零模型真实写入。
- 达人画像使用现有确定性分类规则，不调用 `classify_v2` 的 DeepSeek adapter。
- 邮件使用活动固定模板，不调用 `enrich.gen_draft` 或 `reviewer`。
- 关键词使用固定/结构化词库；耗尽时返回缺口，不调用 DeepSeek 生成。
- 返回结果必须显式包含 `model_calls=0`、`writes=0`、`drafts_created=0`、`emails_sent=0`。

## 测试 seam

1. `relabel.run_profile_records(..., dry_run=True, classification_mode="deterministic")`
   - 输出画像计划；DeepSeek adapter 未被调用；飞书未写入。
2. `launch_runtime.preview_zero_model_refill(...)`
   - 输出库存、候选、资料刷新和固定邮件预览；所有外部副作用计数为 0。
3. `POST /launch/runtime/autonomous-refill`
   - 默认 `dry_run=true`、`ai_mode=zero_model`；真实模式必须继续经过明确确认闸。
4. `keyword_supply.ensure_campaign_supply(..., allow_ai=False)`
   - 固定词耗尽后报告缺口，不调用 DeepSeek。

## 暂不包含

- 不启用真实零模型补池。
- 不创建/修改飞书活动、参与记录、草稿或爬虫任务。
- 不发送测试或真实邮件、卡片。
- 不重新激活 n8n 定时器。

## 实现结果（2026-08-24）

- `autonomous-refill` 默认改为 `dry_run=true + ai_mode=zero_model`。
- 达人画像预演走确定性分类；固定关键词耗尽时只报告缺口。
- 邮件只在内存生成固定模板预览，并执行占位符、内部 SKU、链接和虚构内容检查。
- 零模型真实写入继续关闭；旧 DeepSeek 真补池需要写入开关和精确确认词双重放行。

## 验证记录

- KOL/关键词/运行时/并行证据续跑相关单元测试：104 个通过。
- 路由和安全闸测试：46 个通过。
- 项目全量测试：607 个中 606 个通过；唯一失败为既有 `test_zeabur_watchdog`，与本改动文件无交集。
- Python 编译检查与 `git diff --check` 通过。
- n8n 工作流 `uvBfJBtGH93FPa6w` 回读 `active=false`。
- 审查后加固：dry-run 异常不发飞书告警；暂停/结束/达标活动不输出补池建议；失败预演标记为 `degraded`。
