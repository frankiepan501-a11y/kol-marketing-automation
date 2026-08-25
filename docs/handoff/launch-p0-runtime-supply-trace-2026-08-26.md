# KOL 集中宣发 P0 修复交接（2026-08-26）

## 结论

本轮修复三个会让活动“看似在跑、实际停供”的问题：后台任务状态被长结果截断、食人花确定性词永久耗尽、站外发现结果无法精确归回活动任务。

## 修复内容

1. `app/launch_runtime.py`
   - 活动备注只保存紧凑业务摘要，去掉大诊断数组。
   - 当前任务行超过字段上限时降级为最小摘要，绝不再从左侧截断任务标记。
   - `/launch/runtime/jobs/latest` 因此可跨重启稳定回读任务状态。
2. `app/keyword_supply.py`
   - Dave 与食人花共用确定性词选择器：同词×国家×语言七天内不重复，超过七天按来源健康轮转复用。
   - 食人花七层词不再依赖 DeepSeek 才能继续；AI 只补充新增组合。
   - 竞品层仍只在活动明确选择并且证据就绪时启用，不把 NYXI 设为默认来源。
   - 硬性质量筛选保持原样，`quality_filters_lowered=false`。
3. 站外来源回填
   - 云端只认活动ID、任务record_id、词源、发现词四段精确标记。
   - 本地 daemon 负责给新建和已存在 KOL 写标记；旧数据不按关键词猜归属。

## 验证

- 目标回归：`tests/test_launch_runtime.py`、`tests/test_keyword_supply.py`、`tests/test_launch_routes.py` 共 148 项 + 9 个子测试通过。
- 云端完整回归：792 项 + 23 个子测试通过；另有 1 个本次未改动的 Zeabur 看门狗测试使用 2026-07-07 固定时间，已超出其 24 小时窗口，属于既有时间敏感测试。
- 本地 daemon 完整回归：21 项通过。

## 发布与自然验收

1. 等 Dave、食人花后台 job 与本地 crawler 均无运行中任务。
2. 先更新部署终端 daemon，确认版本正确、只有一个 daemon 进程、日志无 Traceback。
3. 再把云端分支快进到最新 `origin/master` 后推送；Zeabur 只部署一次并验证 `/health`。
4. 不手动触发，等待 Dave 整点、食人花每小时 05/20/35/50 分自然轮次。
5. 验收：latest job 可回读；食人花在 AI 额度不足时仍创建确定性任务；新一轮主表结果出现完整四段来源标记；`quality_filters_lowered=false`。
