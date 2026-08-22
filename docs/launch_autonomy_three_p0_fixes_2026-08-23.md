# 双活动补池与审计三项 P0 修复记录（2026-08-23）

## 结论

本次只修复三项生产故障，不改变国家、语言、粉丝、内容、重复触达和邮件发送门槛，也不手动发送开发信：

1. Dave 已确认的 NYXI 证据快照不再被飞书瞬时读取错误误判为“证据不存在/配置无效”。
2. 15 分钟审计会先分别检查 Dave 与食人花，再统一给出结果；一项异常不会让另一项被跳过。
3. 食人花低产发现来源进入冷却和单条探测，不再用大量任务数冒充有效候选产出。

## 根因与修复

| 问题 | 根因 | 修复 | 业务影响 |
|---|---|---|---|
| Dave 补池长时间重读证据，偶发卡死 | 每轮预览重新读取快照和帖子；飞书 `1254607 Data not ready` 被统一包装成“不存在” | 快照节点 ID 和已校验帖子结果按版本/指纹做 6 小时有期复用；`1254607` 有界重试，耗尽后保留“暂时不可用”分类 | 临时故障不再污染活动证据状态；稳定快照不再每轮重复全量读取 |
| 双活动审计只看到 Dave | Dave 节点直接抛错，导致同一执行里的食人花分支不完整 | 两活动各自产生结构化结果，Merge 后统一判断；HTTP 单点错误也继续汇总 | 同一轮始终能看到两活动检查结果 |
| 食人花持续堆低产任务 | 运行中状态枚举漏了生产真实值 `2-运行中`；供给只看任务数量，不看有效邮箱和已审核通过对象 | 兼容真实运行状态；用近期有效邮箱信号和“新开发池已审核通过数÷完成任务数”判断低产；低产 2 小时冷却，之后每次最多 1 条探测；日志信号丢失时保守降速 | 不放宽硬筛选，也不再机械消耗爬虫任务量 |
| n8n 更新存在覆盖风险 | 旧脚本用本地数组整份替换节点/连接/设置 | 更新前完整 GET；只替换脚本托管节点，保留远端非托管节点/连接/设置；失败自动恢复原启用态 | 避免部署本修复时误删生产后加配置 |

## 改动文件

- `app/launch_evidence.py`
- `app/launch_candidate_preview.py`
- `app/keyword_supply.py`
- `app/launch_runtime.py`
- `scripts/upsert_launch_autonomy_workflows.ps1`
- `tests/test_launch_evidence.py`
- `tests/test_launch_candidate_preview.py`
- `tests/test_keyword_supply.py`
- `tests/test_launch_runtime.py`
- `tests/test_launch_autonomy_workflow_script.py`

## 本地验证

- 聚焦回归：112 项全部通过。
- 全仓回归：500 项中 499 项通过；唯一失败为既有 `test_zeabur_watchdog` 部署状态断言，与本次文件无交集。
- Python 语法、PowerShell 解析和 `git diff --check` 均通过。
- 独立双轴代码审查首轮问题已修复；最终规格复审和工程规范复审均为“无 P0/P1”。

## 生产部署与回读

- 生产 commit：`515fea89743d6a2649d19fb7c6777266f69a20c6`。
- Zeabur deployment：`6a89d705ba5938b7572310a5`，状态 `RUNNING`，`/health=ok`。
- n8n 开始工作流：`uvBfJBtGH93FPa6w`，启用，4 节点，时区 `Asia/Shanghai`。
- n8n 审计工作流：`1WOenWodtTRlUqWz`，启用，7 节点；真实 execution `986912` 为 `success`，完整经过 Dave、食人花、Merge 和最终汇总，输出 `both_campaigns_checked`。
- 食人花真实后台 job `launchruntime-35abd9e75366` 为 `success / supply_cooling_down`：`quality_gate.mode=cooldown`、新建发现任务 `0`、`quality_filters_lowered=false`。
- Dave 真实后台 job `launchruntime-bcf7eb52baf7` 在 24 分 51 秒后完成证据与候选处理，没有再报“NYXI 证据不存在/配置无效”；最终为 `degraded / supply_blocked`，原因已经推进到下一层：固定活动搜索词耗尽且 Dave 尚无备用拓词，和本次证据读取故障不同。
- 全程没有手动发送开发信；生产回放只启动了文档已明确“不直接发邮件”的后台补池入口。

## 新暴露但未混入本次范围的 P0

- Dave 仍缺活动专用备用拓词/低产探测策略。证据读取不再拦截它，但 `keyword_source=none`、`shortfall_tasks=4`，所以没有新增发现任务。
- 该项需要单独设计 Dave 的候选来源边界和回归样本，不能照搬食人花的 Mario/Nintendo 收藏词，也不能为了填满邮箱额度降低国家、语言、内容、重复触达等硬筛选。

## 回滚边界

- 代码回滚：回退本次 commit 并让 Zeabur 重新部署上一版本。
- n8n 回滚：脚本更新前以 GET 到的完整远端 workflow 为基础；若 PUT 失败会自动恢复原启用态。若上线后需回滚，使用部署前完整 payload 恢复并重新启用。
- 不涉及删除记录、修改 Zoho 凭据、改邮箱额度或手动补发邮件。
