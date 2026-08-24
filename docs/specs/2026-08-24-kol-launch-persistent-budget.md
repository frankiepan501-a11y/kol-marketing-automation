# KOL Launch DeepSeek P1 持久预算

## 背景

P0 已把集中宣发的画像刷新、备用拓词和活动草稿例外收进同一模型预算，默认单任务 2、单轮 12、单容器每日 60。当前每日计数写在容器 `/tmp`，重新部署会清零，因此只能算软保护。

## 目标

1. `uvBfJBtGH93FPa6w` 保持 active；本改动不修改 cron、筛选、补池、审核或发送规则。
2. Launch 每日额度写入 `kol-automation` 自有 Zeabur 持久卷，重新部署后继续沿用当天计数。
3. 每日计数用 SQLite 事务原子预占，覆盖同一服务短暂多进程/新旧部署并存时的并发。
4. 每次 `reserve()` 和 `snapshot()` 都重新计算北京时间日期，跨零点自动使用新一天额度。
5. 快照返回预算日期、存储后端、可用状态和最近拒绝原因；不返回密钥、连接串或原始异常。
6. 持久存储不可用时 fail-closed：不调用模型，继续现有确定性降级，不能让 KOL 补池报错。

## 非目标

- 不迁移普通 KOL 富化的既有 JSON 预算。
- 不新增 Redis 或独立数据库服务，不复用 n8n PostgreSQL。
- 不修改 DeepSeek Key、调用模型、模板、业务阈值或 n8n 工作流。

## 公共测试缝

- `EnrichModelBudget.reserve()`：独立实例共享 SQLite 当日计数；存储错误返回 `budget_state_unavailable`。
- `EnrichModelBudget.snapshot()`：跨北京时间零点刷新，暴露 `budget_day/state_backend/state_available/last_denial_reason`。
- `launch_runtime.autonomous_refill()`：继续只传一个预算对象，已有 `model_budget` 结果结构向后兼容地增加可观测字段。

## 生产存储

- Zeabur service：`kol-automation`
- 独立挂载目录：`/data`
- 单变量：`KOL_LAUNCH_MODEL_STATE_PATH=/data/kol_launch_model_budget.sqlite3`
- 挂载卷、变量和支持 SQLite 的代码三者必须同时生效；缺一项均不能宣称跨部署持久预算完成。

## 上线验收

- 红测先失败，最小实现后聚焦测试与全仓测试通过。
- 两路独立代码审查无剩余 P0/P1。
- 部署前确认两个活动均无运行中 job；部署时不切换 n8n active 状态。
- Zeabur deployment RUNNING、`/health` 200；卷与单变量回读正确。
- 用独立探针 SQLite 文件验证“写入一次 → 同 commit 再部署 → 计数仍在”，不调用 DeepSeek。
- 下一次自然 cron 成功，结果中的 `model_budget` 显示 SQLite 可用且业务补池不报错。
