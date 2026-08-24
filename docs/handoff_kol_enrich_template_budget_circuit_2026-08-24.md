# KOL 富化模板、调用预算与熔断交接

## 结论

KOL 富化已改为混合模式：普通英文候选使用固定模板，不调用 DeepSeek；仅非英文或明确高价值例外允许调用模型。所有 KOL 富化模型调用共享任务、单轮和每日预算，并在连续失败后停止当轮后续调用。

## 问题与根因

- 旧流程中，每个过线候选都调用 DeepSeek 生成草稿，随后又调用 AI reviewer，候选量增加时模型调用近似翻倍。
- 多个候选并发执行，模型异常时仍可能继续同时发出请求，缺少费用上限和失败熔断。
- 业务上大部分普通英文 cold email 不需要逐条模型创作，固定模板已能满足基础建联。

## 本次改动

- `app/enrich.py`
  - 普通英文候选走 `kol-cold-template-v1`，并做占位符、内部 SKU、价格/佣金、虚构作品、产品链接等确定性检查。
  - 非英文，或“高分 + 50 万以上粉丝 + 有 IP/时效由头”的英文例外，才允许模型个性化。
  - 模板草稿零模型审核后自动通过；模型例外通过确定性检查后固定进入人工审核，避免第二次 AI reviewer 调用绕过预算。
  - 模型例外串行执行，使连续失败熔断能立即阻止后续请求。
  - 保留原有 KOL UTM ID 回填。
- `app/enrich_model_guard.py`
  - 默认单任务 2 次、单轮 8 次、每日 20 次。
  - 连续 2 次失败后熔断当前轮。
  - 返回 `task_budget_exhausted`、`run_budget_exhausted`、`daily_budget_exhausted`、`circuit_open` 等明确原因。
- `app/config.py`
  - 增加模板模式、预算、熔断阈值和高价值例外阈值配置。
- `tests/test_enrich_model_guard.py`
  - 覆盖零模型模板、三级预算、连续失败熔断、确定性分流、AI reviewer 绕过和 UTM 回填。

## 默认配置

| 配置 | 默认值 | 业务含义 |
|---|---:|---|
| `KOL_ENRICH_TEMPLATE_MODE` | `1` | 普通英文候选使用模板 |
| `KOL_ENRICH_MODEL_PER_TASK` | `2` | 每个任务最多 2 次模型请求 |
| `KOL_ENRICH_MODEL_PER_RUN` | `8` | 每轮定时运行最多 8 次 |
| `KOL_ENRICH_MODEL_DAILY` | `20` | 单容器当日软预算最多 20 次 |
| `KOL_ENRICH_MODEL_FAILURE_THRESHOLD` | `2` | 连续失败 2 次后停止当轮模型请求 |
| `KOL_ENRICH_AI_SCORE_MIN` | `100` | 英文高价值例外最低匹配分 |
| `KOL_ENRICH_AI_MIN_FANS` | `500000` | 英文高价值例外最低粉丝数 |

## 验证

- KOL 富化相关测试：10/10 通过。
- 全仓测试：616 项中 615 项通过；唯一失败为改动前已存在的 `test_zeabur_watchdog...failed_deployment`，与本次 KOL 富化无关。
- `py_compile` 通过。
- `git diff --check` 通过（仅 Windows 换行提示）。

## 剩余风险

- 每日计数使用容器本地 JSON 文件；同一容器内可跨定时轮次累计，但容器重建后会清零。单任务 2 次和单轮 8 次仍是硬上限，已能阻止一次异常造成大量消耗。若后续需要跨重部署的严格日预算，应迁到 Redis/数据库。
- 非英文候选仍需要模型本地化；达到预算或熔断时不会创建不完整草稿。本任务会进入“草稿待审”，剩余例外需人工重新触发/新建任务，不会静默声称已处理。
- 上线后应观察至少一个定时周期的 `model_calls/template_ok/ai_ok/model_skipped`，确认普通英文流量为零模型。
