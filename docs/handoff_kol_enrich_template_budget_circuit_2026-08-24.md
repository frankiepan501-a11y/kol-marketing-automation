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

## 2026-08-24 生产上线补充

### 首次真实渲染发现的问题

- 签名`Tom from FUNLAB Team`后又追加`from FUNLAB`，造成品牌重复。
- 产品品类来自中文字段，导致英文正文出现`a 手柄`。

本次没有把这两处内容直接放进批量真发。修复后，固定英文模板会先映射产品品类；未知中文品类回退为`gaming accessory`，并在发送前拒绝英文正文中的中文混入和重复品牌介绍。

### 代码与测试

- 修复提交：`15e438e`。
- 影响文件：`app/enrich.py`、`tests/test_enrich_model_guard.py`。
- 聚焦回归：27项通过；`py_compile`通过；`git diff --check`通过（仅Windows换行提示）。

### 测试邮箱与raw验证

- 固定模板测试`coldtpl-15e438e-20260824a`：`kol-cold-template-v1`验证通过，raw长度960、预期837，只产生1个测试message_id，生产草稿写入0。
- Gmail真实客户端确认：正文为全英文；产品身份、特性、链接、CTA、签名均完整；没有品牌重复或中文品类。
- 活动发送模板测试`launchq-restore-15e438e-20260824a`：`launch-queue-v1`验证通过，raw长度484、预期361，生产草稿写入0。

### 关闭开关与恢复真发

- `EMAIL_DRY_RUN_TO`使用Zeabur单变量删除，不覆盖其他环境变量。
- 最终production deployment：`6a8bf95ff0c2fe61c934ef75 / RUNNING`；健康检查正常；`dry_run_active=false`。
- 原定恢复的15封Dave真实开发信已由唯一发送中心完成：当日活动发送从6封增至21封。没有手动补触发。
- 随后16:00自然定时任务`autosend-f33f538eda2b`正常发送3封，失败0；发送后Zoho raw核验3/3通过、告警0。
- 最终只读日报`launchreport-1f5d056d5aa5`：Dave今日24封、累计176封、今日回复3、可发送库存0；FUNLAB滚动24小时29/120、剩余91。日报自身`validation.ok=true`且业务写入0。
- Zoho最近发件抽查：19封真实活动邮件对应19个唯一收件人，重复组0，message_id缺失0；测试邮箱邮件不计生产日报。

## 后续P1：Raw证书按模板分槽

当前活动记录只保存一张Raw证书。验证`kol-cold-template-v1`时会覆盖生产发送实际要求的`launch-queue-v1`证书，导致发送中心正确但暂时性地锁住队列。本次已恢复生产模板证书；后续应按模板版本分别保存证书，使测试模板和生产模板互不覆盖。该P1不阻塞当前发送。
