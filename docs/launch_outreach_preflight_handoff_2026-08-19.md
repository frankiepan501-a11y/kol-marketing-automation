# 新品集中上稿：首次真实开发信 P0 闸门

## 结论

首次真实开发信之前新增 3 个只读/测试端点。它们不会创建活动任务、邮件草稿、卡片或 KOL 主表记录；只有邮件测试端点会在 `EMAIL_DRY_RUN_TO` 已开启且调用者明确传入 `TEST_ONLY` 时，向测试邮箱发送 1 封邮件并回查 Zoho 已发送箱原始内容。

## 端点

| 端点 | 用途 | 业务写入 |
|---|---|---:|
| `POST /launch/candidates/preview` | 默认后台启动活动候选只读预览，立即返回 `job_id`；按产品别名家族做全局重复触达预检 | 0 |
| `GET /launch/candidates/preview/jobs/{job_id}` | 查询预览任务最终状态和候选结果；重复点击会复用同参数运行任务 | 0 |
| `GET /launch/candidates/replay` | 回放单个候选为什么可开发、应延续旧线程、暂缓或排除 | 0 |
| `POST /launch/email/test-raw` | 读取指定真实 cold 草稿，只发测试邮箱，回查发件账号、收件人、主题、HTML、正文长度、产品名、链接和残留占位符 | 0 条飞书业务记录 |

## 候选决策

- `eligible_new_cold`：可以进入活动新开发池。
- `reactivation_same_thread`：同一产品家族已有回复或正向关系，只能延续旧线程，不能重新发 cold。
- `blocked_prior_same_product`：同一产品家族已有有效触达，禁止换产品别名重复发。
- `hold_active_or_recent`：同品牌 7 天内有触达或仍在处理中，暂缓。
- `hold_duplicate_identity`：同一邮箱对应多条 KOL/媒体人身份，先合并身份。
- `blocked`：无效邮箱、邮箱退信状态或黑名单等硬拦截。

## 上线与验证顺序

1. 用 Zeabur 单变量方式设置 `EMAIL_DRY_RUN_TO=frankiepan501@gmail.com` 并重新部署。
2. 对食人花二代和戴夫手柄分别启动候选预览，保存返回的 `job_id`，轮询状态接口直到 `success`，再确认结果 `read_only=true`、`writes=0`。不要把 HTTP 已接受误当成计算完成。
3. 各选 1 条候选做单条回放，核对证据草稿 ID 与处理路线。
4. 从两款产品各选 1 条真实 cold 草稿，传 `draft_id` 与唯一 `run_key`；POWKONG、FUNLAB 各执行 1 次 `test-raw`，必须全部校验通过。当前生产单实例内，同一个 `run_key` 会先占位；并发、超时重试只回查原邮件，不重复发送。服务重启后仍先按 `run_key` 查 Zoho 已发送箱。
5. 只允许白名单中的单个规范测试邮箱；删除 `EMAIL_DRY_RUN_TO` 并重新部署后，再次调用 `test-raw` 应被硬拒绝。
6. 首封真实开发信仍需单独授权，不由本次 P0 自动触发。

## 验证记录

- 新品集中上稿相关专项测试：26/26 通过（含并发同 key、索引超时后重试不补发，以及“IP + 单个有效产品关键词”的回归测试）。
- 全仓测试：240 项中 239 通过；唯一失败为既有 `test_zeabur_watchdog`，与本次 KOL 改动无关。
- `git diff --check`：通过。

## 2026-08-19 生产验收

- 生产版本：`189ed9260fe6911387bc56b685d0fce3ac8069b9`；Zeabur deployment `6a8539ff3f743f07b5220cc2`，状态 `RUNNING`，`/health=ok`。
- 后台预览版本：`8b2298c0688085eae9152adc8a7298342b67b769`；Zeabur deployment `6a8576b0764188cac9d77574`。Dave 实测 POST 在约 1.2 秒内返回 `job_id=launchpreview-7127380d00b6`，后台约 3.1 分钟完成 1,544 人全池计算，最终 `read_only=true / writes=0`。
- 食人花二代：真实双链接 cold 草稿 `recvlrgEQvjWFS`，只发 `frankiepan501@gmail.com`；9 项 raw 校验全部通过，飞书业务表写入 0。
- 戴夫联名款：真实 cold 草稿 `recvkNpBRMAyNV`；首次校验暴露“仅剩 1 个有效关键词却固定要求命中 2 个”的规则缺陷。修复并重启后，沿用同一 `run_key` 回查原邮件，返回 `reused=true`，没有补发；9 项 raw 校验全部通过，飞书业务表写入 0。
- `EMAIL_DRY_RUN_TO` 已用单变量方式清空；清空前后环境变量均为 150 项，关键凭据项完整。清空并重启后再次调用端点返回 HTTP 400：`EMAIL_DRY_RUN_TO 未设置；禁止执行邮件测试`。
- 验收后行数复核：海外 KOL 主表 7,674、海外媒体人主表 302、邮件草稿 4,417、KOL 跟进记录 6,306，均未因本次 P0 测试增长。

## 可观测与回放

候选预览返回联系人记录 ID、掩码邮箱、评分、决策、原因和命中的历史草稿 ID；单条回放可复现同一条的判断。响应不返回完整邮箱，也不会把活动状态写回联系人主状态。

## 2026-08-20 首封真实灰度：IndieAlpaca

### 结果

- 活动：`launch-20260915-funlab-dave-ys11-5`
- 参与记录：`recvsLh7iLBxUg`
- KOL：`recvhwpbzedYZ8 / IndieAlpaca`
- 草稿：`recvsLHSKGv4tc / launch-dave-indiealpaca-20260820a`
- 收件邮箱：`contact@indiealpa.ca`
- 主题：`IndieAlpaca, this fits your retro corner`
- Zoho message ID：`1787164088114155100`
- 发送时间：2026-08-20 02:28:08（Asia/Taipei）
- raw：9/9 通过；raw 正文 597 字符，预期 597 字符
- 跟进记录：`recvsLIppE2dOA`
- 活动全局发送授权：发送后回读为 `false`

### 本轮修复

1. 单人真实发送不再加载整个候选池，改为只查目标联系人、同邮箱身份、相关草稿和产品家族；保留全部重复触达规则。commit `5a8280d`。
2. 飞书数字字段可能以字符串返回，`enrich._subscriber_count` 先规范为整数，避免邮件生成阶段格式化失败。commit `9b66315`。
3. 产品身份检查增加“授权 IP + 产品类型”组合，避免正文写明 `Dave the Diver controller` 却因没出现 `Pro` 被误报。commit `1455c60`。

### 验证与安全边界

- 活动相关测试 118 项通过，`py_compile` 和 `git diff --check` 通过。
- 同 nonce 已有草稿后接口只回查、不重发；这封邮件在修复 raw 规则时只读取既有 Zoho 消息，没有补发。
- KOL 主状态保持单调前进为 `待回复`；参与记录仍为 `已入围 / 审核通过`，并关联唯一草稿。
- 本次授权不包含其他 KOL、自动 follow-up、寄样、付费或批量发送；这些仍需后续独立闸门。
