# 新品集中上稿：首次真实开发信 P0 闸门

## 结论

首次真实开发信之前新增 3 个只读/测试端点。它们不会创建活动任务、邮件草稿、卡片或 KOL 主表记录；只有邮件测试端点会在 `EMAIL_DRY_RUN_TO` 已开启且调用者明确传入 `TEST_ONLY` 时，向测试邮箱发送 1 封邮件并回查 Zoho 已发送箱原始内容。

## 端点

| 端点 | 用途 | 业务写入 |
|---|---|---:|
| `POST /launch/candidates/preview` | 活动候选只读预览；按产品别名家族做全局重复触达预检 | 0 |
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
2. 对食人花二代和戴夫手柄分别运行候选预览，确认 `read_only=true`、`writes=0`。
3. 各选 1 条候选做单条回放，核对证据草稿 ID 与处理路线。
4. 从两款产品各选 1 条真实 cold 草稿，传 `draft_id` 与唯一 `run_key`；POWKONG、FUNLAB 各执行 1 次 `test-raw`，必须全部校验通过。同一个 `run_key` 重试只回查原邮件，不重复发送。
5. 只允许白名单中的单个规范测试邮箱；删除 `EMAIL_DRY_RUN_TO` 并重新部署后，再次调用 `test-raw` 应被硬拒绝。
6. 首封真实开发信仍需单独授权，不由本次 P0 自动触发。

## 验证记录

- 新品集中上稿相关专项测试：23/23 通过。
- 全仓测试：233 项中 232 通过；唯一失败为既有 `test_zeabur_watchdog`，与本次 KOL 改动无关。
- `git diff --check`：通过。

## 可观测与回放

候选预览返回联系人记录 ID、掩码邮箱、评分、决策、原因和命中的历史草稿 ID；单条回放可复现同一条的判断。响应不返回完整邮箱，也不会把活动状态写回联系人主状态。
