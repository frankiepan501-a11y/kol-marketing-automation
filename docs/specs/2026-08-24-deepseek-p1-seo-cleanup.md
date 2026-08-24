# DeepSeek P1：SEO 成本闸与草稿清理异步化

## 目标

在不改变文章主题、发布目标、草稿删除范围和通知对象的前提下，修复两条已复现的生产故障：

1. SEO 工作流在 DeepSeek 余额不足时仍继续解析空响应，并且两个 QA 节点额外调用模型、失败时默认通过。
2. 草稿归档清理同步等待超过 120 秒，n8n 先断开连接，无法确认任务最终结果。

## SEO 工作流验收标准

- 调用文章生成前先查询 DeepSeek 余额；`is_available != true`、余额不是有效数字或余额低于 10 元时，明确失败并停止，不进入付费生成节点。
- 文章生成响应必须存在非空 `choices[0].message.content`，且同时包含且只包含可解析的 `===ARTICLE_1===`、`===ARTICLE_2===` 分隔结构；否则停止，不解析、不发布。
- `QA Score PK` 和 `QA Score FL` 改为确定性检查，不再调用 DeepSeek；保留原来的 `quality_score`、`ai_trace_score`、`verdict` 和 `qa_issues` 输出契约。
- QA 至少检查标题、slug、meta、摘要、HTML 长度、H1/H2、外链和具体数字/日期；关键字段缺失或分数低于 70 时拒绝发布。
- 不改变文章 prompt、Shopify 发布目标、索引提交、翻译、图片和通知接收方。

## 草稿清理验收标准

- `POST /draft-cleanup/run` 默认立即返回后台 `job_id`，不再同步等待整批扫描和删除。
- 同一 `days + dry_run` 的任务仍在运行时，重复请求复用同一 `job_id`，不并发重复清理。
- `GET /draft-cleanup/jobs/{job_id}` 可查询 `running / success / error` 和精简结果。
- 增加 `dry_run=true`：完整扫描并返回候选数量，但不调用飞书删除接口。
- 正式模式仍只删除 N 天前状态为“已否决”或“发送失败”的草稿；其他状态继续硬保护。
- n8n 改为“启动后台任务 → 等待 → 查询状态”，不再用一个 120 秒 HTTP 请求等待整个任务。

## 生产验证边界

- SEO 只验证余额不足/无效响应会被拦截，不生成和发布真实文章。
- 草稿清理只跑 `dry_run=true`，不删除真实草稿。
- 更新 n8n 前先暂停目标工作流；GET 当前完整工作流后只改目标节点和连接，再恢复原 active 状态。
