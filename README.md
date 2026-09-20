# YouTube 竞品采集：NYXI 每日优先

## 业务规则

- 北京时间每天 16:30 先运行 NYXI，并向指定站外运营部群发一条日报。
- 每日只刷新近 30 天已登记视频的公开数据；每周一另轮转最多 100 条更老视频。
- 8BitDo 历史补采在 NYXI 成功后运行，并使用本次日任务的保守搜索预算：默认上限 100 次、始终保留 20 次，本任务每次 `search.list`（包括翻页）都计数。该数字只代表本任务自己的调用，不冒充 Google 项目全局余量；若 Google 返回日额度耗尽，立即停止并保留原历史游标。
- 历史补采按“单个关键词 × UTC 日期”逐段进行；每段最多 10 次搜索，完整写入后才在现有 `YouTube历史游标` 保存断点。10 页仍有下一页时继续按时间拆分，并按 10 次计入内部预算；同一段 1 小时仍溢出则停下人工处理。每段前确认内部预算至少还能覆盖该段并保留 20 次；单次运行达到 20 分钟也保存断点下次续跑。
- 查询从上次成功采集时间向前重叠 48 小时开始，成功后才推进水位。
- 以 `YouTube + video_id` 去重；新帖写入，已有帖只更新公开数据。
- 服务按关键词配置表逐行读取已启用的 YouTube 监控任务，可同时处理多个竞品品牌；每个配置行有独立水位和运行状态。
- 服务代码不写 KOL 主表、也不发送开发邮件。日报由 KOL媒体助手发送；其他 KOL 通知不迁移。

## 端点

- `GET /health`：健康与 commit 开关。
- `GET /admin/version`：部署版本哨兵。
- `GET /status`：读取飞书中的最近成功时间、水位、数量和错误摘要；服务重启后仍可查。
- `POST /run`：异步启动。省略 `brand` 和 `config_record_id` 时，运行所有已启用的 YouTube 竞品配置；也可以用 `brand` 或 `config_record_id` 只跑一个配置。示例：`{"platform":"YouTube","mode":"commit"}`。
- `POST /daily`：NYXI 优先日任务。每个业务日只发送一条日报；配额不可核实时 8BitDo 自动暂停。
- `POST /report/test`：每日最多发送一次固定的“测试”群消息，不运行采集。
- `GET /runs/{job_id}`：查看一次运行输入窗口、结果数量和错误类型。
- `GET /runs/{job_id}/assert`：n8n 每 2 分钟检查一次，最长约 30 分钟；仍在运行时重试，失败时明确失败。
- `POST /replay/{video_id}`：只回放一条 YouTube 帖子；请求体可带 `config_record_id` 或 `brand`；默认 `preview` 不写表，`commit` 也不推进全局水位。

`/run` 与 `/runs/*` 使用 `Authorization: Bearer <SERVICE_AUTH_TOKEN>`。

## 环境变量

`YOUTUBE_API_KEY`、`FEISHU_APP_ID`、`FEISHU_APP_SECRET`、`SERVICE_AUTH_TOKEN`、`COMMIT_ENABLED`、`BUILD_VERSION`。

8BitDo 自动补采不再依赖 Google Cloud Monitoring、Cloud Billing 或服务账号。`/daily` 会先用 NYXI 本轮实际 `search_calls` 初始化内部预算，再逐段扣减 8BitDo 的搜索次数。生产环境中旧的 `GOOGLE_QUOTA_*` 变量属于废弃配置，应在新版本验证成功后删除。

## 生产资源（2026-08-12）

- Zeabur 服务：`socialecho-youtube-incremental`，service id `6a7c2e3159162869e08f8437`。
- 健康检查：`https://socialecho-youtube-incremental.zeabur.app/health`。
- n8n 工作流：`Yi5owIF2FolWJp79`，名称 `SocialEcho - NYXI YouTube Incremental (Mon + Launch Wed/Fri)`。
- n8n 使用同一个工作流，改为每天 16:30 调用 `/daily` 并轮询最终状态；旧 `/run` 保留供人工单品牌诊断。
- 飞书配置：Base `KINabIENjak8fRsB6AHcIDALntc`，关键词配置 `tblgWfvdPgbkq541`，帖子库 `tblCDbvLtnLzdxEp`，营销事件 `tblpZaWYEWy54Sll`。新增品牌只需在关键词配置表增加并启用对应行，不需要修改服务代码。

## 查看、回放与停用

- 查看服务版本：`GET /admin/version`；查看一次运行：`GET /runs/{job_id}`。
- 单条回放：带授权调用 `POST /replay/{video_id}`，用 `preview` 不写飞书；只有明确要写时才用 `commit`。
- 紧急停写：把 Zeabur `COMMIT_ENABLED` 改为 `0` 并重新部署，服务会拒绝正式写入。
- 停止定时：停用 n8n 工作流 `Yi5owIF2FolWJp79`；不要删除飞书历史帖子和 E1 事件。
- 服务不含 KOL 主表 ID、不会发邮件或私聊。指定群日报的发送回执写在关键词配置表 `YouTube日报回执JSON`，发送结果不明时必须人工核对，不能自动重发。
