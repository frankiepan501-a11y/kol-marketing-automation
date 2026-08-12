# NYXI YouTube 云端增量采集

## 业务规则

- 北京时间每周一 09:30 固定运行。
- `营销事件表`存在 NYXI、官方确认、人工已确认的正式开售日，且当天处于 D0±30 天时，周三和周五 09:30 额外运行。
- 查询从上次成功采集时间向前重叠 48 小时开始，成功后才推进水位。
- 以 `YouTube + video_id` 去重；新帖写入，已有帖只更新公开数据。
- 服务代码没有 KOL 主表 ID，也没有邮件/消息发送代码。

## 端点

- `GET /health`：健康与 commit 开关。
- `GET /admin/version`：部署版本哨兵。
- `POST /run`：异步启动，body 为 `{"brand":"NYXI","platform":"YouTube","mode":"commit"}`。
- `GET /runs/{job_id}`：查看一次运行输入窗口、结果数量和错误类型。
- `GET /runs/{job_id}/assert`：n8n 等待后验收；仍在运行或失败时返回非 2xx，让 n8n 执行明确失败。

`/run` 与 `/runs/*` 使用 `Authorization: Bearer <SERVICE_AUTH_TOKEN>`。

## 环境变量

`YOUTUBE_API_KEY`、`FEISHU_APP_ID`、`FEISHU_APP_SECRET`、`SERVICE_AUTH_TOKEN`、`COMMIT_ENABLED`、`BUILD_VERSION`。

## 生产资源（2026-08-12）

- Zeabur 服务：`socialecho-youtube-incremental`，service id `6a7c2e3159162869e08f8437`。
- 健康检查：`https://socialecho-youtube-incremental.zeabur.app/health`。
- n8n 工作流：`Yi5owIF2FolWJp79`，名称 `SocialEcho - NYXI YouTube Incremental (Mon + Launch Wed/Fri)`。
- n8n 定时器只在周一、周三、周五触发；服务端二次判断周三、周五是否处于已确认 E1 正式开售日 ±30 天。
- 飞书配置：Base `KINabIENjak8fRsB6AHcIDALntc`，关键词配置 `tblgWfvdPgbkq541`，帖子库 `tblCDbvLtnLzdxEp`，营销事件 `tblpZaWYEWy54Sll`。

## 查看、回放与停用

- 查看服务版本：`GET /admin/version`；查看一次运行：`GET /runs/{job_id}`。
- 单条回放：带授权调用 `POST /run`，用 `preview` 不写飞书；只有明确要写时才用 `commit`。
- 紧急停写：把 Zeabur `COMMIT_ENABLED` 改为 `0` 并重新部署，服务会拒绝正式写入。
- 停止定时：停用 n8n 工作流 `Yi5owIF2FolWJp79`；不要删除飞书历史帖子和 7 个 E1 事件。
- 服务不含 KOL 主表 ID，也不含邮件、私聊或群消息目的地；候选 KOL 数只回写配置表的数字。
