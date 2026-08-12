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

`/run` 与 `/runs/*` 使用 `Authorization: Bearer <SERVICE_AUTH_TOKEN>`。

## 环境变量

`YOUTUBE_API_KEY`、`FEISHU_APP_ID`、`FEISHU_APP_SECRET`、`SERVICE_AUTH_TOKEN`、`COMMIT_ENABLED`、`BUILD_VERSION`。

