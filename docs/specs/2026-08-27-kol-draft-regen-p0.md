# KOL 草稿退回重生 P0 修复

## 问题

生产 KOL 服务缺少 `KOL_DEEPSEEK_API_KEY`，导致运营点击“退回重生”后后台生成失败。Event Hub 又把异步接口的 `accepted`（仅代表接单）提前显示成“已处理”，并会对飞书重复回调再次发送相同文字，形成假成功和重复回执。

## 目标

1. KOL 专用 Key 缺失时，`/health` 明确显示 degraded，`POST /draft/regen` 在创建后台任务前返回 503。
2. 同一草稿的重复异步请求复用已有 job；运行中和成功终态不重复创建任务，并给 Event Hub `suppress_reply` 信号。失败终态保留 60 秒去重窗口，之后允许重试。服务重启后再用确定性的 `邮件草稿ID` 回读并复用已建新版草稿，即使旧草稿已经标成“已否决”也必须先恢复已建新版。
3. 首次卡片回调只显示“重生处理中”，不显示“已处理”。
4. 后台 job 成功、业务失败或直接抛异常后，使用聪哥分身3号 PATCH 原卡为明确终态；新审核卡路由失败不得显示绿色成功。PATCH 失败重试一次，再给原操作人发送 App3 兜底结果卡；失败卡保留同 App 的重试按钮。
5. n8n Event Hub 保持 active/published，节点数与连接不变，只修改 `Draft Action Handler`。

## 安全边界

- 不恢复通用 `DEEPSEEK_API_KEY` 回退，只使用 KOL 专用渠道 Key。
- Key 仅在 Zeabur API 调用内存中同步，不输出、不落盘、不进入 Git。
- 不手动重生张佳烨或其他运营的真实草稿；上线验证使用模拟 job 和 Frankie-only 卡片。
- `EMAIL_DRY_RUN_TO` 在修改与验证期间保持开启。

## 验收

- 新增服务端与 n8n 迁移测试全部通过。
- 生产 `/health` 返回 `status=ok` 且 `kol_ai_configured=true`。
- KOL Key 与 n8n 安全副本指纹一致、余额接口鉴权成功。
- Event Hub 回读 active=true 且 `versionId=activeVersionId`。
- 同一个模拟回调：第一次显示处理中；重复回调无第二条文字；终态更新原卡。
