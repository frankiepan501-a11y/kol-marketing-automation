# SocialEcho 客服 P0-5B：Frankie-only 飞书审核卡交接

## 结论

P0-5B 已完成真实单卡闭环：一条明确标记为非真实客户的测试工单，只向 Frankie 发送一张客服助手卡；点击只保存审核草稿，不回复客户、不写 X、不写 SocialEcho。真实点击、原卡更新、重复回放和持久审计均通过。

## 安全边界

- 发送和回调使用同一个客服助手 App：`cli_aab6bdb724e1dcdb`。
- 收件人固定为 Frankie；不支持运营、群聊或其他用户。
- action 固定为 `social_cs_review_save`，`send_mode=manual_only`。
- 回调只写审核草稿、审核时间、审核人和外发审计；工单保持`待回`。
- 没有调用历史`cs_send_reply`或`_dispatch_reply`客户发送分支。
- 没有客户、X、SocialEcho 或 SocialEcho 网页内部接口写入。

## 主要改动

- `app/cs_dispatch.py`
  - 新增合成测试记录硬闸、Frankie-only 发卡、发送占位和 UUID 幂等。
  - 新增安全审核 action、短修改保存、重复回调保护和原卡 PATCH。
  - 把客户/社媒外发 0 次写为持久审计，不使用接口写死值冒充证据。
  - 兼容飞书 GET 消息把交互卡归一化为旧格式的回读行为。
- `app/main.py`
  - 新增健康、单卡 dry-run/commit 和单条回读入口。
- `tests/test_cs_social_review.py`
  - 覆盖卡片结构、安全 action、非 Frankie 拒绝、单卡幂等、回调幂等、持久外发审计和归一化回读。

## 部署版本

- `ca12ce5` — `feat: add safe SocialEcho review card test`
- `6c9b735` — `fix: harden SocialEcho review card evidence`
- `529f5eb` — `fix: persist SocialEcho outbound audit`
- `42921ec` — `fix: label Feishu normalized card readback`
- Zeabur service：`kol-automation`
- 线上健康：`/cs/social-review/health` 返回 `version=p0-5b-v1`、`frankie_only=true`、`send_mode=manual_only`、`customer_send_enabled=false`。

## 单卡验收证据

- 测试工单：`TEST-P0-5B-20260826-001`
- Bitable record：`recvtq5CRMzE0H`
- 唯一飞书卡：`om_x100b67c5486830a0c3d972fe0342152`
- run：`P0-5B-20260826-001`
- 第一次发卡：`sent=true`
- 相同发卡请求重跑：`sent=false, duplicate=true`，message_id 不变。
- Frankie 真实点击后：
  - `review_saved=true`
  - 工单状态仍为`待回`
  - 原卡标题为“客服审核测试已处理”
  - 原卡为静态结果态
  - 客户发送尝试=`0`
  - 社媒平台写入尝试=`0`
  - `最近出站Message-ID`为空
- 相同回调单条回放：返回“该审核已经保存”；回放前后审核时间和草稿 SHA-256 完全一致，没有第二次写入。

## 验证

- P0-5B 专项测试：7/7 通过。
- 相关客服回归：`test_cs_dispatch_card` 5/5、`test_cs_resources` 9/9、`test_cs_info_request` 10/10 通过。
- 全仓 Python：215 条中 214 通过；唯一失败为既有 `test_zeabur_watchdog...failed_deployment`，与本次客服文件无关，且与开工前已知基线一致。
- 合并最新远端后重新运行专项和客服卡回归通过。

## 剩余边界与下一步

- P0-5B 只证明“草稿进入飞书人审并安全保存”；没有启用自动客户回复。
- 下一步是 P0-6：状态回写、SLA 提醒预览、失败告警、审计事件和按 `message_key/run_id` 单条回放；继续默认 dry-run。
- 任何真实客户发送、X 写入、SocialEcho 写入或扩大收件人范围，都必须另开审批，不继承本次授权。
