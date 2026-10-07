# 客服助手邮箱漏件修复交接（2026-10-07）

## 结论

本次只修复客服邮箱采集与正确回信通道，不改卡片回调 App、不扩权、不开启自动给客户发邮件。

- `support@powkong.com`：配置不全时不再默默返回 0 封，而是明确报错并由现有端点告警路由通知。
- `support@fireflyfunlab.com`：新增 Firefly Zoho 客服邮箱源，工单前缀为 `CSZ`；仅收客服地址邮件及 Shopify 联系表单通知，不把 KOL/合作邮件当客诉。
- `support@funlabswitch.com`：保留原网易企业邮箱源，工单前缀仍为 `CSF`。
- 三个邮箱源各自返回 `status / fetched / latest_received_ms`，水位从工单台恢复；连续两次水位倒退才告警，单次只留诊断，避免员工归档邮件造成噪声。某一源失败不会被其他源的成功掩盖。
- Zoho 按页扫描至多 1000 封，先过滤 Firefly 客服邮件再取本轮处理数，避免客诉被 KOL/合作邮件挤出窗口；扫满上限仍取不够时明确报异常，不把“没扫到”误报成健康。
- Zoho 以“邮箱源 + message ID”做持久去重；mail thread 只在同一次采集里合并连续邮件，并把合并邮件的全部正文、附件和 message ID 记入工单，避免下一轮把较早邮件重复派卡。同一会话里的客户新回复不会被吞掉：待补订单号/站点时归并原工单，其他状态则生成新的待处理动作。不同邮箱即使 provider ID 相同也不会互相吞掉。
- `CSZ` 工单点击回复时使用 Firefly Zoho 账号；`CSP` 仍用 Powkong Zoho，`CSF` 仍用网易。

## 根因

1. POWKONG 采集代码在凭据缺失时直接返回空列表，定时任务看起来成功，但实际一封也没拉。
2. Firefly 客服地址所在的 Zoho 账号没有接入客服采集，系统只拉了 Funlab 网易邮箱。
3. 回信代码原来只能区分 Powkong Zoho 和 Funlab 网易，无法识别 Firefly Zoho。

## 改动文件

- `app/cs_ingest.py`：邮箱源、显式配置错误、单封回放、独立水位、Firefly 回信通道。
- `app/cs_dispatch.py`：`CSZ` 卡片回复走 Firefly Zoho，已发箱回读也使用同一账号。
- `app/main.py`：回放端点接受 `funlab / powkong / firefly` 显式源。
- `tests/test_cs_ingest_recovery.py`、`tests/test_cs_dispatch_recovery.py`：回归测试。

## 验证

- 本次定向测试：采集 25 条、派卡/回信 30 条，全部通过。
- 全库测试中，本次相关测试通过；另有 4 条 KOL/Zoho 旧测试在未改代码的基线提交上也同样失败，不属于本次回归。

## 生产收尾

上线后应保留以下证据：

1. 三个邮箱源的 dry-run 都返回 `status=ok`，且有各自的 `latest_received_ms`。
2. 历史 17 个客户线程逐条回放，回放参数 `allow_info_request=False`，确保不发客户邮件。
3. 12 条已人工回复和 2 条等补货只留记录；仅 3 条仍需处理的工单定向派卡给陈翔宇。
4. 每张卡片核对工单 `record_id`、卡片 `message_id`、接收人为客服助手 App 命名空间下的陈翔宇。

