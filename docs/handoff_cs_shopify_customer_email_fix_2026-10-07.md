# 客服助手 Shopify 客户邮箱识别修复（2026-10-07）

## 结论

Shopify 联系表单邮件的外层发件人是 `mailer@shopify.com`，旧逻辑把它直接写入工单「客户标识」，导致卡片显示错误，并让回复路径可能选择平台地址。修复后，客服助手按以下顺序确定客户邮箱：有效 `Reply-To` → 表单正文里唯一的 Email/E-Mail 字段 → 普通发件人。

## 安全边界

- `mailer@shopify.com` 等平台/系统地址在入库和实际发送两处都会被拦截。
- 表单正文出现多个候选邮箱时不自动选择，保留给人工核对。
- 存量纠偏只更新能从工单原文唯一确定真实邮箱的记录。
- 已存在的待回复卡片使用客服助手 App 原位更新，不补发新卡，不给客户自动发信。

## 改动文件

- `app/cs_ingest.py`：保留 Reply-To、提取表单邮箱、平台地址拦截、存量工单纠偏与写后回读。
- `app/cs_dispatch.py`：发送前二次拦截、原卡 PATCH 与回读、陈翔宇完成通知。
- `app/main.py`：增加受控纠偏/通知接口和部署版本标记。
- `tests/test_cs_info_request.py`
- `tests/test_cs_ingest_recovery.py`
- `tests/test_cs_dispatch_recovery.py`

## 验证

- `git diff --check` 通过。
- 三个改动模块 `py_compile` 通过。
- `tests/test_cs_*.py` 共 101 项通过。
- 生产验收应依次执行：健康版本检查 → 纠偏 dry-run → `confirm=true` 写入并更新原卡 → 工单及卡片回读 → 通知陈翔宇并保留 `message_id`。

## 回滚

- 代码回滚到部署前提交可恢复旧版本。
- 工单纠偏记录会在「沟通历史摘要」留下系统纠偏说明；若需回退，按本次返回的 record_id 定向恢复，不能全表覆盖。
- 卡片通过 PATCH 原位更新，可用当前工单字段重新生成并 PATCH，不需要删除消息。

## 剩余风险

- 没有 Email/E-Mail 标签或存在多个候选邮箱的旧表单不会自动纠偏，需人工确认。
- 新增语言标签前应先补回归样本，不应放宽成“抓正文任意邮箱”。
