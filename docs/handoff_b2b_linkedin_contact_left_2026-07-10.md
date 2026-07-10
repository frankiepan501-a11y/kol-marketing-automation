# B2B LinkedIn 联系人离职回执

日期：2026-07-10

## 问题

外贸同事在 LinkedIn 每日开发卡里发现联系人已离职时，原来只能点「不合适」或不回执。这样会把“联系人失效”误处理成“客户公司不适合”，导致 CRM 里丢失仍有开发价值的客户公司。

## 改动

- 每张 LinkedIn 开发卡新增「联系人已离职」按钮。
- 点击后线索写入：
  - `开发状态=联系人已离职`
  - `触达状态=联系人失效`
  - `触达验证结果=联系人已离职`
  - `下一步行动=保留客户公司，重新找采购/BD/Category/Product 相关联系人后再开发。`
- CRM 同步为公司级客户跟进日志和跟进记录，不把公司改成「不合适」。
- 如果 CRM 还没有该公司，会新建公司记录，`合作状态=未联系`，`核心联系人=待补联系人`，离职联系人只写入日志。
- `/b2b-linkedin-daily-card/run` 新增可选 `record_id` 参数，用于手工重发指定线索卡；不影响每日批量 cron。

## 验证

- `python -m py_compile app/b2b_assistant.py app/b2b_crm_sync.py app/b2b_linkedin_daily_card.py app/main.py`
- `python -m unittest tests.test_b2b_linkedin_contact_left tests.test_b2b_crm_sync -v`：通过
- `python -m unittest discover -s tests -v`：B2B 相关测试通过；剩余一个既有失败 `tests.test_zeabur_watchdog.ZeaburWatchdogTests.test_run_once_alerts_any_project_service_failed_deployment`，与本次改动无关。

## 部署与通知

- Commit：`6ffc00270a773a038793a0c27a235610ff7369ab`
- Zeabur deployment：`6a5068dd019866a087e694cd`，状态 `RUNNING`
- 已重发指定卡片：`Extra Stores / Wael Abuzaid`，record_id `recvoJVEhiNV3w`，接收人 `冼浩华`，message_id `om_x100b6a32db1218a0c1f46d4e4508bed`
- 已发群操作指引：message_id `om_x100b6a32d9e6cca8c4b35d95671b254`

## 操作指引

- 联系人仍在该公司：按原流程打开联系人 LinkedIn，发连接后点「已加人」。
- 接受后已发私信：点「已发私信」。
- 发现联系人已离职/不在该公司：点「联系人已离职」。
- 「不合适」只用于客户公司本身不符合开发逻辑，不用于单个联系人离职。

## 剩余风险

- 本次不新增 Base 字段，只写现有字段的新状态值。若生产表的单选字段不允许新选项自动写入，首次点击可能需要在 Base 字段选项里补「联系人已离职 / 联系人失效」。

## 2026-07-10 生产点击复核

阿华点击 `Extra Stores / Wael Abuzaid` 的「联系人已离职」后，线索表状态、CRM客户记录和CRM跟进记录均已写入成功：

- 线索 `recvoJVEhiNV3w`：`开发状态=联系人已离职`，`触达状态=联系人失效`，`触达验证结果=联系人已离职`，已关联 CRM `recvoWeG387MQW`。
- CRM客户 `recvoWeG387MQW`：`合作状态=未联系`，`核心联系人=待补联系人`，公司未被判为“不合适”。
- CRM跟进记录：出现两条重复离职记录 `recvoWeGAOSagF`（actor=外贸助手）和 `recvoWeH9Bqqle`（actor=冼浩华）。

原因：飞书事件/后台转发存在重复送达可能；旧版 `sync_linkedin_contact_left` 虽传了 dedupe_key，但日志正文没有包含该 key，且跟进记录创建没有幂等判断，导致重复事件能产生第二条 CRM 跟进。

修复：离职回执日志追加稳定 `线索ID`，并在 CRM 已存在相同联系人离职日志时跳过新增跟进记录；兼容旧日志中未带线索ID的历史格式。
