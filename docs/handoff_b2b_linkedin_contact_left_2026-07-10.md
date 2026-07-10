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

## 2026-07-10 重复数据清理

已按生产复核建议清理阿华本次点击产生的重复数据：

- 删除重复 CRM 跟进记录：`recvoWeGAOSagF`（actor=外贸助手）。
- 保留 CRM 跟进记录：`recvoWeH9Bqqle`（actor=冼浩华）。
- CRM客户 `recvoWeG387MQW` 的 `跟进日志` 已移除 `外贸助手 [LinkedIn]` 那条重复行，保留 `冼浩华 [LinkedIn]` 记录。
- 删除后复核：`recvoWeGAOSagF` 返回 `RecordIdNotFound`，CRM日志中不再包含外贸助手重复行，仍包含阿华离职记录。

## 2026-07-10 卡片状态闭环 P1

问题：LinkedIn 每日开发卡点击回执后，Base/CRM 已写入，但原飞书卡片仍保持蓝色和可点击按钮。外贸同事无法从卡片本身判断“这个客户我已经操作过”，容易重复点击或在群里二次确认。

根因：`/b2b-assistant/event` 的 LinkedIn 回执只做业务写回和文本回复，没有用外贸助手 App PATCH 原始 interactive card；旧按钮 payload 也没有携带整张卡的 `record_id` 列表，不能安全重建多客户卡片。

修复：

- `app/feishu.py` 新增 `update_b2b_assistant_card()`，确保用发卡的外贸助手 App PATCH 原卡。
- `app/b2b_linkedin_daily_card.py` 的按钮 value 追加 `card_record_ids/card_index/card_total/owner_name`，用于点击后重建整张卡。
- `build_card()` 支持已操作状态：
  - 全部终态：灰色 `LinkedIn·已处理`。
  - 全部已操作但仍有下一步：绿色 `LinkedIn·已操作`。
  - 部分未操作：保持蓝色。
- 单条线索终态（`联系人已离职 / 不合适 / 已回复`）后不再显示重复回执按钮。
- 非终态保留下一步按钮：例如 `已加人` 后仍可点 `已发私信 / 已回复 / 联系人已离职 / 不合适`。
- 如果缺 `message_id` 或 PATCH 失败，发送一张结果卡兜底；如果旧卡缺多行上下文，则退回原文本回复，避免误把多任务卡覆盖成单任务卡。

验证：

- `.venv\Scripts\python.exe -m py_compile app/b2b_assistant.py app/b2b_linkedin_daily_card.py app/feishu.py`
- `.venv\Scripts\python.exe -m unittest tests.test_b2b_linkedin_contact_left`
- `.venv\Scripts\python.exe -m unittest tests.test_b2b_crm_sync tests.test_b2b_linkedin_contact_left`
- `.venv\Scripts\python.exe -m unittest tests.test_b2b_crm_sync tests.test_b2b_linkedin_async_audit tests.test_b2b_linkedin_auto_pool tests.test_b2b_linkedin_contact_left tests.test_b2b_linkedin_discovery`
