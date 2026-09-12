# B2B Base 专用身份迁移 P1-1

## 结论

`kol-automation` 的 B2B Base 读写已从共享 `bitable` 身份迁到逻辑身份 `b2b_base`。`b2b_base` 复用现有外贸助手 App 的 `FEISHU_B2B_ASSISTANT_APP_ID/SECRET`，不新增飞书 App，也不修改全局 `FEISHU_BITABLE_*`。

联系人查询、IM 回复、交互卡发送、原卡更新和回调继续使用 `b2b_assistant`，因此卡片归属和收件人规则没有变化。

## 问题与根因

- 问题：多个 B2B 定时任务共同依赖共享 App2 的 Base 身份；该 Secret 轮换后，`kol-automation` 未同步，导致认证失败。
- 根因：B2B Base 数据调用和其他业务共用 `which="bitable"`，没有独立业务身份，单个共享凭据失效会同时打断多条 B2B 任务。

## 本次改动

- `app/feishu.py`：新增 `b2b_base` 到现有外贸助手凭据的明确映射，缓存键与 `b2b_assistant` 分开。
- `app/b2b_assistant.py`
- `app/b2b_crm_sync.py`
- `app/b2b_linkedin_auto_pool.py`
- `app/b2b_linkedin_daily_card.py`
- `app/b2b_mail_reminder.py`
- `app/b2b_outreach_email.py`

以上六个 B2B 模块共 28 个 Base 读写调用改为 `which="b2b_base"`。没有修改非 B2B 模块、环境变量、卡片发送、回调、收件人、邮件逻辑或业务筛选规则。

版本：P1-1 / 2026-09-12；Git 版本为本文件所在提交。

## 验证

- 外贸助手认证：飞书返回 `code=0`。
- 七张 B2B 表：字段、视图及最小记录读取均成功。
- 写权限：只对明确标注“样张测试、非正式客户”的 LinkedIn 测试记录临时追加标记，读回成功后立即恢复；恢复值再次读回一致。
- 副作用检查：跟进、提醒、开发信队列记录数未变化；未发送卡片、IM 或邮件。
- TDD：新增身份测试先准确失败，最小实现后通过。
- B2B 回归：58 项通过。
- KOL 身份及通用飞书读取回归：38 项通过。
- 全仓回归：在完整临时依赖环境中 1054 项通过。
- 静态检查：`app/b2b_*.py` 中 `which="bitable"` 为 0，`which="b2b_base"` 为 28；卡片/IM 路由仍为 `b2b_assistant`。
- 语法与补丁检查：`py_compile`、`git diff --check` 通过。

## 回滚与剩余风险

- 回滚：回退本文件所在提交即可让 B2B Base 调用恢复到共享 `bitable` 身份；旧 App2 配置本次未删除。
- 剩余风险：P1-2 至 P1-6 不属于本次范围，仍需逐项确认后执行；尤其 `_refresh_token()` 对飞书业务错误的明确报告属于 P1-3，本次未顺手修改。
