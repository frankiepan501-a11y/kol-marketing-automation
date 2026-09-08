# B2B LinkedIn 入池日报改为负责人私聊

## 结论

`LinkedIn线索入池日报` 不再发送到“B2B商务与分销渠道群”，改为只通过外贸助手 App 私发给汇总负责人。默认负责人是吴晓丹，实际接收身份仍从 `B2B_LINKEDIN_OWNER_NOTIFY_JSON` 中按姓名读取，确保使用外贸助手自己的用户命名空间。

## 影响

- 只影响 `/b2b-linkedin-pool-summary/run` 的日报通知目标。
- 不改变每日三位业务员各自收到的 `LinkedIn每日开发卡`。
- 不改变 n8n 工作流 `WYIlpphakiiVFzrE` 的时间、参数和启用状态。
- 不改变卡片内容、数据统计、外贸助手 App 或任何回调。

## 问题与根因

- 问题：全体线索入池汇总每天发到 18 人业务群，通知范围过大。
- 根因：`run_pool_summary()` 直接把 `B2B_GROUP_CHAT_ID` 写成默认发送目标，没有单独的汇总负责人路由。

## 改动

- `app/b2b_linkedin_daily_card.py`
  - 新增 `B2B_LINKEDIN_POOL_SUMMARY_OWNER`，默认 `吴晓丹`。
  - 汇总日报从 `B2B_LINKEDIN_OWNER_NOTIFY_JSON` 读取该负责人的私聊目标。
  - 缺少负责人映射时明确返回发送错误，不再回退群聊。
  - 汇总日报只接受负责人姓名下的精确 `open_id / union_id / email` 映射；不接受 `*` 通配和任何 `chat_id`。
- `.env.example`
  - 登记汇总负责人配置和“不回退群聊”说明。
- `tests/test_b2b_linkedin_pool_summary_routing.py`
  - 覆盖负责人私聊、缺映射、群目标拦截和 Frankie-only 预览。

## 验证方式

```powershell
.venv\Scripts\python.exe tests\test_b2b_linkedin_pool_summary_routing.py -v
```

上线后只做 dry-run（只演练不发消息）和版本回读；不要为了验证而手动触发 `notify=true`，避免负责人收到额外日报。

## 回滚

代码回滚到本次提交前即可恢复旧路由。除非 Frankie 明确要求，否则不要恢复群发送。
