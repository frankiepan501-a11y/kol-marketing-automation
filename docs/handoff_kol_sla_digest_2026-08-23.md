# KOL SLA 汇总卡改造交接

## 结论

旧 `/sla/check` 对每条超过 24 小时的草稿分别发卡到业务群、运营负责人和 Frankie。2026-08-23 一天产生 27 条独立升级，虽然单条去重正常，但形成卡片风暴，并把 Frankie 拉回逐条业务审批。

本次改造把 SLA 通知收口为三个队列级提醒：

- P1：每次 6 小时 cron 最多给当前「独立站运营专员」私聊 1 张汇总卡；不发群、不抄送 Frankie。
- Frankie 例外：只有 P1 草稿超过 48 小时仍未处理时，给 Frankie 1 张异常汇总；卡片明确无需逐条审批。
- P2：其他来源每天北京时间 12:00 最多给运营负责人 1 张汇总卡。

## 分级规则

- P1：`reply / affiliate_quote / ship_confirm / tracking_followup`
- P2：其余来源，包括 `cold / followup / secondary_outreach / warm_recap` 和空来源

`tracking_followup` 同时覆盖状态为「待修改」的运单号跟进草稿；其他来源只统计状态为「待审」的记录。

## 卡片交接界面

每张汇总卡都显示：负责人、处理截止时间、待处理数量、最久等待、来源分布、系统已检查项、真实发送风险提示、最老 5 条记录直达链接，以及「在途草稿」视图按钮。

汇总卡本身不带审批回调，不改正文、审批状态或寄样状态，也不发邮件；P2 会写提醒元数据，并以独立日期文件作原子幂等键（重复执行仍只发一次）。运营仍在原审核卡或草稿表选择通过、否决、退回重生或修改后通过。运单跟进必须先补齐运单号和物流商，再检查正文并通过。

## 改动文件

- `app/sla_check.py`：收集、分级、汇总卡、收件人路由、P2 时间闸
- `app/feishu.py`：增加显式 `frankie` 通知角色
- `app/config.py` / `.env.example`：Frankie-only 生产闸、Frankie 唯一身份、P2 小时、时区、在途视图配置
- `tests/test_sla_digest.py`：P1/P2/48h/禁发群行为测试
- `scripts/kol_sla_digest_selftest.py`：fixture、生产只读预演、Frankie-only 样卡

## 验证与发布闸

- 本地目标测试：6 条通过。
- fixture 自测：通过，不写草稿、不发邮件、不发群。
- 生产只读预演：P1 25 条、P2 2 条、48h 异常 0 条。
- 下一步：用当前生产数据给 Frankie 发 1 张样卡；Frankie 确认排版与链接后，才允许推送并部署生产。

### 固定发布顺序

1. 保持 `KOL_SLA_CARD_FRANKIE_ONLY=1` 部署；此时定时工作流即使触发，所有 SLA 汇总卡也只会发给 Frankie。
2. 运行 `scripts/kol_sla_digest_selftest.py --live --send-frankie`，由 Frankie 在真实飞书客户端确认排版、数量、按钮和记录链接。
3. Frankie 明确回复样卡通过后，才把 `KOL_SLA_CARD_FRANKIE_ONLY=0`，重新部署并验证运营负责人收到、群聊未收到。

该开关必须长期保留。以后改卡片时先切回 `1`，不能直接拿运营或群聊做首测。

## 回滚

部署前固定点为 commit `956f574`。如上线后路由异常，回滚本次 SLA commit 并重新部署；不要修改或清空草稿表数据。
