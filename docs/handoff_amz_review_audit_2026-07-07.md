# 亚马逊 Listing 差评 / Feedback 审计卡片交接记录

日期：2026-07-07

## 目标

把“运营是否处理差评”从口头确认改成系统审计闭环：

```text
新差评 / Feedback
→ 负责人卡片提醒
→ 运营提交处理方式
→ T+7 首页复检
→ 首页无差评则恭喜关闭
→ 首页仍有差评则负责人私聊 + 亚马逊运营群公开升级
```

## 改动

- 新增 `app/amz_review_audit.py`
  - 归一化 Review / Seller Feedback。
  - 生成三类卡片：新增提醒卡、每日巡检卡、T+7 复检失败公开升级卡。
  - 支持多选处理方式表单；`客观无法移除，申请观察` 是表单选项，不再做独立按钮。
  - 底部只保留辅助动作按钮：`同步到客服库（可选） / 异常升级主管`，两者不是处理方式，也不是必点。
  - 支持 T+7 复检、首页无差评恭喜卡、负责人审计指标。
  - 支持 `amz_issue_*` 卡片回调幂等处理。
- 修改 `app/cs_dispatch.py`
  - 在现有 `/cs/callback` 里按 `value.action` 前缀把 `amz_issue_*` 分流到新模块。
  - 原客服按钮 `cs_send_reply / cs_reassign / cs_escalate` 不变。
- 修改 `app/main.py`
  - 新增 `POST /cs/amz-review-audit/run`。
- 修改 `.env.example`
  - 新增审计表、Amazon 群、首页巡检服务、领星代理相关配置。
- 新增 `tests/test_amz_review_audit.py`
  - 覆盖字段归一化、提醒阈值、卡片多选表单、审计指标、T+7 样例复检和处理提交回调。

## Endpoint

```http
POST /cs/amz-review-audit/run?kind=delta|daily|recheck|all&mode=dry_run|commit&notify=false|true&limit=50&sample=false
Authorization: Bearer <INTERNAL_TOKEN>
```

- `kind=delta`：拉取新增 Review / Feedback，写审计表并发负责人卡。
- `kind=daily`：按负责人生成每日首页差评巡检卡。
- `kind=recheck`：复检 `T+7待复检` 记录；失败公开升级，成功发一次恭喜卡。
- `mode=dry_run`：只返回预览。
- `mode=commit`：写飞书审计表 / 客服库。
- `notify=true`：发送飞书卡片；否则只写状态。
- `sample=true`：用内置样例跑 dry-run，不依赖领星和飞书。

## 审计状态表字段

需要在飞书 Base 创建一张“亚马逊差评审计状态表”，并把 token 配到 env。

建议字段：

- `问题键` 文本，唯一键。
- `来源类型` 单选：`Review / Feedback`。
- `来源ID` 文本。
- `状态` 单选：`待处理 / T+7待复检 / 复检通过：首页无差评 / 复检失败：首页仍有差评 / 客观无法移除，观察中 / 已升级`。
- `店铺名` 文本。
- `站点` 文本。
- `ERP品名` 文本。
- `ASIN` 文本。
- `ASIN链接` URL。
- `负责人` 文本。
- `严重级别` 单选：`P0 / P1 / P2 / P3`。
- `星级` 数字。
- `标题` 文本。
- `摘要` 多行文本。
- `首次发现时间` 日期。
- `处理时间` 日期。
- `处理人` 文本。
- `处理方式` 多选，选项为：
  - `已发起合规留评拉升动作`
  - `已提交违规评价举报 / 删除申请`
  - `已投诉Amazon / 已开Case`
  - `已联系买家售后处理`
  - `已完成Listing / 产品整改`
  - `客观无法移除，申请观察`
- `处理备注` 多行文本。
- `T+7复检日期` 日期。
- `当前首页状态` 文本。
- `首页差评数` 数字。
- `最近提醒时间` 日期。
- `恭喜已发送` checkbox。
- `客服工单ID` 文本。
- `卡片消息ID` 文本。

## 配置

```env
AMZ_REVIEW_AUDIT_APP_TOKEN=J2fibLgBZaLGTNsQOPHcQXLonZe
AMZ_REVIEW_AUDIT_TABLE_ID=tbltzQqIeEIPtJ2l
AMZ_REVIEW_AUDIT_OBSERVE=1
AMZ_REVIEW_OBSERVE_UNION=on_6e85dd60606f76f2d5af892785ac1dfe
AMZ_OPS_GROUP_CHAT_ID=oc_26e58d95b78670ed10a8bf4373da81f1
AMZ_REVIEW_FRONTEND_CHECK_URL=
LINGXING_PROXY_URL=https://frankiepan501.zeabur.app/webhook/lingxing-proxy
LINGXING_PROXY_TOKEN=<existing Zeabur env>
```

默认 `AMZ_REVIEW_AUDIT_OBSERVE=1`，即使 `notify=true` 也只发 Frankie。灰度稳定后改为 `0` 才按负责人发。

## P0-P2 rollout status

- P0 done: Feishu Base `J2fibLgBZaLGTNsQOPHcQXLonZe` has table `亚马逊差评审计状态表` / `tbltzQqIeEIPtJ2l` with 26 audit fields.
- P1 done: Zeabur `kol-automation` env has the AMZ audit table, observe union, Amazon group, Lingxing proxy, and `AMZ_REVIEW_AUDIT_OBSERVE=1`.
- P1 done: `kol-auto.zeabur.app/health` and `frankiepan501.zeabur.app/healthz` returned 200 after the Tokyo server recovered.
- P1 done: deployment `6a4d49a66ec90535ce441427` is `RUNNING` on commit `90202d04180104bc33a0a108366b5547b71bf8d6`.
- P1 done: online sample smoke passed with `sample=true&mode=dry_run&notify=false`, returning 2 eligible sample issues, 2 daily owner cards, and T+7 sample split as 1 failed / 1 passed.
- P1 done: real `delta` run with `mode=commit&notify=false` succeeded; current source returned 0 rows, so no records/cards were created.
- P1 done: real `all` run with `mode=commit&notify=true` succeeded in observe mode; current source returned 0 issues and `recheck_sent_group=0`.
- P2 done: three n8n workflows are active for Frankie observe grey release:
  - `ZcxVGSRV6ujhHn8m` - `AMZ - 差评/Feedback新增提醒 observe` - active, triggerCount=1, every 60 minutes.
  - `eKTyrlsU0JPDTD6F` - `AMZ - Listing首页差评每日巡检 observe` - active, triggerCount=1, daily 09:30 BJ.
  - `R8kXoqn0LAOkAFXI` - `AMZ - 差评T+7复检公开升级 observe` - active, triggerCount=1, daily 10:10 BJ.
- P2 safety: n8n production env does not currently expose `INTERNAL_TOKEN` / `KOL_AUTO_URL`, so the imported runtime workflows contain the Bearer token inside n8n only. Repo JSON files keep the env-template form and do not store secrets.
- P2 safety: T+7 group escalation is suppressed while `AMZ_REVIEW_AUDIT_OBSERVE=1`; observe mode sends to Frankie only, even if `AMZ_OPS_GROUP_CHAT_ID` exists.
- Card preview sent to Frankie via CS Assistant App on 2026-07-07:
  - issue card: `om_x100b6be4e8df18acc2e972411bec1fa`
  - daily card: `om_x100b6be4e8d6d8a0c06dcfb9695c833`
  - recheck failed card: `om_x100b6be4e8ee68a4c4397de94262a9c`
  - success card: `om_x100b6be4e8e1b0a4c1e171c90d20c52`
- V2 visual preview sent after card-design review:
  - issue card: `om_x100b6be497b04ca4c3bd592c53d22cd`
  - daily card: `om_x100b6be497453100c451663cd56edb4`
  - recheck failed card: `om_x100b6be4975c70acc05406f76b3d513`
  - success card: `om_x100b6be49757e0a8c37d47dd444dbcc`
- V3 button-clarity preview sent after action semantics review:
  - issue card: `om_x100b6be4b5a2f8a0c1c0112204f0bbf`
  - Change: primary action is form multi-select + confirm submit; customer-service sync and supervisor escalation are auxiliary actions, not four mutually exclusive buttons.

### 2026-07-09 parent-level homepage audit P0/P1

- P0 done: homepage-review issues now use parent-level grain by `site + parent_asin`.
  - New issue key format: `AMZ_HOMEPAGE:{site}:{parent_asin}`.
  - Same parent variation under the same site is not split into duplicate child-ASIN cards.
  - The card still shows representative child ASIN and all active child ASINs for operator context.
- P1 done: homepage review position context is now persisted and rendered.
  - New audit fields added to Feishu Base `tbltzQqIeEIPtJ2l`: `父体ASIN`, `代表子体ASIN`, `在售子体ASIN`, `Listing标签`, `首页差评位置`, `最靠前差评位置`, `挤走难度`, `跨站点同ERP差评`.
  - Existing `来源类型` option list now includes `Homepage`.
  - Cards show `首页差评位置`, `最靠前位置`, `挤走难度`, and cross-site same-ERP negative-review context.
- Test coverage added:
  - Parent homepage issue uses parent key and exports new Base fields.
  - Issue/daily/recheck cards render active children, positions, difficulty, and cross-site context.
- Real sample card sent to Frankie via `lark-cli` bot for design confirmation:
  - message: `om_x100b6bc2cecd54a0c4a90b8350d92c7`
  - sample listing: `Fanlepu-CA / 加拿大 / switch2 砖块拓展坞 / parent B0GX9MY9WX`
  - local preview JSON: `C:/tmp/amz_parent_homepage_sample_card_20260709.json`

### 2026-07-08 recovery audit

- Previous Zeabur blocker is resolved. Server events show the Tokyo server rebooted and completed a spec update; `status.isOnline=true`, `vmStatus=RUNNING`, `provisioningStatus=READY`.
- Zeabur redeploy completed and the service is serving the AMZ audit endpoint from commit `90202d0`.
- Local verification after the observe group-send guard:
  - `.venv\Scripts\python.exe -m unittest tests.test_amz_review_audit` passed 10 tests.
  - `.venv\Scripts\python.exe -m unittest discover -s tests` passed 79 tests.
  - Added regression coverage that `recheck_due(mode="commit", notify=true)` does not send group cards while observe mode is on.

## 合规边界

卡片不写“刷好评 / 买好评 / 要求删差评”。操作选项用合规措辞：

- `已发起合规留评拉升动作` = Request a Review / 售后体验后自然留评。
- `已提交违规评价举报 / 删除申请` = 只针对违反 Amazon 规则的评价。
- 不允许记录或引导“补偿换改评 / 删评”。

## 剩余上线步骤

1. 观察第 1 天 Frankie observe 卡片是否符合运营使用习惯。
2. 卡片确认后灰度 1-2 名负责人，再把 `AMZ_REVIEW_AUDIT_OBSERVE=0` 放开负责人私聊。
3. 7 天后再允许公开 T+7 群提醒，避免历史数据误伤。
4. 长期优化：把 n8n 的 `INTERNAL_TOKEN` / `KOL_AUTO_URL` 改成服务环境变量或 credential，避免 token 固化在 workflow definition。
