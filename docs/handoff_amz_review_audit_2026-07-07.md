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
  - 支持多选处理方式表单和 `录入客服工单 / 申请观察 / 升级红线` 按钮。
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
AMZ_REVIEW_AUDIT_APP_TOKEN=xxx
AMZ_REVIEW_AUDIT_TABLE_ID=xxx
AMZ_REVIEW_AUDIT_OBSERVE=1
AMZ_REVIEW_OBSERVE_UNION=on_6e85dd60606f76f2d5af892785ac1dfe
AMZ_OPS_GROUP_CHAT_ID=oc_xxx_amazon_ops
AMZ_REVIEW_FRONTEND_CHECK_URL=https://example.com/amz/homepage-negative-check
LINGXING_PROXY_URL=https://frankiepan501.zeabur.app/webhook/lingxing-proxy
LINGXING_PROXY_TOKEN=xxx
```

默认 `AMZ_REVIEW_AUDIT_OBSERVE=1`，即使 `notify=true` 也只发 Frankie。灰度稳定后改为 `0` 才按负责人发。

## 合规边界

卡片不写“刷好评 / 买好评 / 要求删差评”。操作选项用合规措辞：

- `已发起合规留评拉升动作` = Request a Review / 售后体验后自然留评。
- `已提交违规评价举报 / 删除申请` = 只针对违反 Amazon 规则的评价。
- 不允许记录或引导“补偿换改评 / 删评”。

## 剩余上线步骤

1. 在飞书 Base 创建“亚马逊差评审计状态表”，按上方字段建表。
2. 配置 Zeabur env：审计表 token、Amazon 群 chat_id、Lingxing proxy、首页巡检服务。
3. 调用 `sample=true&mode=dry_run` 验证三类卡片预览。
4. `mode=commit&notify=false` 跑一次真实数据，只写审计表不发卡。
5. `notify=true` + `AMZ_REVIEW_AUDIT_OBSERVE=1` 只发 Frankie observe。
6. 灰度 1-2 名负责人后，把 `AMZ_REVIEW_AUDIT_OBSERVE=0` 放开负责人私聊。
7. 7 天后再配置 `AMZ_OPS_GROUP_CHAT_ID` 开公开复检失败卡，避免历史数据误伤。
