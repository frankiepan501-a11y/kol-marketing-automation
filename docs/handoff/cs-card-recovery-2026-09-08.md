# 客服助手卡片断流 P0 修复记录（2026-09-08）

## 结论

张佳烨在 2026-08-23 后收不到客服卡片，不是个人飞书权限问题，而是两个生产故障叠加：

1. `kol-automation` 缺少 FUNLAB 邮箱账号和授权码，采集器把该来源静默当成 0 封邮件。
2. 派卡观察变量缺失，代码默认把卡片发给 Frankie；定时任务仍显示 success，掩盖了业务断流。

2026-08-24 起至修复时，FUNLAB 邮箱最终核对 133 封入站消息未被完整覆盖。

## 本次改动

- `app/cs_ingest.py`
  - 邮箱配置缺失时明确失败，不再静默返回空列表。
  - 支持按单个 Message-ID 回放，复用既有去重、分类和写表逻辑。
- `app/main.py`
  - 新增受鉴权的 `POST /cs/ingest/replay`。
  - 采集/派卡出现来源、配置、发送或未预期异常时返回非 200，避免 n8n 假绿。
- `app/cs_dispatch.py`
  - 观察模式、观察收件人和新工单截止时间必须显式配置。
  - 飞书发送失败保留原因，且不把工单误写为“待回”。
  - 自然 cron 只派修复时间后的新工单；历史补录只能按 `rids` 明确放行。
- `tests/test_cs_ingest_recovery.py`、`tests/test_cs_dispatch_recovery.py`
  - 覆盖断流、回放、非 200、发送失败和历史卡片隔离。

## 生产配置

继续使用客服助手 App `cli_aab6bdb724e1dcdb`，不改 App namespace、工单 Base、卡片 action 或回调入口。

Zeabur 采用单变量恢复，未全量覆盖：

- `NETEASE_FUNLAB_CS_USER`
- `NETEASE_FUNLAB_CS_AUTHCODE`
- `CS_DISPATCH_OBSERVE=0`
- `CS_DISPATCH_OBSERVE_UNION=<Frankie union_id>`
- `CS_DISPATCH_NOT_BEFORE_MS=1788867182124`
- 已确认 `CS_INFO_REQUEST_LIVE=0`

## 验证

- 修复 commit：`8287ebb`。
- 客服相关测试：49/49 通过。
- 全仓测试：960 项中 959 通过；唯一失败 `test_zeabur_watchdog` 已在修复前基线 commit `ce20a2b` 独立复现，与本次客服改动无关。
- 历史补采最终对账：邮箱入站 133 封，其中 132 封作为工单主邮件覆盖、1 封作为既有工单合并回复覆盖，未覆盖 0。
- 历史补采期间客户邮件发送保持关闭，未给客户补发邮件；修复前工单由截止时间闸拦截，未向张佳烨补发历史卡片。
- Frankie-only 预览卡验证成功：记录 `recvuDiI9y6iwy`，消息 `om_x100b653436e118a4c4eeafd187a4533`；预览未改变工单状态。
- 正式切换并重新部署后，派卡实跑为 `observe=false`、21 条历史候选全部跳过、eligible=0、sent=0、错误 0。
- FUNLAB 采集实跑 fetched=25、new=0、skipped=25、errors=0，证明去重正常。
- n8n 采集 `nvNxNzDapXho8ShJ` 与派卡 `f7vsalTE5Yybk8OE` 已恢复，回读均 `active=true`。

## 当前待观察

1. P1：等待下一条自然新工单，核对工单生成、张佳烨收到卡片、按钮回调后原卡更新三项证据。
2. P2：补独立业务断流监控，直接比较邮箱最新时间、工单最新时间和派卡数量，避免以后只看 n8n success。

## 运维修复注意

Zeabur 控制台保存环境变量后，当前容器不会自动加载新值。本次必须手动“重新部署”后，`/cs/dispatch` 才从 `observe=true` 变为 `observe=false`。以后以业务接口返回和运行结果为准，不以控制台“保存成功”作为生效证明。

## 历史漏回分派（2026-09-08）

Frankie 审阅审计结果后授权按建议分派。执行顺序为清账 → P1 → 回读 → P2 → 回读：

- 清账：2 条误归档客户问题恢复为待派；1 条 FL1017 较早重复催问合并为已解决并注明未单独回复客户；1 条已有网易已发送证据的工单改为已回复并回填出站 Message-ID。
- P1：向张佳烨发送 12 张客服卡，12/12 回读为“待回”。
- P2：向张佳烨发送 9 张客服卡，9/9 回读为“待回”。
- 飞书实读：常规客服 21/21 个 `message_id` 均由客服助手 App 读回为 `interactive` 消息。
- Amazon 初始处理：`recvuDhaNgJMMJ` 仍缺订单号和国家站点，曾保持“待客户补充 / 待定·领星反查站点”。
- 安全边界：本次未调用客户邮件发送函数；卡片提示运营先在原邮箱/Discord 手动回复。

Zeabur 管理 API 在本次执行时返回 401。没有绕过权限、没有改生产变量或 n8n；历史派卡改由同一客服助手 App 直接发送，复用生产 `_build_card()`、`value.action`、`cs_dispatch:<record_id>` 幂等键，并在成功后写回原工单。执行器与完整名单保存在 `D:/Documents/客服/.codex/plan/2026-09-08-cs-unanswered-audit/`。

### Amazon 信息收集断点纠正

Frankie 随后指出：仅标记“待客户补充”但没有人向客户发补问邮件，无法真正等到资料。已按这个纠正补齐交接界面：

- 将 `recvuDhaNgJMMJ` 指派张佳烨作为客服信息收集人，状态改为待派后发送客服卡。
- AI 草稿明确索取 Amazon 订单号和 Amazon 国家站点/Marketplace。
- 卡片只要求张佳烨在原邮箱手动补问；本次系统没有直接发送客户邮件。
- 客户回复后，再按国家站点转对应亚马逊运营；当前没有误派具体亚马逊运营。
- 最终总验收：22/22 张客服卡存在，22/22 个 `message_id` 可由客服助手 App 读回为 `interactive`；Amazon 工单回读为“待回 / 张佳烨”。
