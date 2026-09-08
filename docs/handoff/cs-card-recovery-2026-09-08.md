# 客服助手卡片断流 P0 修复记录（2026-09-08）

## 结论

张佳烨在 2026-08-23 后收不到客服卡片，不是个人飞书权限问题，而是两个生产故障叠加：

1. `kol-automation` 缺少 FUNLAB 邮箱账号和授权码，采集器把该来源静默当成 0 封邮件。
2. 派卡观察变量缺失，代码默认把卡片发给 Frankie；定时任务仍显示 success，掩盖了业务断流。

2026-08-24 至 2026-09-08 的 FUNLAB 邮箱至少 130 封消息未进入客服工单台。

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
- `CS_DISPATCH_OBSERVE=1`
- `CS_DISPATCH_OBSERVE_UNION=<Frankie union_id>`
- `CS_DISPATCH_NOT_BEFORE_MS=1788867182124`
- 已确认 `CS_INFO_REQUEST_LIVE=0`

## 验证

- 客服相关测试：49/49 通过。
- 全仓测试：960 项中 959 通过；唯一失败 `test_zeabur_watchdog` 已在修复前基线 commit `ce20a2b` 独立复现，与本次客服改动无关。
- 生产采集和派卡 cron 在配置恢复、部署和小样本验证期间保持暂停。

## 待完成

1. 提交并部署本修复，验证部署版本和健康接口。
2. 对最老 5 封缺失邮件做 dry-run，按实测耗时评估完整补采容量。
3. 分批补采缺失邮件；全过程保持 `CS_INFO_REQUEST_LIVE=0` 和派卡历史截止闸。
4. 给 Frankie 发 1 张不改工单、不发客户邮件的样卡，核对客服助手 App 发卡身份。
5. 样卡通过后，把观察模式切到生产并恢复两个 n8n cron；确认新工单只派给正确运营。
