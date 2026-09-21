# KOL 飞书 `1254607 Data not ready` P0 修复（2026-09-21）

## 结论

四张报错卡来自同一个飞书多维表格瞬态故障，不是四个独立事故。受影响轮次会跳过或延后，现有证据不表示邮件误发。线上健康检查随后恢复为 `ok`，说明本次外部故障已经自行恢复，但旧告警策略会按 endpoint 每小时重复提醒。

端点故障卡仍只发 Frankie。KOL 业务审核卡保持原路由：电商运营部群 + 按职务实时查询“独立站运营专员”私聊 + Frankie 抄送。报错卡没有发给运营是既有设计，不能用它判断业务卡是否正常产生。

## 根因

- 飞书返回 HTTP 400 / code `1254607` / `Data not ready`。
- `api()` 已有 5/10/20 秒短重试，但 `/auto-send/run`、`/dashboard/refresh` 和单条产品读取没有使用已有的后台长恢复窗口。
- 告警按 endpoint 去重，因此同一根因会分别产生多张卡，并在一小时后继续提醒。

## 改动

- `app/feishu.py`
  - 仅在明确启用的 KOL 后台任务中，把 30/60 秒额外读重试扩到产品、看板和 launch 相关表。
  - `get_record()` 单条读取也支持相同恢复窗口。
  - 写表和发邮件不增加盲重试，避免重复外部动作。
- `app/main.py`
  - `/auto-send/run`、`/dashboard/refresh` 后台任务接入长读恢复；launch autonomous 保留原接入并补恢复回执。
  - `1254607` 按根因合并，不再按 endpoint 分散告警。
  - 单次瞬态失败静默；持续 30 分钟后发一次 P2；未恢复时每 6 小时最多提醒一次；全部受影响任务恢复后发一次 P3。
  - 非瞬态的权限、OAuth、发信等失败仍即时按 P1 处理。
- `app/endpoint_alert_dedup.py`
  - 用现有持久化 SQLite 保存合并事故、涉及 endpoint、首次失败、提醒次数和恢复状态，服务重启后仍有效。

## 验证

- 新测试先在旧实现上得到 6 个预期失败，再完成实现。
- `tests.test_endpoint_alert_dedup` + `tests.test_feishu_fetch_all_records`：23/23 通过。
- KOL App 身份、审核卡发送和 SLA 路由：21/21 通过。
- endpoint 告警卡主流程：9/9 通过。
- `py_compile`：`app/endpoint_alert_dedup.py`、`app/feishu.py`、`app/main.py` 通过。
- 线上只读 `/health` 在修复前的瞬态异常结束后返回 `status=ok`、`base_access.ok=true`、持久去重状态正常。

## 剩余风险

- 本修复提高对飞书瞬态未就绪的容忍度，不等于修复飞书平台本身。
- 业务卡“最近没有收到”仍需结合草稿表的“卡片发送状态/错误/时间”和 n8n 最近执行核对；本次四张报错只能解释受影响轮次，不足以证明所有业务卡都被阻断。
- 不自动回放历史业务动作，也不自动补发邮件。

## 回滚

只回退本修复 commit；不要把共享服务重置到旧版本。无需修改 Zeabur 环境变量或飞书表结构。
