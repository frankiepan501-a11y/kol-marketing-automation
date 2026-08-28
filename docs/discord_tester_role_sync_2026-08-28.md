# FUNLAB Tester Selected 自动同步修复（2026-08-28）

## 问题

此前只对具体报名者手工调用 Discord API 补角色，新增报名者不会自动进入 `Tester Selected`，运营仍需反复人工检查。

## 根因

生产代码没有“飞书入选状态 → Discord角色”的持续同步程序。一次性 PUT 只能修复单个成员，不能覆盖未来新增人员。

## 修复

- 新增 `app/discord_tester_role_sync.py`。
- 每5分钟读取报名台账。
- 只有同时满足 `报名状态=已入选` 和 `核验状态=已通过` 才处理。
- 已有角色时不重复写；缺少角色时由 FUN Bot 添加并再次读取成员角色确认。
- 单个成员失败不会阻塞其他成员；健康接口保留扫描、符合、添加、已存在和失败数量。
- 默认关闭，生产通过 `DISCORD_TESTER_ROLE_SYNC_ENABLED=1` 明确启用。

## 观察入口

- `GET /discord/tester/admin/role-sync/health`：查看最近运行时间和结果。
- `POST /discord/tester/admin/role-sync/run`：带内部授权手动重跑一次。

## 安全边界

- 本流程只添加 `Tester Selected`，不自动删除角色。
- 不读取或保存配送信息、购买凭证、地址或电话。
- Discord Bot Token 只从生产环境变量读取，不写入代码、测试或文档。

## 验证

- 双条件门槛测试通过。
- 重复执行测试通过。
- 单人失败继续处理测试通过。
- Discord PUT 后 GET 回读测试通过。
