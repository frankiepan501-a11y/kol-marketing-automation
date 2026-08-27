# 亚马逊差评巡检负责人刷新 P0 修复记录

## 结论

2026-08-27 已修复「Listing 首页差评巡检」继续按历史负责人派卡的问题。每日汇总现在会在分组和发卡前，从领星读取当前 Listing 负责人，并把确认后的新负责人回写到差评审计表。

## 问题与影响

- 差评记录在 2026-07-10 首次写入时保存了负责人快照。
- Listing 后续转交给其他运营后，`daily_digest()` 仍直接按旧字段分组。
- 结果是已经转交的 8 条 Listing 仍显示在余培霓名下。

## 根因

`app/amz_review_audit.py` 的每日汇总只读取飞书审计表，没有在派卡前重新读取领星当前分配。

## 修复规则

- 当前负责人真相源：领星有效 Listing（`is_delete=0`）。
- 匹配键：店铺名 + 差评代表子 ASIN；不按父体匹配，避免同一父体下不同子体归属不同运营时误派。
- 找到唯一当前负责人：本次卡片按新负责人分组；`commit` 模式回写飞书 `负责人`。
- 查不到或同一子 ASIN 出现多个负责人：本次归入“待确认负责人”，不沿用历史负责人，也不把占位值写回飞书。
- 领星读取报错：在飞书更新和发送卡片之前中止，避免用旧数据继续派卡。

## 改动

- 代码提交：`8d2dc9d`（`fix: refresh AMZ review audit owners`）
- 文件：
  - `app/amz_review_audit.py`
  - `tests/test_amz_review_audit.py`

## 验证

- 目标测试：25/25 通过。
- 覆盖：旧负责人刷新、无负责人兜底、无通知回填、精确有效子 ASIN、数值参数、第二页分页、多负责人冲突、领星异常时禁止写入/发送。
- 语法检查和 `git diff --check` 通过。
- 全仓测试：735/736 通过；唯一失败的 Zeabur watchdog 测试已在未修改基准 `d4d4ebd` 单独复现，属于既有问题。
- 当前领星只读核对：截图 8 条应为黄奕纯 2 条、陈翔宇 6 条。
- 生产 dry-run：29 条问题中精确识别 8 条负责人变化，与截图清单一致；`persisted=0`、`sent=0`。
- 生产回填：`notify=false`，成功回写 8 条；黄奕纯 2 条、陈翔宇 6 条，未发送任何卡片。
- 回读 dry-run：`changed=0`、`persisted=0`、`sent=0`，证明 8 条历史数据已纠正。
- 当前分组：林明坚 14、黄奕纯 4、陈翔宇 10、待确认负责人 1。

## 上线与回滚

- 已上线：`master` 提交 `ab4db071e8a35020c6186d71e5e3c4c3a07c3f51`，Zeabur `kol-automation` 部署 `6a8fbbdd04336e45712040d1` 为 `RUNNING`。
- 回滚代码：revert `8d2dc9d` 及本记录提交后重新部署。
- 回滚数据：飞书审计表 `负责人` 仅更新为领星当前负责人；如需恢复，可按部署前 dry-run 的 `old_owner` 清单逐条回填。

## 部署事故与恢复

- 部署时误用 Zeabur `updateEnvironmentVariable(data: Map)` 设置单个变量；该操作实际会全量覆盖服务环境变量。
- 发现后立即停止业务写入，使用已验证的 `recover_kol_zeabur_env_20260824.ps1` 通过 `createEnvironmentVariable` / `updateSingleEnvironmentVariable` 逐项恢复 122 项缺失配置。
- 亚马逊助手凭据从同一生产项目的万词服务可信副本恢复；过程未输出密钥。
- 恢复后环境变量总数 170，`/health` 返回正常，`/amz/feishu/callback` 恢复为 `configured=true`，并再次通过差评巡检 dry-run。

## 剩余风险

- 领星与飞书店铺名必须保持一致；名称不一致会进入“待确认负责人”，不会误派给历史负责人。
- 全仓原有 Zeabur watchdog 测试失败需另立任务处理，不属于本次 P0 范围。
- 当前仍有 1 条 `DRIESNAUDE-UK / B0F941B3LF` 无法从领星确认唯一负责人；系统已将其路由到“待确认负责人”，不再误发给历史负责人。
