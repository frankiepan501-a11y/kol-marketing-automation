# Progress

## 2026-07-23

- 已根据用户截图补写第三个产品 `B0D1CLBFD9 / recvq1QtUEEcXv`：采购成本 `12.5`，供应商链接 `https://detail.1688.com/offer/1049232514744.html?spm=a26352.b28411319/2508.0.0`。
- 已 PATCH 原卡 `om_x100b69249b8e70a0c00088987697b04`，当前读回显示 `3/4` 已回填、剩余 `B0CNRH4GRJ` 待回填。
- 初步发现风险点：现有测试只覆盖标准 `action.form_value`，没有覆盖飞书真实表单可能返回的嵌套结构或列表结构。
- 已在 `app/amz_procurement_quote.py` 增加表单值扁平化解析，兼容 flat / nested / input_values list 三类结构。
- 已新增 `scripts/amz_procurement_card_selftest.py`，本地自测通过：三个 URL 按钮、输入框、submit payload、模拟写表、原卡 PATCH。
- 单测第一次用 `python -m unittest tests...` 失败，因为 `tests` 不是 package；第二次 discover 被本机 `C:\tmp\ml-data-sync\app` 同名包污染。后续用 `top_level_dir=os.getcwd()` 强制从当前仓库导入。
- 目标单测通过：`16 tests OK`。
- 当前 P0 真实 4 条记录 dry-run 自测通过：`validate_quote_card` 返回空错误；已回填 `B0CH1817WW / B0CSCXSHPQ / B0D1CLBFD9`，待回填 `B0CNRH4GRJ`。
- 已写入 lesson candidate：`C:\Users\Administrator\.codex\memory-candidates\2026-07-23-amz-procurement-card-preflight.md`。
- 已提交并推送代码修复 `a2759b6 fix: harden AMZ procurement card callbacks` 到 `master`。
- 文档/计划更新提交 `73383ae docs: record AMZ procurement card verification` 也已推送并部署到 `RUNNING`。
- 线上 smoke 通过：`/health=ok`、`/amz/feishu/callback configured=true`、受保护 dry-run 返回 `card_selftest=passed`；显式 4 条 P0 record_ids dry-run 也返回 `count=4`、`card_selftest=passed`。
