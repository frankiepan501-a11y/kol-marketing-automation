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
- 用户已在原卡填写第 4 个产品 `B0CNRH4GRJ / recvq1Quaar3h2`，卡片前端显示“已收到”，但候选表读回仍为 `采购回填状态=待回填`。
- 根因定位：飞书 URL 样式字段不能稳定接收纯字符串，本次回调/手工复现报 `1254068 URLFieldConvFail`；已确认已成功记录的 URL 字段原始结构为 `{link,text}` 对象。
- 已按 `{link,text}` 对象格式手工补写第 4 个产品：采购成本 `20`，供应商链接 `https://detail.1688.com/offer/6150807684`，候选表读回为 `已回填`。
- 已修复 `app/amz_procurement_quote.py`，回调写 `1688供应商链接` 和 `采购链接` 时统一使用飞书 URL 单元格对象；新增单测覆盖裸 1688 offer 链接。
- 本地验证通过：`py_compile`、`17 tests OK`、`scripts/amz_procurement_card_selftest.py` 通过。
- 已提交并推送 `521d823 fix: write AMZ procurement URL fields as links`，Zeabur deployment `6a61a9b89cfc4cd5e6896eda` 已 `RUNNING`。
- 已用线上 `/amz/feishu/callback` 重放第 4 个产品提交并 PATCH 原卡；原卡读回标题为 `已全部回填`，不再包含 `确认回填本产品` 按钮/表单，第 4 个显示 `采购成本: 20.0 RMB` 和可点击 1688 链接。
- 受保护线上 dry-run 显式 4 条 P0 record_ids 返回 `ok=true`、`count=4`、`card_selftest=passed`。
- 已确认长期记忆 note `2026-07-23T13-45-00-feishu-card-preflight-selftest.md` 已落地：以后飞书交互卡发给业务人员前必须自测 URL 按钮、输入框 name、submit payload、真实/回放回调、业务写回读回、原卡 PATCH 和重复点击反馈。
- 已对 P0 四条采购回填记录做只读毛利重算，口径为 `采购后毛利RMB = 采购前可用毛利RMB - 采购成本RMB`，`采购后毛利率 = 采购后毛利RMB / (售价€ * 汇率EUR_RMB)`。
- 重算排序结论：`B0CH1817WW` 明显通过，建议继续推进；`B0CSCXSHPQ` 仅 A/FBA经济线刚过 30% 线，建议条件推进/50 件验证并压价；`B0D1CLBFD9` 现有 C/FBM-4PX 为 28.5%，暂缓补 FBA 费或压采购价；`B0CNRH4GRJ` 现有 C/FBM-4PX 为 22.3%，当前路径淘汰。
- 发现采购卡展示口径风险：卡片当前读取 `C-采购前可用毛利RMB / C-采购前毛利率% / C-物流成本RMB / C-货运比`，但同时展示 `三方案推荐履约`，会出现“推荐 FBA 经济线但展示 C/FBM 数字”的错位。后续采购卡应按推荐履约映射展示对应 A/B/C 字段，或直接展示三方案对比。
- 用户追问 `B0D1CLBFD9`、`B0CNRH4GRJ` 为什么没有 FBA 费和尺寸；复查发现 Sorftime `ProductRequest` 返回 `FbaFee=-1`，不是可用 FBA fee，且 `B0CNRH4GRJ` 的 Sorftime `Size=null`。已补充候选表：`B0D1CLBFD9` 用 Sorftime `Size=27,12,5.5cm`、`Weight=210g`，按 Amazon Europe 2026 标准 FBA Small Parcel 1 + CEP德国费率 + 1.5% fuel/logistics surcharge 补算 `FBA€=3.19`；`B0CNRH4GRJ` 用 Amazon 详情页 `Verpackungsabmessungen=20.6 x 16.41 x 5.31cm; 150g` 补算 `FBA€=3.14`。
- 已写回并读回确认：两条 `包装尺寸`、`商品重量g`、`FBA€`、A/B/C采购前与采购后毛利字段、`三方案推荐履约=FBA头程-经济线`、`数据缺口=["认证"]` 均已更新。补完后 `B0D1CLBFD9` A/FBA经济线采购后毛利率 `41.3%`，`B0CNRH4GRJ` A/FBA经济线采购后毛利率 `32.8%`。
- 用户追问采购卡是否已改成“三渠道对比”；复查确认旧卡仍只展示一组 `建议履约 / 采购前空间 / 物流成本 / 货运比`，且这组数字来自 C/FBM 字段，存在口径错位。
- 已升级 `app/amz_procurement_quote.py`：候选表读取增加 `FBA€`、`佣金€` 以及 A/B/C 三套 `采购前可用毛利 / 采购前毛利率 / 物流成本 / 货运比 / 采购后毛利 / 采购后毛利率` 字段；卡片每个产品直接展示 `三渠道对比`，A=FBA经济线、B=FBA快速线、C=FBM-4PX，并标注推荐方案。
- 已补 `tests/test_amz_procurement_quote.py` 和 `scripts/amz_procurement_card_selftest.py`，本地验证通过：`17 tests OK`、`py_compile` 通过、selftest 覆盖 Amazon Listing 按钮、主图按钮、候选表按钮、三渠道展示、成本/链接/备注输入、form_submit payload、回调写表、原卡 PATCH。
