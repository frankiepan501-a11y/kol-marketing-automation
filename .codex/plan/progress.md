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
- 本轮 P0 灰度已从“采购群发送”改为“采购部人员 union_id 点对点发送”：采购群 `oc_73d455d69842f2104da68201dc282677` 发送失败，飞书返回 `230002 Bot/User can NOT be out of the chat`，原因是亚马逊助手 bot 不在该群。
- 已按采购部实时通讯录结果灰度给 2 名采购专员，`mode=commit`、`frankie_only=false`、`card_selftest=passed`、4 个主图和 4 个 Listing 链接均已嵌入，返回消息：
  - `om_x100b6928d28768b0c1f80bf9323d663`
  - `om_x100b6928d2a91ca0dd11dc43cf24e7b`
- 灰度证据文件：`D:\Documents\AI知识库\.codex_tmp\amz_procurement_gray_p0_commit_union_20260723.json`；群发送失败证据：`D:\Documents\AI知识库\.codex_tmp\amz_procurement_gray_p0_commit_instrumented_20260723.json`。
- 已进入“带采购成本的毛利重算/候选排序”P2：五站点、双店铺、三渠道补齐后，生成推进清单 `D:\Documents\AI知识库\.codex_tmp\four_asin_p2_action_list_20260723.md` 和 CSV。
- 已回写德国站候选表四条 P0 样品记录，补齐 A/B/C 三渠道德国站中企号毛利、物流成本、货运比、FBA费、状态和下一步动作；回读验证通过：
  - `B0CH1817WW`：`待合规核查 / 50件验证 / 财务通过`，A/B/C 毛利率 `56.3% / 55.7% / 53.2%`。
  - `B0D1CLBFD9`：`待合规核查 / 50件验证 / 财务通过`，A/B/C 毛利率 `41.5% / 36.9% / 28.7%`。
  - `B0CSCXSHPQ`：`待财务复核 / 50件验证 / 财务暂缓`，A/B/C 毛利率 `30.6% / 24.1% / 14.1%`。
  - `B0CNRH4GRJ`：`暂缓 / 暂缓 / 财务暂缓`，A/B/C 毛利率 `28.8% / 23.1% / 12.5%`。
- 已替换四条记录旧人审备注中的“欧洲卖家占优单独淘汰”口径，改为“毛利、货运比、供应同款确认、合规/型号适配”综合判断。
- 用户截图确认采购专员端灰度卡片已显示 `[AMZ·P0] 德国站采购成本回填 · 已全部回填`；卡片真实飞书渲染中可见商品主图、Amazon Listing 链接、主图原图链接、候选表记录链接、三渠道对比和采购已回填结果态。P0 采购卡片可作为后续正式工作流节点复用，但每批正式发送前仍必须跑 selftest + Frankie-only 样卡确认。
- 项目节点总览已沉淀到 `D:\Documents\AI知识库\.planning\2026-07-22-germany-light-category-pool\project_flow_status_20260723.md`，用于后续从“候选记录→发采购卡→回填采购价→自动重算→自动排序→发合规卡→50件验证卡”接入正式流程。
- 已新增德国站 P0 合规/型号适配核查卡代码闭环，默认只发两条继续推进记录：`B0CH1817WW / recvq1QtafnVjX`、`B0D1CLBFD9 / recvq1QtUEEcXv`。
- 新模块 `app/amz_compliance_fit_card.py` 已实现：卡片展示产品主图、Amazon Listing、主图原图、候选表记录、1688供应商链接、三渠道毛利、型号适配/GPSR/品牌词核查重点；每个产品独立提交 `Go / 需整改 / No-Go`、`IP/外观风险`、核查备注。
- 回调已接入 `app/amz_assistant.py`：`value.action=amz_fit_check_submit` 进入合规卡处理器；仍复用 `/amz/feishu/callback`，保持发卡 App 和 PATCH App 一致。
- 已新增受保护发送 endpoint：`POST /cs/amz-compliance-fit/send`，默认 `mode=dry_run`、`frankie_only=true`，灰度前必须先 Frankie-only 样卡确认。
- 合规卡写回使用候选表已有字段，不新增字段：`合规闸结论`、`IP/外观风险`、`侵权风险说明`、`当前状态`、`综合结论`、`数据缺口`、`下一步动作`、`人审备注`。
- 本地验证通过：`py_compile`、`scripts/amz_compliance_fit_card_selftest.py`、`test_amz_compliance_fit_card.py` 9 tests、采购卡回归 17 tests、差评审计回归 18 tests。`unittest discover`/直接跑测试文件会被旧 `C:\tmp\ml-data-sync\app` 包污染，需用 inline runner 强制当前仓库优先。
- 交接文档已写入 `docs/handoff_amz_compliance_fit_card_p0_2026-07-23.md`。
- 已提交并推送 `d6bb568 feat: add AMZ compliance fit card` 到 `master`，Zeabur deployment `6a61fc479cfc4cd5e689838e` 已 `RUNNING`，线上 `/health=ok`，`/openapi.json` 已暴露 `/cs/amz-compliance-fit/send` 和 `/amz/feishu/callback`。
- 已用 Zeabur 当前环境变量里的 `INTERNAL_TOKEN` 做受保护线上 dry-run，返回 `ok=true`、`count=2`、`card_selftest=passed`；生成卡结构确认包含 2 个独立表单、4 个结果/风险下拉、2 个备注输入、`amz_fit_check_submit`、Listing 链接和三渠道毛利。
- 已真实发送 Frankie-only 合规/适配核查样卡，`message_id=om_x100b692b9e03c0a4df9d31f797d0b99`，commit 模式上传并嵌入 2 张产品图；飞书消息读回确认 `msg_type=interactive`，可见产品图片、Listing/主图/候选表/1688 按钮和三渠道毛利文本。
- 飞书读回 API 对 interactive card 返回的是简化 card body，未正常展开 form 节点；但线上 dry-run 和本地 selftest 均已确认生成卡含活跃表单。下一步需要 Frankie 在样卡上点击 1 个产品做真实回调测试，再读回候选表与原卡 PATCH 状态。
- 2026-07-24 用户纠正合规/适配节点边界：卡片不应让采购/运营人工核查 `Go/No-Go`、IP/外观/专利风险；应由自动化先扫描风险，再把系统发现的问题点、证据和建议动作反馈给运营处理例外。
- 已按该纠正改造 `app/amz_compliance_fit_card.py`：旧人工动作 `amz_fit_check_submit` 已停用并返回提示；新动作为 `amz_fit_check_feedback_submit`。卡片展示自动风险扫描结果、自动发现的问题点、证据、系统建议和处理动作，不再展示 `fit_result_*` / `fit_iprisk_*` / `确认核查本产品`。
- 自动扫描 P0 当前覆盖：兼容品牌词、原厂/官方/正版/OEM 暗示、型号/套装资料缺口、1688供应商资料缺口、知名品牌耗材外观/专利线索、EU/GPSR基础资料、限制类关键词。该扫描是业务风险线索，不是法律结论。
- 新回调写回由系统扫描决定：`采纳系统建议，自动进入下一步` 才按自动判断写 `Go/暂缓/No-Go`；`系统判断有误，退回复核`、`资料不够，采购补资料`、`风险较高，升级合规复核` 都写为暂缓并保留自动问题清单到 `侵权风险说明`。旧动作名仍保留回调兼容。
- 本地验证通过：`py_compile`、`scripts/amz_compliance_fit_card_selftest.py`、`test_amz_compliance_fit_card.py` 12 tests、采购卡回归 17 tests、AMZ审计回归 18 tests。
- 已提交并推送 `4bc7c1d fix: make AMZ compliance card automated risk feedback` 到 `master`，Zeabur deployment `6a624de99cfc4cd5e689957e` 已 `RUNNING`，线上 `/health=ok`。
- 已跑线上受保护 dry-run：`ok=true`、`count=2`、`card_selftest=passed`、2 个独立异常处理表单、2 个 action 下拉、2 个备注输入、`amz_fit_check_feedback_submit`，且不含旧 `fit_result_*` / `fit_iprisk_*` / `确认核查本产品`。
- 已发送新的 Frankie-only 自动风险扫描结果样卡，`message_id=om_x100b6910aa1d9ca0ded8a5f95a39ce0`，读回确认为 `msg_type=interactive`，含自动风险扫描结果、自动发现的问题点、2 张产品图、8 个业务按钮，且不含旧人工风险字段。2026-07-24 用户指出下拉动作名不清晰；已改为业务动作文案，下一张样卡需验证“怎么选”说明和新动作名渲染。
- 2026-07-24 已完成合规/适配卡动作清晰化：卡片顶部新增“怎么选”，四个动作改为 `采纳系统建议，自动进入下一步` / `系统判断有误，退回复核` / `资料不够，采购补资料` / `风险较高，升级合规复核`，并在卡内说明点选后的系统写回结果。旧动作名仍作为回调别名兼容已发旧卡，但新卡不展示旧文案。
- 本地验证通过：`py_compile`、`scripts/amz_compliance_fit_card_selftest.py`、`test_amz_compliance_fit_card.py` 12 tests、采购卡回归 17 tests、AMZ审计回归 18 tests、`git diff --check` 无错误。
- 已提交并推送 `2c5902a fix: clarify AMZ compliance card actions` 到 `master`，Zeabur deployment `6a62d3389cfc4cd5e6899e36` 已 `RUNNING`，线上 `/health=ok`。
- 已跑线上受保护 dry-run：`ok=true`、`count=2`、`card_selftest=passed`；生成卡 JSON 确认包含“怎么选”和四个新动作，且不含旧 `确认系统建议`、`处理系统建议`、`fit_result_*`。
- 已发送新的 Frankie-only 清晰化样卡，`message_id=om_x100b69190ff3a8b4c4cdbdacbd8da8c`；飞书消息读回确认 `msg_type=interactive`，含“怎么选”和四个新动作，旧文案和旧人工控件均不存在。下一步需要 Frankie 在这张新卡上点一个产品做真实回调写回测试，再读候选表和原卡 PATCH 状态。
- 2026-07-24 Frankie 已在清晰化样卡点击 `B0CH1817WW / recvq1QtafnVjX` 的 `采纳系统建议，自动进入下一步`；候选表读回确认 `合规闸结论=暂缓`、`IP/外观风险=中`、`当前状态=待合规核查`、`综合结论=暂缓`、`数据缺口=认证`、`下一步动作=按自动风险点补资料/改文案后重扫`，`人审备注` 含处理动作，`侵权风险说明` 已写入自动问题清单。
- 原飞书卡 `om_x100b69190ff3a8b4c4cdbdacbd8da8c` 读回确认 `msg_type=interactive`、`updated=true`、含 `自动风险处理已完成`；同卡第二条 `B0D1CLBFD9 / recvq1QtUEEcXv` 仍为 `合规闸结论=待核`，说明单产品回调隔离正常。
- 2026-07-24 用户再次校正铺货节奏：铺货/精铺优先速度和数量，风险分 `<=60` 的普通问题点不应再发人工卡片提醒；系统应直接写 `Go / 待50件验证`，并把注意点留在 `侵权风险说明`。
- 已修改合规/适配发送分流：`AMZ_COMPLIANCE_FAST_PASS_SCORE=60`；`<=60` 自动通过并写候选表，`>60` 或硬风险才进入飞书例外处理卡。旧已发卡上的“采纳系统建议”回调也会按新阈值写 `Go`。
- 已提交并推送 `5982587 fix: fast-pass AMZ puhuo compliance scan`，Zeabur deployment `6a62e5599cfc4cd5e689a0d2` 已 `RUNNING`，线上 `/health=ok`。
- 已跑线上受保护 dry-run：两条 P0 记录均 `60分 / 中风险 / auto_pass_with_notes`，`card_count=0`，不生成新卡；commit 后返回 `sent=false`、`message_ids=[]`、`auto_written_record_ids=[recvq1QtafnVjX,recvq1QtUEEcXv]`。
- 候选表读回确认 `B0CH1817WW` 和 `B0D1CLBFD9` 均已写成 `合规闸结论=Go`、`当前状态=待50件验证`、`综合结论=50件验证`、`下一步动作=发起50件验证`，`侵权风险说明` 保留品牌/IP、外观/专利线索、EU/GPSR 注意点。
- 2026-07-24 进入 50件验证节点前，先读候选表字段结构：已存在专用字段 `50件验证状态`，选项为 `未开始 / 进行中 / 已通过 / 未通过`；无需新增字段或写脏选项。
- 已新增 `app/amz_validation50.py` 和受保护 endpoint `POST /cs/amz-validation50/start`。节点进入条件：`合规闸结论=Go`、`当前状态=待50件验证`、`综合结论=50件验证`、`采购回填状态=已回填`、有采购成本和 1688 供应商链接、且 `50件验证状态` 不是 `进行中/已通过`。
- 50件验证启动卡只做启动交接，不含人工审批表单；展示产品图、Amazon Listing、主图原图、候选表、1688供应商、A/B/C 三渠道经济性、50件采购+物流粗算、验证要看和系统注意点。
- 本地验证通过：`py_compile app\amz_validation50.py app\main.py`、`tests.test_amz_validation50` 5 tests、`scripts\amz_validation50_selftest.py`、采购卡 17 tests、合规卡 14 tests、AMZ 差评审计 18 tests、合规卡 selftest。
- 交接文档已写入 `docs/handoff_amz_validation50_p0_2026-07-24.md`。下一步是提交推送、线上 dry-run，然后 commit 启动 `recvq1QtafnVjX` 和 `recvq1QtUEEcXv` 的 50件验证。
- 已提交并推送 `7037eb7 feat: add AMZ 50-unit validation start node` 到 `master`；Zeabur deployment `6a62ef2d9cfc4cd5e689a206` 已 `RUNNING`，线上 `/health=ok`。
- 线上受保护 dry-run 通过：`eligible_count=2`、`skipped_count=0`、`card_selftest=passed`、`image_url_count=2`、`listing_url_count=2`、`would_update_count=2`。
- 已执行线上 commit：`updated_record_ids=recvq1QtafnVjX,recvq1QtUEEcXv`，`sent=true`，Frankie-only 启动卡 `message_id=om_x100b691ab7d71ca0dfd33119ed4643b`，图片嵌入数 `2/2`。
- 候选表回读确认两条均为 `当前状态=待50件验证`、`综合结论=50件验证`、`下一步动作=发起50件验证`、`50件验证状态=进行中`，`人审备注` 已追加 `进入50件验证` 批次记录。
- 飞书消息读回确认 `msg_type=interactive`，卡内包含 `B0CH1817WW`、`B0D1CLBFD9`、`50件验证要看`、`打开1688供应商`、图片 key，且不含 `form_submit`。
- 2026-07-24 用户重新定义项目大阶段：亚马逊欧洲铺货/精铺工作流拆为 `选品阶段 -> 采购阶段 -> Listing上架阶段 -> 运营与监控阶段`。本 session 实际完成的是选品阶段的自动化样板验证：类目定位、候选 ASIN 采集、选品闸门筛选、成本与毛利试算、采购成本回填、自动合规/适配扫描。
- 2026-07-24 流程纠偏：旧 `50件验证` 节点位置错误。当前还未采购、到货、上架，不能称为真实验证；应改为选品阶段收尾的 `小批量采购建议`。该建议应根据竞品/月销、类目容量、毛利、货运比、MOQ、装箱倍数和风险等级动态计算首批采购量，不固定 50 件。
- 2026-07-24 选品阶段收尾任务：生成 `选品结果确认表/卡` 给运营和上司确认，内容包括候选产品、站点、图片/Listing、采购价、三渠道毛利、货运比、中企号/本土号差异、合规注意点、推荐采购量、淘汰/暂缓原因。确认通过后才触发采购阶段；真正验证放在 Listing 上架和库存可售后，用 7/14/30 天运营数据判断扩采、改款或淘汰。
- 2026-07-24 已按用户新增要求输出 `选品结果确认表/卡` 需求稿：`C:\Users\Administrator\workspace\outputs\sop\2026-07-24-amz-eu-selection-result-confirmation-card.md`。关键口径：每站展示竞品售价和建议售价；每站分别计算建议采购量并汇总总采购量；采购量基于 `竞品平均月销量 × 60% + 类目新品平均月销量 × 40%`、新品入场系数和覆盖天数动态计算；按国民哥哥回款比模型展示回款、投入、预留押款和投入回收率；四个动作按钮为 `Go / 条件推进 / 暂缓 / 淘汰`，点击后只改变选品确认状态并触发对应下一步，不能绕过采购阶段。
- 2026-07-24 已完成 `选品结果确认卡` 本地 P0 实现：新增 `app/amz_selection_confirmation.py`、接口 `POST /cs/amz-selection-confirmation/send`、飞书回调路由 `amz_selection_`、自测 `scripts/amz_selection_confirmation_selftest.py`、单测 `tests/test_amz_selection_confirmation.py`，交接文档 `docs/handoff_amz_selection_confirmation_p0_2026-07-24.md`。卡片展示五站竞品售价/建议售价/建议采购量、三渠道毛利、回款/投入分析、合规注意点，并用 `Go / 条件推进 / 暂缓 / 淘汰` 四个按钮写回候选表已有状态字段。
- 本地验证通过：`py_compile app\amz_selection_confirmation.py app\amz_assistant.py app\main.py`、`tests\test_amz_selection_confirmation.py` 4 tests、`scripts\amz_selection_confirmation_selftest.py`。selftest 确认含图片、Listing、候选表、1688链接、四个决策按钮，样例月销下建议采购总量为 20 件；按钮写回语义分别为待采购确认/待采购复核/暂缓/淘汰。相邻回归也通过：采购卡 17 tests、合规卡 14 tests、旧 50 件节点 5 tests。直接执行旧测试文件会被 `C:\tmp\ml-data-sync\app` 路径污染，需用 inline runner 或给测试文件补 repo-root `sys.path`。
- 当前生产数据边界：候选表已有采购价、供应商链接、三渠道毛利、FBA费/佣金、合规注意点；但五站竞品均价/中位价、竞品平均月销量、类目新品平均月销量、本土号毛利率仍未保证结构化入表。P0 代码不会硬猜月销量，字段缺失时卡片显示 `需补月销`，不能作为最终下单数量。

## 2026-07-28

- 用户截图指出 `选品结果确认卡` 的 UK / FR / IT / ES 行没有数据显示。复查确认不是飞书渲染问题，而是卡片只读飞书候选表字段，未接入此前五站双账号三渠道毛利重算快照。
- 已新增快照数据文件 `data/amz_selection/four_asin_5site_margin_snapshot_20260723.json`，覆盖四个 ASIN × DE/UK/FR/IT/ES 的售价和中企号/本本号最佳毛利。
- 已修复 `app/amz_selection_confirmation.py`：
  - 候选表字段优先；候选表缺站点售价/毛利时，用五站快照兜底。
  - 缺竞品月销/类目新品月销时展示 `待补`，不再显示空白。
  - `data_gap_count` 改为统计售价、采购量和毛利结构化缺口，避免“缺数据但计数为 0”。
- 已补 `tests/test_amz_selection_confirmation.py` 回归测试：模拟 UK/FR/IT/ES 候选表字段缺失，必须展示快照中的 `£28.45`、中企/本本号最佳毛利，并把月销标成 `待补`。
- 本地验证通过：`py_compile`、`tests/test_amz_selection_confirmation.py` 5 tests OK、`scripts/amz_selection_confirmation_selftest.py`、`git diff --check` 仅 CRLF 提示。
- 首次线上 dry-run 仍显示非 DE 站点空白；二次定位发现 Dockerfile 只 `COPY app/`，未把新增 `data/` 目录打入镜像。已补 `COPY data/ /app/data/`，与代码默认快照路径 `/app/data/...` 对齐。
- 当前真实缺口：UK / FR / IT / ES 的竞品平均月销量和类目新品平均月销量还没有结构化数据源；后续应通过 Sorftime/SellerSprite 补写候选表，才能自动算各站建议采购量。
- 用户指出卡片里竞品月销数据不正确/缺失，已暂停继续发新卡，先补清楚月销口径。
- 已用 Sorftime `CategoryProducts` 按 4 个 ASIN × 5 站的 BSR 叶子类目拉取 page 1-2（每页 100 个热销品），并在本地按吸尘器配件关键词、价格带和型号词过滤，生成月销快照 `data/amz_selection/four_asin_5site_sales_metrics_20260728.json`。
- 已额外验证 `AsinSalesVolume`：20 个 ASIN×站点请求中除 `B0CH1817WW/IT` 可从类目页命中 8 件月销外，其余样本 ASIN 直接销量均返回空；因此卡片不能硬写样本 ASIN 月销，必须标注“不可用/未命中”。
- 已把卡片月销展示拆成：`样本月销`、`产品级竞品中位/均值/n`、`类目新品中位/n`、`参考月销`、`数据质量`、`数据来源说明`。采购量计算优先用产品级竞品中位数；产品级样本不足时标注并回退到类目可比竞品中位数。
- 本地验证通过：`py_compile app/amz_selection_confirmation.py app/amz_assistant.py app/main.py`、`tests/test_amz_selection_confirmation.py` 5 tests OK、`scripts/amz_selection_confirmation_selftest.py` 返回 `card_selftest=passed`。
- 线上首次 dry-run 发现卡片虽然显示 `产品级高/低`，但没有显式标注字段名 `数据质量`；已提交 `f349a60 fix: label AMZ selection sales data quality`，把站点月销行改为 `数据质量 产品级高/低/类目级可用`，并纳入卡片自测必检项。
- 抽取线上 dry-run 文本后发现候选表旧字段会污染 Sorftime 月销快照：例如 `产品竞品中位 741/均值 5/n=1`、`参考月销 655`。根因是 DE 站 `_site_value` 会回退读取候选表泛字段 `月销量/平均月销量`。已提交 `34e395e fix: prioritize AMZ selection sales snapshot`，选品确认卡优先使用本次 Sorftime 快照的月销字段；产品级样本数低于 3 时，展示产品级样本，但采购量用类目可比竞品回退。
- 线上验证已通过：Zeabur 最新部署 `34e395e` 为 `RUNNING`，`/health=ok`，受保护 dry-run 返回 `count=4`、`card_selftest=passed`、`suggested_total_qty=225`、`data_gap_count=5`。必检文本均存在：`样本月销`、`产品竞品中位`、`类目新品中位`、`参考月销`、`数据质量`、`月销取Sorftime快照`、`产品级样本不足，采购量用类目可比竞品`；旧错误片段均不存在。
- 已发送新的 Frankie-only 选品结果确认样卡，批次 `AMZ-EU-SELCONF-20260728-P2`，`message_id=om_x100b69bcbcd29ca8df9eac1fc15f3d0`。上一张 `om_x100b69bca7985ca4c23a6af74efc307` 是中间版，不作为最终确认卡。
- 2026-07-28 用户指出 `产品竞品中位/均值/n=1` 和 `类目新品中位/n=25` 对运营不可读。已提交 `33d9287 fix: clarify AMZ selection sales wording`：卡片改为 `近似竞品月销：中位 X，均值 Y，样本 Z 个`、`类目新品月销：中位 X，样本 Z 个`、`数据可信度`，并移除 `n=`。线上 dry-run 和飞书读回均确认新文案存在、旧文案不存在；新版 Frankie-only 样卡 `message_id=om_x100b69bea73bc8b0c4c29f5ece39993`。
- 2026-07-28 用户继续反馈站点前缺国旗/站点简称，且站点月销段落仍过密。已提交 `a64223c fix: improve AMZ selection card readability`：五站增加 `🇩🇪 DE / 🇬🇧 UK / 🇫🇷 FR / 🇮🇹 IT / 🇪🇸 ES`，站点信息由一段长文本改为两栏小块：`采购结论`（建议采购、竞品价、建议价、数据可信度）+ `月销依据`（样本ASIN、近似竞品、类目新品、参考月销），下方单独列 `毛利` 和 `说明`。线上 dry-run 校验国旗、分块字段均存在，旧长段落和 `n=` 均不存在；新版 Frankie-only 样卡 `message_id=om_x100b69beb5d7eca4c0b8885eef24f3a`。
- 2026-07-28 用户指出 `Go / 条件推进 / 暂缓 / 淘汰` 四个按钮虽然能点击，但反馈和作用不够清晰，运营不知道各自代表什么、点后会进入哪个流程。已修改 `app/amz_selection_confirmation.py`：顶部说明改为“运营要怎么点”，按钮改为 `通过入采购 / 条件采购复核 / 暂缓补资料 / 淘汰归档`，底层 `value.action` 不变；未处理产品按钮区前新增“只点一个按钮，点后写回候选表并更新原卡”；点击成功 toast 改为 `已写回：<结论>；下一步：<下一步动作>`；已处理状态显示确认动作、后续流向和“无需重复点击”。本地验证通过：`py_compile`、`tests/test_amz_selection_confirmation.py` 5 tests OK、`scripts/amz_selection_confirmation_selftest.py`；已提交并推送 `2041f1b fix: clarify AMZ selection decision buttons` 到 `master`。线上公开检查：`https://kol-auto.zeabur.app/health` 返回 `ok`，`openapi.json` 仍暴露 `/cs/amz-selection-confirmation/send`；但本机 `ZEABUR_API_KEY` 查询 deployments 返回 401，且无 `INTERNAL_TOKEN`，所以尚未完成线上 commit 级验证和新版 Frankie-only 样卡发送。
- 已通过已授权 n8n 工作流通道恢复线上受保护 endpoint 调用，并完成 `选品结果确认卡` 按钮清晰化的线上验证。dry-run 返回 `ok=true`、`count=4`、`card_selftest=passed`、`has_buy_button=true`、`has_conditional_button=true`、`has_hold_button=true`、`has_reject_button=true`、`old_plain_go_button_absent=true`、`contains_action_payloads=true`，确认新版按钮文案和 action payload 同时存在。
- 已发送新版 Frankie-only 样卡：批次 `AMZ-EU-SELCONF-20260728-P3-BUTTONS`，`message_id=om_x100b69b95ad0b0acde3f08182cef22b`。飞书读回确认 `msg_type=interactive`、发卡方为亚马逊助手 App，卡片标题为 `🟡 [AMZ·P0] 欧洲站选品结果确认 · 待确认 4/4`，包含“运营要怎么点”、四个新按钮、4 张产品图、Listing/主图/候选记录/1688 链接和每个产品的“请选择本产品的最终处理动作”。按钮尚未点击，候选表状态尚未被这张新卡改写。

## 2026-07-29

- Frankie 已在新版样卡 `om_x100b69b95ad0b0acde3f08182cef22b` 点击 `B0CH1817WW / recvq1QtafnVjX` 的 `通过入采购`。飞书读回确认 `msg_type=interactive`、`updated=true`、标题变为 `待确认 3/4`，该产品卡内显示 `选品确认已处理 / 确认动作：Go / 后续流向：进入采购阶段`，其余 3 个产品仍保留按钮。
- 候选表读回确认 `B0CH1817WW` 已写成 `当前状态=待采购确认`、`综合结论=Go`、`下一步动作=进入采购阶段：采购复核MOQ/交期/同款后下单`，`人审备注` 追加 `2026-07-29 00:21 采购回填: 选品结果确认=Go; 系统建议=Go; 建议采购总量=150件`。`B0D1CLBFD9 / B0CSCXSHPQ / B0CNRH4GRJ` 未被本次点击改写，说明单产品动作隔离正常。
- 回调真实点击中发现小问题：原卡 PATCH 后批次显示回落到默认 `AMZ-EU-SELCONF-20260724-P0`，原因是按钮 payload 未携带 `build_selection_confirmation_card()` 当前批次，只从候选行字段或默认值取批次。已修复 `_payload/_button/_product_elements`，后续新卡按钮会保留当前批次；本地验证通过 `py_compile`、`tests/test_amz_selection_confirmation.py` 5 tests、`scripts/amz_selection_confirmation_selftest.py`，selftest 已新增 `contains_batch_payload=true`。
- 二次发卡前发现新卡只从 `选品确认状态` 字段判断已处理，旧点击只写入 `当前状态/综合结论/人审备注`，导致重新发卡时已通过产品会重新出现按钮。已补兼容读取：当 `选品确认状态` 为空时，从 `人审备注` 的 `选品结果确认=Go/条件推进/暂缓/淘汰` 读取最后一次决策；新增回归测试 `test_review_note_marks_previously_clicked_product_processed`。本地验证通过：`py_compile`、`tests/test_amz_selection_confirmation.py` 6 tests、`scripts/amz_selection_confirmation_selftest.py`。
- 已提交并推送 `7ef7fb3 fix: keep AMZ selection decisions across card resends`。线上 dry-run 用批次 `AMZ-EU-SELCONF-20260729-P4-BATCHFIX` 验证通过：`ok=true`、`count=4`、`card_selftest=passed`、旧默认批次不再出现、待确认按钮 payload 中含 12 个新批次、`B0CH1817WW` 已显示已处理。
- 已发送新的 Frankie-only 选品结果确认样卡：`message_id=om_x100b69b9819bf8a0c3b1940d247b8e5`，批次 `AMZ-EU-SELCONF-20260729-P4-BATCHFIX`。飞书回读确认发卡方为亚马逊助手 App `cli_aac5d3d3e8f91cc6`，`msg_type=interactive`，标题为 `待确认 3/4`；第一条 `B0CH1817WW` 已关闭按钮并显示 `选品确认已处理 / Go`，剩余 `B0D1CLBFD9 / B0CSCXSHPQ / B0CNRH4GRJ` 仍保留 `通过入采购 / 条件采购复核 / 暂缓补资料 / 淘汰归档` 四个按钮。
- Frankie 已在新样卡点击 `B0D1CLBFD9 / recvq1QtUEEcXv` 的 `通过入采购`。飞书回读确认原卡已 PATCH 为 `待确认 2/4`、该产品显示 `选品确认已处理 / Go`；候选表回读确认 `当前状态=待采购确认`、`综合结论=Go`、`下一步动作=进入采购阶段：采购复核MOQ/交期/同款后下单`，`人审备注` 已追加批次 `AMZ-EU-SELCONF-20260729-P4-BATCHFIX`。
- 成本口径澄清：选品确认卡里的 `采购成本RMB` 为单套/单个 Amazon 售卖单位采购价，不按套装件数再次相乘；`套装件数` 是每套 listing 内含配件数量；三渠道毛利同样是卖出 1 套 listing 的单套毛利。为避免误读，已把卡片字段改为 `采购成本（单套）`、`套装件数（每套内含）`，回款分析改为 `单套采购+物流投入`；本地验证通过 `py_compile`、`tests/test_amz_selection_confirmation.py` 6 tests、`scripts/amz_selection_confirmation_selftest.py`。
- 2026-07-29 用户指出 `B0CSCXSHPQ` 的 `系统建议=条件推进` 与按钮 `条件采购复核` 文案不完全一致，且蓝色主按钮仍默认是 `通过入采购`，会导致运营不知道该点哪个。已修正：按钮改为 `Go｜通过入采购 / 条件推进｜采购复核 / 暂缓｜补资料 / 淘汰｜归档`，待处理产品增加 `系统建议：X；建议优先点【对应按钮】`，且蓝色主按钮跟随系统建议。新增单测验证 `系统建议=条件推进` 时 `条件推进｜采购复核` 为 primary、`Go｜通过入采购` 为 default；本地验证通过 `py_compile`、`tests/test_amz_selection_confirmation.py` 7 tests、`scripts/amz_selection_confirmation_selftest.py`。
- 已提交并推送 `f0551f9 fix: align AMZ selection recommendation buttons` 到 `master`。线上受保护 dry-run 批次 `AMZ-EU-SELCONF-20260729-P4-BUTTONALIGN` 返回新版文案；结构化解析确认 `B0CSCXSHPQ / recvq1QtFKPwoI` 四个按钮为：`Go｜通过入采购` default、`条件推进｜采购复核` primary、`暂缓｜补资料` default、`淘汰｜归档` danger。
- 选品确认卡批次 `AMZ-EU-SELCONF-20260729-P4-BATCHFIX` 已全部确认。飞书原卡 `om_x100b69b9819bf8a0c3b1940d247b8e5` 读回为 `已全部确认`；四条候选表写回结果为：`B0CH1817WW=Go/待采购确认/建议采购150件`，`B0D1CLBFD9=Go/待采购确认/建议采购60件`，`B0CSCXSHPQ=条件推进/待采购复核/建议采购15件`，`B0CNRH4GRJ=暂缓/建议采购0件`。本批选品阶段收尾已完成，下一步进入采购阶段触发清单：2 条直接采购复核，1 条条件采购复核，1 条暂缓归档待补资料。
- 2026-07-29 用户选择 B 路径：先做 Frankie-only “采购阶段预览卡”，确认采购阶段口径后再发采购部。已新增 `app/amz_procurement_preview.py`，接入受保护 endpoint `POST /cs/amz-procurement-preview/send`；卡片只读，不写候选表、不写采购阶段触发表、不发采购部，不含 form 或回调按钮。
- 采购阶段预览卡展示：选品确认结论、采购阶段动作、建议采购总量、单套采购成本、推荐履约、包装尺寸、重量、套装件数、FBA配送费/佣金、三渠道毛利、采购部下一步、采购条件/注意、合规/适配留档、Amazon Listing/主图/候选表/1688 链接。四条默认记录按当前确认结果分流为：2 条 `直接入采购`、1 条 `条件采购复核`、1 条 `暂缓不发采购`。
- 已新增 `tests/test_amz_procurement_preview.py` 和 `scripts/amz_procurement_preview_selftest.py`。本地验证通过：`py_compile app\amz_procurement_preview.py app\main.py`、预览卡 3 tests、预览卡 selftest、采购回填卡 17 tests、旧 50 件节点 5 tests、选品确认卡 7 tests、选品确认卡 selftest、`git diff --check`。注意：本机裸 `python` 不在 PATH，且直接 discover/执行部分旧测试会被 `C:\tmp\ml-data-sync\app` 同名包污染；本轮用 `C:\tmp\py311-embed\python.exe` 和 importlib 强制当前仓库路径验证。
- 采购阶段预览卡已线上 commit 发送 Frankie-only：批次 AMZ-EU-PROCPREV-20260729-P0，message_id=om_x100b69acc2efc0a4c1bb15cbf05f6a3，线上 dry-run 先确认 count=4、card_selftest=passed、would_write_count=0、would_send_to_procurement=false、无 form/回调按钮；commit 返回 sent=true，4 张图片上传，分流为 2 条直接入采购、1 条条件采购复核、1 条暂缓不发采购。
- 真实回读 `om_x100b69acc2efc0a4c1bb15cbf05f6a3` 时发现通用三渠道推荐标记 bug：`_recommended_suffix()` 把字母 `B` 误匹配到 `FBA头程-经济线` 里的 `B`，导致 B 快速线也显示 `（推荐）`。已修复为只按完整渠道名/别名匹配，字母 A/B/C 仅在履约字段完全等于该字母时生效；预览卡 selftest 和采购回填卡单测均新增 `B FBA快速线（推荐）` 不得出现的断言。旧卡不作为最终确认依据，需重发修正版预览卡。
- 已修复推荐渠道误标后重发采购阶段预览卡：批次 `AMZ-EU-PROCPREV-20260729-P1-FIX`，message_id=`om_x100b69acd91a64a4dedd4a28189ab06`。线上 dry-run 确认 `B FBA快速线（推荐）` 已不存在；飞书读回确认 msg_type=interactive、发卡方=亚马逊助手 App、含图片/Listing/1688/候选表链接、只读说明和 `A FBA经济线（推荐）`，无 B 线误推荐。
- 2026-07-29 已按用户要求单独补 `B0CNRH4GRJ / recvq1Quaar3h2` 的德国站主字段并重算：候选表写回 `售价€=15.19`、`月销量=313`、`月销售额€=4754.47`、`FBA€=3.14`，读回确认成功。这里的 `月销量=313` 是德国站参考月销（Sorftime 近似竞品中位 344 × 60% + 类目新品中位 268 × 40%，不是样本 ASIN 精确月销；样本 ASIN 直接月销接口未命中）。现有中企号三渠道毛利仍为 A/B/C `28.8% / 23.1% / 12.5%`，所以保留 `当前状态=暂缓 / 综合结论=暂缓`，只在人审备注中沉淀“本本号 DE/ES 可作为条件推进备选”。若后续用户确认改用本本号条件推进，建议只覆盖 DE+ES，条件推进采购量约 DE 30 件 + ES 10 件。
- 2026-07-29 Frankie 确认 `B0CNRH4GRJ / recvq1Quaar3h2` 按本本号 VAT 优势推进，已写回候选表：`当前状态=待采购复核`、`综合结论=条件推进`、`下一步动作=条件进入采购阶段：仅 DE 30 件 + ES 10 件，不五站铺开；采购复核 MOQ、同款、套装和供应商报价，条件不满足退回暂缓`，`人审备注` 追加确认记录。财务闸原有中企号缺口不覆盖，避免误读为全店铺类型通过。
- 同步修正采购阶段预览卡口径：正式采购部卡/采购待办只允许展示 `Go` 和 `条件推进` 产品；`暂缓/淘汰` 只作为候选表留档或摘要计数，不生成采购产品区块、不要求采购部操作。`app/amz_procurement_preview.py` 已移除搜索暂缓记录的默认条件，并在卡片顶部补清 `本卡目的 / 采购部收到后要做 / 不会发给采购部 / 下一步`。
- 本地验证通过：`py_compile app\amz_procurement_preview.py app\main.py`、`tests\test_amz_procurement_preview.py` 4 tests OK、`scripts\amz_procurement_preview_selftest.py`。真实 4 条候选 dry-run 返回 `count=4 / source_count=4`、分流 `直接入采购=2 / 条件采购复核=2 / 暂缓不发采购=0 / 淘汰归档=0`，`B0CNRH4GRJ` 已在预览卡中，且 `暂缓不发采购｜` 产品区块不存在。
- 已提交并推送 `791555c fix: make AMZ procurement preview actionable only` 到 `master`。线上 `/health=ok`；Zeabur API Key 仍返回 401，改用 n8n 已授权 AMZ 工作流中的 Bearer 通道调用线上受保护 endpoint。
- 线上 dry-run 批次 `AMZ-EU-PROCPREV-20260729-P2-ACTIONONLY` 通过：`ok=true`、`card_selftest=passed`、`count=4`、`source_count=4`、分流 `直接入采购=2 / 条件采购复核=2 / 暂缓=0 / 淘汰=0`，卡片含 `本卡目的 / 采购部收到后要做 / 不会发给采购部 / 下一步 / B0CNRH4GRJ`，且不含 `暂缓不发采购｜`。
- 已线上 commit 发送 Frankie-only 修正版采购阶段预览卡：`message_id=om_x100b69acb16744a0de32e83f389a046`，`sent=true`、`frankie_only=true`、4 张产品图上传、2 条直接入采购、2 条条件采购复核。默认飞书 App 本地读回该 message 返回 `230002 Bot/User can NOT be out of the chat`，原因是消息由亚马逊助手 App 发送而本机缺该 App 凭据；不影响发送结果，但后续如需读回需用亚马逊助手 App 凭据或线上读回接口。
- 2026-07-29 Frankie 确认采购阶段预览卡口径 OK 后，新增采购部执行版模式：`audience=procurement` + `procurement_approved=true` 才允许越过 Frankie-only 安全闸。执行版标题/文案改为 `欧洲站采购阶段执行清单`、`已确认口径，采购部执行`，不再出现 `待 Frankie 确认/给 Frankie 确认`。
- 本地验证通过：`py_compile app\amz_procurement_preview.py app\main.py`、`tests\test_amz_procurement_preview.py` 7 tests OK、`scripts\amz_procurement_preview_selftest.py`。已提交并推送 `646b517 feat: add AMZ procurement execution card mode` 到 `master`。
- 线上先等 `openapi.json` 出现 `audience/procurement_approved` 参数后再跑正式执行版 dry-run；批次 `AMZ-EU-PROCEXEC-20260729-P0` 返回 `ok=true`、`audience=procurement`、`procurement_approved=true`、`frankie_only=false`、`card_selftest=passed`、`count=4`、分流 `直接入采购=2 / 条件采购复核=2 / 暂缓=0 / 淘汰=0`，且 `would_send_to_procurement=true`、不含 `待 Frankie 确认/给 Frankie 确认/暂缓不发采购｜`。
- 已正式发送采购部执行版卡给 2 名采购专员 union_id（继续不用采购群，因亚马逊助手 bot 不在采购群）：`sent=true`、`recipient_count=2`、4 张产品图上传、4 个可执行产品。message_id：`om_x100b69ad51ecc8a4c2cf958ab1a85ef`、`om_x100b69ad51e208b0dfd3edae0a4e004`。
- 2026-07-29 Frankie 要求撤回发给蔡宗佑的采购部执行版卡，只保留郭嘉美收卡。已新增受保护 endpoint `POST /cs/amz-message/revoke`，由亚马逊助手 App 执行 Feishu message recall；提交并推送 `7679688 feat: add AMZ message revoke endpoint`，线上 `openapi.json` 已暴露新接口。已通过 n8n 已授权 AMZ Bearer 通道调用撤回 `om_x100b69ad51ecc8a4c2cf958ab1a85ef`，飞书返回 `feishu_code=0 / success`。郭嘉美卡片 `om_x100b69ad51e208b0dfd3edae0a4e004` 保留，不重复发送。撤回证据文件：`D:\Documents\AI知识库\.codex_tmp\amz_revoke_cai_procurement_card_20260729.json`。
- 2026-07-29 session 分工已沉淀：本 session 继续跟进 `采购阶段`，当前节点是采购复核/采购执行清单确认；新 session 并行开展 `Listing 上架阶段预备工作`，只做 SOP、字段、资料缺口、合规文案框架和站点级上架准备，不直接创建真实 Listing、不写 Seller Central 生产后台。交接文件：`docs/handoff_amz_eu_procurement_and_listing_parallel_2026-07-29.md`。

## 2026-08-19 KOL 集中宣发 P0 三池修复

- Dave 活动生产名单已从混合的“新开发+旧关系复联”修复为三池隔离；当前 `evidence-v3` 有效名单 20/20 均为新开发池。
- 生产批次 `p0-three-pool-v3-20260819a` 已回读通过：8 人通过、12 人待运营审核；历史对象分流为二次发布 4、现有流程 8、排除 7。
- 代码 commit `bd7af3fff38cd92e333e70572398bb9e9eed6406` 已部署并健康；所有外联和费用授权保持关闭。
- 详细证据见 `docs/launch_evidence_participation_handoff_2026-08-19.md`。

## 2026-08-19 NYXI 全证据补接 + 3 人补位 + G4 测试邮箱

- 用户明确授权执行三项动作，但没有授权真实 KOL 发信；本轮真实发送闸继续关闭。
- 已读取项目规格、参与记录交接、首次开发信 P0 交接、飞书 App 路由和相关技能规则。
- Agent Memory 首次 precheck 带了不支持的 `--session` 参数并报错；已去掉参数重跑。由于预估任务超过默认上下文容量，已把完整执行计划写入 `.codex/plan`，后续每阶段同步进度。
- 当前进入 Phase 1：先回读生产活动、20 条参与记录、NYXI 证据池和测试邮箱环境，不做写入。
- Phase 1 已完成：活动仍只关联 60 条样本证据；20 条当前参与记录中 17 条人工通过、3 条人工排除；所有真实发送、寄样、付费授权均关闭。
- 已新增活动证据索引与覆盖率测试：批量预览只建一次索引，保留旧排序输出；专项 39 项测试通过。
- 已新增大证据集一次性读表校验和排序版本单调递增：现有 `evidence-v3` 下一版固定为 `evidence-v4`。
- NYXI 全证据已生产接入：3,423 条帖子中排除官方 435 条，2,988 条非官方合作证据覆盖 1,457 位作者；采用 50 个有序只读快照节点（前 49 个各 60 条、末块 48 条），数量、连续分片、去重和 SHA-256 回读均通过。
- 活动已原子升级到 `evidence-v4 / 配置版本2`；主表继续保留 60 条代表证据和 7 个营销事件，所有证据节点均为 `允许外部动作=false`。
- 后台全池预览 job `launchpreview-5e7044f8f01d` 成功：14分37秒完成，确认从 `activity_node_snapshot` 读取 2,988 条有效证据、1,457 位作者；返回 495 名可用新开发对象，解决同步点击约 4.6 分钟可能超时的问题。
- 修复补位口径：历史已取消记录保留在防重复集合，但不再算作当前名单流出；复用后台预览前强制核对活动、产品、排序版本、证据数及全部当前有效参与者。
- 3 人补位已提交并回读：IndieAlpaca、La Poción Roja、Mundatos；均为 `已入围 / 待审核 / 新开发 / 新开发池 / evidence-v4`。当前有效名单 20、坏记录 0、关联任务或草稿 0。
- G4 已通过：临时启用 `EMAIL_DRY_RUN_TO=frankiepan501@gmail.com`，只发送 1 封 Dave 测试邮件，Zoho raw 9 项检查全部通过；随后用单变量方式清空该环境变量并重新部署，端点已恢复 HTTP 400 硬拒绝测试发送。
- 最终代码 commit `73826ec30d357d7cdeab964ca63b3ceb86f4ae8f` 已部署；活动专项 113 项和 3 个子测试通过，全仓 333 项中仅 1 项既有 Zeabur watchdog 用例失败，与本次无关。真实 KOL 发信、寄样、付费和储备金授权继续关闭。

## 2026-08-20 IndieAlpaca 首封真实灰度

- Frankie 明确授权选择 `https://www.youtube.com/@IndieAlpaca` 作为本次 Dave 活动的首名真实 KOL 灰度发送对象。
- 执行范围固定为：先回读人工审核、参与状态、邮箱、现有草稿、历史触达和全局重复触达；全部通过后只发送 1 封；发后立即回查 Zoho raw 和飞书写回。其他 KOL、后续跟进、寄样、付费与批量发送不在本次授权范围内。
- 首次用 `lark-cli base +record-get --fields` 读取时，本机 1.0.64 不支持该参数，未产生写入；已改为按本机 help 选择受支持的只读调用。
- 已新增活动单人真实发送端点 `POST /launch/outreach/send-one`：要求 `REAL_ONE_ONLY`、人工审核通过、活动/产品/联系人/名单版本/主页一致、活动锁关闭、Zoho 未暂停、产品为活动专用、全局重复触达通过；同一 nonce 有任何草稿后永不自动重发。
- 单人预检已从全池回放改为定向查询：只读该联系人、同邮箱身份、相关历史草稿与产品别名家族；避免约 4.6 分钟全池计算卡住单封发送。commit `5a8280d`。
- 首次真实请求在建草稿前失败，根因为飞书数字字段 `粉丝数` 在线返回字符串，旧邮件生成器使用数字格式化报错；Zoho 与参与记录回读均确认 0 发件、0 草稿。已在 `enrich._subscriber_count` 统一数值类型，117 项活动测试通过，且只生成不发送实测正文成功。commit `9b66315`。
- 唯一真实邮件已于 2026-08-20 02:28:08（Asia/Taipei）发至 `contact@indiealpa.ca`：草稿 `recvsLHSKGv4tc`，Zoho message ID `1787164088114155100`，主题 `IndieAlpaca, this fits your retro corner`。
- 首轮 raw 返回 8/9：正文已经明确包含 `officially licensed Dave the Diver controller`，但旧规则额外要求出现主关键词剩余 token `Pro`，属于校验误报，不是邮件错误。修复为“IP 名称 + 产品类型”也可确认身份；直接读取同一 Zoho raw 复核为 9/9，正文 raw 597 字符与预期 597 一致，没有补发。commit `1455c60` 已 `RUNNING`。
- 生产回读：草稿 `邮件草稿状态=已发送 / 发送状态=已发`；参与记录关联该唯一草稿；KOL 合作状态保持 `待回复`；新增跟进记录 `recvsLIppE2dOA`；活动 `发送邮件授权=false`；审批意见已回填 `raw=9/9通过` 和 Zoho message ID。
- 本次只创建 1 条活动 cold 草稿、发送 1 封真实邮件；未触发其他 KOL、follow-up、寄样、费用或批量发送。

## 2026-08-20 两活动当日批量放量准备度审计

- Frankie 明确不再等待 IndieAlpaca 灰度观察，希望 Dave 与食人花今天开始批量放量。
- 本轮先做生产只读审计：名单、人审、产品活动锁、邮箱余量、活动授权、预算/寄样闸和批量释放能力；在尚未确认具体发送人数前不执行批量真实邮件。
- 活动表回读：Dave 已绑定产品 `recvkJOoCsNb1s`、名单版本 `evidence-v4`；参与记录当前 20/20 为已入围/新开发池/审核通过。活动草稿仅 1 条已发送，因此剩余 19 人尚未发送。
- 食人花活动 `launch-20260915-powkong-piranha-v2` 尚未填写产品主记录ID、目标国家/语言、竞品证据模式与名单版本；只读预览 job `launchpreview-cbf49df92d42` 被系统以“活动缺少产品主记录ID”拒绝，未产生写入。
- 产品回读：食人花主记录 `recvhAqrCyCPgl` 与别名 `recvqD87uSM1Fh`、Dave `recvkJOoCsNb1s` 均为活动专用锁；日常派单不会与活动冲突。
- 发送通道回读：`/health=ok`，自动发送未暂停、`EMAIL_DRY_RUN_TO` 未开启；系统配置每次总量12、每品牌每次6、单品牌滚动24小时上限120。Zoho 直接分页回读最近24小时约 FUNLAB 13、POWKONG 4。
- 现有活动生产接口只有 `POST /launch/outreach/send-one`，能逐条人工审核/重复触达/幂等/raw校验，但没有后台批量任务、状态查询、按批限速和大名单续跑；批量放量前需补这一层，不能用循环瞬间突发。
- 目标口径校正：每款 20 是实际上稿目标；当前模型还要求 29 个承诺、中性情景 427 封开发信。Dave 的20人只是首批优先名单，不是完整开发池；食人花则连首批名单尚未建立。

## 2026-08-20 自适应正常放量决策

- Frankie 明确“正常放量”不是只发完 Dave 当前19人，而是两个品牌每日持续运行，在邮箱健康前提下尽可能用满各自 Zoho 每日安全额度。
- Frankie 明确不把中性测算 `427` 当开发池上限；系统应按有效回复率、正向回复率、合作承诺缺口和预计按时上稿数动态继续扩池。
- 两款产品的售价/KOL Deals 暂不在本轮直接改产品库，登记为 P1 待办；原因是需要同时确定活动价、渠道、有效期、佣金、折扣与最低毛利。
- 当前最小可执行口径：每品牌每批6封、滚动24小时总外发不超过120；现有代码按真实待发 reply/ship/quote 数动态预留、最多30封，没有时间敏感邮件时自动把额度释放给 cold。
- 代码复核后修正实施方案：不再新建第二个活动发件器；复用现有 `auto_send` 作为唯一发送出口，只增加活动队列喂入、活动身份白名单和状态查询，避免两套流程重复计算额度。
- 当前真正 P0 缺口是：活动草稿尚不能通过产品活动锁进入 `auto_send`；现有单人灰度端点会直接调用 `send_one`，没有走 `scan_ready` 的品牌日限额选择；草稿表24小时计数也可能滞后于 Zoho 手工/其他流程真实发件。
- Dave 需要追加式动态候选队列；现有名单锁接口最多100人且采用整批替换，会取消未出现在下一批的旧参与者，不能直接承载400+开发漏斗。

## 2026-08-20 双品牌正式放量实施

- Frankie 已授权按最短顺序执行：活动队列接入发送中心 → Dave补池 → 食人花配置及建池 → 双品牌测试回放 → 正式放量 → 每日反馈控制。
- 已完成代码安全修正：活动队列只复用唯一 `auto_send`；普通 cold 仍受活动锁阻止；活动草稿需正式模式、正式状态、邮件授权、名单锁授权、锁定版本=证据版本、参与记录精确关联和活动 Raw 证书全部通过。
- 草稿生成已恢复原有质量闸：先写 `待审`，禁用句式命中则强制 `需人改`，其余调用 `draft_router`；不再直接标记自动通过。
- 产品类型提示已按产品名/品类动态生成：食人花按 dock，Dave 按 controller，防止跨品类误写。
- Zoho 发件箱滚动24小时计数已接入现有120封/品牌上限；活动计数获取失败时只阻止活动批量流，不影响常规回复链。
- 每日反馈已改为活动内口径：已发/回复来自该活动关联草稿，明确承诺来自参与记录“承诺上稿时间”，预计按时上稿按活动窗口判断，停止线读取活动“目标承诺数”（当前29），不再读取 KOL 全局合作状态。
- 生产活动表已新增文本字段 `邮件Raw验证证书`（field_id=`fldtnLtn1N`），用于绑定活动ID、产品、品牌、模板版本和9项 Raw 校验结果；没有证书不能正式发送。
- 食人花活动已完成产品、市场、语言、无竞品证据模式、版本及名单锁配置回读；所有价格、寄样、付费和储备金授权保持关闭。
- 本地验证：137项活动相关测试全部通过；全仓357项通过、1项既有 Zeabur watchdog 测试失败（独立运行亦失败，与本次代码无关）。
- 首次生产建池结果：Dave 全池预览得到494名可用新开发对象，其中100名资料完整且系统建议通过，活动有效池已从20追加到120；食人花旧库20名可用新对象中18名为百万级以上头部、2名内容资料超过180天，均按人审规则拦截，未擅自建草稿。
- Dave 119条草稿首次生成全部被 DeepSeek 返回 `402 Payment Required` 拒绝；系统未创建草稿、未外发。为避免外部模型余额成为单点阻塞，新增仅在402时启用的确定性三语备用模板（en/de/es）：只写达人名、产品英文名、官方UTM链接和9月15日节点，不写价格/佣金、不编造看过具体内容；仍受活动身份、禁占位符、Raw、Zoho额度和发送暂停闸控制。相关活动测试增至138项通过。
- Dave 队列补跑已幂等收敛：142条活动参与记录中120条绑定活动草稿，重复补跑返回 eligible=0/queued=0；1条为既有IndieAlpaca已发送，119条为待发。
- 食人花模板-only测试邮件只发测试邮箱，9项raw全部通过、生产草稿写入0；活动已回填Raw证书。旧池20名候选已建为活动参与记录，其中18名路由Frankie例外审核、2名路由KOL运营审核，全部待审核且邮件授权关闭。
- 活动参与记录生产开关已用Zeabur单变量方式开启；食人花20条写入逐条唯一键回查完成，活动无名单阻塞，锁定版本为`no-evidence-v1`。
- 每日反馈工作流 `VWTMNkXf0zcs4Kvz` 已启用，Asia/Shanghai时区每天17:00运行，依次调用Dave与食人花活动反馈；自动发信工作流 `UIShaANEi8M0rx1v` 保留10分钟频率。
- 首次同步触发`/auto-send/run`超过网关等待时间，未能拿到批次结果；已新增`async_mode=true`后台受理和`/auto-send/jobs/{job_id}`状态查询，n8n自动发信URL已切换后台模式。commit `e9bc362` 已部署，接口10项+活动运行7项测试通过。
- Dave正式首两批后台发送成功：每批6封，共新增12封，加上既有IndieAlpaca共13封活动开发信；两批均`fail=0 / lock_validation_failures=0 / zoho_count_errors={} / paused={}`。第二批于定时器06:50 UTC自动受理并完成，证明后台模式和防重叠可用。
- 首批详情审计发现1条爬虫邮箱把页面问候语粘到`.com`后（`...comHowdy`）；Zoho已接受该无效地址，无法撤回。已立即关闭Dave邮件授权，修复`clean_email`：同一邮箱多版本时选干净短版本，单条粘连地址直接拒绝；主表该KOL邮箱已纠正为`totallytubularjonathan@incharacteragency.com`并回读。commit `c6dc0ec`已推送，3项邮箱测试+10项接口测试通过，待线上部署后再恢复Dave持续放量。
- `c6dc0ec`已部署为线上运行版本，Dave邮件授权已重新开启；第三批后台任务`autosend-94bc7b1eb26d`完成，6封全部成功、失败0、活动锁失败0、Zoho计数错误0。三批共新增18封，加上IndieAlpaca灰度后，本活动已发19封。
- 07:00定时器与手动触发命中同一后台任务，返回`already_running=true`，证明10分钟定时任务不会与运行中批次重叠发送；第三批完成时FUNLAB滚动24小时总外发33封，其中活动cold 18封，剩余总额度87封。
- 食人花20名候选已进入参与记录，但18名为头部例外审核、2名为内容资料超过180天的运营审核；系统没有把“正式放量授权”解释为“跳过候选适配审核”，POWKONG邮件授权继续关闭。
- 15:07手动触发双品牌每日反馈控制器作为上线实测：Dave job `launchruntime-5981d875efa6`、食人花 job `launchruntime-465d4ea71d01` 均被后台接受；两者采用后台计算和状态查询，不占用网关同步等待。
- 15:10定时器自动受理第四批`autosend-45da78e9670c`。因同时运行全池反馈计算，飞书短暂返回`1254607 Data not ready`，发送器保持运行锁并逐条重试，15:20定时触发没有重叠开新批；最终6封全部成功、失败0、活动锁失败0。Dave累计批量24封，加灰度共25封；完成时FUNLAB滚动24小时总外发39封，剩余总额度81封，活动待发96封。
- 食人花每日反馈job `launchruntime-465d4ea71d01`已完成：参与20、已发/回复/承诺均0，动作=`expand`；全池7,732条中只有现有20名符合基础冷开发条件，但自动审核通过0，因此新增参与0、草稿0、真实发送0。Dave反馈job `launchruntime-5981d875efa6`仍在后台全池核对，现有96封队列不受影响。
- 15:30定时器已自动受理第五批`autosend-6ee58a807c4f`，返回后台运行状态；无需人工再次触发。

## 2026-08-20 活动参与记录按产品分视图与审核列整理

- 新建并启用食人花专用视图 `食人花二代活动｜人工审核`（view_id=`vewTsJRx9G`），固定筛选活动 `launch-20260915-powkong-piranha-v2`、对象类型 KOL、参与状态已入围；当前页面回读为 20 条。
- Dave 既有视图 `戴夫活动｜人工审核`（view_id=`vewH5ud840`）保留原产品筛选，字段布局已同步为同一套运营审核顺序。
- 两个审核视图均收敛为 20 个判断字段：参与记录ID后依次前置关联KOL、达人主页、主证据帖子、系统审核分流/说明、审核结论/原因、国家/语言/平台/粉丝、内容与历史触达、竞品证据、最终优先级、进入方式、活动分池和选择原因；活动ID、内部关联、批次、上稿后字段等不再占据审核首屏。
- 保留 `Grid View` 不变，不删除表字段；只改两个产品审核视图的显示与顺序。创建过程中的旧食人花临时视图 `vewDiqdXhI` 已在新视图页面验证后删除，避免运营看到重复入口。
- 飞书页面截图核验：食人花视图首屏为 KOL→主页→主证据→系统审核→人工结论→国家/语言/平台/粉丝，底部显示 20 条；Dave 页面首屏字段顺序一致。

## 2026-08-20 食人花人工审核完成与正式放量

- 回读 `食人花二代活动｜人工审核`（`vewTsJRx9G`）20条真实结果：1条通过（DSimphony）、18条排除、1条待补资料（Summoning Salt）；系统没有把排除或待补资料对象送入草稿/发送队列。
- DSimphony 参与记录 `recvsOEgTwBJoE` 已在后台队列幂等关联活动草稿 `recvsPgZQvoNcP`；重复触发 queue 返回 `eligible=0/queued=0`，原因是该草稿已存在，不是漏建。
- 草稿为西班牙语、POWKONG发件身份、产品 `Piranha Plant Switch 2 Dock`，包含 Amazon 与独立站 UTM 链接；DeepSeek 402 时使用受控备用模板，不含价格、佣金或付费承诺。
- 食人花活动 `recvsFoRmeMM9Y` 已从 `影子试跑/待人工批准/邮件授权=false` 切换并回读为 `正式运行/正式执行中/邮件授权=true`；名单锁、版本 `no-evidence-v1` 和Raw证书保持一致，寄样/付费/储备金授权仍关闭。
- 自动发送批次 `autosend-c587ee336421` 完成：DSimphony 于北京时间17:21发至 `contacto@dsimphony.com`，Zoho消息ID `1787217661360155100`；草稿回读 `邮件草稿状态=已发送 / 发送状态=已发`，活动锁失败0、POWKONG未暂停。
- 同批唯一失败为FUNLAB另一条 `intheblackmedia.com` 聚合域名，被既有邮箱风控主动拦截；与食人花活动无关，没有影响POWKONG发送。
- 当前食人花现有旧池只产生1名可发对象，距离目标仍远；下一P0不是放宽审核标准，而是扩充更贴合Switch/Nintendo/游戏硬件的新候选来源，并让系统继续按审核结论自动入队。

## 2026-08-20 食人花审核原因反哺审计

- 只读回读食人花审核视图20条：1通过、18排除、1待补资料。18条排除均涉及受众/核心游戏/IP/内容形态与Switch 2食人花底座不匹配，多条同时存在低活跃。
- 逐条回读对应KOL主表：17/20没有v2标签版本；16/18被排除对象没有近期视频标题/抓取时间；20/20“决策反哺日志”为空。
- 定位到三处规则问题：Switch底座映射允许粗粒度“游戏”独立命中；IP喜好未命中只是不加分而不拦截；资料缺失/过期只转人工审核，未先后台刷新。
- 定位到一处触达路由缺陷：正向合作状态只有命中同产品草稿后才生效，导致主表“洽谈中”仍可能被判为`eligible_new_cold`。
- 本轮未改生产表和候选代码；审计与最小字段/规则方案已写入`docs/launch_candidate_review_feedback_audit_2026-08-20.md`，后续需先做20条回放验证，再授权写表和扩池。

## 2026-08-20 食人花审核反哺 P0 实施

- 用户授权执行P0后，先把活动`recvsFoRmeMM9Y`的`发送邮件授权`关闭并回读为false；Dave活动未动。
- KOL主表已新增6个客观画像/路由字段，活动参与记录表新增`审核原因代码`；20组主表+参与记录写入全部成功，抽样读回一致。
- 代码已修复两类误选：①待回复/建联中/洽谈中/已合作即使无同产品草稿也禁止新cold；②Nintendo/Mario Switch产品要求结构化Switch生态+主机游戏/硬件评测+有效资料，通用“游戏”标签不再单独放行。
- 新增5项回归测试，候选相关71项全部通过；全仓368项通过、1项既有Zeabur watchdog测试失败，与本次无关。
- 下一步：提交代码、部署、用20条生产数据逐条只读回放；确认18排除不回流、DSimphony保持匹配、Summoning Salt因活跃度不足被拦后，再决定是否恢复食人花邮件授权。

### P0 代码复核补强

- 规格复核发现仅凭“主机游戏/硬件评测”仍会让 Minecraft 等泛主机内容误入；现已增加 Nintendo/Mario/Yoshi/Zelda 等受众信号，或近期硬件评测/开箱/桌搭/底座/手柄内容二选一的硬闸。
- 机器资料不再只信任一个状态字段：必须同时满足 `标签版本=v2`、抓取时间60天内、近90天有发布；本轮运营刚人工核实的记录用`人工核实有效`保留可审计的时效例外。
- KOL主表新增`资料核实时间`（`fldUD52e5r`）并为本轮20条统一回填真实审核时间；`人工核实有效`仅在60天内生效，过期自动回到待刷新。
- 混合IP增加反向冲突闸：Mario与Minecraft/Roblox/Fortnite等同时出现时，若近期内容没有Nintendo/Mario或硬件证据，不再因一个正向词误放行。
- 候选计算已把全局关系路由放到产品画像前：已有关系对象即使画像失败，仍输出“沿用原线程/禁止新cold”，不再被基础筛选静默吞掉。
- 单条回放已接入当前名单版本的活动参与记录审核结论/原因代码，并返回画像证据字段；旧版本或已取消记录只标记为历史，不作为当前结论展示。新增专项测试后该文件31项全通过；全仓364项中363项通过，唯一失败仍为既有Zeabur watchdog用例。
- 活动`发送邮件授权`继续保持关闭，待部署和20条生产单条回放通过后才恢复。

### P0 生产验收完成

- commit `1a33c1f3880797c57db7efe049098fde0f61b013` 已部署为 Zeabur deployment `6a86d0be29f0931a12bf889d`，状态`RUNNING`，`/health=ok`，回放端点存在。
- DSimphony线上单条回放保持产品匹配，但因同产品已有触达被判`blocked_prior_same_product`，不会重复发新cold。
- 复用生产环境和同一份代码一次性回放20条：18排除全部基础筛选失败；Summoning Salt因活跃度不足失败；DSimphony基础筛选通过；`acceptance_failures=[] / all_safe_from_new_send=true / writes=0`。
- 食人花活动`recvsFoRmeMM9Y`的`发送邮件授权`已恢复true并回读确认，运行模式仍为正式运行、名单版本仍为`no-evidence-v1`；Dave未动。

## 2026-08-20 双活动无人值守自治补池 P0 上线

- 自治代码 commit `9a5a41f` 与状态审计补强 commit `912756c` 已推送 `master`；Zeabur deployment `6a870e0a29f0931a12bf96b3` 为 `RUNNING`，`/health=ok`，OpenAPI 已暴露自治启动、后台状态和最新持久任务查询能力。
- 已拆成两条短 n8n 流程并启用：启动流 `uvBfJBtGH93FPa6w` 在北京时间每小时00分受理Dave、05分受理食人花；完成审计流 `ijIcjoYO9Jm1Vdkw` 在50分分别读取两活动最新持久状态，只接受70分钟内更新的`success`。两条工作流经本地n8n validator检查均为0错误/0警告。
- 保留17:00每日反馈 `VWTMNkXf0zcs4Kvz` 和10分钟唯一发送中心 `UIShaANEi8M0rx1v`，小时级控制器只补池、刷新资料、建立审核记录和活动草稿，不直接调用Zoho发信。
- 生产结果：食人花 `launchruntime-f2cc8266945d` 成功，滚动24小时13/120，刷新并写回30名画像、建立5个活动专属YouTube任务、新增19名运营审核对象并发1张运营卡；审核对象草稿0、邮件0。Dave `launchruntime-4f08fd685492` 成功，滚动24小时120/120，活动已发104、回复3、承诺0；可发库存33→35，系统自动补入并排队2名，另建3个发现任务和20名运营审核对象；两边均未下调质量阈值。
- 食人花5个发现任务最终4个完成、1个失败，成功任务共新增24名KOL；失败来自既有本地`yt_mvp.py`对异常YouTube响应缺少`data`字段的处理，活动小时补池会用未使用的关键词继续补给，不阻塞主链。
- 真实定时验证：启动流 execution `968537` 于22:31准时成功并受理Dave后台任务 `launchruntime-bce6ffca1b9a`；完成审计首版因持久态字段为`updated_ts`而非`started_ts`报错，修正后 execution `968515` 于22:30成功。正式频率已恢复。
- 回归验证：聚焦38项+3子测试全过；合并远端并行项目后全仓427项+3子测试通过，唯一失败仍为既有Zeabur watchdog日期窗口用例，与本次改动无关。

## 2026-08-20 食人花候选质量 P1 完成

- 已在活动主表新增并回读 `KOL粉丝下限=5000 / KOL粉丝上限=1000000`。候选预览优先使用活动范围，未配置的旧活动继续沿用产品价格带。
- 本轮候选发现共新增129名对象；全池后台预览 `launchpreview-19c6d503aa17` 读取7,861名KOL，筛出3名可进入新开发人工审核的对象。
- 3名新对象已写入活动参与记录，批次=`review-20260820-1787227774`：SwitchRank、Metricar、Aliens in the garage。全部为 `参与状态=已入围 / 审核结论=待审核`，有主页和内容证据；`关联邮件草稿`为空，真实外发0。
- 误判样本 EL GUERRERO DEL JUGUETE 的10条近期内容仅2条涉及游戏/主机。新规则要求至少3条内容证据，并在无近期人工核实保护时清除旧机器标签，防止后续活动再次误入池。
- 代码提交：`59e6f6e`（活动粉丝范围）、`2220f44`（持续内容证据）、`dea23ae`（精准清理旧机器误判标签）。相关测试47项全部通过；全仓400项通过，唯一失败为既有 Zeabur watchdog 用例，与本次无关。
- 安全边界保持：P1只补人工审核池，没有新建草稿、没有调用Zoho、没有改变3名对象的人工结论。
- 最终版本`3429d75`在线状态`RUNNING`、健康检查`ok`；EL GUERRERO单条修正任务`kolprofile-d5a8a59e8521`写入成功，主表读回旧机器内容风格/IP标签均已清空。

## 2026-08-20 食人花新增3名候选审核通过并正式发送

- 用户确认 SwitchRank、Metricar、Aliens in the garage 3名新增候选全部审核通过；生产回读均为`审核结论=通过 / 参与状态=已入围`，当时`关联邮件草稿`为空。
- 仅针对本活动触发后台队列 job `launchruntime-0dbbc7a1bdba`：eligible=3、queued=3、failed=0；3条参与记录均唯一关联新活动草稿，未复用其他草稿。
- 触发现有唯一发送中心后台 job `autosend-51a67ea78d3c`：sent=3、fail=0、scheduled_later=0、skipped=0、活动锁失败=0；没有触发其他活动草稿。
- 真实发件结果：Metricar→Zoho `1787229751642155100`；Aliens in the garage→Zoho `1787229770928138400`；SwitchRank→Zoho `1787229788380138300`。
- 发后直接读取POWKONG已发送文件夹raw复核：3封均命中正确收件人、主题非空、正文953-995字符、产品英文名/产品链接/HTML完整、无未替换占位符；2封西班牙语正文含`15 de septiembre`，1封英语正文含`September 15`。
- 发送批次完成时POWKONG滚动24小时计数=10/120，可用110；FUNLAB=120/120，本批未向FUNLAB继续发送。现有回复/寄样/预算闸保持原规则不变。

## 2026-08-20 食人花自治补池假成功 P0（实施中）

- 已用失败测试复现：额度剩余107、库存0、发现/刷新/入队均0时，旧代码仍固定持久化为success。
- 已增加业务结果和degraded状态；待补资料记录同步写结构化原因代码。
- 活动固定关键词耗尽后改为自动生成新一批Nintendo/Switch/Mario/硬件评测定向发现词，后续继续经过原硬筛。
- 食人花补池与独立业务审计脚本已改为15分钟周期；待提交、部署和生产回读。
- 活动相关156项测试通过；全仓417项中416项通过，唯一失败为既有Zeabur watchdog日期fixture。

### P0 第二轮复核补强

- 规格与标准双复核发现3个边界并已修正：原因代码改为候选预览源头直接生成；补池进度改为布尔结论+分项安全计数；定时审计不再自己重复推导规则，只接受服务给出的稳定业务结果，且缺字段直接失败。
- 两小时内尚未消费的活动爬虫任务会进入`active_discovery_tasks`并判为`supply_in_progress`；超过两小时则记为过期，不再掩盖真实阻塞。
- 动态AI拓词只对本次食人花主题启用；Dave继续使用既有确定性词库，避免范围外改动和来源质量漂移。
- 活动候选/运行时/关键词66项全部通过；全仓424项中423项通过，唯一失败仍为既有Zeabur watchdog日期窗口fixture，与本次改动无关。
- 暂停、窗口结束、库存充足等提前返回已统一补齐业务合同；定时审计对合法结果不误报，对缺失/非法字段仍失败。
- 首次更新15分钟审计时发现脚本遗漏传入`$auditExisting`，额外创建了新审计任务；已加回归测试并改为复用现有任务，同时自动停用同名迁移遗留的旧小时审计，保证生产只有一条有效审计。
- 生产部署切换暴露出“旧进程已终止、持久状态仍是running”的假运行窗口；已先写失败测试，再把并发判断收紧为当前单实例内存任务。部署重启后可立即续开，避免最多45分钟空等；同一实例重复触发仍由任务表和活动锁拦截。
- P0生产回读完成：部署`6a872ac729f0931a12bf9e06`为RUNNING；任务`launchruntime-1d80deea2fd6`返回`supply_in_progress`，画像刷新30、定向发现任务4、可发库存0、剩余额度107。定时审计执行`969233`（运行态）和`969242`（完成态）均成功；00:35自动启动执行`969251`创建下一轮任务，确认不依赖Codex会话。
- n8n审计恢复正式15分钟频率，工作流`1WOenWodtTRlUqWz`active，旧审计`ijIcjoYO9Jm1Vdkw`inactive。全仓428项中427项通过，唯一失败为既有Zeabur watchdog日期窗口fixture。

## 2026-08-21 双活动日发信与全链路只读审计

- 审计时点：2026-08-21 11:17（北京时间）。本轮未写生产表、未改n8n、未触发真实邮件。
- 活动口径当日真实发送：Dave 1封，食人花1封。累计活动发送分别为105封、5封；Dave累计回复3，食人花累计回复0。
- Dave活动当前关联草稿140条：已发105、发送失败1、未发34；11:00自治任务`launchruntime-2b8f897bde62`仍在后台运行。
- 食人花活动当前关联草稿5条，均已发送；11:17自治任务`launchruntime-abb80ae27178`完成，业务结果`supply_in_progress`，活动累计发送5、回复0、承诺0、可发库存0；本轮刷新画像30条但没有新增审核通过对象或草稿，POWKONG品牌滚动24小时总外发14/120。
- 该食人花任务的分项结果为：新增自动通过0、排队草稿0、新建发现任务0、有效在途发现任务0、新增待审核0，仅`profile_refresh_writes=30`。这说明现有“供给进展”仍可能把重复资料刷新误判成业务推进；应列P0，连续刷新不产出候选/任务/草稿时转`degraded`并触发新的有效拓新来源。
- 两活动参与记录均只有`对象类型=KOL`：Dave 182条历史记录、食人花43条；当前`已入围`分别160、43。食人花43条中通过5、待补资料20、排除18，排除记录仍保留“已入围”状态，发送闸能拦住，但参与状态口径会放大活动人数，应后续统一语义。
- 两活动`承诺上稿时间`和`实际上稿时间`均为0。当前自治停止线依赖承诺数，但回复/洽谈和上稿登记没有自动回填活动参与记录，这是最重要的P0闭环缺口。
- 核心执行流均为active且近期成功：日常派单、KOL/媒体人富化、唯一自动发送、回复监听、reviewer兜底、SLA、退信处理、自治启动、15分钟业务审计和17:00活动反馈。
- 发现定时任务时区/频率漂移：n8n实例按Asia/Shanghai解释cron，但5条旧workflow没有显式timezone且仍使用按UTC换算的表达式。实际结果为：Follow-up 02:00而非10:00；寄样对账01:20而非09:20；手动发送补登记01:40而非09:40；草稿归档清理每天20:00而非周一04:00；上稿登记卡每天02:30而非周一10:30。
- 两条周报工作流均已启用，但暂无任何execution历史，不能仅凭active判断已完成生产验收。
- 早前要求的每周日任务管理卡、每日17:00邮箱额度/限额卡仍未找到生产workflow；现有17:00 `VWTMNkXf0zcs4Kvz`是活动反馈，不等同邮箱额度卡。
- KOL数据看板容量P0已完成，但看板刷新仍约11分钟、最终状态监控尚未接入；完整表字段合并/删除和接近行数上限的治理仍未完成。

## 2026-08-21 三项P0连续实施

- 食人花补池真实产出判定已完成本地回归：先新增“只有`profile_refresh_writes=30`、其他供给均为0”的失败用例，确认旧逻辑错误返回`supply_in_progress`；再将资料刷新从真实供给进展布尔值中排除，同时保留刷新数量供审计查看。
- 相关3项回归均通过：资料刷新单独发生时返回`supply_blocked`；有效在途发现任务仍返回`supply_in_progress`；过期发现任务不再掩盖阻塞。下一步为全套回归、部署和生产单活动回放。
- commit `6750be9`已部署为Zeabur deployment `6a87c75529f0931a12bfadf2`，状态`RUNNING`且`/health=ok`。生产回放`launchruntime-20432240a6c5`正确返回`degraded/supply_blocked`：刷新30、真实供给七项均为0、库存0、剩余额度106。
- 继续下钻发现实际停摆原因：首批固定词耗尽后，DeepSeek动态拓词返回402，导致`keyword_source=none`、目标发现任务5条而创建0条。已新增受控的食人花长尾词降级池；只有外部模型不可用或产出不足时才使用，并继续受全局去重、目标语言和活动前缀约束。新增失败回归后，关键词供给10项测试全部通过。
- commit `222aada`已部署为`6a87c9a629f0931a12bfae37 RUNNING`。第二轮生产回放`launchruntime-cef5ebdacaa5`返回`success/supply_in_progress`，`discovery.created=5`、`keyword_source=curated_fallback`；爬虫任务台逐条回读5条均为`触发=true / 1-待触发`，语言分布en2/de2/es1。食人花“真实产出”P0完成。
- 两条错误频率工作流已按全量读取、最小修改、停用/PUT/重新启用完成修正：`ugM1hX94RrzDWmhj`为`Asia/Shanghai + 0 4 * * 1`，`0wViUZQ6nyJpNtMJ`为`Asia/Shanghai + 30 10 * * 1`；两者active且均保留3节点。配置修复已完成，2026-08-24周一首次真实execution另列11.4A回读，避免把配置正确冒充真实执行验收。
- 活动结果闭环首版commit `1e323d3`已部署并完成生产dry-run；后续安全修正版`aed1da3`移除了KOL主表上稿自动归因。当前自动事实回填只接受：①已剥离英/德/西语引用历史的回复中“明确发布动作+同分句具体日期”；②`live_link_received`里的视频/帖子/文章级公开内容链接；③事实来源草稿必须唯一归属当前活动参与记录。普通兴趣、报价、泛洽谈、频道/官网首页均不推断。
- 自治指标已纠正：`commitments`仍读明确承诺日期；`ontime_posts`只读`实际上稿时间`并与活动窗口比较，不再把承诺日期当真实上稿。每轮活动反馈/自治补池计算扩池或停止前，先自动同步结果事实。
- 生产dry-run及commit回放均无错误：Dave扫描182条、食人花扫描43条，当前两者`updates_planned=0 / updates_written=0 / ambiguous=0 / missing_live_link=0`。这表示现有邮件回复暂无满足严格口径的明确承诺或上稿事实，不是漏写；后续可核验邮件回复会由活动反馈自动回填，人工上稿卡仍须完成11.6A后才会直接写活动事实。
- 最终安全修复后，活动相关测试195项全部通过；完整回归共460项、459项通过，唯一失败仍为既有`test_zeabur_watchdog`部署日期窗口fixture，与本次活动结果改动无关。
- 独立代码审查拦下首版4项事实边界：频道主页不能当内容链接；邮件回复时间不能当实际发布时间；KOL主表全局上稿不能因“当前只有一个活动”自动归因；飞书写入成功必须逐条回读。修复版`aed1da3`已移除主表推断，仅接受内容级URL和同回复明确“已发布+具体日期”，并增加写后回读、60秒草稿快照复用、只读指标与写入同步函数分名；西语`Publicaré...`也补了回归。
- 第二轮独立审查又拦下媒体人官网首页误判、德/西语引用历史泄漏、传入跨活动参与记录、共享草稿跨活动归因、并发重复扫表和回填失败静默继续扩池。最终加固版已增加保守的媒体文章URL白名单、三语引用剥离、活动ID精确复核、源草稿唯一归属、并发单飞缓存，以及`hold + outcome_reconcile_failed + degraded + 告警`错误上冒；失败参与记录ID会写入后台任务摘要。两位独立审查员最终复核均无剩余P0/P1。
- 现有人工上稿登记卡仍只写KOL主表且用点击时间记日期，不能安全自动归到活动参与记录，已拆为11.6A；在补卡片前，自动控制器只消费邮件里能核验的活动事实，宁可少计、不假计。

### Errors Encountered

| Error | Attempt | Resolution |
|---|---|---|
| 读取计划文件时假定`.codex/plan/lessons.md`存在 | 收尾读取 | 文件不存在；本次不新造项目文件，通用时区审计经验按`lesson-capture`写入memory candidate |
| 直接用`python -m unittest tests...`运行单测 | P0回归首次执行 | 本机嵌入式Python没有把仓库根目录加入`sys.path`，且被`C:\tmp\ml-data-sync\app`同名包污染；改用内联runner把当前仓库插入`sys.path[0]`后再discover |
| `unittest discover`指定`top_level_dir`但tests不是包 | P0回归第二次执行 | 去掉`top_level_dir`，仅以tests目录discover并按测试名筛选 |
| PowerShell直接写`git rev-parse @{u}`被解析为哈希字面量 | 本轮分支预检 | 将`@{u}`整体加引号后读取upstream；未影响仓库 |
| PowerShell把`foreach {...} | ConvertTo-Json`直接接管道触发空管道解析错误 | 首次工作流只读检查 | 先把循环结果存入`$out`，再统一转JSON；随后完成两流回读 |
| `rg app/launch_*.py`在Windows把通配符当成非法文件路径 | 查找活动模块 | 改为按目录搜索并使用`--glob`/明确文件名；未改生产文件 |
| 本机lark-cli 1.0.64不支持`base field list`子命令 | 查KOL主表寄样/产品字段 | 停止升级或盲试；改读现有代码和生产工作流，只做只读检查 |

## 2026-08-21 KOL集中宣发任务日报卡开发

- 已实现Card JSON 2.0日报：标题固定`KOL集中宣发任务日报 · YYYY-MM-DD`，每个执行中活动显示真实彩色状态标签、按时上稿进度条和邮箱滚动24小时额度进度条。
- 活动开发信只统计参与记录关联的唯一`cold`草稿；同一草稿跨两个执行中活动时从双方统计排除并把双方标红，避免重复算业绩。
- 全池读取改为后台任务+状态查询；后台去重键包含北京时间日期、明确收件人和当前执行中活动ID集合，活动集合变化会重新计算。
- 发卡防重采用三层：进程内异步锁、飞书确定性`uuid`、活动`数据口径备注`持久技术回执。显式发卡前后各写一次回执并回读；业务字段写入始终为0，`notify=false`完全只读。
- 聚焦日报/路由测试40项通过；全部活动相关225项通过；两轮独立复审最终均无P0/P1。全仓493项通过，仍只有既有`test_zeabur_watchdog`日期窗口fixture失败，与日报改动无关。
- 生产只读预览`launchreport-d74b82051649`成功：2项活动、4条进度图、结构校验通过、业务写入0、技术回执写入0。Dave为绿色“进度正常”，食人花为橙色“进度落后”。
- 首次Frankie-only真实创建被飞书在消息创建前拒绝：HTTP 400 / 外层`230099` / 卡片内部`200861`，定位到Card 2.0不支持旧`note`组件；没有产生坏消息。已把说明区统一改为`markdown`，并只允许明确的消息创建前400拒绝进入`rejected`后重试；5xx、超时和空message_id继续保持`sending`硬暂停。
- 修复commit`92e2e0d`已精确部署为Zeabur deployment`6a87ea1b29f0931a12bfb25b`且状态`RUNNING`，`/health=ok`，日报运行与状态查询接口均存在。
- 卡住的技术回执只在Dave活动`recvsFoRmeGj4Y`中把同一键由`sending`改为`rejected`；非技术业务备注逐字比对未变。随后Frankie-only任务`launchreport-12df0a22e5cd`成功，message_id=`om_x100b674afba06cb0c00611d1b8a2689`，业务写入0、发送回执写入2、最终状态`sent`；运营群未发送。
- 最新聚焦测试42项、活动相关228项通过；全仓494项通过，唯一失败仍是既有Zeabur watchdog旧日期fixture。两位独立复审最终均无P0/P1。

## 2026-08-21 日报卡真实前端比例修复

- 用户提供真实飞书截图后，确认首版虽然通过API/Card JSON结构校验，但没有完成客户端视觉验收；卡片在超宽桌面窗口被严重纵向拉伸。
- 已在已登录飞书网页版亲自复现：`width_mode=fill`配合`chart aspect_ratio=2:1 + height=auto`，导致每条进度图高度约等于消息宽度的一半，4条图跨越4—5屏。
- 已移除全部`chart/linearProgress`和全宽设置，改为每活动两行10格紧凑文字进度条；校验器今后直接拒绝`chart`和`width_mode=fill`。
- 聚焦回归18项通过；全仓495项通过，唯一失败仍是既有Zeabur watchdog旧日期fixture。
- 修复commit`48ecdae6fca1e175399f29e3462f962f2f3115c6`已推送并部署，`kol-auto /health=ok`。
- 修复版Frankie-only样卡message_id=`om_x100b674b5edc3cb0df3b391838d8cd8`；已在飞书网页版打开并截图，两个活动在一屏完整显示。旧/新前端证据分别保存为`.codex/plan/feishu-bad-card-frontend-20260821.jpg`和`.codex/plan/feishu-compact-card-frontend-20260821.jpg`。
- 运营群定时发送继续关闭；等待Frankie只确认修复版真实截图后再启用17:15群日报。

## 2026-08-21 日报卡17:15生产启用

- Frankie已确认紧凑版真实前端排版通过；Zeabur环境开关`KOL_LAUNCH_DAILY_GROUP_ENABLED=1`已用单变量方式新增，未全量覆盖其他环境变量。
- `kol-automation`已重新部署为`6a87f41da158dec40572653b RUNNING`，部署后`/health=ok`；生产代码仍锁定当前KOL运营群`oc_8b71a652a25ec0dd1c8af2c53e86ed93`和聪哥分身1号发送路由。
- 已新建并启用独立n8n工作流`3GDllutHPUNPEDHs / KOL Launch - Daily Report (17:15 BJ)`：`Asia/Shanghai + 15 17 * * *`。它不会覆盖现有17:00活动反馈。
- 工作流用`notify=true + frankie_only=false + async_mode=true`启动后台日报，等待12分钟后查询`job_id`；仅当任务成功、卡片校验成功、确实群发1张且`business_writes=0`时通过，否则execution失败并留下节点级错误。
- 启用后只读回放`launchreport-170b75618057`成功：2项活动、`validation.ok=true`、`notified=false`、业务写入0、技术回执写入0。该回放没有提前发群。
- 首次真实群发安排在2026-08-21 17:15北京时间；发出后仍需回读真实群消息并截图确认，不能只看n8n执行成功。
- 部署脚本经双轴复核后加固：固定workflow ID、全分页查重、只复制kol-auto唯一Authorization、写前/读回校验唯一触发器及完整单链路、失败恢复原payload和原启用状态；同ID幂等重跑与假ID硬失败均已验证。

## 2026-08-23 Dave 可选竞品证据与七层词源灰度

- Frankie确认 Dave 词池方向无误，并要求先沉淀分析、用 Dave 完整测试、结果符合预期后再固化规格。
- 已创建独立 worktree `C:\tmp\kol-dave-keyword-pilot-20260823` 和分支 `codex/dave-keyword-pilot`，避免覆盖原工作区未提交文件。
- 已读取全局公司思考框架、workspace知识规则、现有集中宣发概念卡、`kb-digest`、`prototype`、`planning-with-files` 与 `implement` 说明。
- 第一性原理初判：本项目属于企业改造 L2＋L3＋L4；验收应是系统替代重复选词决策、交接输入最小化、Frankie只审规则/异常，而不是任务数增长。
- 当前处于13.1方法论沉淀；尚未修改生产代码、生产表、n8n或发送邮件。
- 13.1已完成：方法论已沉淀到项目分析文档和公司复利知识库，且明确“不生成KOL个人业务数据副本”。
- 13.2已完成：浏览器加载`app/prototypes/dave_keyword_supply_logic_prototype.html`，实测结果为：引用NYXI=10词/4探测/2个NYXI词；不使用竞品=8词/4探测/0个NYXI词；新竞品=9词/4探测/0个NYXI残留；连续两轮零产出后竞品与平台来源进入cooling，下一轮4个任务全部切换到IP来源。
- Browser Harness首轮因默认CDP握手超时未执行页面；启动隔离Chrome并显式设置`BU_CDP_URL=http://localhost:9222`后连接成功。此问题只影响测试工具连接，不影响原型逻辑或生产系统。
- 下一步进入13.3：读取真实Dave活动、产品及现有NYXI证据配置，生成零写入的真实任务预览；预览通过后才允许13.4最多写入4个发现任务。
- 已完成灰度代码与安全入口：Dave固定词耗尽后改由活动字段编译结构化词；竞品只在任务选择“新调查/引用历史”且证据状态为“已就绪”时启用，未写死NYXI。
- 首批最多4条，优先覆盖竞品英语、IP英语、平台德语、平台西语；任务名写入`[词源:...]`，爬虫新增KOL继续在`迁移备注`保留实际抓取关键词，形成来源归因。
- 灰度入口`/launch/runtime/keyword-pilot`默认只读；真实提交需活动队列开关、精确确认口令，且代码强制最多4条，只创建爬虫任务，草稿0、邮件0。
- 关键词与路由相关44项测试通过；全仓505项中504项通过，唯一失败为未改动的Zeabur watchdog旧fixture，单独重跑仍失败，确认为既有基线。
- 浏览器重新验证原型首批选择：NYXI竞品词1条、Dave IP词1条、德语平台词1条、西语平台词1条；不再由两个竞品词占满英语名额。
- 下一步仍是13.3生产只读预演；只有读回真实活动得到4条合规且未使用的词，才进入13.4真实创建。
