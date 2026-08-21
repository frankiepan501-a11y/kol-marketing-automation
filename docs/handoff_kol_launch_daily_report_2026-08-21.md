# KOL集中宣发任务日报卡｜生产交接（2026-08-21）

## 结论

- 日报卡已完成开发、测试、部署、Frankie-only真实试发和飞书网页版前端截图验收。
- 首版样卡虽能发送，但真实前端出现严重纵向拉伸；该版已废弃，禁止用于群发。
- 修复版样卡message_id：`om_x100b674b5edc3cb0df3b391838d8cd8`。
- Frankie已确认修复版真实前端排版通过；运营群每天17:15定时发送已启用。

## 本次事故与修复

- 问题：首张样卡被飞书在消息创建前拒绝，没有生成坏消息。
- 根因：payload声明Card JSON 2.0，但正文仍使用旧`note`组件；飞书返回HTTP 400、外层`230099`、卡片内部`200861`。
- 改动：`app/launch_daily_report.py`把说明组件改为`markdown`；只在明确的消息创建前400结构拒绝时把技术回执标为`rejected`。任何5xx、超时或空message_id仍停在`sending`，防止重复发卡。
- 回执修复：只把Dave活动记录`recvsFoRmeGj4Y`中已证实未创建消息的同一回执由`sending`改为`rejected`，非技术业务备注读写前后完全一致。

### 真实前端比例失调（第二次修复）

- 问题：首版通过API和Card JSON结构校验，但在约2500px宽的飞书桌面窗口里，每条进度图被拉到约1250px高；4条进度图令整张卡跨越4—5屏，并出现大面积留白。
- 根因：卡片同时设置`width_mode=fill`，进度图使用`chart + aspect_ratio=2:1 + height=auto`。飞书按消息区域宽度计算图表高度，结构合法不代表客户端比例可用。
- 改动：彻底移除`chart/linearProgress`和全宽模式，改为10格紧凑文字进度条；每个活动只保留“上稿进度”和“邮箱额度”两行，两个活动可在同一屏完整阅读。
- 防回归：结构校验现在直接拒绝任何`chart`组件和`width_mode=fill`，测试同时检查卡片含进度标签、当前值/目标值、百分比和填充/空格进度字符。
- 前端证据：旧版复现截图`.codex/plan/feishu-bad-card-frontend-20260821.jpg`；修复版截图`.codex/plan/feishu-compact-card-frontend-20260821.jpg`。两张均来自已登录飞书网页版，不是本地HTML模拟图。
- 身份说明：修复版只发Frankie时曾使用聪哥分身2号做前端验证；正式日报仍按既定聪哥分身1号生产路由，只发当前KOL运营群。

## 生产证据

- 实现commit：`f2571ee368575896080755967c9b46ca800c617b`
- 兼容修复commit：`92e2e0dde67101ffa10daf7ac9f3319692bdefe4`
- Zeabur deployment：`6a87ea1b29f0931a12bfb25b`，状态`RUNNING`
- 后台样卡任务：`launchreport-12df0a22e5cd`，状态`success`
- 业务写入：0；技术发送回执写入：2；最终回执：`sent`
- 飞书消息回读：`msg_type=interactive`、未删除，回退摘要标题为`KOL集中宣发任务日报 · 2026-08-21`。
- 前端比例修复commit：`48ecdae6fca1e175399f29e3462f962f2f3115c6`，已推送并由Zeabur记录为`kol-auto`生产部署成功；`/health=ok`。
- 群发环境开关：`KOL_LAUNCH_DAILY_GROUP_ENABLED=1`，使用单变量新增，未覆盖其他Zeabur环境变量。
- 群发开关部署：`6a87f41da158dec40572653b`，状态`RUNNING`，部署后`/health=ok`。
- 17:15工作流：`3GDllutHPUNPEDHs / KOL Launch - Daily Report (17:15 BJ)`，`Asia/Shanghai + 15 17 * * *`，active。
- 定时任务链：异步启动群日报 → 等待12分钟 → 按`job_id`回读 → 校验任务成功、卡片结构合格、群发恰好1张、业务写入0。
- 启用后只读验证任务：`launchreport-170b75618057`，2项活动，`validation.ok=true`，未发卡，业务写入0。
- 部署脚本：`scripts/upsert_launch_daily_report_workflow.ps1`。脚本固定生产workflow ID并全分页查重，只复制kol-auto唯一Authorization；更新失败会恢复原工作流内容与原启用状态，任何未知触发器、重复边或额外出入边都会拒绝部署。

## 验证

- 日报/路由聚焦测试：42 passed + 3 subtests。
- 活动相关测试：228 passed + 3 subtests。
- 全仓：494 passed；仅既有`test_zeabur_watchdog`旧日期fixture失败，与日报卡无关。
- 两位独立复审最终均无P0/P1。
- 比例修复聚焦测试：18 passed。
- 比例修复后全仓：495 passed；仍仅既有`test_zeabur_watchdog`旧日期fixture失败，与卡片改动无关。
- 修复版已在飞书网页版亲自打开并截图：两个活动均在首屏完整显示，卡片内部无大面积空白；卡片右侧空白是聊天背景，不是卡片内容区。

## 剩余待办

- P0：首次真实群发后回读当前KOL运营群里的消息并截图，确认生产机器人、标题、颜色、进度条、留白和活动数量均正确；不以工作流`success`替代视觉验收。
- P0：按原计划完成食人花当日只读验证；该验证只由Codex回查，不创建飞书提醒卡。
