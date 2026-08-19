# KOL 活动竞品证据与参与记录实施计划

状态：待 Frankie 确认执行；本文件本身不授权生产写入或真实外联

需求规格：`docs/superpowers/specs/2026-08-19-kol-campaign-evidence-participation-design.md`

## 1. 实施目标

在现有集中上稿影子活动和只读候选预览上增量实现：

1. 活动创建时支持“发起新分析 / 引用历史证据 / 不使用竞品证据”三种模式。
2. NYXI 证据只影响戴夫活动的 KOL 候选排序。
3. 新建活动参与记录，保存每位 KOL/媒体人的入围依据和名单版本。
4. KOL 与媒体人分别锁定、分别恢复，一类失败不影响另一类。
5. 全部预览、回放和影子试跑不创建任务、草稿、卡片、寄样或真实邮件。

执行形态：确定性 Python/FastAPI 服务 + 飞书 Base；不使用多 Agent 做线上判断。人工确认只放在证据确认、表结构正式写入、活动名单锁定和未来真实外联四个位置。

## 2. 本期安全边界

- 新增两个总开关，默认均为关闭：
  - `LAUNCH_EVIDENCE_ENABLED=0`：关闭时只保留现有按产品预览，不接受活动证据写入。
  - `LAUNCH_PARTICIPATION_WRITE_ENABLED=0`：关闭时名单锁定接口返回拒绝，参与表写入为 0。
- 预览和单条回放无论开关状态都必须返回 `read_only=true / writes=0`。
- 参与状态本期只允许 `锁定准备中 / 已入围 / 已取消`。
- 禁止创建 KOL/媒体人任务、邮件草稿、运营卡片、寄样单或发送邮件。
- `auto_send`、`reply_monitor`、`followup`、`ship_recon`、`sla_check` 不在本期改动范围。
- 真实开发信仍需未来单独设计一次性放行记录；本计划不实现。

## 3. 文件改动总览

| 文件 | 动作 | 目的 |
|---|---|---|
| `app/config.py` | 修改 | 增加活动、节点、竞品帖子、竞品事件、参与表和功能开关配置 |
| `app/launch_evidence.py` | 新建 | 三种证据模式、状态转换、版本校验、合法组合验证 |
| `app/launch_competitor_evidence.py` | 新建 | 稳定身份匹配、P75、长期合作和 A/B/C 证据等级 |
| `app/launch_candidate_preview.py` | 修改 | 从活动读取产品和证据，输出活动排序与回放依据 |
| `app/launch_participation.py` | 新建 | 完整名单锁定、按对象类型提交版本、回滚与恢复 |
| `app/main.py` | 修改 | 暴露证据配置、状态转换、活动预览/回放、名单锁定接口 |
| `scripts/apply_launch_evidence_schema.py` | 新建 | 表结构 dry-run、幂等写入、写后回读和恢复报告 |
| `tests/test_launch_evidence.py` | 新建 | 三种模式、状态机和配置版本测试 |
| `tests/test_launch_competitor_evidence.py` | 新建 | 身份匹配、P75 和戴夫隔离测试 |
| `tests/test_launch_participation.py` | 新建 | 完整替换、对象隔离、回滚和恢复测试 |
| `tests/test_launch_candidate_preview.py` | 修改 | 活动级预览、回放和 `writes=0` 回归测试 |
| `tests/test_launch_routes.py` | 新建 | HTTP 404/409/422、功能开关和鉴权测试 |
| `docs/launch_evidence_participation_handoff_2026-08-19.md` | 新建 | 生产事实、验证、回滚和剩余边界交接 |

## 4. 分步实施

### 任务 1：固定配置与接口合同

先写失败测试：

- 缺少参与表配置时，写接口必须拒绝，不能落到其他表。
- 两个功能开关默认关闭。
- 预览兼容旧 `product_id` 只读方式；传入 `campaign_id` 后必须以活动产品为准。
- 证据写接口必须携带 `expected_config_version`。

再修改：

- 在 `app/config.py` 增加以下配置：
  - `T_LAUNCH_CAMPAIGN`
  - `T_LAUNCH_NODE`
  - `T_COMPETITOR_POST`
  - `T_COMPETITOR_EVENT`
  - `T_LAUNCH_PARTICIPANT`
  - 两个功能开关
- 现有活动表和节点表 ID 可使用已确认的非敏感默认值；新参与表必须由迁移脚本创建并通过环境变量注入。

验证命令：

```powershell
C:\tmp\py311-embed\python.exe -m unittest tests.test_launch_routes -v
```

完成标准：默认配置下所有写接口关闭，现有只读端点不回退。

### 任务 2：建立幂等表结构迁移脚本

先写失败测试：

- 重复运行 dry-run 不产生字段写入。
- 已存在同名同类型字段时复用。
- 同名不同类型字段时整批停止。
- 参与表已存在时不得重复建表。
- 写后字段缺失或类型不符时返回失败。

再实现 `scripts/apply_launch_evidence_schema.py`：

1. 读取活动表、节点表和参与表当前结构。
2. 输出“将新增 / 已存在 / 类型冲突 / 不会删除”的 dry-run 报告。
3. `--commit` 才允许创建参与表及规格第 6、7 节字段。
4. 每次创建后立即回读字段 ID、名称和类型。
5. 输出 JSON 备份清单；本期不删除表或字段，因此回滚方式是关功能开关并停止使用新增字段。

执行顺序：

```powershell
C:\tmp\py311-embed\python.exe scripts\apply_launch_evidence_schema.py --dry-run
C:\tmp\py311-embed\python.exe scripts\apply_launch_evidence_schema.py --verify-only
# 只有 Frankie 再次确认 dry-run 报告后才允许：
C:\tmp\py311-embed\python.exe scripts\apply_launch_evidence_schema.py --commit
```

完成标准：迁移可重复运行，第二次运行新增数为 0；没有修改联系人主表、任务表和草稿表。

### 任务 3：实现证据模式和状态机

先在 `tests/test_launch_evidence.py` 覆盖：

- `发起新分析` 只能写入 `待分析`，帖子/事件为空。
- `引用历史证据` 至少一条已确认、相关、同品牌帖子。
- `不使用竞品证据` 必须清空品牌、帖子和事件，状态为 `不适用`。
- `start / submit / confirm / retry` 只接受规格允许的状态转换。
- 每次成功只把配置版本增加 1；旧版本请求返回冲突。
- `submit` 候选集合能在换审核人后读取；`confirm` 只能确认当前提交集合的子集。
- 活动最终更新失败时，新节点版本不生效，旧活动配置保持不变。

再实现 `app/launch_evidence.py`：

- 用活动级 `asyncio.Lock` 防止同一活动并发修改。
- 所有写入先完成关联帖子、事件、品牌、状态和版本预校验。
- 节点先写目标版本，活动记录最后写入；活动版本是唯一提交标志。
- 返回业务错误类型，由路由映射为 404/409/422。
- 每次读取重新验证证据是否仍存在且仍为已确认；发现漂移时禁止锁名单。

验证命令：

```powershell
C:\tmp\py311-embed\python.exe -m unittest tests.test_launch_evidence -v
```

完成标准：三条路径都有确定状态，任何失败不留下“半生效配置”。

### 任务 4：实现 NYXI 证据评分

先在 `tests/test_launch_competitor_evidence.py` 覆盖：

- 身份匹配顺序：关联 KOL → 平台+creator_id → 规范化主页 URL → 平台+Handle。
- 仅展示名相似不得匹配。
- 只采用“已确认 + 相关 + 明确合作”帖子。
- P75 按平台和内容类型分别算，样本少于 8 不授予前 25%。
- nearest-rank、并列值、曝光量为空时降级覆盖量均符合规格。
- 2 条帖子但跨度不足 90 天不算长期合作。
- A/B/C 分数为 `3000+基础分 / 2000+基础分 / 基础分+5`。
- NYXI 只对指定戴夫活动和 KOL 生效；食人花、其他活动、媒体人均为基础排序。

再实现 `app/launch_competitor_evidence.py`：

- 独立输出身份匹配结果、证据等级、阈值样本和人话理由。
- 不写 KOL 主表，不把竞品播放量复制到联系人字段。
- 不用跨平台原始曝光做同分裁决；最终同分按联系人 record_id。

验证命令：

```powershell
C:\tmp\py311-embed\python.exe -m unittest tests.test_launch_competitor_evidence -v
```

完成标准：戴夫活动 A/B/C/无证据四类样本都可稳定重算，其他活动结果不变。

### 任务 5：把活动证据接入只读预览与单条回放

先扩展 `tests/test_launch_candidate_preview.py`：

- 传 `campaign_id` 后从活动读取产品主记录，不接受外部产品替换。
- 证据未就绪时基础预览仍可返回，但 `evidence_pending=true`。
- 三种模式分别返回正确证据状态和排序版本。
- 单条回放返回身份匹配路径、帖子 ID、等级、基础过滤和重复触达决策。
- 回放已锁定记录时优先使用参与记录保存的排序快照。
- 所有预览/回放继续断言 `create_record`、`update_record` 从未调用。

再修改 `app/launch_candidate_preview.py`：

- 把当前产品上下文扩展为活动上下文。
- 复用现有国家、语言、平台、粉丝、内容风格和全局重复触达预检。
- 新开发池按 A→B→其他排序；暖关系池仍单独展示。
- 返回当前与历史排序原因，不返回完整邮箱。

验证命令：

```powershell
C:\tmp\py311-embed\python.exe -m unittest tests.test_launch_candidate_preview -v
```

完成标准：只读端点可解释“为什么这个人排在前面”，且飞书写入为 0。

### 任务 6：实现活动参与记录与完整名单锁定

先在 `tests/test_launch_participation.py` 覆盖：

- 唯一键为 `campaign_id|product_family_id|object_type|contact_record_id`。
- 名单请求是完整替换，不是追加；第二版省略的旧候选改为 `已取消/不再符合`。
- 锁定时再次执行资格与重复触达预检，不能靠直接传 contact_id 绕过。
- 排序版本不一致时写入 0 行。
- KOL 与媒体人各自使用当前名单版本和阻塞字段。
- KOL 升级或失败不改变媒体人当前名单，反向同理。
- 提交点前失败且回滚完整时，旧名单继续有效并标记 `LOCK_BATCH_RETRYABLE`。
- 恢复请求必须匹配原失败批次 ID。
- 回滚、取消或清理不完整时标记 `LOCK_BATCH_MANUAL_REVIEW`，自动重试被拒绝。
- 唯一键查到多行时标记 `DUPLICATE_PARTICIPANT_MANUAL`，不猜测保留记录。
- 参与记录最多保存 10 个排序快照；达到上限后阻止继续变更。
- 本期拒绝写入 `已批准触达` 及后续状态。

再实现 `app/launch_participation.py`：

1. 取得活动锁并回读活动、产品和该对象类型当前名单。
2. 生成完整活动预览并预校验全部 contact_ids。
3. 保存本批每条记录的旧状态和旧版本。
4. 准备新版本记录并逐条回读。
5. 更新该对象类型的活动名单版本作为提交点。
6. 清理旧版本中被省略且尚未外联的记录。
7. 失败时按规格回滚，并写入对应对象类型的阻塞信息。

验证命令：

```powershell
C:\tmp\py311-embed\python.exe -m unittest tests.test_launch_participation -v
```

完成标准：任何返回成功的名单都有唯一当前版本；任何失败都能明确自动重试或人工修复，不出现半批放行。

### 任务 7：接入 FastAPI 路由和统一错误

先在 `tests/test_launch_routes.py` 覆盖：

- 证据配置、start、submit、confirm、retry 和名单锁定都要求内部鉴权。
- 不存在返回 404，版本冲突返回 409，非法组合/状态返回 422。
- 功能开关关闭时返回 403 或 409，不写飞书。
- 预览和回放仍兼容现有调用方。
- 异常告警不得包含邮箱、完整帖子正文或 token。

再修改 `app/main.py`，新增：

- `POST /launch/campaigns/evidence/configure`
- `POST /launch/campaigns/evidence/start`
- `POST /launch/campaigns/evidence/submit`
- `POST /launch/campaigns/evidence/confirm`
- `POST /launch/campaigns/evidence/retry`
- `POST /launch/participants/lock`
- 扩展现有 preview/replay 的 `campaign_id`

完成标准：接口错误稳定、可回放，不把内部异常当成功返回。

### 任务 8：全仓回归与影子环境验证

先运行：

```powershell
C:\tmp\py311-embed\python.exe -m unittest tests.test_launch_evidence tests.test_launch_competitor_evidence tests.test_launch_candidate_preview tests.test_launch_participation tests.test_launch_routes -v
C:\tmp\py311-embed\python.exe -m unittest discover -s tests -p "test_*.py"
git diff --check
```

然后只在功能开关关闭状态部署代码，验证：

- `/health=ok`；
- 现有日常 KOL cron 无回归；
- 所有新写接口因开关关闭而拒绝；
- 旧产品级 preview/replay 仍为 `writes=0`。

完成标准：专项测试全过；全仓只允许保留已知且有证据与本次无关的失败，不新增失败。

### 任务 9：戴夫影子活动最小生产试跑

本任务需要新的明确授权后才能执行生产 Base 写入。

顺序：

1. 运行表结构 dry-run，把字段和表差异报告交 Frankie 查看。
2. 获准后执行幂等 schema commit，回读全部字段。
3. 只开启 `LAUNCH_EVIDENCE_ENABLED=1`，参与写入仍保持关闭。
4. 给 `launch-20260915-funlab-dave-ys11-5` 配置“引用历史 NYXI 证据”。
5. 分别回放 A/B/C/无证据各一位 KOL；食人花和媒体人做隔离对照。
6. Frankie 核对证据链接、排序和隔离范围。
7. 再单独确认后开启 `LAUNCH_PARTICIPATION_WRITE_ENABLED=1`，只锁定一份小型影子 KOL 名单。
8. 回读参与记录、唯一键、KOL 当前版本；确认媒体人版本未变化。
9. 立即关闭参与写开关，保留影子记录供审计。

禁止事项：不创建任务、不生成草稿、不发卡、不发邮件、不付费、不寄样。

完成标准：Dave 影子活动排序与参与记录都可回放；食人花、媒体人和日常派单没有变化。

### 任务 10：交接与修复记录

新建 `docs/launch_evidence_participation_handoff_2026-08-19.md`，至少记录：

- 生产版本和部署 ID；
- 新表/字段 ID；
- Dave 影子活动配置版本、排序版本和 KOL 名单版本；
- 专项/全仓测试结果；
- 影子试跑前后各表行数；
- 功能开关最终值；
- 回滚方法；
- 尚未授权的真实外联事项。

同时更新本项目 `.planning` 的 `task_plan.md`、`progress.md`、`findings.md` 和 `decision-log.md`。

## 5. 人审与授权闸

| 闸门 | Frankie 需要确认什么 | 确认前系统状态 |
|---|---|---|
| G0 实施计划 | 是否按本计划开始写代码 | 不改生产 |
| G1 表结构 | dry-run 的新增表/字段是否正确 | 不写生产 Base |
| G2 证据试跑 | Dave 引用哪些 NYXI 帖子 | 只读预览 |
| G3 参与名单 | 首个影子名单及对象类型 | 不创建参与记录 |
| G4 真实外联 | 名单、预算、邮件 raw、邮箱额度和一次性放行 | 本计划始终关闭真实发送 |

## 6. 回滚策略

- 代码异常：两个功能开关设为 0 并重新部署；现有日常流程继续使用原入口。
- 证据配置异常：恢复活动上一配置版本；未锁名单时无外部影响。
- 名单写入失败：按对象类型回滚；KOL 与媒体人互不连带。
- 表结构：本期只新增、不删除；回滚时停用新增字段和表，不做破坏性删除。
- 邮件：本期没有发送路径，无需客户侧补救。

## 7. 总体验收

- 三种证据模式均能配置、校验和回放。
- 新分析未就绪时可做基础预览，但不能锁名单。
- NYXI 只改变 Dave 活动 KOL 新开发池排序。
- 活动参与记录能回答“谁、参加哪次活动、为什么入围、当前是哪一版名单”。
- KOL/媒体人主表、日常派单、回复、寄样、SLA 和上稿登记不被覆盖。
- KOL 与媒体人名单版本、失败和恢复互相隔离。
- 所有本期外部动作开关最终为关闭。
