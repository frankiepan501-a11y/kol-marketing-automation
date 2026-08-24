# KOL Launch 跨重部署持久预算交接

## 结论

集中宣发的 DeepSeek 每日预算已从容器 `/tmp` 软计数升级为“服务独立持久卷 + SQLite 原子计数”。重新部署不会重置当天额度；同一服务短暂出现多个进程时，也只能共同使用一份当日额度。

## 问题与根因

- P0 的单任务 2 次、单轮 12 次是硬限制，但每日 60 次写在 `/tmp/kol_launch_model_budget.json`。
- Zeabur 重新部署会替换容器，`/tmp` 随之清空，导致当天预算重新从 0 开始。
- 原 JSON 方案只有进程内线程锁，无法覆盖新旧容器短暂并存或多进程同时预占额度。
- `budget_day` 只在对象创建时计算，跨北京时间零点的长任务不会自动换日；快照也看不到最近拒绝原因。

## 本次改动

- `app/enrich_model_guard.py`
  - 状态路径以 `.sqlite/.sqlite3/.db` 结尾时启用 SQLite。
  - 用 `BEGIN IMMEDIATE` 事务完成“读取当日计数 → 判断上限 → 加 1”，避免并发覆盖。
  - 每次 `reserve()` 和 `snapshot()` 重新计算北京时间日期。
  - 增加 `budget_day`、`state_backend`、`state_available`、`last_denial_reason`。
  - SQLite 损坏、不可写或被锁超时时返回 `budget_state_unavailable`，不调用模型；上层继续确定性降级。
  - JSON 路径行为保留，因此普通 KOL 富化本轮不迁移。
- `app/config.py` / `.env.example`
  - 生产路径明确为 `/data/kol_launch_model_budget.sqlite3`。
- `tests/test_enrich_model_guard.py`
  - 覆盖独立实例、真实双进程原子预占、跨北京时间零点、熔断跨零点、不可写路径、损坏数据库和文件句柄清理。
- `docs/specs/2026-08-24-kol-launch-persistent-budget.md`
  - 固化范围、非目标、测试缝和生产验收标准。

## 生产配置

| 配置 | 值 | 业务含义 |
|---|---|---|
| Zeabur service | `kol-automation` | KOL 生产服务 |
| volume mount | `/data` | 重新部署后保留文件 |
| `KOL_LAUNCH_MODEL_STATE_PATH` | `/data/kol_launch_model_budget.sqlite3` | Launch 当日计数数据库 |
| `KOL_LAUNCH_MODEL_PER_TASK` | `2` | 单任务硬上限 |
| `KOL_LAUNCH_MODEL_PER_RUN` | `12` | 单轮硬上限 |
| `KOL_LAUNCH_MODEL_DAILY` | `60` | 北京时间当日共享上限 |
| `KOL_LAUNCH_MODEL_FAILURE_THRESHOLD` | `2` | 连续失败熔断阈值 |

挂载卷、单变量和支持 SQLite 的代码必须同时生效。仅把路径改成 `/data`、但没有挂载卷，仍然不能称为跨部署持久预算。

## 验证

- 预算聚焦测试：26/26 通过。
- Launch 相关测试：126/126 通过。
- 全仓测试：667 项中 666 项通过；唯一失败是改动前已存在的 Zeabur watchdog 日期样例，与本次预算改动无关。
- 双路独立复审最终均为 P0/P1/P2 无 finding。
- `py_compile` 与 `git diff --check` 通过；秘密扫描未发现新密钥。

## 部署与回滚

- 部署前确认 Dave 与食人花均无 `running` job；不启停 `uvBfJBtGH93FPa6w`。
- 先挂载 `/data`，再用单变量方式设置状态路径，最后部署包含本文件的源代码版本。
- 部署后用独立探针 SQLite 文件验证重新部署前后计数不变，再等待自然 cron 验收 `model_budget`。
- 若 SQLite 不可用，运行时会自动禁用模型并继续确定性降级；无需关闭 KOL 工作流。
- 如需代码回滚，保留 `/data` 卷不会影响旧版启动；旧版仍使用配置路径时会把 SQLite 当 JSON 并 fail-closed，因此回滚时应同时把 `KOL_LAUNCH_MODEL_STATE_PATH` 单变量改回 `/tmp/kol_launch_model_budget.json`。

## 剩余事项

- P0：本改动不轮换 n8n→KOL 的内部调用令牌；该令牌需在独立安静窗口同时更新服务变量和工作流请求头。
- P1：当前 Launch 每日预算完成；普通 KOL enrich 每日计数仍是单容器软预算，后续若要同等级保护可复用本方案。
- P2：SQLite 会每天保留一行计数，数据量极小；无需自动清理。
