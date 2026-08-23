# Lessons: Dave 可选竞品证据与七层词源灰度

## Candidate Lessons

| Time | Symptom | Cause | Prevention | Promote to |
|---|---|---|---|---|
| 2026-08-23 | 一个活动的竞品被误认为以后所有活动默认竞品 | 灰度实例参数与通用任务模型没有显式分开 | 活动必须显式保存竞品模式/品牌/证据版本；测试覆盖换竞品与无竞品两种反例 | project docs / knowledge concept |
| 2026-08-23 | 关键词任务可持续增加，但合格候选仍为0 | 把任务数或有效邮箱当成业务产出 | 以合格可触达候选为主指标；低产来源自动冷却、换源并报告排除原因 | project docs / AGENTS candidate |
| 2026-08-23 | 灰度逻辑在真实验证前进入共享自治入口 | 把“已有实现”误当成“已可生产复用” | 新策略先用显式pilot开关和单活动白名单隔离；真实验收通过后才迁移共享入口 | project docs / engineering handoff |
| 2026-08-23 | 飞书记录存在却被报成“竞品帖子不存在” | `1254607 Data not ready`临时错误被包装成404式业务结论 | 区分“记录不存在”和“数据暂未就绪”；只读长任务采用可重试状态并保留原错误码 | project docs / Feishu candidate |
| 2026-08-23 | 20名作者资料补全却重算近3000条证据，任务耗时失控 | 样本回读与全证据验证耦合 | 复用已完成、版本一致的证据样本，只重查会变化的主页/邮箱/主表状态 | project docs / engineering handoff |
| 2026-08-23 | YouTube主页可打开但国家和邮箱被解析为空 | 页面内存在多个`aboutChannelViewModel`，首个可能是空壳 | 遍历全部模型并选择字段最完整者；至少用两条真实主页做解析回归 | project docs / parser tests |
| 2026-08-23 | 高证据作者可能已经在KOL主表、媒体人主表或已评测目标产品 | 仅按原始作者ID判断“新对象”不足 | 新增前必须同时查两张主表的身份键、邮箱和目标产品历史；命中后归并旧关系 | project docs / KOL SOP |

## Failed Attempts

| Attempt | What was tried | Result | Do not repeat because |
|---|---|---|---|
| 1 | 只给 Dave 增加一批固定备用词 | 设计阶段否决 | 固定词再次耗尽后仍需人工介入，不能替代重复决策 |
| 2 | 直接运行裸`python -m unittest` | PowerShell找不到`python` | 本机脚本按全局规范固定使用`C:\tmp\py311-embed\python.exe` |
| 3 | 嵌入式Python直接discover当前仓库 | 导入了`C:\tmp\ml-data-sync\app`同名包 | 内联runner先把当前仓库插入`sys.path[0]`再discover，避免同名包污染 |
| 4 | Browser Harness新开页面后立即调用页面函数 | 首次出现脚本尚未可见的瞬时`ReferenceError` | 先回读页面文字/函数状态，或通过真实按钮触发并读DOM结果，不把工具时序误判成业务失败 |
| 5 | 直接给嵌入式Python设置`PYTHONPATH`后重跑 | `python311._pth`启用了隔离模式，仍导入`C:\tmp\ml-data-sync\app` | 用内联runner在进程内把当前仓库插入`sys.path[0]`，不要把模块污染误判成测试失败 |
| 6 | 用默认`python -m pytest`直接跑最终回归 | 再次被`C:\tmp\ml-data-sync\app`同名包抢占，收集阶段报ImportError | 本仓库固定用内联runner先插入当前worktree，再调用pytest；最终108项通过 |

## User Corrections

| Correction | Correct rule | Where to persist |
|---|---|---|
| Dave 使用 NYXI 只是一次灰度 | 每个活动独立选择竞品模式与品牌，允许无竞品 | project docs / workspace concept |

## Secret/Privacy Review

- [x] Contains no API keys, passwords, tokens, cookies, private auth blobs, or raw customer secrets.
- [x] Contains no unnecessary raw transcript.
- [x] Durable enough to help future tasks.
# 2026-08-23 Browser Harness 本机连接

- 现象：隔离Chrome已在9222端口运行，但Browser Harness默认发现路径的WebSocket握手超时；`127.0.0.1:9222/json/version`返回404，而`localhost:9222/json/version`正常返回Chrome DevTools信息。
- 最小恢复：本轮显式设置`BU_CDP_URL=http://localhost:9222`后执行成功。
- 业务影响：仅影响本机可视化原型验收，不影响KOL生产服务；不要把此工具连接错误误判为词源逻辑失败。
