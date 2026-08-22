# Lessons: Dave 可选竞品证据与七层词源灰度

## Candidate Lessons

| Time | Symptom | Cause | Prevention | Promote to |
|---|---|---|---|---|
| 2026-08-23 | 一个活动的竞品被误认为以后所有活动默认竞品 | 灰度实例参数与通用任务模型没有显式分开 | 活动必须显式保存竞品模式/品牌/证据版本；测试覆盖换竞品与无竞品两种反例 | project docs / knowledge concept |
| 2026-08-23 | 关键词任务可持续增加，但合格候选仍为0 | 把任务数或有效邮箱当成业务产出 | 以合格可触达候选为主指标；低产来源自动冷却、换源并报告排除原因 | project docs / AGENTS candidate |

## Failed Attempts

| Attempt | What was tried | Result | Do not repeat because |
|---|---|---|---|
| 1 | 只给 Dave 增加一批固定备用词 | 设计阶段否决 | 固定词再次耗尽后仍需人工介入，不能替代重复决策 |
| 2 | 直接运行裸`python -m unittest` | PowerShell找不到`python` | 本机脚本按全局规范固定使用`C:\tmp\py311-embed\python.exe` |
| 3 | 嵌入式Python直接discover当前仓库 | 导入了`C:\tmp\ml-data-sync\app`同名包 | 内联runner先把当前仓库插入`sys.path[0]`再discover，避免同名包污染 |
| 4 | Browser Harness新开页面后立即调用页面函数 | 首次出现脚本尚未可见的瞬时`ReferenceError` | 先回读页面文字/函数状态，或通过真实按钮触发并读DOM结果，不把工具时序误判成业务失败 |

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
