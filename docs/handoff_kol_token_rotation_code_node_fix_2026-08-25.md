# KOL token 轮换遗漏 Code 节点修复 — 2026-08-25

## 结论

KOL 审核卡「退回重生」失败不是运营操作问题。2026-08-24 的内部 token 轮换只更新了 n8n HTTP Request 节点，遗漏了 Code 节点 `jsCode` 里的静态 Bearer token，导致 KOL 服务返回 `401 Invalid token`。

## 生产证据

- 对应 n8n execution：`1011494`。
- 业务输出：`重生调用异常: [object Object]`。
- KOL 服务健康和 `/draft/regen` 路由正常。
- 旧 Code 节点 token 的只读鉴权探针返回 401。

## 生产修复

工作流：`YjTXaoWAcy89xZpT`（飞书事件中心 - Event Hub）。

已同步 3 个 Code 节点：

- `Draft Action Handler`
- `TP Action Handler`
- `Card Resend Handler`

全量扫描还发现客服助手回调工作流 `4K9HeJsyp5iHCsTj` 的 `Handle` 节点也使用同一个旧 token：修改前指纹 `ae8db86d87e8`，与 Zeabur 当前指纹 `4e6e723ec350` 不一致。Frankie 已在根因确认后明确要求“现在修复”；该节点属于同一服务、同一轮换遗漏、同一按钮回调风险面，因此按同一授权做最小同步，没有改业务逻辑、接收人或卡片内容。

并把 `Draft Action Handler` 的错误格式从 `String(e)` 改为优先显示安全的 response body/message，避免再次出现 `[object Object]`。

发布后：

- active 状态保持 `true`。
- 节点数保持 76。
- 连接关系哈希保持不变。
- 发布版本：`b42237a4-4193-441d-bacf-faa8ab6eef5d`。
- 客服回调版本：`5684953d-c5d2-499e-8d92-5c2f2b603c9c`；active 保持 true、节点数保持 3、连接哈希保持不变。
- 只读鉴权探针连续两次通过，未发送真实邮件。

## 防复发代码

`scripts/rotate_kol_internal_token_20260824.ps1` 现在同时覆盖：

- HTTP Request 节点的 Authorization header。
- 调用 `kol-auto.zeabur.app` 的 Code 节点静态 Bearer token。

轮换前会检查 HTTP/Code 节点 token 是否与 Zeabur 当前 `INTERNAL_TOKEN` 一致；dry-run 输出匹配/漂移数量和不可逆指纹，发现漂移不会被静默忽略。Code 扫描只认生产已确认的直接请求语法 `HR({...})` / `this.helpers.httpRequest({...})`，并要求同一请求对象内同时存在 kol-auto 的 `url` 属性和静态 Authorization；属性前后顺序均可，兄弟对象、函数代码块、注释中的 URL 和其他服务 token 都不会被改。新增其他请求写法时必须先补 fixture。轮换后会读回两类节点，分别核对预期数量和新 token，并检查节点数、连接关系与 active 状态。Code-only 工作流也有专门自测覆盖。

## 验证

```powershell
& C:\tmp\py311-embed\python.exe tests\test_rotate_kol_internal_token_script.py
& .\scripts\rotate_kol_internal_token_20260824.ps1 -SelfTest
& .\scripts\rotate_kol_internal_token_20260824.ps1
```

结果：

- 自测覆盖 1 个 HTTP 引用 + 1 个 Code 引用、Code-only 工作流、URL/headers 反序、顶层/代码块/共同父对象/函数块/注释反例和读回结构校验，并确认不改同节点内的无关 token。
- 生产 dry-run 成功识别 53 个工作流、62 个 token 引用（58 个 HTTP、4 个 Code）；匹配 62、漂移 0，未打印 secret、未写生产。

## 剩余验证

- 让 Frankie 或运营在原失败卡上重试一次「退回重生」，确认新草稿卡正常到达。
- 真实点击后读回旧草稿/新草稿状态和原卡 PATCH 结果；不要用“n8n execution success”单独判定业务成功。
