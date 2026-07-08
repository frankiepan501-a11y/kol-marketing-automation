# 客服卡片兜底路由与标题脱敏修复记录

日期：2026-07-08

## 问题

一张生产客服卡片兜底发给 Frankie 时，卡片标题直接展示了邮箱原始 Message-ID：

`CSF-<CAFYLfD+G=ssqGGMQ7ZpP8-DCcv3kngR63=fWaxBgfZrDEDLmfg@mail.gmail.com>`

同时卡片没有说明为什么发给 Frankie，容易被理解成测试卡或普通待回卡。

## 根因

- `工单ID` 对邮箱工单复用了原始 Message-ID，用于邮件线程追踪是合理的，但不适合直接渲染在飞书卡片标题。
- `cs_dispatch` 在无法解析 `分配运营` 时会兜底发给 Frankie，但卡片仍显示 `客服·待回`，没有标明这是“待判责/待分配”的兜底场景。
- 卡片没有按 Codex 飞书卡片设计 memory 把决策关键上下文放进卡内：为什么给我、当前缺哪个路由信息、下一步应判定什么。

## 改动

- 修改 `app/cs_dispatch.py`：
  - 新增 `_ticket_label()`，卡片展示短工单标签，例如 `CSF · recxxx`，不再暴露原始 Message-ID。
  - 新增 `_card_status_label()`，无法解析负责人时标题显示 `客服·待判责`。
  - 新增 `_routing_notice_md()`，卡片正文明确说明当前负责人无法解析，所以兜底发给 Frankie 判定站点/负责人。
  - 结果卡也统一使用短工单标签，避免后续处理态卡片再次暴露原始 ID。
- 新增 `tests/test_cs_dispatch_card.py`：
  - 覆盖邮箱 Message-ID 不出现在卡片标题/正文。
  - 覆盖待定负责人显示 `待判责` 且带兜底说明。
  - 覆盖已知负责人仍显示正常 `待回`，不插入兜底说明。

## 线上处理

- 已对现有卡片 `om_x100b6bef98fa7488c1b90dbc69073f0` 原卡 PATCH：
  - 标题改为 `🟠 [客服·待判责] FUNLAB · Firefly Pro Controller · 未知`
  - 卡内保留 `CSF · recvoKhTOB7ZJa`
  - 原始 Message-ID 不再渲染
  - 增加兜底路由说明

## 验证

```powershell
.venv\Scripts\python.exe -m py_compile app\cs_dispatch.py
.venv\Scripts\python.exe -m unittest tests.test_cs_dispatch_card tests.test_cs_resources
```

结果：9 tests passed。

## 剩余风险

- 这次只修复标题脱敏和兜底判责表达，没有重构整张卡片布局。
- 后续 P1 可继续按飞书卡片设计规范，把“长正文修改”从卡片输入框迁到表格/草稿编辑入口，并把按钮区做成更清晰的横向动作组。
