# KOL ROI 归因 P1 交接 - 2026-07-16

## 目标

把 KOL/媒体人合作 ROI 从“人工导表后 Frankie 判断”改成“系统先自动归因，只有映射缺口才让运营通过卡片确认”。这次 P1 先完成数据入口、映射 SSOT、缺口队列和运营命名规范。

## 进度表

| 模块 | 状态 | 结果 | 说明 |
|---|---:|---|---|
| KOL 归因映射表 | ✅ | `tblzxyUxNF7gWqJe` | 归因键 SSOT，字段覆盖 UpPromote、Shopify、Amazon Attribution |
| KOL 归因缺口处理台 | ✅ | `tbliU8GDl6SU9b4y` | 只承接系统无法自动映射的记录；运营不直接改映射表 |
| 本地缺口扫描脚本 | ✅ | `scripts/kol_roi_p1_gap_scan.py` | 读取本地导出 + KOL Base，生成 matched/gaps CSV，可选写缺口台 |
| 7 月 16 日样本扫描 | ✅ | matched 41 / gaps 57 / actionable 8 | 可执行缺口已写入缺口处理台；重复执行写入 0 条 |
| UpPromote 免费套餐处理 | ✅ | 导出明细兜底 | 无 API key 时只让运营导出 referrals/payments/top affiliates |
| Amazon 建连接规范 | ✅ | `kol_roi_amazon_attribution_sop_2026-07-16.md` | campaign/ad group/link name 必须放 KOL handle token，避免导出映射不上 |
| 交互卡回填 | 🟡 | 后端入口已就绪 | `/kol-roi/gap-card/send` + `/kol-roi/mapping/callback`；下一步 Frankie-only 灰测 |

## 新增资源

| 资源 | ID / 路径 | 用途 |
|---|---|---|
| KOL 归因映射表 | `KINabIENjak8fRsB6AHcIDALntc/tblzxyUxNF7gWqJe` | 映射 SSOT |
| KOL 归因缺口处理台 | `KINabIENjak8fRsB6AHcIDALntc/tbliU8GDl6SU9b4y` | 缺口、卡片回执、处理结果 |
| 匹配结果 CSV | `D:\Users\Administrator\Desktop\ROI归因\kol_roi_p1_matched_20260716.csv` | 当前可自动归因样本 |
| 缺口结果 CSV | `D:\Users\Administrator\Desktop\ROI归因\kol_roi_p1_gaps_20260716.csv` | 当前无法自动归因样本，含 actionable |
| 缺口扫描脚本 | `scripts/kol_roi_p1_gap_scan.py` | 可复跑；默认只输出 CSV |
| 缺口卡发送入口 | `POST /kol-roi/gap-card/send` | 默认 `dry_run=true&frankie_only=true` |
| 缺口卡回调入口 | `POST /kol-roi/mapping/callback` | n8n event-hub 转发 card action 后写映射 |

## 已验证命令

```powershell
$env:PYTHONIOENCODING='utf-8'
.\.venv\Scripts\python.exe scripts\kol_roi_p1_gap_scan.py
.\.venv\Scripts\python.exe scripts\kol_roi_p1_gap_scan.py --write-gaps
```

验证结果：

| 命令 | 状态 | 结果 |
|---|---:|---|
| dry-run 扫描 | ✅ | contacts=7266, matched=41, gaps=57 |
| 首次写缺口 | ✅ | wrote_gaps=8 |
| 二次写缺口 | ✅ | wrote_gaps=0，缺口 ID 幂等 |

## 当前缺口口径

| 来源 | 总缺口 | 可执行缺口 | 写入缺口台 | 说明 |
|---|---:|---:|---:|---|
| UpPromote | 43 | 4 | 4 | 有成交/佣金但 KOL 主表未匹配 |
| Amazon Attribution | 14 | 4 | 4 | 有点击/成交信号但 campaign 未匹配 |
| 零数据候选 | 49 | 0 | 0 | 保留 CSV 审计，不派卡打扰运营 |

## 缺口回填设计

| 节点 | 执行动作 | 合格标准 |
|---|---|---|
| 1. 系统扫描 | 导出表 + Shopify/Amazon API 数据进入归因扫描 | 自动命中直接写 ROI；缺口进入 `KOL归因缺口处理台` |
| 2. 发运营卡 | 每条可执行缺口发交互卡，候选项 + 输入框 | 运营只在卡片选/填，不直接打开映射表 |
| 3. 回调处理 | `value.action=kol_roi_map_confirm` | 校验 `gap_id`、`mapping_key`、`operator`、`message_id` |
| 4. 写映射 | 后端写 `KOL归因映射表` | 幂等：同映射键重复提交不新增脏记录 |
| 5. 关闭缺口 | 更新缺口台状态 + PATCH 原卡 | 原卡显示已处理，避免重复点击 |
| 6. 重跑归因 | 下一次扫描自动吃新映射 | 同一缺口不再派卡 |

## P1 后续待办

| 优先级 | 待办 | 阻塞点 | 建议处理 |
|---:|---|---|---|
| P1 | Frankie-only 灰测归因缺口卡 | 需要确认使用聪哥 3 号 event-hub 还是单独 webhook | 先发 1 条 UpPromote + 1 条 Amazon 测试卡给 Frankie |
| P1 | 把 Amazon SOP 发给亚马逊运营 | 已定位群 `oc_26e58d95b78670ed10a8bf4373da81f1`，但当前 lark-cli bot 不在群，发送返回 230002 | 把发送 App 加入群，或指定负责人私聊后重发 |
| P2 | 把 `sales_attribution.py` 接入新映射表 | 现在仍优先读 KOL 主表字段 | 先不改生产归因，等卡片灰测通过后迁移 |
| P2 | Shopify 成本/毛利收口 | Shopify 订单可 API 拉，成本沿用独立站毛利报表方式 | 后续补毛利字段，不让运营导出 Shopify 订单 |

## 系统化判断

| 框架 | 判断 |
|---|---|
| 企业 4 层 | 属于 L3 交接界面 + L4 系统审计：运营只确认缺口，不参与表结构和归因逻辑 |
| 8 级赚钱模型 | 目标是 L7 系统复制：ROI 计算不依赖 Frankie 逐单判断 |
| 真伪自动化 | 替代的是“每次导出后人工判断 KOL 对应关系”这个重复决策，不是单次报表整理 |
