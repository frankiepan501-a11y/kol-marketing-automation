# UpPromote 导出与 KOL ROI 回填 SOP - 2026-07-16

## 背景

当前 UpPromote 使用免费套餐，不能生成 API key。因此 P1 口径是：Shopify 订单、样品成本和毛利继续走 API/现有毛利报表链路；UpPromote 只由运营导出联盟归因明细，系统读取后自动比对 KOL 映射。

## 运营只需要导出 3 类表

| 表 | 必须性 | 路径 | 用途 |
|---|---:|---|---|
| Referrals / Orders 明细 | ✅ | UpPromote Affiliate → Referrals | 每笔联盟订单的 affiliate、order_id、sales、commission |
| Approved payments / Commissions | ✅ | UpPromote Affiliate → Payments | 已批准佣金、付款信息、总销售额 |
| Top affiliates / Affiliates 列表 | 🟡 | UpPromote Affiliate → Affiliates / Analytics | 补 affiliate_name/email；零销售只做审计，不派卡 |

## 文件命名

| 文件类型 | 命名格式 | 示例 |
|---|---|---|
| referrals | `uppromote_referrals_YYYY_MM_DD.xlsx` | `uppromote_referrals_2026_07_16.xlsx` |
| payments | `uppromote_approved_payments_YYYY_MM_DD.xlsx` | `uppromote_approved_payments_2026_07_16.xlsx` |
| affiliates | `UpPromote-Top-Affiliates-*.xlsx` | `UpPromote-Top-Affiliates-170430.xlsx` |

导出后放到：

```text
D:\Users\Administrator\Desktop\ROI归因
```

## 系统处理规则

| 数据 | 自动处理 | 缺口处理 |
|---|---|---|
| affiliate_email 命中 KOL/媒体人主表邮箱 | ✅ 强匹配 | 不派卡 |
| affiliate_name 命中 KOL/媒体人名称 | ✅ 中匹配 | 不派卡，但后续建议补邮箱 |
| 有销售/佣金但未匹配 | 🟡 写缺口台 | 发运营卡确认 KOL |
| Top affiliates 零 referrals/零 sales | 🟢 只保留 CSV | 不打扰运营 |

## 运营回填方式

| 场景 | 正确动作 | 禁止动作 |
|---|---|---|
| 收到缺口卡 | 在卡片里选择 KOL 或输入正确 KOL 名/邮箱，点确认 | 不打开 `KOL归因映射表` 手改 |
| 找不到对应 KOL | 在卡片备注写“新 KOL / 需入库” | 随便选相似名称 |
| UpPromote 名称错拼 | 卡片里填写正确 KOL 名称 | 改 UpPromote 历史数据 |

## 执行命令

```powershell
$env:PYTHONIOENCODING='utf-8'
.\.venv\Scripts\python.exe scripts\kol_roi_p1_gap_scan.py
.\.venv\Scripts\python.exe scripts\kol_roi_p1_gap_scan.py --write-gaps
```

默认不写表，只输出 CSV。只有确认要进入运营卡片队列时才加 `--write-gaps`。
