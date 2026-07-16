# Amazon Attribution 连接创建规范 - 给亚马逊运营

## 一句话要求

亚马逊运营创建 Amazon Attribution 连接时，必须让 campaign / ad group / link name 中包含“独立 KOL handle token”，并在导出或回填时保留 campaignId。否则报表导出来后系统无法稳定映射到 KOL。

## 命名格式

| 层级 | 必填格式 | 示例 |
|---|---|---|
| Campaign | `YYYYMM-品牌-产品短名-KOLHANDLE` | `202607-POWKONG-piranha2-metalfear4` |
| Ad group / Line item | `KOLHANDLE-ASIN或产品短名` | `metalfear4-B0XXXXXXX` |
| Link name / Tag name | `KOLHANDLE-渠道-国家` | `metalfear4-youtube-US` |

## KOLHANDLE 规则

| 规则 | 正确 | 错误 |
|---|---|---|
| 只能用英文/数字 | `metalfear4` | `澳Metal Fear` |
| 必须作为独立 token | `202607-piranha2-metalfear4` | `202607piranha2metalfear4` |
| 不用空格 | `dr_rulo` 或 `drrulo` | `Dr. Rulo Retro Gamer` |
| 不用中文地区简称 | `rogerdiluigi` | `澳Roger DiLuigi` |
| 一个链接只对应一个 KOL | `mumblesvideos-youtube-US` | `kol-batch-social` |

独立 token 的意思：前后用 `-` 或 `_` 分开。系统会按 token 精确匹配，避免 `robert` 误撞 `roberto`。

## 创建连接时必须记录的 4 个字段

| 字段 | 填什么 | 为什么 |
|---|---|---|
| KOL handle | 和 KOL 主表 `UTM ID` 去掉 `kol_` 后一致 | 用于导出表兜底匹配 |
| Campaign name | 按上面格式 | 给人看，也给导出 CSV 匹配 |
| Campaign ID | Amazon 后台/报表里的 ID | API 报告优先用 ID，最稳定 |
| Measurement URL | 最终给 KOL 的链接 | 后续排查该 KOL 是否用错链接 |

## 创建步骤

| 步骤 | 操作 | 检查点 |
|---:|---|---|
| 1 | 在 KOL 主表确认 KOL 名称、邮箱、主平台、UTM ID | 如果没有 `UTM ID`，先让系统生成/补齐 |
| 2 | 取 `UTM ID` 去掉 `kol_` 作为 KOLHANDLE | 例：`kol_metalfear4` → `metalfear4` |
| 3 | 在 Amazon Attribution 新建 campaign/link | 名称必须包含 KOLHANDLE 独立 token |
| 4 | 复制 measurement URL 发给 KOL | 不要把多个 KOL 共用一个 URL |
| 5 | 导出 Amazon Attribution 报表后放入 ROI 文件夹 | 文件名保留 `Amazon_Attribution` |
| 6 | 如系统发缺口卡，按卡片选择 KOL 或补 campaignId | 不要直接改映射表 |

## 导出文件要求

| 项 | 要求 |
|---|---|
| 文件编码 | 中文后台导出一般是 GBK，系统脚本已兼容 |
| 文件名 | 包含 `Amazon` 和 `Attribution` |
| 放置路径 | `D:\Users\Administrator\Desktop\ROI归因` |
| 必须保留列 | 广告活动 / campaign、点击量、购买、商品销量 |

## 例子

| KOL | UTM ID | Campaign | Link name |
|---|---|---|---|
| MetalFear4 | `kol_metalfear4` | `202607-POWKONG-piranha2-metalfear4` | `metalfear4-youtube-US` |
| Dr. Rulo Retro Gamer | `kol_drrulo` | `202607-POWKONG-piranha2-drrulo` | `drrulo-youtube-MX` |
| Mumbles Videos | `kol_mumblesvideos` | `202607-POWKONG-piranha2-mumblesvideos` | `mumblesvideos-youtube-UK` |

## 禁止做法

| 禁止项 | 原因 |
|---|---|
| `202607-食人花特供版-社媒` | 没有 KOL handle，导出后无法归因 |
| `202605-二代食人花-澳Metal Fear` | 中文地区词 + display name，不稳定 |
| 一个 Attribution link 发给多个 KOL | ROI 会混在一起，无法拆分 |
| 只保存 measurement URL，不保存 campaignId | API 报告优先按 campaignId 汇总，缺 ID 会增加人工缺口 |

## 系统如何匹配

| 优先级 | 匹配方式 | 可靠性 |
|---:|---|---:|
| 1 | campaignId 精确匹配 KOL 主表/映射表 | ✅ 强 |
| 2 | campaign/link name 中的 KOLHANDLE token | 🟡 中 |
| 3 | 名称子串弱匹配 | 🔴 仅用于提示候选，不自动写 ROI |

运营只要遵守命名格式，后续导出的 Amazon 报表就能自动接到 KOL ROI。
