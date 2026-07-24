# AMZ Europe P0 50件验证启动节点交接

## 结论

- 已新增 50件验证启动节点：`POST /cs/amz-validation50/start`。
- P0 默认处理德国站两条已通过候选：
  - `recvq1QtafnVjX` / `B0CH1817WW`
  - `recvq1QtUEEcXv` / `B0D1CLBFD9`
- 该节点不做人工审批，只做启动写回和启动卡通知。合规普通风险点已经在上一节点自动留档，50件验证阶段只看真实采购、物流、上架、销售、退货和适配投诉。

## 数据表

- Base token: `UvNcbvWufaPMSvseOogcBhbFn1y`
- Table ID: `tblrIPsxm3E8ZCXn`
- 使用字段：
  - 读取：`ASIN`、`产品中文名`、`Amazon链接`、`样本ASIN主图URL`、`采购成本RMB`、`1688供应商链接`、`三方案推荐履约`、`A/B/C物流成本RMB`、`A/B/C毛利RMB`、`A/B/C毛利率%`、`A/B/C货运比`、`合规闸结论`、`当前状态`、`综合结论`、`50件验证状态`、`侵权风险说明`。
  - 写回：`50件验证状态=进行中`、`当前状态=待50件验证`、`综合结论=50件验证`、`下一步动作=发起50件验证`、`人审备注`追加系统启动记录。

## 进入条件

单条候选必须同时满足：

- `合规闸结论=Go`
- `当前状态=待50件验证`
- `综合结论=50件验证`
- `采购回填状态=已回填`
- `采购成本RMB` 有值
- `1688供应商链接` 有值
- `50件验证状态` 不能已经是 `进行中` 或 `已通过`

不满足的记录不会写回，会在接口返回 `skipped_records` 说明原因。

## 发送卡内容

启动卡只做信息交接，不含表单和审批按钮：

- 产品主图
- Amazon Listing 链接
- 主图原图链接
- 候选表记录链接
- 1688 供应商链接
- 采购成本、包装尺寸、重量、件数、FBA费、佣金
- A/B/C 三渠道经济性
- 50件粗算：采购额 + 推荐渠道物流成本，明确不含平台费/VAT
- 50件验证要看：采购同款、上架文案、实际物流、7/14/30 天订单/退货/适配投诉
- 系统注意点：上一节点自动风险扫描留档的重点问题

## Endpoint

```http
POST /cs/amz-validation50/start?mode=dry_run&record_ids=recvq1QtafnVjX,recvq1QtUEEcXv&batch_id=AMZ-DE-VAL50-20260724-P0&qty=50
Authorization: Bearer <INTERNAL_TOKEN>
```

参数：

- `mode=dry_run|commit`：dry-run 只返回卡和 would_update；commit 才写候选表并发送卡。
- `record_ids`：逗号分隔。不传时走默认 P0 两条。
- `batch_id`：默认 `AMZ-DE-VAL50-20260724-P0`。
- `qty`：默认 `50`。
- `frankie_only`：默认 `true`。

## 本地验证

```powershell
C:\tmp\py311-embed\python.exe -m py_compile app\amz_validation50.py app\main.py
C:\tmp\py311-embed\python.exe scripts\amz_validation50_selftest.py
```

单测建议使用当前仓库优先的 inline runner，避免本机旧 `C:\tmp\ml-data-sync\app` 包污染：

```powershell
@'
import os, sys, unittest
root = os.getcwd()
sys.path = [root] + [p for p in sys.path if 'C:\\tmp\\ml-data-sync' not in p and p != root]
suite = unittest.defaultTestLoader.loadTestsFromName('tests.test_amz_validation50')
result = unittest.TextTestRunner(verbosity=1).run(suite)
raise SystemExit(0 if result.wasSuccessful() else 1)
'@ | C:\tmp\py311-embed\python.exe -
```

## 后续节点边界

50件验证的下一节点不是“再审核一次”，而是数据复盘：

- 7 天：是否出单、广告/自然曝光、是否有明显适配咨询。
- 14 天：转化率、退款/退货、物流妥投/入仓成本偏差。
- 30 天：是否加量、是否改套装、是否淘汰。

