# 客服缺订单号/站点自动补询交接记录

日期：2026-07-08

## 问题

客户只描述产品故障但没有提供订单号或 Amazon 国家站点时，系统此前会把工单兜底发给 Frankie 判责。Frankie 仍然无法判断该派给哪个运营，实际缺的是客户侧可路由字段。

## 改动

- 工单台新增状态选项：`待客户补充`。
- 工单台新增字段：
  - `信息缺口`
  - `沟通历史摘要`
  - `最近客户补充`
  - `最近出站Message-ID`
  - `补充信息请求时间`
  - `补充信息次数`
- `app/cs_ingest.py`：
  - 邮箱工单缺订单号/国家站点时进入 `待客户补充`，不进入普通派卡。
  - 生成英文补询模板，只安抚并索要订单号、Amazon 国家站点或订单截图，不承诺换新/退款/补发。
  - 记录入站 `In-Reply-To / References`，并支持通过 `最近出站Message-ID`、原始线程 ID、同客户唯一待补充工单归并客户后续回复。
  - 客户补充订单号后优先领星反查；客户只补国家站点时按站点临时路由并保留 `缺订单号` 信息缺口。
  - 自动补询超过 `CS_INFO_REQUEST_MAX` 仍无法判定时，转为 `待派 + 待定·客户补充仍不足`，由卡片显示 `待判责`。
- `app/cs_dispatch.py`：
  - 运营卡片新增“接手上下文”，展示历史、客户补充、仍缺字段、最近出站 Message-ID。
- `.env.example`：
  - 新增 `CS_INFO_REQUEST_LIVE=0`
  - 新增 `CS_INFO_REQUEST_DRY_RUN_TO=`
  - 新增 `CS_INFO_REQUEST_MAX=2`

## 安全开关

自动补询默认不真发客户：

```env
CS_INFO_REQUEST_LIVE=0
CS_INFO_REQUEST_DRY_RUN_TO=
CS_INFO_REQUEST_MAX=2
```

上线建议：

1. 先设置 `CS_INFO_REQUEST_DRY_RUN_TO=frankiepan501@gmail.com`，触发一条缺信息工单，检查测试邮箱 raw content 和线程头。
2. 验证无换新/退款/补发承诺、无占位符、主题与线程正确。
3. 删除 dry-run 邮箱并设置 `CS_INFO_REQUEST_LIVE=1`，再让 cron 真发客户。

## 验证

```powershell
.venv\Scripts\python.exe -m py_compile app\cs_ingest.py app\cs_dispatch.py
.venv\Scripts\python.exe -m unittest tests.test_cs_info_request tests.test_cs_dispatch_card tests.test_cs_resources
```

结果：14 tests passed。

## 剩余待办

- P1：把 Discord 工单频道也接入同类补询与归并。
- P1：把客服卡片长正文编辑迁到表格/草稿编辑入口，卡片只保留判责、改派、发送等明确动作。
