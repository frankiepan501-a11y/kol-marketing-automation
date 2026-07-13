# 客服客户原始证据附件 P0 交接

## 背景

运营反馈：客服邮件进入 AI 流程后，客户原邮件里的图片、视频、截图等证据不能只被 AI 概括。工厂或运营接手时需要看到原始证据，否则无法判断故障和继续沟通。

本次 P0 目标是锁住 L3 交接界面：客户邮件 -> 原始证据 -> 工单记录 -> 客服卡片。AI 摘要只能辅助阅读，不能替代原始文件。

## 本次改动

- `app/cs_ingest.py`
  - 解析网易 IMAP 邮件 MIME 附件，保存图片、视频、PDF、zip 等客户原始证据。
  - 解析 Zoho 邮件附件信息和下载接口，兼容 Powkong 邮箱来源。
  - 解析 HTML/纯文本正文里的证据链接，例如 Shopify CDN、Google Drive、`.mp4`、`.jpg`、`.pdf`。
  - 上传可保存附件到飞书 Bitable 附件字段，保留文件元数据到 JSON 字段。
  - 新增单条回放函数 `backfill_evidence(record_id, dry_run=False)`，支持对旧工单补跑证据提取。
- `app/cs_dispatch.py`
  - 客服卡片新增“客户证据附件”区块。
  - 展示附件状态、文件名、类型、大小、跳过原因、外部证据 URL、工单记录链接。
  - `无附件` 也显式展示，避免运营误判为系统没检查。
  - 新增 `send_preview_card(record_id)`，只发测试预览卡，不改变工单状态。
- `app/main.py`
  - 新增 `POST /cs/evidence/backfill`。
  - 新增 `POST /cs/evidence/preview-card`。
- `.env.example`
  - 新增证据附件字段名、单文件大小上限、允许类型配置。

## 飞书字段

客服工单表：`J2fibLgBZaLGTNsQOPHcQXLonZe / tblAhXMA9uDbGEMS`

- `客户证据附件`，附件字段，`fld88HBOM2`
- `客户附件JSON`，文本字段，`fldlDNGDIn`
- `客户附件摘要`，文本字段，`fld22160xi`
- `客户附件数量`，数字字段，`fld4l6wF81`
- `客户附件状态`，单选字段，`fldlupO7c4`
  - `无附件`
  - `已保存`
  - `部分跳过`
  - `保存失败`

## 当前真实样例回放

工单：`recvp6Ui7VtpiD`

客户：`mailer@shopify.com`

订单：`701-2298071-8514637`

原始邮件内容只有 Shopify contact form 文本：

- 国家：CA
- 客户：Aisha / `aishaaltaf@gmail.com`
- 订单号：`701-2298071-8514637`
- 问题：turbo switch glitching，希望能否换货

回放结果：

- `found_message=true`
- `attachment_count=0`
- `saved_count=0`
- 飞书字段已写入：
  - `客户附件状态=无附件`
  - `客户附件数量=0`
  - `客户附件摘要=未检测到客户图片/视频/PDF附件。`
  - `客户附件JSON=[]`

已发测试预览卡：

- `message_id=om_x100b6a7318346110b306b9c663a51ba`
- 预览卡不改变原工单状态，不给客户发信。

## 验证

```powershell
.\.venv\Scripts\python.exe -m py_compile app\cs_ingest.py app\cs_dispatch.py app\main.py
.\.venv\Scripts\python.exe -m unittest tests.test_cs_info_request tests.test_cs_dispatch_card tests.test_cs_resources
```

测试覆盖：

- 邮件图片附件会被提取并标记为“图片”。
- HTML 邮件里的客户视频 URL 会进入证据 JSON。
- 客服卡片会展示已保存图片/视频。
- 客服卡片会展示“无附件”检查结果。

## 剩余风险

- P0 不做 AI 视频理解或图片识别，只保留原始证据。
- 飞书单文件上传默认 20MB，超出会标记为跳过，需要后续 P1 设计更大文件的对象存储或网盘策略。
- Discord 客服附件归并未在本次 P0 覆盖。
- Zoho 附件接口在不同账号返回结构可能有差异，当前为防御式解析，需要真实 Powkong 附件邮件继续抽检。
