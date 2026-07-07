# 客服资源真相源与真实 URL 插入方案 v2 交接记录

日期：2026-07-07

## 问题

同事反馈 FF05A 客服草稿里写了 “firmware file attached” 和 `[link]`，但当前客服发信链路只发送文本/HTML，没有附件 manifest，也没有真实下载 URL 注入。风险是客服以为系统会自动带附件或链接，实际客户收到的是占位内容或虚假附件承诺。

## 根因

- `cs_ingest` 的 AI 草稿生成没有官方资源解析层，模型会按语义自行写链接/附件话术。
- `cs_dispatch` 只做通用占位符扫描，缺少“资源需求 -> 官方 URL -> 发信正文”的一致性校验。
- 卡片没有展示资源命中、缺口和歧义，运营无法判断 FF05 多固件候选时应按哪个版本发。

## 改动

- 新增 `app/cs_resources.py`：
  - 解析 FUNLAB 官方固件页、固件手册页、How-to Video 页和 Firefly YouTube playlist。
  - 内置已核验 FF05 V454/V459、英文/中文手册、升级说明页、站内 How-to 页兜底资源。
  - 根据工单识别 `FUNLAB / Luminex / FF05 / firmware_download + firmware_manual + how_to_video`。
  - FF05 缺当前固件版本时返回两个候选并标记为“有歧义”；V453 只命中 V454，V432 只命中 V459。
  - 发信闸拦截 `[link]`、虚假附件话术、资源缺失、固件回复缺官方 URL。
- 修改 `app/cs_ingest.py`：
  - 分类后调用资源 resolver。
  - 固件类工单用确定性官方资源模板覆盖 AI 草稿。
  - 可选回写 `资源状态 / 资源需求JSON / 资源命中JSON`。
- 修改 `app/cs_dispatch.py`：
  - 卡片新增官方资源区块，展示命中 URL、适用条件、来源页面和歧义。
  - 按钮 payload 带 `action`、`resource_status`、`resource_keys`。
  - 发送前调用资源安全闸。
- 修改 `app/main.py`：
  - 新增 `POST /cs/resources/index?commit=false|true`，默认只解析预览，`commit=true` 才写资源真相源表。
- 修改 `.env.example`：
  - 新增 `CS_RESOURCE_TABLE_ID` 和 `CS_RESOURCE_WRITEBACK_FIELDS`。
- 新增 `tests/test_cs_resources.py`：
  - 覆盖官网解析、FF05 版本匹配、Firefly/Luminex 视频边界、发信闸和当前 FF05 示例 dry-run 文案。
- 飞书 Base 已创建“客服资源真相源表”并初始化 46 条资源：
  - Base：`J2fibLgBZaLGTNsQOPHcQXLonZe`
  - Table：`tblY3HNzoPPxqQPg`
- 客服工单台已新增资源回写字段：
  - `资源状态`
  - `资源需求JSON`
  - `资源命中JSON`

## 当前 FF05A 回复逻辑

- 不说附件。
- 不写 `[link]`。
- 如果没有客户当前固件版本：同时给 V454/V459 两个官方 Google Drive 链接和适用版本条件。
- 附上官方升级说明页、英文手册、站内 How-to Video 页。
- 如果客户看到 V411 或其他版本，要求先发升级工具截图再确认。

## 环境变量

```env
CS_RESOURCE_TABLE_ID=tblY3HNzoPPxqQPg
CS_RESOURCE_WRITEBACK_FIELDS=1
```

如果资源表读取失败，系统会自动使用代码内置的已核验 FUNLAB 资源兜底。后续更新资源可先调用 `/cs/resources/index?commit=false` 预览，再 `commit=true` 写入/更新资源真相源表。

## 剩余上线步骤

1. 部署前先调用 `/cs/resources/index?commit=false` 看解析样本，再 `commit=true` 刷新资源真相源表。
2. 用 `CS_REPLY_DRY_RUN_TO` dry-run 当前 FF05A 工单，抽检测试邮箱 raw content，确认没有附件承诺和占位链接。
3. 再放行 `CS_REPLY_LIVE` 或继续保持人工复制发送。
