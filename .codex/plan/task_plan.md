# AMZ Procurement Card Callback Fix

## Goal
修复德国站采购成本回填卡片的真实回调问题，并把发卡前自测固化到代码，覆盖按钮、输入框、确认按钮、写表和原卡 PATCH。

## Phases

| Phase | Status | Success Check |
|---|---|---|
| 1. 补写用户已填的第三个产品 | complete | `B0D1CLBFD9` 候选表读回成本 12.5 和 1688 链接，原卡显示 3/4 已回填 |
| 2. 修复表单回调解析 | complete | 单测覆盖飞书嵌套/列表/标准表单返回结构 |
| 3. 增加卡片发前自测 | complete | 本地脚本检查三个 URL 按钮、输入框、form_submit payload 和模拟回调 |
| 4. 验证、提交、部署 | complete | 单测和自测通过，commit 推送 master，Zeabur 健康检查通过 |
| 5. 沉淀教训 | complete | 写入 memory-candidate，后续卡片必须先自测再发 |

## Decisions

- 不用真实用户继续点卡验证；本轮先用本地模拟回调和结构化自测覆盖可控问题。
- 不创建测试脏记录写生产候选表，避免污染采购数据。
