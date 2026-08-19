# Findings

## Feishu Card Callback

- 卡片设计要求：同一张采购卡里每个产品独立输入和提交，提交只影响当前 record。
- 当前代码原来只从 `event.action.form_value` 或 `event.card_form_value` 取表单值；如果飞书真实回调把输入值包在表单名下，或返回 `input_values`/列表结构，旧解析会取不到成本和链接。
- 原卡 GET 读回可以确认展示态和标题，但不一定保留完整表单 JSON，因此不能只靠消息读回来判断按钮/输入框有效。
- 本地 Python 环境存在同名 `app` 包污染；跑仓库单测时必须显式设置 `top_level_dir=os.getcwd()` 或在内联脚本中把仓库根目录放到 `sys.path[0]`。

## KOL Launch P0

- 活动新开发目标不能接纳 `reactivation_same_thread`；旧关系价值应保留在现有流程贡献池或二次发布池，但不能占用新开发人数。
- 人工排除必须驱动状态出池和冷候选补位，不能只写审核备注。
- KOL 主表国家字段可能与近期内容地区冲突；重复出现的非目标地区是冻结信号，单条内容不是。
- 当前 P0 已以 `evidence-v3 / p0-three-pool-v3-20260819a` 回填生产，20 个有效对象均为新开发池；G4 仍关闭。
