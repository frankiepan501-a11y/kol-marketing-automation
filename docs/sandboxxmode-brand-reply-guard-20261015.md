# SandboxxMode 错品牌自动回复止血（2026-10-15）

问题：SandboxxMode 的 POWKONG Plantdock 2 合作仍收到 FUNLAB 自动回复。已由人工在原线程纠正，但系统仍可能再次误发。

排查：KOL 主库的同一达人关联了 FUNLAB 手柄草稿；`reply_monitor.find_draft` 在当前收件箱品牌无草稿时还会退回选另一品牌草稿。即使有同品牌草稿，同一达人多合作的通用主题也可能选错产品。原错误回复是 FUNLAB 自动草稿，故单修跨品牌退回不足以覆盖本案。

改动：
- `reply_monitor` 无同品牌草稿时停止自动生成；同一达人有多个品牌历史时强制人审。
- `auto_send` 唯一发件入口把 SandboxxMode 仍为“自动通过”的 reply 草稿转为“待审”，包括既有在途草稿；人工核对品牌、产品和原线程后才可批准。其他收件人的自动回复规则不变。

验证：`test_sandbox_brand_guard.py` 三项定向测试通过；SwitchPlay 同线程选择和 NYXI 隔离测试通过。完整 1095 项因当前本机测试环境缺 `fastapi`、`cryptography`、`pytest` 等依赖，另有一条与本次变更无关的客服断言失败，不能宣称全量通过。

剩余风险：精确原线程匹配尚未完全普适化；此补丁选择对多品牌联系人降级人工审核，避免在证据不足时自动发错。上线后仍须核对生产版本、下一次 SandboxxMode 的自动草稿转待审、无 Zoho 新误发及 KOL 主库记录回填。
