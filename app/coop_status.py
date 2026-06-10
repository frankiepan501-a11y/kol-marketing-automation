# -*- coding: utf-8 -*-
"""KOL/媒体人「合作状态」单调前进守卫 (SSOT, 2026-06-10).

背景: 多个写状态的环节(reply_monitor intent 映射 / auto_send 发信后置)只看"这一封信/
这一次动作", 不看 KOL 已走到哪一阶段 → 已合作/已上稿 KOL 被回信或 followup 打回早期阶段
(实测已上稿 14 KOL 中 6 个被打回洽谈中)。本模块集中定义阶段 rank + 前进判定, 各写入点统一调用。

设计边界 (哪些写入该用本守卫, 哪些不该):
  ✅ 用: "前期状态写入"(reply_monitor 感兴趣/要报价→洽谈中; auto_send cold/followup→待回复/建联中)
        —— 这些是 stage-blind 副作用, 不该把更后阶段的 KOL 打回。
  ❌ 不用: decision_feedback (ROI 决策引擎, 已合作三档**有意升级** 免费→多次→付费, 三档同 rank
        会被误拦); sla_check"未产出" / kol_dedup"不合适" (终止态, 业务正确的"失败/弃用"事件)。
  注: 终止态(不合适/黑名单/未产出)在本守卫里**总是放行**(对方拒绝/退订/判失败可从任何阶段写)。

已合作三档 (免费/免费(多次)/付费) 同 rank=5 —— 三档之间的升降级由 decision_feedback 专管,
本守卫只负责"不退回 rank<5 的早期阶段"。
"""
from typing import Optional

COOP_STATUS_RANK = {
    "": 0, "未建联": 0,
    "待回复": 1, "建联中": 1,
    "洽谈中": 2,
    "样品评估": 3,
    "已合作": 5, "已合作-免费": 5, "已合作-免费(多次)": 5, "已合作-付费": 5,
}

# 终止态: 对方拒绝/退订/判失败 → 总允许写入(不受单调前进限制)。
TERMINAL_STATUSES = {"不合适", "黑名单", "未产出"}


def coop_rank(s: str) -> int:
    """合作状态阶段 rank; 未知值(含终止态)返回 -1。"""
    return COOP_STATUS_RANK.get((s or "").strip(), -1)


def advance_coop_status(cur: str, new: str) -> Optional[str]:
    """单调前进守卫。返回应写入的状态值, 或 None(= 不写, 防倒退)。
    - new ∈ 终止态 → 返回 new (对方拒绝/退订/失败, 总允许)
    - rank(new) > rank(cur) → 返回 new (严格前进)
    - 否则 → None (相等或倒退, 不写)
    """
    new = (new or "").strip()
    if not new:
        return None
    if new in TERMINAL_STATUSES:
        return new
    if coop_rank(new) > coop_rank(cur or ""):
        return new
    return None
