# -*- coding: utf-8 -*-
"""按品牌从草稿历史算 KOL/媒体人 **该品牌线** 的状态信号 (2026-06-17 双品牌混淆修).

同一 KOL/媒体人可被 FUNLAB/POWKONG 多产品触达, 但主表(上稿日期/寄样次数/上次寄样订单号/
合作状态)是 **KOL 级混合值**, 一条品牌线的状态会串到另一条线的决策 (MikelTube/Carlos/TG_Geek)。
这里只看该 contact **该品牌的已发送草稿**(草稿「发送邮箱」别名定品牌)派生该线信号, 互不串台。

用法: 调用方有预取草稿(如 reply_monitor 的 all_matched) → from_drafts(brand, drafts) 纯函数省查询;
否则 line_state(contact_rid, contact_type, brand) 自查该 contact 全部已发送草稿。
"""
from . import config, feishu, stage_model
from .feishu import ext

# 寄样阶段「已过早期」信号
LATE_SHIP = {"已发货", "在途", "已签收", "已产出"}
# 场景标签 funnel: 发布收口=已上稿; 进入寄样物流及以后=已过早期
PUBLISH_FUNNEL = "发布收口"
LATE_FUNNELS = {"寄样物流", "brief拍摄", "草稿", "发布收口"}


def from_drafts(brand: str, drafts: list) -> dict:
    """纯函数: 从(任意品牌)已发送草稿列表算该 brand 线信号。drafts 元素需含 fields。"""
    bd = [d for d in (drafts or [])
          if config.brand_from_text(ext(d["fields"].get("发送邮箱"))) == brand]
    shipped = uploaded = quoted = late_funnel = has_draft = False
    last_ms = 0
    for d in bd:
        has_draft = True
        f = d["fields"]
        try:
            last_ms = max(last_ms, int(f.get("发送时间") or 0))
        except (ValueError, TypeError):
            pass
        ship = ext(f.get("寄样阶段")) or ""
        if ship in LATE_SHIP:
            shipped = True
        if ship == "已产出":
            uploaded = True
        scn = ext(f.get("场景标签")) or ""
        if scn:
            fn = stage_model.funnel_stage_of(scn)
            if fn == PUBLISH_FUNNEL:
                uploaded = True
            if fn in LATE_FUNNELS:
                late_funnel = True
        if (ext(f.get("邮件草稿来源")) or "") == "affiliate_quote":
            quoted = True
    return {
        "has_draft": has_draft,
        "last_contact_ms": last_ms,
        "shipped": shipped,          # 该线已寄样(发货/在途/签收/产出)
        "uploaded": uploaded,        # 该线已上稿(寄样阶段=已产出 或 场景=发布收口漏斗)
        "quoted": quoted,            # 该线已发 affiliate_quote 报价(进入条款谈判)
        "is_late_stage": shipped or uploaded or quoted or late_funnel,
    }


async def _fetch_sent_drafts(contact_rid: str, contact_type: str) -> list:
    link_field = "关联媒体人" if contact_type == "editor" else "关联KOL"
    return await feishu.search_records(config.T_DRAFT, [
        {"field_name": link_field, "operator": "contains", "value": [contact_rid]},
        {"field_name": "邮件草稿状态", "operator": "is", "value": ["已发送"]},
    ], field_names=["发送邮箱", "寄样阶段", "场景标签", "邮件草稿来源", "发送时间"])


async def line_state(contact_rid: str, contact_type: str, brand: str, drafts: list = None) -> dict:
    """async: 不传 drafts 则自查该 contact 全部已发送草稿; 返回 from_drafts 的 brand 线信号。"""
    if drafts is None:
        drafts = await _fetch_sent_drafts(contact_rid, contact_type)
    return from_drafts(brand, drafts)
