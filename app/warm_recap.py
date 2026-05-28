# -*- coding: utf-8 -*-
"""P3: 寄样后「确认收到 + brief recap」暖信 (寄样后 brief 重设计).

GRIN gift-first 节奏 Day3-5 黄金窗口 + brief recap 合一, **低压力不催稿**。
触发: 寄样阶段=已签收 且 该 KOL 还没暖信草稿 → 生成暖信(正文留折扣占位符)→ 强制人审。
折扣比例/折扣码 由运营在草稿表填(灵活按 KOL/产品), 发送前 auto_send 用 shopify_discount
建 Shopify 码并替换占位符 [DISCOUNT_CODE]/[DISCOUNT_PCT]。

外部方法论依据: reference_kol_gifting_brief_playbook (brief=护栏非脚本/1页/痛点收益/不催稿)。
"""
import time
import re
from . import config, feishu, draft_router, utm
from .feishu import ext, xrid, ext_url

WARM_RECAP_SOURCE = "warm_recap"

# 占位符: 发送前 auto_send 用运营填的 折扣比例/折扣码 → Shopify 码 替换
TEMPLATE_WARM_RECAP = (
    "Hi {first_name},\n\n"
    "So glad the {product_name} arrived — hope you love it! 🎉\n\n"
    "A few things that tend to land well with audiences (totally optional, your call):\n"
    "{brief_points}\n\n"
    "If you end up sharing, we'd love to repost you — no pressure at all.\n\n"
    "For your audience: use code **[DISCOUNT_CODE]** for [DISCOUNT_PCT]% off"
    "{link_line}\n"
    "(Quick note: please tag it #ad or #gifted when you post — just for FTC compliance 🙏)\n\n"
    "Anything we can do to make it easier? Just hit reply.\n\n"
    "Best,\n{signature}"
)


def _brand_from_alias(alias: str) -> str:
    s = (alias or "").lower()
    if "powkong" in s:
        return "POWKONG"
    return "FUNLAB"


async def _product_brief(prod_rid: str):
    """从产品库取 (产品英文名, brief 要点 bullet 串, 官网链接 raw)."""
    name, points, link = "our product", [], ""
    if not prod_rid:
        return name, "", link
    try:
        prod = await feishu.get_record(config.T_PRODUCT, prod_rid)
        pf = prod["fields"]
        name = ext(pf.get("产品英文名")) or name
        # ⚠️ 海外营销永远用英文 (Scott Stein 中英混杂事故铁律): 只用英文字段 Talking Points/拍摄角度建议,
        # **绝不降级用中文「卖点1/2/3」**(那是内部 SKU 卖点, 是中文, 会让英文暖信中英混杂)。
        tp = ext(pf.get("Talking Points")).strip()
        ang = ext(pf.get("拍摄角度建议")).strip()
        if tp:
            points += [l.strip() for l in re.split(r"[\n;；]", tp) if l.strip()]
        if ang:
            points += [f"(angle) {l.strip()}" for l in re.split(r"[\n;；]", ang) if l.strip()]
        link = ext_url(pf.get("官网链接")) or ""
    except Exception as e:
        print(f"[warm_recap] 读产品 {prod_rid} 失败: {e}")
    # 英文字段没填 → 通用英文默认 (不注入中文); 运营/产品库补 Talking Points 后未来暖信更丰富
    bullets = "\n".join(f"• {p}" for p in points[:5]) if points else (
        "• Show it in your setup / share your honest first impressions\n"
        "• Feel free to highlight whatever feature stands out most to you")
    return name, bullets, link


async def _has_warm_recap(contact_rid: str, link_field: str) -> bool:
    """该 contact 是否已有暖信草稿 (去重, 不重复生成)."""
    items = await feishu.search_records(config.T_DRAFT, [
        {"field_name": "邮件草稿来源", "operator": "is", "value": [WARM_RECAP_SOURCE]},
    ])
    return any(xrid(r["fields"].get(link_field)) == contact_rid for r in items)


async def build_for_ship_draft(ship_draft: dict) -> dict:
    """给一条 寄样阶段=已签收 的草稿生成暖信草稿. Returns {ok, rid|skip, reason}."""
    sf = ship_draft["fields"]
    ctype = ext(sf.get("对象类型")) or "KOL"
    is_editor = (ctype == "媒体人")
    link_field = "关联媒体人" if is_editor else "关联KOL"
    contact_rid = xrid(sf.get(link_field))
    if not contact_rid:
        return {"ok": False, "skip": "no contact link"}
    if await _has_warm_recap(contact_rid, link_field):
        return {"ok": False, "skip": "warm_recap exists"}

    master_tbl = config.T_EDITOR if is_editor else config.T_KOL
    try:
        contact = await feishu.get_record(master_tbl, contact_rid)
    except Exception as e:
        return {"ok": False, "skip": f"contact fetch fail: {e}"}
    cf = contact["fields"]
    name = ext(cf.get("媒体人姓名")) if is_editor else ext(cf.get("账号名"))
    first = (name.strip().split()[0][:30] if name and name.strip() else "there")
    email = feishu.clean_email(ext(cf.get("邮箱")))[0] or ""
    if not email:
        return {"ok": False, "skip": "no email"}

    alias = ext(sf.get("发送邮箱"))
    brand = _brand_from_alias(alias)
    if not alias:
        alias = config.BRAND_CONFIG[brand]["alias_from"]

    prod_rid = xrid(sf.get("关联产品"))
    product_name, brief_points, link_raw = await _product_brief(prod_rid)
    link = utm.make_utm_link(link_raw, brand, product_name, name) if link_raw else ""
    link_line = f" at {link}" if link else ""

    from . import reply_drafter
    body = TEMPLATE_WARM_RECAP.format(
        first_name=first, product_name=product_name, brief_points=brief_points,
        link_line=link_line, signature=reply_drafter._sender_signature(brand),
    )
    subj = "Re: " + (ext(sf.get("邮件主题")) or f"{product_name}")[:150]

    now_ms = int(time.time() * 1000)
    fields = {
        "邮件草稿ID": f"warm-{contact_rid[-8:]}-{int(time.time())}",
        link_field: [contact_rid],
        "邮件主题": subj[:200],
        "邮件正文": body,
        "邮件语言": "en",
        "邮件草稿状态": "待审",
        "邮件草稿来源": WARM_RECAP_SOURCE,
        "对象类型": ctype,
        "发送邮箱": alias,
        "发送人署名": "Frankie",
        "生成时间": now_ms,
        "建议发送时间": now_ms,
        "重生次数": 0,
        "收件邮箱": email,
        "UTM 链接": link,
        "审批意见": ("[暖信待填折扣] 请在本草稿填「折扣比例」(如 15) + 「折扣码」(可留空→按 KOL 名自动生成), "
                     "再点通过。正文 [DISCOUNT_CODE]/[DISCOUNT_PCT] 会在发送前自动替换为真实 Shopify 码。")[:500],
    }
    if prod_rid:
        fields["关联产品"] = [prod_rid]
    task_rid = xrid(sf.get("关联任务"))
    if task_rid:
        fields["关联任务"] = [task_rid]

    rid = await feishu.create_record(config.T_DRAFT, fields)
    # 强制人审 (复用 force_review_reason; 运营要填折扣)
    try:
        await draft_router.route_draft(rid, force_review_reason="warm_recap 待运营填折扣比例+折扣码")
    except Exception as e:
        print(f"[warm_recap] route_draft fail rid={rid}: {e}")
    return {"ok": True, "rid": rid, "contact": name, "product": product_name}


async def run() -> dict:
    """扫 寄样阶段=已签收 的草稿 → 给还没暖信的 KOL 生成暖信草稿."""
    ship_drafts = await feishu.search_records(config.T_DRAFT, [
        {"field_name": "寄样阶段", "operator": "is", "value": ["已签收"]},
    ])
    results = []
    generated = 0
    for sd in ship_drafts:
        try:
            r = await build_for_ship_draft(sd)
        except Exception as e:
            r = {"ok": False, "skip": f"error: {str(e)[:120]}"}
        if r.get("ok"):
            generated += 1
        results.append(r)
    return {"已签收草稿": len(ship_drafts), "生成暖信": generated, "results": results[:30]}
