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

    # G-A/G-B (2026-05-31): per-KOL 定制 brief(框架推荐+5 hooks+TikTok SEO). 现场生(暖信量小/天),
    # per-(KOL×产品) 精确。命中则暖信正文 brief 段用 per-KOL 软要点; 失败降级 per-product(上面已拼)。
    # 仅 KOL(媒体人=PR 非 TikTok 创作者, 不适用)。
    per_kol_brief_md = ""
    if not is_editor and prod_rid:
        try:
            from . import talking_points
            kb = await talking_points.generate_for_kol(prod_rid, contact_rid)
            if kb.get("ok"):
                per_kol_brief_md = kb.get("brief_md") or ""
                eb = kb.get("email_bullets") or []
                if eb:
                    brief_points = "\n".join(f"• {b}" for b in eb[:5])
        except Exception as e:
            print(f"[warm_recap] per-KOL brief 生成失败 (降级 per-product): {e}")

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
        "审批意见": ("[暖信待发] 运营在飞书交互卡粘 UpPromote 券码 + 填折扣% → 提交即自动替换正文并发出。"
                     "无需打开本草稿改正文; [DISCOUNT_CODE]/[DISCOUNT_PCT] 由系统替换。")[:500],
    }
    if prod_rid:
        fields["关联产品"] = [prod_rid]
    if per_kol_brief_md:
        fields["Per-KOL Brief"] = per_kol_brief_md[:4000]
    task_rid = xrid(sf.get("关联任务"))
    if task_rid:
        fields["关联任务"] = [task_rid]

    rid = await feishu.create_record(config.T_DRAFT, fields)
    # 强制人审, 但 skip_notify=True 不发旧聪哥1号卡 — 改由下面发聪哥3号 form 卡(粘 UpPromote 券码)
    try:
        await draft_router.route_draft(rid, force_review_reason="warm_recap 待运营粘 UpPromote 券码",
                                       skip_notify=True)
    except Exception as e:
        print(f"[warm_recap] route_draft fail rid={rid}: {e}")
    # 发聪哥3号交互卡给 reviewer (按职务实时查在职名单 → union_id), 运营粘券码+填% → 提交回 n8n
    try:
        await _notify_warm_recap_card(rid, name or first, product_name, subj, per_kol_brief_md,
                                       contact_rid=contact_rid, is_editor=is_editor,
                                       brand=brand, email=email)
    except Exception as e:
        print(f"[warm_recap] send card fail rid={rid}: {e}")
    return {"ok": True, "rid": rid, "contact": name, "product": product_name}


def _build_warm_recap_card(draft_rid: str, kol_name: str, product_name: str, subject: str,
                           brief_md: str = "", contact_info: dict = None,
                           brand: str = "", email: str = "") -> dict:
    """聪哥3号 form 卡: 运营粘 UpPromote 券码 + 填折扣% → 提交回 n8n event-hub.
    button.value 带 {action:warm_recap_send, app_token, table_id, record_id} → n8n 按 record_id 写草稿.
    form_value {code, pct} → n8n 写 折扣码/折扣比例 + 状态=通过.
    brief_md: per-KOL 定制 brief(框架+5 hooks+TikTok SEO), 有则卡上多展示一段供运营看/转给 KOL.
    contact_info/brand/email: 调用方用 feishu.resolve_contact_info 解析后传 (2026-05-31 统一字段).
    """
    base_val = {
        "action": "warm_recap_send",
        "app_token": config.FEISHU_APP_TOKEN,
        "table_id": config.T_DRAFT,
        "record_id": draft_rid,
        "kol": kol_name,
    }
    elements = [
        feishu.build_contact_info_block(
            contact_info=contact_info, product_name=product_name, brand=brand,
            email=email, contact_type="KOL"),
        {"tag": "div", "text": {"tag": "lark_md", "content": (
            f"**{kol_name}** 已签收 **{product_name}** 样品 — 这是寄样后「确认收到 + 轻 brief」暖信"
            "(**不是催稿**)。")}},
        {"tag": "div", "text": {"tag": "lark_md", "content": (
            "**你只需 2 步**:\n"
            "1️⃣ 在 **UpPromote** 给该 KOL 建联盟券 → 复制券码\n"
            "2️⃣ 下面**粘券码 + 填折扣%**(首批一般 `10`)→ 点「确认发送」\n"
            "系统会把券码 + % 替换进暖信正文并发出, 你**全程不用打开草稿改正文**。")}},
        {"tag": "div", "text": {"tag": "lark_md", "content": f"**主题**: {subject[:80]}"}},
    ]
    # per-KOL 定制 brief (AI 推荐框架 + 5 hook 句式 + TikTok SEO) — 运营可看/可转给 KOL 参考
    if brief_md:
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content":
            "**🎬 给这位 KOL 的定制 brief**(可转给达人参考)\n" + brief_md[:1500]}})
    elements += [
        {"tag": "hr"},
        {"tag": "form", "name": f"wr_{draft_rid}", "elements": [
            {"tag": "input", "name": "code", "label_position": "left",
             "label": {"tag": "plain_text", "content": "UpPromote 券码:"},
             "placeholder": {"tag": "plain_text", "content": "粘贴 UpPromote 券码, 如 THAO10"}},
            {"tag": "input", "name": "pct", "label_position": "left",
             "label": {"tag": "plain_text", "content": "折扣 %:"},
             "placeholder": {"tag": "plain_text", "content": "填数字, 如 10"}},
            {"tag": "button", "action_type": "form_submit", "name": "submit",
             "text": {"tag": "plain_text", "content": "✅ 确认发送暖信"}, "type": "primary",
             "value": base_val},
        ]},
        {"tag": "note", "elements": [{"tag": "plain_text", "content": (
            "提交后约 10min 内 auto-send cron 发出。链接已是独立站(带券码追踪), 勿加亚马逊。")}]},
    ]
    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {"template": "turquoise",
                   "title": {"tag": "plain_text", "content": f"🎁 寄样暖信待发 · {kol_name}"}},
        "elements": elements,
    }


async def _notify_warm_recap_card(draft_rid: str, kol_name: str, product_name: str, subject: str,
                                  brief_md: str = "", contact_rid: str = "",
                                  is_editor: bool = False, brand: str = "",
                                  email: str = "") -> int:
    """发暖信卡给 reviewer (独立站运营专员, 按职务实时查→turnover-safe). open_id→union_id→聪哥3号发.
    contact_rid/is_editor/brand/email: 调用方传入用于统一信息块 (2026-05-31 字段标准)."""
    ctype = "媒体人" if is_editor else "KOL"
    ci = await feishu.resolve_contact_info(contact_rid, ctype) if contact_rid else {}
    card = _build_warm_recap_card(draft_rid, kol_name, product_name, subject, brief_md,
                                   contact_info=ci, brand=brand, email=email)
    targets = await feishu.resolve_notify_targets("reviewer")  # [(name, open_id), ...] 聪哥1号 namespace
    sent = 0
    _unions = []  # 看板「关联运营」 + /card/resend 撤老卡用
    _mids = {}
    for name, oid in targets:
        uid = await feishu.open_id_to_union_id(oid)
        if not uid:
            print(f"[warm_recap] {name} open_id→union_id 失败, skip")
            continue
        try:
            msg_id = await feishu.send_card_via_app3("union_id", uid, card)
            sent += 1
            if msg_id:
                _unions.append(uid)
                _mids[uid] = msg_id
        except Exception as e:
            print(f"[warm_recap] send card to {name} fail: {e}")
    if _unions or _mids:
        await feishu.write_card_recipients_msgids(draft_rid, _unions, _mids)
    print(f"[warm_recap] card sent to {sent}/{len(targets)} reviewers, draft={draft_rid}")
    return sent


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
